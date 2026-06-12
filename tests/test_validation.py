"""Split from test_ingestion_bridge_contracts.py — see scripts/split_contract_tests.py.
Shared imports/helpers live in tests/_helpers.py."""
from __future__ import annotations

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import (  # noqa: F401
    _FailingNeonBranchClient,
    _FakeNeonBranchClient,
    _MINIMAL_MD_PDB,
    _avma_vctr_study_card,
    _cleanup_workspace,
    _contains_key,
    _md_queue_item,
    _md_compute_input,
    _ready_for_therapy_ideas_program,
    _research_program_fixture,
    _seed_evaluated_brief,
    _seed_full_text_source_claim,
    _seed_minimal_source_claim,
    _seed_program_committee_corpus,
    _write_minimal_xlsx,
    _xlsx_column_name,
)

def test_validation_plan_contracts_validate():
    assay_context = ValidationAssayContext(
        disease_context="canine hemangiosarcoma and human angiosarcoma",
        species=["canine", "human", "canine"],
        model_system="Human-reviewed literature packet.",
        assay_type="structured expert evidence review",
        readout="ready/not-ready decision",
        evidence_refs=["brief:1", "C1"],
    )
    validation_request = ValidationRequest(
        validation_type="expert_review",
        objective="Review the hypothesis before validation.",
        require_approval=True,
        assay_context=assay_context,
        quality_gates=["approval_required", "approval_required"],
    )
    task = ValidationPlanTask(
        task_type="expert_review",
        title="Expert review",
        objective="Assess whether this should move into validation.",
        rationale="The brief is source-traceable and ready for review.",
        validation_request=validation_request,
        evidence_refs=["brief:1", "evaluation:1", "C1"],
    )
    result = ValidationPlanResult(
        brief_id=uuid4(),
        topic="VEGF in canine HSA",
        status="ready_for_review",
        readiness="ready_for_expert_review",
        tasks=[task],
    )
    record = ValidationPlanRecord(
        plan_id=result.plan_id,
        brief_id=result.brief_id,
        topic=result.topic,
        status=result.status,
        readiness=result.readiness,
        task_count=1,
        result_payload=result.model_dump(mode="json"),
    )

    assert record.status == "ready_for_review"
    assert result.tasks[0].validation_request.validation_type == "expert_review"
    assert result.tasks[0].validation_request.assay_context.species == ["canine", "human"]
    assert result.tasks[0].validation_request.quality_gates == ["approval_required"]
    with pytest.raises(ValueError):
        ValidationPlanTask(task_type="bad", title="x", objective="x", rationale="x")
    with pytest.raises(ValueError):
        ValidationPlanResult(brief_id=uuid4(), topic="x", status="bad")
    with pytest.raises(ValueError):
        ValidationPlanResult(brief_id=uuid4(), topic="x", readiness="bad")


def test_validation_tool_catalog_covers_recommend_only_lanes():
    catalog = list_validation_tool_catalog()
    expected_keys = {
        "expert_review",
        "assay_design_review",
        "target_expression_review",
        "biomarker_response_assay_design",
        "omics_expression_review",
        "mutation_function_review",
        "peptide_specialist_review",
        "safety_translational_risk_review",
    }

    assert {entry.tool_key for entry in catalog} == expected_keys
    assert len(catalog) == len(expected_keys)
    assert all(entry.runner_status == "recommend_only" for entry in catalog)
    assert all(entry.mode == "draft" for entry in catalog)
    assert all(entry.assay_context_template.species == ["canine", "human"] for entry in catalog)
    assert get_validation_tool("safety_translational_risk_review").validation_type == "safety"
    assert get_validation_tool("mutation_function_review").task_type == "target_validation"
    assert get_validation_tool("peptide_specialist_review").recommended_agent_name == (
        "peptide_specialist_validation_agent"
    )
    for entry in catalog:
        task = entry.as_plan_task(
            rationale="Source-traceable recommend-only catalog task.",
            candidate_name="sorafenib",
            target_name="KDR",
            evidence_refs=["C1"],
        )
        assert isinstance(task.validation_request, ValidationRequest)
        assert task.validation_request.validation_type == entry.validation_type
        assert task.validation_request.metadata["validation_tool_catalog"]["tool_key"] == entry.tool_key
        assert task.validation_request.metadata["validation_tool_catalog"]["runner_status"] == "recommend_only"
    with pytest.raises(KeyError):
        get_validation_tool("wet_magic")


def test_validation_tool_catalog_builds_existing_validation_task_contracts():
    task = build_validation_tool_task(
        "biomarker_response_assay_design",
        title="Biomarker response assay: KDR",
        objective="Design a conservative response assay for KDR-high canine HSA models.",
        rationale="The committee needs a recommend-only assay design before any dispatch.",
        candidate_name="sorafenib",
        target_name="KDR",
        priority=42,
        evidence_refs=["C1", "C1", "brief:1"],
        assay_context_overrides={"sample_context": "KDR-high and KDR-low canine HSA samples."},
        metadata={"origin": "unit-test"},
    )

    assert task.task_type == "wet_lab"
    assert task.tool_hint == "biomarker_response_assay_design"
    assert task.evidence_refs == ["C1", "brief:1"]
    assert task.validation_request is not None
    assert isinstance(task.validation_request, ValidationRequest)
    assert task.validation_request.validation_type == "wet_lab"
    assert task.validation_request.require_approval is True
    assert task.validation_request.candidate_name == "sorafenib"
    assert task.validation_request.target_name == "KDR"
    assert task.validation_request.assay_context.sample_context == "KDR-high and KDR-low canine HSA samples."
    assert task.validation_request.assay_context.evidence_refs == ["C1", "brief:1"]
    assert task.validation_request.metadata["origin"] == "unit-test"
    assert task.validation_request.metadata["recommend_only"] is True
    assert task.validation_request.metadata["validation_tool_catalog"] == {
        "version": "v1",
        "tool_key": "biomarker_response_assay_design",
        "display_name": "Biomarker-response assay design",
        "runner_status": "recommend_only",
        "mode": "draft",
        "recommended_agent_name": "assay_design_validation_agent",
        "tool_hint": "biomarker_response_assay_design",
    }


def test_validation_tool_catalog_contracts_and_matching_service(tmp_path):
    with pytest.raises(ValueError):
        ValidationToolCapability(
            tool_key="bad-tool",
            display_name="Bad tool",
            category="unknown",
            description="Invalid category should fail.",
            compatible_validation_types=["expert_review"],
            compatible_task_types=["expert_review"],
            tool_hint="bad",
        )

    service = HSAResearchService(SQLiteResearchRepository(tmp_path / "catalog.sqlite3", seed=False))
    catalog = service.list_validation_tool_catalog(ValidationToolCatalogRequest(query="omics"))
    matched = service.match_validation_tools(
        ValidationToolMatchRequest(
            validation_type="omics",
            task_type="omics",
            objective="Review KDR expression in canine HSA and human angiosarcoma datasets.",
            target_name="KDR",
            required_inputs=["gene or biomarker terms", "canine HSA datasets", "human angiosarcoma datasets"],
        )
    )

    assert catalog.tool_count >= 1
    assert matched.match_count >= 1
    assert matched.matches[0].tool.runner_status == "recommend_only"
    assert matched.matches[0].tool.tool_key in {"omics_expression_review", "target_expression_review"}

    peptide_matched = service.match_validation_tools(
        ValidationToolMatchRequest(
            validation_type="expert_review",
            task_type="expert_review",
            objective="Review a cyclic peptide for target engagement, delivery, stability, and immunogenicity.",
            required_inputs=[
                "peptide sequence or modality",
                "target/pathway rationale",
                "delivery route or formulation context",
                "stability or protease risk context",
            ],
        )
    )

    assert peptide_matched.match_count >= 1
    assert peptide_matched.matches[0].tool.tool_key == "peptide_specialist_review"
    assert peptide_matched.matches[0].tool.category == "peptide_specialist"


def test_program_to_validation_decision_end_to_end(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "program-validation-decision-e2e.sqlite3", seed=False)
    _seed_program_committee_corpus(repo)
    program = repo.upsert_research_program(_ready_for_therapy_ideas_program())
    service = HSAResearchService(repo)

    committee = service.run_therapy_committee(
        TherapyCommitteeRequest(
            program_id=program.program_id,
            review_mode="deterministic_only",
            max_claims=0,
            max_ideas_per_perspective=3,
        )
    )
    library = service.list_therapy_ideas(
        TherapyIdeaLibraryRequest(source_program_id=program.program_id, limit=10)
    )
    target_idea = library.ideas[0]
    promotion = service.build_hypothesis_promotion_report(
        HypothesisPromotionReportRequest(therapy_idea_id=target_idea.therapy_idea_id)
    )
    packets = service.build_validation_packets(
        ValidationPacketRequest(therapy_idea_id=target_idea.therapy_idea_id, limit=1)
    )
    decision_report = service.build_validation_decision_report(
        ValidationDecisionReportRequest(therapy_idea_id=target_idea.therapy_idea_id, limit=1)
    )

    assert committee.source_program_id == program.program_id
    assert len(committee.ranked_ideas) == 3
    assert library.idea_count == 3
    assert promotion.candidate_count == 1
    assert promotion.candidates[0].source_type == "therapy_idea"
    assert packets.packet_count == 1
    assert packets.packets[0].therapy_idea_id == target_idea.therapy_idea_id
    assert decision_report.decision_count == 1
    decision = decision_report.decisions[0]
    assert decision.therapy_idea_id == target_idea.therapy_idea_id
    assert decision.outcome == "promote_broader_program"
    assert decision.validation_ready is False
    assert decision.broader_program_signal == "strong"
    assert decision.recommended_program_thesis
    assert "Research Program Board" in decision.recommended_downstream_action
    assert decision.evidence_summary["packet_readiness"] == packets.packets[0].readiness


def test_validation_packets_build_from_ready_therapy_idea(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-packets.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief, evaluation = _seed_evaluated_brief(repo, topic="Pazopanib KDR packet", duplicate_count=0)
    idea = TherapyIdea(
        title="KDR/PIK3CA biomarker-enriched pazopanib strategy",
        hypothesis="Pazopanib should be reviewed in KDR/PIK3CA-enriched canine splenic HSA.",
        rationale="The committee found enough cited VEGFR/KDR rationale to plan recommend-only validation.",
        candidate_therapies=["pazopanib"],
        targets=["KDR", "PIK3CA"],
        biomarkers=["VEGFR2"],
        evidence_refs=["C1", "C2"],
        evidence_strength="medium",
        risks=[
            "direct canine pazopanib response data may be sparse",
            "canine pazopanib PK/PD and dose tolerance remain unknown",
        ],
        next_experiments=["Review KDR/PIK3CA mutation and expression evidence."],
        priority_score=0.78,
    )
    repo.upsert_therapy_idea(
        TherapyIdeaRecord(
            idea=idea,
            source_brief_id=brief.brief_id,
            source_evaluation_id=evaluation.evaluation_id,
            topic=brief.topic,
            status="ready_for_promotion",
            score=0.78,
        )
    )
    repo.upsert_validation_plan(
        ValidationPlanRecord(
            brief_id=brief.brief_id,
            evaluation_id=evaluation.evaluation_id,
            topic="Different validation lane for the same evaluated brief",
            status="ready_for_review",
            readiness="ready_for_expert_review",
            task_count=1,
            result_payload={
                "tasks": [
                    ValidationPlanTask(
                        task_type="expert_review",
                        title="Unrelated plan task",
                        objective="Review a separate non-therapy-idea hypothesis.",
                        rationale="This plan lacks therapy_idea_id linkage and must not attach to the packet.",
                        validation_request=ValidationRequest(
                            validation_type="expert_review",
                            objective="Review a separate non-therapy-idea hypothesis.",
                        ),
                    ).model_dump(mode="json")
                ]
            },
        )
    )

    result = service.build_validation_packets(
        ValidationPacketRequest(therapy_idea_id=idea.idea_id, limit=1)
    )

    assert isinstance(result, ValidationPacketResult)
    assert result.packet_count == 1
    packet = result.packets[0]
    assert isinstance(packet, ValidationPacket)
    assert packet.therapy_idea_id == idea.idea_id
    assert packet.status == "ready_for_review"
    assert packet.readiness == "ready_for_validation_plan"
    assert packet.discovery_readiness == "ready_for_validation_strategy"
    assert packet.validation_strategy_readiness == "ready_for_validation_strategy"
    assert packet.protocol_readiness == "needs_protocol_inputs"
    assert packet.validation_plan is None
    assert packet.queue_items == []
    assert packet.validation_tasks
    assert packet.matched_tools
    assert "human_approval_required" in packet.dispatch_blockers
    assert "recommend_only_runner" in packet.dispatch_blockers
    assert "direct canine pazopanib response data may be sparse" in packet.missing_evidence
    assert "canine pazopanib PK/PD and dose tolerance remain unknown" in packet.risk_annotations
    assert "canine pazopanib PK/PD and dose tolerance remain unknown" in packet.protocol_blockers

    payload = packet.model_dump(mode="json")
    payload["status"] = "bad"
    with pytest.raises(ValueError):
        ValidationPacket.model_validate(payload)
    payload = packet.model_dump(mode="json")
    payload["protocol_readiness"] = "bad"
    with pytest.raises(ValueError):
        ValidationPacket.model_validate(payload)


def test_validation_packet_truncates_long_safety_context(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-packet-long-risk.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief, evaluation = _seed_evaluated_brief(repo, topic="Long risk packet", duplicate_count=0)
    idea = TherapyIdea(
        title="Long safety context strategy",
        hypothesis="A candidate therapy should be reviewed despite verbose risk annotations.",
        rationale="The packet builder should keep assay context fields inside contract limits.",
        candidate_therapies=["sorafenib"],
        targets=["KDR"],
        biomarkers=["VEGFR2"],
        evidence_refs=["C1"],
        evidence_strength="medium",
        risks=["safety risk " * 120],
        priority_score=0.7,
    )
    repo.upsert_therapy_idea(
        TherapyIdeaRecord(
            idea=idea,
            source_brief_id=brief.brief_id,
            source_evaluation_id=evaluation.evaluation_id,
            topic=brief.topic,
            status="ready_for_promotion",
            score=0.7,
        )
    )

    result = service.build_validation_packets(
        ValidationPacketRequest(therapy_idea_id=idea.idea_id, limit=1)
    )

    safety_context = result.packets[0].validation_tasks[0].validation_request.assay_context.safety_context
    assert safety_context
    assert len(safety_context) <= 500


def test_validation_packet_addendum_includes_research_hunt_synthesis_followups(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-packet-hunt-addendum.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief, evaluation = _seed_evaluated_brief(repo, topic="Therapy idea source brief", duplicate_count=0)
    idea = TherapyIdea(
        title="Hunt addendum strategy",
        hypothesis="A therapy idea should carry research-hunt synthesis follow-up evidence.",
        rationale="Research hunt synthesis rows are validation-gap follow-ups.",
        candidate_therapies=["sorafenib"],
        targets=["KDR"],
        biomarkers=["VEGFR2"],
        evidence_refs=["C1"],
        evidence_strength="medium",
        risks=["primary evidence remains unresolved"],
        priority_score=0.7,
    )
    repo.upsert_therapy_idea(
        TherapyIdeaRecord(
            idea=idea,
            source_brief_id=brief.brief_id,
            source_evaluation_id=evaluation.evaluation_id,
            topic=brief.topic,
            status="ready_for_promotion",
            score=0.7,
        )
    )
    followup_brief, followup_eval = _seed_evaluated_brief(
        repo,
        topic="Research hunt synthesis follow-up",
        duplicate_count=0,
    )
    origin_queue_item_id = uuid4()
    lead_id = uuid4()
    repo.upsert_research_brief_queue_item(
        ResearchBriefQueueItem(
            topic=followup_brief.topic,
            status="completed",
            priority=15,
            last_brief_id=followup_brief.brief_id,
            last_agent_run_id=followup_brief.agent_run_id,
            metadata={
                "research_hunt_synthesis_queue": {
                    "lead_id": str(lead_id),
                    "origin_record_id": str(origin_queue_item_id),
                    "origin": "research_hunt_ready_lead",
                    "control_status": "ready_for_synthesis",
                }
            },
        )
    )

    result = service.build_validation_packets(
        ValidationPacketRequest(
            therapy_idea_id=idea.idea_id,
            queue_item_id=origin_queue_item_id,
            limit=1,
        )
    )

    addendum = result.packets[0].evidence_addendum
    assert followup_eval.evaluation_id
    assert addendum.follow_up_count == 1
    assert addendum.follow_up_briefs[0].brief_id == followup_brief.brief_id
    assert addendum.follow_up_briefs[0].evaluation_id == followup_eval.evaluation_id
    assert addendum.follow_up_briefs[0].lead_id == lead_id
    assert addendum.follow_up_briefs[0].origin_queue_item_id == origin_queue_item_id
    assert "research_hunt_synthesis_queue" in addendum.follow_up_briefs[0].metadata


def test_validation_decision_contract_rejects_invalid_outcome():
    candidate = HypothesisPromotionCandidate(
        candidate_id="therapy_idea:test",
        source_type="therapy_idea",
        source_id=str(uuid4()),
        title="Decision candidate",
        hypothesis="A candidate should be decided.",
        promotion_state="needs_more_evidence",
    )
    packet = ValidationPacket(
        packet_id="validation_packet:test",
        candidate_id=candidate.candidate_id,
        source_type="therapy_idea",
        source_id=candidate.source_id,
        promotion_candidate=candidate,
        title=candidate.title,
        hypothesis=candidate.hypothesis,
    )
    decision = ValidationDecisionPacket(
        decision_id="validation_decision:test",
        packet_id=packet.packet_id,
        candidate_id=packet.candidate_id,
        source_type=packet.source_type,
        source_id=packet.source_id,
        title=packet.title,
        outcome="narrow_to_preclinical_question",
        rationale="Keep the decision finite.",
        recommended_downstream_action="Narrow the question.",
        decisive_questions=["What evidence changes confidence?"],
        evidence_tasks=["Collect one decisive evidence packet."],
    )
    payload = decision.model_dump(mode="json")
    payload["outcome"] = "keep_iterating_forever"
    with pytest.raises(ValueError):
        ValidationDecisionPacket.model_validate(payload)


def test_validation_decision_report_promotes_broader_program_for_weak_specific_claim(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-decision.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief, evaluation = _seed_evaluated_brief(repo, topic="Sorafenib source brief", duplicate_count=0)
    idea = TherapyIdea(
        title="Biomarker-stratified vascular TKI strategy",
        hypothesis=(
            "A genomics-guided vascular/TKI strategy may be stronger than sorafenib monotherapy "
            "for canine HSA."
        ),
        rationale="The broader program should not be reduced to one weak drug claim.",
        candidate_therapies=["sorafenib"],
        targets=["KDR", "PDGFRB"],
        biomarkers=["VEGFR2", "PDGFR-beta"],
        evidence_refs=["C1", "C2"],
        evidence_strength="medium",
        risks=[
            "sorafenib canine HSA signal is non-significant p=0.079 and canine PK/PD is absent",
        ],
        priority_score=0.74,
    )
    repo.upsert_therapy_idea(
        TherapyIdeaRecord(
            idea=idea,
            source_brief_id=brief.brief_id,
            source_evaluation_id=evaluation.evaluation_id,
            topic=brief.topic,
            status="ready_for_promotion",
            score=0.74,
        )
    )
    followup_brief, followup_eval = _seed_evaluated_brief(
        repo,
        topic="Genomics-guided targeted therapy follow-up",
        duplicate_count=0,
        evaluation_weaknesses=[
            "Drug-specific sorafenib evidence remains weak, but targeted therapy subgroup evidence supports a broader biomarker program.",
        ],
    )
    origin_queue_item_id = uuid4()
    repo.upsert_research_brief_queue_item(
        ResearchBriefQueueItem(
            topic=followup_brief.topic,
            status="completed",
            priority=15,
            last_brief_id=followup_brief.brief_id,
            last_agent_run_id=followup_brief.agent_run_id,
            metadata={
                "research_hunt_synthesis_queue": {
                    "origin_record_id": str(origin_queue_item_id),
                    "therapy_idea_id": str(idea.idea_id),
                    "control_status": "ready_for_synthesis",
                }
            },
        )
    )

    result = service.build_validation_decision_report(
        ValidationDecisionReportRequest(
            therapy_idea_id=idea.idea_id,
            queue_item_id=origin_queue_item_id,
            limit=1,
        )
    )

    assert isinstance(result, ValidationDecisionReportResult)
    assert followup_eval.evaluation_id
    assert result.decision_count == 1
    assert result.outcome_counts == {"promote_broader_program": 1}
    decision = result.decisions[0]
    assert decision.outcome == "promote_broader_program"
    assert decision.validation_ready is False
    assert decision.recommended_program_thesis
    assert "Research Program Board" in decision.recommended_downstream_action
    assert decision.evidence_summary["evaluated_follow_up_count"] == 1
    assert result.persisted_decision_count == 1
    persisted = repo.get_validation_decision(decision.decision_id)
    assert persisted is not None
    assert persisted.outcome == "promote_broader_program"


def test_validation_packet_includes_followup_brief_addendum(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-packet-addendum.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief, evaluation = _seed_evaluated_brief(repo, topic="Pazopanib KDR packet", duplicate_count=0)
    idea = TherapyIdea(
        title="KDR/PIK3CA biomarker-enriched pazopanib strategy",
        hypothesis="Pazopanib should be reviewed in KDR/PIK3CA-enriched canine splenic HSA.",
        rationale="The committee found enough cited VEGFR/KDR rationale to plan recommend-only validation.",
        candidate_therapies=["pazopanib"],
        targets=["KDR", "PIK3CA"],
        biomarkers=["VEGFR2"],
        evidence_refs=["C1", "C2"],
        evidence_strength="medium",
        risks=["direct canine pazopanib response data may be sparse"],
        next_experiments=["Review KDR/PIK3CA mutation and expression evidence."],
        priority_score=0.78,
    )
    repo.upsert_therapy_idea(
        TherapyIdeaRecord(
            idea=idea,
            source_brief_id=brief.brief_id,
            source_evaluation_id=evaluation.evaluation_id,
            topic=brief.topic,
            status="ready_for_promotion",
            score=0.78,
        )
    )
    task_id = uuid4()
    plan = repo.upsert_validation_plan(
        ValidationPlanRecord(
            brief_id=brief.brief_id,
            evaluation_id=evaluation.evaluation_id,
            topic="Pazopanib validation plan",
            status="ready_for_review",
            readiness="ready_for_expert_review",
            task_count=1,
            metadata={"therapy_idea_id": str(idea.idea_id)},
            result_payload={
                "tasks": [
                    ValidationPlanTask(
                        task_id=task_id,
                        task_type="expert_review",
                        title="Expert review",
                        objective="Review pazopanib evidence.",
                        rationale="Evidence needs review before validation.",
                        validation_request=ValidationRequest(
                            validation_type="expert_review",
                            objective="Review pazopanib evidence.",
                        ),
                    ).model_dump(mode="json")
                ]
            },
        )
    )
    validation_queue_item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=plan.plan_id,
            task_id=task_id,
            brief_id=brief.brief_id,
            evaluation_id=evaluation.evaluation_id,
            topic="Pazopanib validation plan",
            task_type="expert_review",
            title="Expert review",
            objective="Review pazopanib evidence.",
            rationale="Evidence needs review before validation.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review pazopanib evidence.",
            ),
        )
    )
    ready_brief, ready_eval = _seed_evaluated_brief(
        repo,
        topic="Explicit dose route schedule for pazopanib",
        duplicate_count=0,
    )
    needs_brief, _old_eval = _seed_evaluated_brief(
        repo,
        topic="Canine pazopanib PK safety profile",
        duplicate_count=0,
    )
    needs_eval = repo.upsert_research_brief_evaluation(
        ResearchBriefEvaluationRecord(
            brief_id=needs_brief.brief_id,
            topic=needs_brief.topic,
            source_key=needs_brief.source_key,
            overall_score=0.62,
            passes_quality_bar=False,
            readiness="needs_more_evidence",
            summary={"overall_score": 0.62},
            result_payload={
                "weaknesses": ["No canine pazopanib PK or adverse-event profile was found."],
                "recommendations": ["Run a veterinary pharmacology retrieval pass."],
            },
        )
    )
    common_resolver = {
        "plan_id": str(plan.plan_id),
        "queue_item_id": str(validation_queue_item.queue_item_id),
        "origin": "validation_agent_gap",
        "task_type": "expert_review",
        "validation_type": "expert_review",
    }
    repo.upsert_research_brief_queue_item(
        ResearchBriefQueueItem(
            topic=ready_brief.topic,
            source_key="pubmed",
            status="completed",
            priority=10,
            last_brief_id=ready_brief.brief_id,
            last_agent_run_id=ready_brief.agent_run_id,
            metadata={
                "evidence_gap_resolver": {
                    **common_resolver,
                    "lead_id": str(uuid4()),
                    "lane": "pkpd",
                }
            },
        )
    )
    repo.upsert_research_brief_queue_item(
        ResearchBriefQueueItem(
            topic=needs_brief.topic,
            source_key="pubmed",
            status="completed",
            priority=20,
            last_brief_id=needs_brief.brief_id,
            last_agent_run_id=needs_brief.agent_run_id,
            metadata={
                "evidence_gap_resolver": {
                    **common_resolver,
                    "lead_id": str(uuid4()),
                    "lane": "safety_signal",
                }
            },
        )
    )

    result = service.build_validation_packets(
        ValidationPacketRequest(therapy_idea_id=idea.idea_id, plan_id=plan.plan_id, limit=1)
    )

    addendum = result.packets[0].evidence_addendum
    packet = result.packets[0]
    assert isinstance(addendum, ValidationPacketEvidenceAddendum)
    assert result.ready_count == 0
    assert result.blocked_count == 1
    assert packet.status == "blocked"
    assert packet.readiness == "needs_more_evidence"
    assert packet.discovery_readiness == "needs_more_evidence"
    assert packet.validation_strategy_readiness == "blocked"
    assert packet.protocol_readiness == "needs_protocol_inputs"
    assert "validation_follow_up_needs_more_evidence" in packet.dispatch_blockers
    assert "No canine pazopanib PK or adverse-event profile was found." in packet.protocol_blockers
    assert addendum.follow_up_count == 2
    assert addendum.evaluated_follow_up_count == 2
    assert addendum.passing_follow_up_count == 1
    assert addendum.needs_more_evidence_count == 1
    assert {row.evaluation_id for row in addendum.follow_up_briefs} == {
        ready_eval.evaluation_id,
        needs_eval.evaluation_id,
    }
    assert any("Explicit dose route schedule" in update for update in addendum.material_updates)
    assert "No canine pazopanib PK or adverse-event profile was found." in addendum.unresolved_blockers


def test_validation_packet_skips_superseded_evidence_ref_repair_followup(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-packet-superseded-repair.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief, evaluation = _seed_evaluated_brief(repo, topic="Pazopanib KDR repair packet", duplicate_count=0)
    idea = TherapyIdea(
        title="KDR repair lane",
        hypothesis="Pazopanib should be reviewed in KDR-enriched canine splenic HSA.",
        rationale="The cited evidence supports a recommend-only validation plan.",
        candidate_therapies=["pazopanib"],
        targets=["KDR"],
        biomarkers=["VEGFR2"],
        evidence_refs=["C1", "C2"],
        evidence_strength="medium",
        risks=["C3 was superseded by C2 after provenance repair."],
        priority_score=0.78,
    )
    repo.upsert_therapy_idea(
        TherapyIdeaRecord(
            idea=idea,
            source_brief_id=brief.brief_id,
            source_evaluation_id=evaluation.evaluation_id,
            topic=brief.topic,
            status="ready_for_promotion",
            score=0.78,
        )
    )
    plan = repo.upsert_validation_plan(
        ValidationPlanRecord(
            brief_id=brief.brief_id,
            evaluation_id=evaluation.evaluation_id,
            topic="Pazopanib validation plan",
            status="ready_for_review",
            readiness="ready_for_expert_review",
            task_count=0,
            metadata={"therapy_idea_id": str(idea.idea_id)},
        )
    )
    queue_item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=plan.plan_id,
            task_id=uuid4(),
            brief_id=brief.brief_id,
            topic="Expert review",
            task_type="expert_review",
            title="Expert review",
            objective="Review pazopanib evidence.",
            rationale="Evidence needs review.",
            validation_request=ValidationRequest(validation_type="expert_review", objective="Review pazopanib evidence."),
        )
    )
    followup_brief, followup_eval = _seed_evaluated_brief(
        repo,
        topic="C3 stale citation repair follow-up",
        duplicate_count=0,
    )
    repo.upsert_research_brief_queue_item(
        ResearchBriefQueueItem(
            topic=followup_brief.topic,
            source_key="pubmed",
            status="completed",
            priority=20,
            last_brief_id=followup_brief.brief_id,
            metadata={
                "evidence_gap_resolver": {
                    "plan_id": str(plan.plan_id),
                    "queue_item_id": str(queue_item.queue_item_id),
                    "origin": "validation_agent_gap",
                    "superseded_by_evidence_ref_repair": True,
                },
                "evidence_ref_repair": {
                    "superseded": True,
                    "replacement_ref": "C2",
                },
            },
        )
    )

    result = service.build_validation_packets(
        ValidationPacketRequest(therapy_idea_id=idea.idea_id, plan_id=plan.plan_id, limit=1)
    )

    assert followup_eval.evaluation_id
    assert result.packets[0].evidence_addendum.follow_up_count == 0


def test_validation_packet_addendum_blockers_flag_citation_traceability():
    addendum = ValidationPacketEvidenceAddendum(
        follow_up_count=1,
        completed_follow_up_count=1,
        evaluated_follow_up_count=1,
        needs_more_evidence_count=1,
        unresolved_blockers=["C21 citation traceability is unresolved."],
        follow_up_briefs=[
            ValidationPacketAddendumBrief(
                queue_item_id=uuid4(),
                topic="C21 provenance repair",
                status="completed",
                readiness="needs_human_review",
                passes_quality_bar=False,
                key_weaknesses=["C21 is cited but absent from the supplied evidence payload."],
            )
        ],
    )

    blockers = service_module._validation_packet_addendum_blockers(addendum)
    status, readiness = service_module._validation_packet_status_after_addendum(
        "queued_for_validation",
        "queued_for_validation",
        addendum_blockers=blockers,
    )

    assert "validation_follow_up_needs_more_evidence" in blockers
    assert "validation_follow_up_needs_human_review" in blockers
    assert "validation_citation_provenance_unresolved" in blockers
    assert status == "blocked"
    assert readiness == "needs_more_evidence"


def test_validation_packet_addendum_blockers_ignore_generic_citation_quality_text():
    addendum = ValidationPacketEvidenceAddendum(
        follow_up_count=1,
        completed_follow_up_count=1,
        evaluated_follow_up_count=1,
        needs_more_evidence_count=1,
        unresolved_blockers=[
            "All citations are title/abstract level only and need stronger primary evidence before planning.",
            "All 14 citations are missing DOI, PMID, PMCID, source identifiers, and publication year metadata.",
            "Carry research_brief:79dca761-4c4f-4589-b206-d534bf3cd908#C22 as source-qualified provenance.",
            "C8's immune microenvironment finding leaves unresolved questions in the translational framework.",
            "The absence of negative evidence leaves the safety and futility gates unresolved despite a citation set.",
        ],
    )

    blockers = service_module._validation_packet_addendum_blockers(addendum)

    assert "validation_follow_up_needs_more_evidence" in blockers
    assert "validation_citation_provenance_unresolved" not in blockers


def test_validation_plan_includes_catalog_hints_and_queue_blockers(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-plan-catalog.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief, evaluation = _seed_evaluated_brief(repo, duplicate_count=0)

    plan = service.plan_validation(
        ValidationPlanRequest(brief_id=brief.brief_id, evaluation_id=evaluation.evaluation_id)
    )
    task = next(task for task in plan.tasks if task.validation_request is not None)
    queued = service.queue_validation_requests_from_plan(
        ValidationRequestQueueRequest(plan_id=plan.plan_id, dry_run=True)
    )

    assert task.metadata["validation_tool_catalog"]["tool_key"]
    assert task.validation_request.metadata["validation_tool_catalog"]["runner_status"] == "recommend_only"
    assert queued.queue_items
    assert queued.queue_items[0].metadata["tool_hint"] == task.tool_hint
    assert queued.queue_items[0].dispatch_blockers
    assert all("human_approval_required" in item.dispatch_blockers for item in queued.queue_items)
    assert all("recommend_only_runner" in item.dispatch_blockers for item in queued.queue_items)


def test_peptide_specialist_validation_planning_and_routing(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-plan-peptide.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief, evaluation = _seed_evaluated_brief(
        repo,
        topic="Cyclic peptide VEGFR pathway blockade in canine HSA",
        duplicate_count=0,
    )
    payload = dict(brief.result_payload)
    hypothesis = dict(payload["ranked_hypotheses"][0])
    hypothesis["claim"] = "A cyclic peptide could disrupt VEGFR pathway signaling in canine HSA."
    hypothesis["reasoning"] = (
        "The peptide modality needs specialist review for target engagement, protease stability, "
        "delivery, immunogenicity, and translational feasibility."
    )
    hypothesis["open_questions"] = ["What sequence, delivery route, and stability data are available?"]
    payload["ranked_hypotheses"] = [hypothesis]
    payload["final_brief"] = (
        "A cyclic peptide VEGFR-pathway concept should go through peptide specialist review [C1] [C2]."
    )
    brief = repo.upsert_research_brief(
        brief.model_copy(
            update={
                "topic": "Cyclic peptide VEGFR pathway blockade in canine HSA",
                "final_brief": payload["final_brief"],
                "result_payload": payload,
            }
        )
    )

    plan = service.plan_validation(
        ValidationPlanRequest(brief_id=brief.brief_id, evaluation_id=evaluation.evaluation_id, max_tasks=6)
    )
    peptide_task = next(task for task in plan.tasks if task.tool_hint == "peptide_specialist_review")
    queued = service.queue_validation_requests_from_plan(
        ValidationRequestQueueRequest(plan_id=plan.plan_id, task_ids=[peptide_task.task_id], dry_run=True)
    )

    catalog = peptide_task.metadata["validation_tool_catalog"]
    assert peptide_task.validation_request is not None
    assert peptide_task.task_type == "expert_review"
    assert peptide_task.validation_request.validation_type == "expert_review"
    assert catalog["tool_key"] == "peptide_specialist_review"
    assert catalog["recommended_agent_name"] == "peptide_specialist_validation_agent"
    assert "peptide_identity_required" in peptide_task.validation_request.quality_gates
    assert queued.queue_items
    assert validation_agents.validation_agent_name(queued.queue_items[0]) == "peptide_specialist_validation_agent"


def test_validation_request_queue_contracts_validate():
    plan_id = uuid4()
    task_id = uuid4()
    item = ValidationRequestQueueItem(
        plan_id=plan_id,
        task_id=task_id,
        brief_id=uuid4(),
        topic="VEGF validation path",
        task_type="expert_review",
        title="Review target validation",
        objective="Review whether this target is ready for validation.",
        rationale="The plan is source-traceable.",
        validation_request=ValidationRequest(
            validation_type="expert_review",
            objective="Review whether this target is ready for validation.",
        ),
    )
    request = ValidationRequestQueueRequest(plan_id=plan_id, task_ids=[task_id])

    assert item.status == "needs_approval"
    assert item.identity_key == f"validation_request_queue:{plan_id}:{task_id}"
    assert request.dry_run is True
    autopilot_request = ValidationAutopilotRequest(
        allowed_task_types=["expert_review", "expert_review"],
        allowed_validation_types=["expert_review", "Expert Review"],
        source_keys=["PubMed", "pubmed"],
    )
    assert autopilot_request.dry_run is True
    assert autopilot_request.max_per_run == 2
    assert autopilot_request.allowed_task_types == ["expert_review"]
    assert autopilot_request.allowed_validation_types == ["expert_review"]
    assert autopilot_request.source_keys == ["pubmed"]
    with pytest.raises(ValueError):
        ValidationAutopilotRequest(allowed_task_types=["wet_magic"])
    source_pack_request = ValidationGapSourcePackRequest(
        source_keys=["PubMed", "pubmed"],
        lanes=["safety_signal"],
    )
    assert source_pack_request.source_keys == ["pubmed"]
    assert source_pack_request.lanes == ["safety_signal"]
    source_query = ValidationGapSourceQuery(
        lane="safety_signal",
        source_key="pubmed",
        query_name="validation_gap_safety_pubmed",
        query_text="sorafenib AND canine AND safety",
        reason="Need direct canine tolerability evidence.",
        required_terms=["sorafenib", "sorafenib"],
    )
    assert source_query.required_terms == ["sorafenib"]
    assert source_query.as_source_query().track == "validation_gap"
    with pytest.raises(ValueError):
        ValidationGapSourcePackRequest(lanes=["bad_lane"])
    ingest_request = ValidationGapSourceIngestRequest(
        source_keys=["PubMed", "pubmed"],
        query_names=["gap_a", "gap_a"],
    )
    assert ingest_request.source_keys == ["pubmed"]
    assert ingest_request.query_names == ["gap_a"]
    agent_result = ValidationAgentResult(
        queue_item_id=item.queue_item_id,
        plan_id=plan_id,
        task_id=task_id,
        task_type="expert_review",
        validation_type="expert_review",
        agent_name="evidence_review_validation_agent",
        model_profile="deterministic_only",
        decision="hold",
        confidence=0.51,
        summary="Evidence needs expert review before promotion.",
        evidence_used=["C1", "C1"],
    )
    assert agent_result.evidence_used == ["C1"]
    with pytest.raises(ValueError):
        ValidationRequestQueueItem(
            status="bad",
            plan_id=plan_id,
            task_id=task_id,
            brief_id=uuid4(),
            topic="VEGF validation path",
            task_type="expert_review",
            title="Review target validation",
            objective="Review whether this target is ready for validation.",
            rationale="The plan is source-traceable.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review whether this target is ready for validation.",
            ),
        )
    with pytest.raises(ValueError):
        ValidationAgentResult(
            queue_item_id=item.queue_item_id,
            plan_id=plan_id,
            task_id=task_id,
            task_type="expert_review",
            validation_type="expert_review",
            agent_name="evidence_review_validation_agent",
            model_profile="deterministic_only",
            decision="bad",
            summary="bad",
        )


def test_full_text_ops_requests_partition_or_ingest_when_validation_is_missing(tmp_path):
    clean_repo = SQLiteResearchRepository(tmp_path / "full-text-ops-partition.sqlite3", seed=False)
    _seed_full_text_source_claim(clean_repo, "europe_pmc")

    partition_result = FullTextOpsAgent(clean_repo).run(
        FullTextOpsRequest(
            source_keys=["europe_pmc"],
            partition_date="2026-04-27",
            review_mode="deterministic_only",
        )
    )

    assert partition_result.schedule_readiness == "needs_partition_validation"
    assert any(action.action == "run_source_date_partition" for action in partition_result.actions)

    empty_repo = SQLiteResearchRepository(tmp_path / "full-text-ops-empty.sqlite3", seed=False)
    ingest_result = FullTextOpsAgent(empty_repo).run(
        FullTextOpsRequest(source_keys=["europe_pmc"], review_mode="deterministic_only")
    )

    assert ingest_result.schedule_readiness == "keep_stopped"
    assert any(action.action == "run_ingest_smoke" for action in ingest_result.actions)


def test_request_validation_can_queue_without_approval(tmp_path):
    service = make_service(tmp_path)

    handle = service.request_validation(
        ValidationRequest(
            validation_type="admet",
            candidate_name="propranolol",
            objective="Screen canine safety risk",
            require_approval=False,
        )
    )

    assert handle.status == "queued"
    assert service.get_candidate(CandidateDossierRequest(candidate_name="propranolol")) is not None


def test_command_center_web_validation_queue_actions(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web.sqlite3", seed=False)
    service = HSAResearchService(repo)
    queue_item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            source_key="pubmed",
            topic="VEGF validation plan",
            task_type="expert_review",
            title="Expert review: VEGF translational signal",
            objective="Review whether the hypothesis has enough evidence for validation.",
            rationale="Human approval is required before dispatch.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review whether the hypothesis has enough evidence for validation.",
            ),
            quality_gates=["human_approval_required"],
            priority=25,
        )
    )

    listed = command_center_web.list_validation_queue_payload(service, {"status": ["needs_approval"]})
    approved = command_center_web.approve_validation_request_payload(
        service,
        str(queue_item.queue_item_id),
        {"approved_by": "operator"},
    )
    dispatched = command_center_web.dispatch_validation_request_payload(
        service,
        str(queue_item.queue_item_id),
        {"model_profile": "deterministic_only"},
    )

    assert listed["visible"] == 1
    assert listed["items"][0]["queue_item_id"] == str(queue_item.queue_item_id)
    assert approved["item"]["status"] == "approved"
    assert approved["item"]["approved_by"] == "operator"
    assert dispatched["item"]["status"] == "completed"
    assert dispatched["item"]["dispatch_blockers"] == []
    assert dispatched["item"]["last_run_id"]
    assert dispatched["item"]["metadata"]["validation_agent_result"]["decision"] in {"promote", "hold", "demote"}


def test_command_center_web_validation_autopilot_payloads(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-autopilot.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            source_key="pubmed",
            topic="Autopilot command center preview",
            task_type="expert_review",
            title="Expert review: command center autopilot",
            objective="Review the evidence packet.",
            rationale="The command center should preview conservative selection.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review the evidence packet.",
            ),
            created_at=datetime.now(UTC) - timedelta(hours=2),
        )
    )

    preview = command_center_web.validation_autopilot_preview_payload(
        service,
        {
            "model_profile": ["deterministic_only"],
            "minimum_queue_age_hours": ["0"],
            "max_per_run": ["2"],
        },
    )
    dry_run = command_center_web.run_validation_autopilot_payload(
        service,
        {
            "dry_run": True,
            "model_profile": "deterministic_only",
            "minimum_queue_age_hours": 0,
        },
    )
    stored = service.get_validation_request_queue_item(item.queue_item_id)

    assert preview["selected_count"] == 1
    assert preview["selected"][0]["queue_item_id"] == str(item.queue_item_id)
    assert dry_run["dry_run"] is True
    assert dry_run["agent_run_id"]
    assert stored is not None
    assert stored.status == "needs_approval"


def test_therapy_committee_validation_queue_promotes_ranked_ideas(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "therapy-committee-validation.sqlite3", seed=False)
    idea = TherapyIdea(
        title="KDR/VEGFR2 mutation-gated TKI validation",
        hypothesis="KDR/FLT4 altered HSA may respond to VEGFR-targeting TKIs.",
        rationale="The committee connected cross-species vascular tumor biology with KDR/FLT4 evidence.",
        candidate_therapies=["toceranib", "sorafenib"],
        targets=["KDR", "FLT4"],
        biomarkers=["KDR mutation", "phospho-VEGFR2"],
        mechanism="VEGFR blockade should suppress downstream angiogenic signaling.",
        evidence_refs=["C1", "C2"],
        evidence_strength="medium",
        translational_path="Use canine HSA as a comparative model for human angiosarcoma.",
        risks=["coagulation risk", "species PK/PD uncertainty"],
        next_experiments=["TKI dose-response assay", "coagulation safety review"],
        priority_score=0.82,
    )
    committee = TherapyCommitteeResult(
        topic="KDR VEGFA mTOR therapy ideas for canine hemangiosarcoma",
        disease_scope="canine hemangiosarcoma and human angiosarcoma",
        review_mode="openrouter_required",
        ranked_ideas=[idea],
        decision_summary="Top idea is KDR/VEGFR2 mutation-gated TKI validation.",
        evidence={"citation_count": 2, "recommend_only": True},
    )
    agent_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            model_profile="therapy_committee",
            status=RunStatus.COMPLETED,
            output_payload=committee.model_dump(mode="json"),
            summary={"idea_count": 1},
        )
    )
    service = HSAResearchService(repo)

    preview = service.queue_therapy_committee_validation_requests(
        TherapyCommitteeValidationQueueRequest(agent_run_id=agent_run.agent_run_id)
    )
    applied = service.queue_therapy_committee_validation_requests(
        TherapyCommitteeValidationQueueRequest(agent_run_id=agent_run.agent_run_id, dry_run=False)
    )
    duplicate = service.queue_therapy_committee_validation_requests(
        TherapyCommitteeValidationQueueRequest(agent_run_id=agent_run.agent_run_id, dry_run=False)
    )
    persisted = service.list_validation_request_queue_items(status="needs_approval", limit=10)

    assert isinstance(preview, TherapyCommitteeValidationQueueResult)
    assert preview.dry_run is True
    assert preview.candidate_idea_count == 1
    assert preview.candidate_task_count == 3
    assert preview.queued_count == 0
    assert applied.queued_count == 3
    assert duplicate.existing_count == 3
    assert len(persisted) == 3
    assert {item.task_type for item in persisted} == {"expert_review", "wet_lab", "safety"}
    assert {item.validation_request.validation_type for item in persisted} == {"expert_review", "wet_lab", "safety"}
    assert all(item.metadata["queued_from"] == "therapy_committee" for item in persisted)
    assert all(item.validation_request.assay_context is not None for item in persisted)
    assert all("source_traceability_required" in item.quality_gates for item in persisted)
    assert repo.get_validation_plan(applied.plan_id) is not None


def test_validation_planning_service_persists_ready_recommend_only_plan(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-planning.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:validation-plan",
            content_hash="validation-plan-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/validation-plan/",
            raw_payload={"pmid": "validation-plan"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF therapy and target validation in canine hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/validation-plan/",
            dedupe_key="pmid:validation-plan",
            identifiers={"pmid": "validation-plan"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma and human angiosarcoma share vascular biology. "
                "VEGF, KDR, toxicity, biomarker expression, docking, and translational "
                "target validation are discussed as testable research paths."
            ),
            content_hash="validation-plan-chunk",
        )
    )
    service = HSAResearchService(repo)
    brief = service.run_research_brief(
        ResearchBriefRequest(
            topic="VEGF omics biomarker expression target validation in canine hemangiosarcoma",
            review_mode="deterministic_only",
            max_chunks_per_perspective=3,
            max_claims=0,
        )
    )
    evaluation = service.evaluate_research_brief(ResearchBriefEvaluationRequest(brief_id=brief.brief_id))

    result = service.plan_validation(
        ValidationPlanRequest(evaluation_id=evaluation.evaluation_id, max_tasks=6)
    )
    saved = repo.get_validation_plan(result.plan_id)
    runs = repo.list_agent_runs(agent_name="validation_planning_agent", status="completed")

    assert result.status == "ready_for_review"
    assert result.readiness == "ready_for_expert_review"
    assert result.agent_run_id is not None
    assert result.hypothesis_drafts
    assert result.tasks
    omics_tasks = [task for task in result.tasks if task.task_type == "omics"]
    assert omics_tasks
    assert all(task.validation_request is not None for task in omics_tasks)
    assert {task.validation_request.validation_type for task in omics_tasks if task.validation_request} == {"omics"}
    assert all(task.requires_human_approval for task in result.tasks)
    assert all(task.validation_request is None or task.validation_request.require_approval for task in result.tasks)
    validation_requests = [task.validation_request for task in result.tasks if task.validation_request is not None]
    assert validation_requests
    assert all(request.assay_context is not None for request in validation_requests)
    assert any("canine" in request.assay_context.species for request in validation_requests if request.assay_context)
    assert all("source_traceability_required" in request.quality_gates for request in validation_requests)
    omics_requests = [request for request in validation_requests if request.validation_type == "omics"]
    assert all("omics_dataset_context_required" in request.quality_gates for request in omics_requests)
    assert saved is not None
    assert saved.brief_id == brief.brief_id
    assert saved.evaluation_id == evaluation.evaluation_id
    assert saved.task_count == len(result.tasks)
    assert runs[0].output_payload["plan_id"] == str(result.plan_id)
    assert service.get_run_status(uuid4()) is None


def test_validation_request_queue_promotes_ready_plan_tasks(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-request-queue-service.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:validation-request-queue",
            content_hash="validation-request-queue-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/validation-request-queue/",
            raw_payload={"pmid": "validation-request-queue"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF target validation request queue evidence",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/validation-request-queue/",
            dedupe_key="pmid:validation-request-queue",
            identifiers={"pmid": "validation-request-queue"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma target validation evidence discusses VEGF, "
                "KDR, toxicity, docking, and translational biomarker review."
            ),
            content_hash="validation-request-queue-chunk",
        )
    )
    service = HSAResearchService(repo)
    brief = service.run_research_brief(
        ResearchBriefRequest(
            topic="VEGF omics biomarker expression target validation in canine hemangiosarcoma",
            review_mode="deterministic_only",
            max_chunks_per_perspective=3,
            max_claims=0,
        )
    )
    evaluation = service.evaluate_research_brief(ResearchBriefEvaluationRequest(brief_id=brief.brief_id))
    plan = service.plan_validation(ValidationPlanRequest(evaluation_id=evaluation.evaluation_id, max_tasks=6))

    preview = service.queue_validation_requests_from_plan(
        ValidationRequestQueueRequest(plan_id=plan.plan_id, dry_run=True)
    )
    applied = service.queue_validation_requests_from_plan(
        ValidationRequestQueueRequest(plan_id=plan.plan_id, dry_run=False)
    )
    duplicate = service.queue_validation_requests_from_plan(
        ValidationRequestQueueRequest(plan_id=plan.plan_id, dry_run=False)
    )
    queued_item = applied.queue_items[0]
    blocked_dispatch = service.dispatch_validation_request_queue_item(queued_item.queue_item_id)
    approved = service.approve_validation_request_queue_item(
        queued_item.queue_item_id,
        approved_by="unit-test",
        approval_note="Ready for controlled validation.",
    )
    dispatched = service.dispatch_validation_request_queue_item(
        queued_item.queue_item_id,
        model_profile="deterministic_only",
    )

    assert preview.dry_run is True
    assert preview.queued_count == 0
    assert preview.queue_items
    assert applied.queued_count == len(applied.queue_items)
    assert any(item.validation_request.validation_type == "omics" for item in applied.queue_items)
    assert duplicate.queued_count == 0
    assert duplicate.existing_count == len(applied.queue_items)
    assert blocked_dispatch is not None
    assert blocked_dispatch.status == "needs_approval"
    assert "approved before dispatch" in blocked_dispatch.last_error
    assert approved is not None
    assert approved.status == "approved"
    assert dispatched is not None
    assert dispatched.status == "completed"
    assert dispatched.last_run_id is not None
    assert service.get_agent_run(dispatched.last_run_id) is not None
    assert dispatched.metadata["validation_agent_result"]["decision"] in {"promote", "hold", "demote"}


def test_validation_request_queue_blocks_dispatch_without_assay_context(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-request-queue-blocked.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Docking queue guardrail",
            task_type="docking",
            title="Dock KDR candidate",
            objective="Run docking only after target, candidate, and assay context are present.",
            rationale="Execution lanes need enough context to be reproducible.",
            validation_request=ValidationRequest(
                validation_type="docking",
                target_name="KDR",
                candidate_name="candidate A",
                objective="Dock candidate A against KDR.",
                require_approval=True,
            ),
        )
    )
    approved = service.approve_validation_request_queue_item(
        item.queue_item_id,
        approved_by="unit-test",
        approval_note="Approval alone should not bypass execution context.",
    )
    blocked = service.dispatch_validation_request_queue_item(item.queue_item_id)

    assert approved is not None
    assert approved.status == "approved"
    assert blocked is not None
    assert blocked.status == "blocked"
    assert "assay_context_required" in blocked.dispatch_blockers
    assert "dispatch blocked" in blocked.last_error.lower()
    assert blocked.last_run_id is None


def test_omics_validation_request_dispatches_after_approval(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "omics-validation-request.sqlite3", seed=False)
    service = HSAResearchService(repo)
    context = ValidationAssayContext(
        disease_context="canine hemangiosarcoma and human angiosarcoma",
        species=["canine", "human"],
        model_system="Comparative canine and human molecular dataset review.",
        assay_type="omics evidence review",
        readout="species-conserved signal, expression context, and dataset caveats",
        endpoint="translational molecular support",
        evidence_refs=["brief:1", "evaluation:1", "C1"],
        negative_evidence_needs=["Check whether negative or null expression datasets exist."],
    )
    item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Omics support queue guardrail",
            task_type="omics",
            title="Omics support check",
            objective="Check expression and biomarker evidence across canine and human datasets.",
            rationale="The synthesis identified a molecular support question.",
            validation_request=ValidationRequest(
                validation_type="omics",
                objective="Review comparative omics support.",
                require_approval=True,
                assay_context=context,
                quality_gates=["omics_dataset_context_required"],
            ),
            quality_gates=["omics_dataset_context_required"],
            metadata={
                "evidence_refs": ["brief:1", "evaluation:1", "C1"],
                "expected_outputs": ["dataset support", "species translation notes"],
            },
        )
    )

    approved = service.approve_validation_request_queue_item(
        item.queue_item_id,
        approved_by="unit-test",
        approval_note="Omics context is present.",
    )
    dispatched = service.dispatch_validation_request_queue_item(
        item.queue_item_id,
        model_profile="deterministic_only",
    )

    assert approved is not None
    assert approved.status == "approved"
    assert dispatched is not None
    assert dispatched.status == "completed"
    assert "omics_dataset_context_required" in dispatched.quality_gates
    assert dispatched.metadata["validation_agent_result"]["agent_name"] == "omics_validation_agent"
    assert dispatched.metadata["validation_agent_result"]["validation_type"] == "omics"


def test_validation_autopilot_dry_run_selects_allowlisted_items(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-autopilot-preview.sqlite3", seed=False)
    service = HSAResearchService(repo)
    eligible = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Autopilot eligible review",
            task_type="expert_review",
            title="Expert review: VEGF evidence packet",
            objective="Review the evidence packet.",
            rationale="This is a low-risk recommend-only review.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review the evidence packet.",
            ),
            created_at=datetime.now(UTC) - timedelta(hours=2),
        )
    )
    repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Autopilot excluded wet lab",
            task_type="wet_lab",
            title="Wet lab protocol",
            objective="Design an experiment.",
            rationale="Wet lab work stays manual.",
            validation_request=ValidationRequest(
                validation_type="wet_lab",
                objective="Design an experiment.",
            ),
            created_at=datetime.now(UTC) - timedelta(hours=2),
        )
    )

    result = service.preview_validation_autopilot(
        ValidationAutopilotRequest(
            model_profile="deterministic_only",
            minimum_queue_age_hours=1.0,
        )
    )
    stored = service.get_validation_request_queue_item(eligible.queue_item_id)

    assert result.dry_run is True
    assert result.selected_count == 1
    assert result.selected[0].queue_item_id == eligible.queue_item_id
    assert any(record.reason == "task_type_not_allowlisted:wet_lab" for record in result.skipped)
    assert stored is not None
    assert stored.status == "needs_approval"
    assert stored.approved_by is None


def test_validation_autopilot_apply_dispatches_deterministic_item(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-autopilot-apply.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Autopilot apply review",
            task_type="expert_review",
            title="Expert review: KDR mutation signal",
            objective="Review KDR mutation support.",
            rationale="This is a low-risk recommend-only review.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review KDR mutation support.",
            ),
            metadata={"expected_outputs": ["go/no-go validation readiness"], "evidence_refs": ["C1", "C2"]},
        )
    )

    result = service.run_validation_autopilot(
        ValidationAutopilotRequest(
            dry_run=False,
            force=True,
            model_profile="deterministic_only",
            minimum_queue_age_hours=0.0,
        )
    )
    stored = service.get_validation_request_queue_item(item.queue_item_id)

    assert result.agent_run_id is not None
    assert result.dispatched_count == 1
    assert result.actual_cost_usd == 0.0
    assert stored is not None
    assert stored.status == "completed"
    assert stored.approved_by == "validation_autopilot"
    assert stored.metadata["validation_autopilot"]["result_status"] == "completed"
    assert stored.metadata["validation_agent_result"]["decision"] in {"promote", "hold", "demote"}
    assert service.get_agent_run(result.agent_run_id) is not None


def test_validation_autopilot_blocks_recent_manual_activity(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-autopilot-grace.sqlite3", seed=False)
    service = HSAResearchService(repo)
    candidate = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Autopilot grace candidate",
            task_type="expert_review",
            title="Expert review candidate",
            objective="Review the evidence packet.",
            rationale="This should wait because an operator was active.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review the evidence packet.",
            ),
            created_at=datetime.now(UTC) - timedelta(hours=2),
        )
    )
    manual = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            status="approved",
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Recent manual activity",
            task_type="expert_review",
            title="Manually approved review",
            objective="Review manually.",
            rationale="This records recent operator activity.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review manually.",
            ),
            approved_by="operator",
            metadata={"approved_at": datetime.now(UTC).isoformat()},
        )
    )

    result = service.run_validation_autopilot(
        ValidationAutopilotRequest(
            dry_run=False,
            model_profile="deterministic_only",
            minimum_queue_age_hours=0.0,
            manual_grace_period_hours=6.0,
        )
    )
    stored = service.get_validation_request_queue_item(candidate.queue_item_id)

    assert manual.approved_by == "operator"
    assert "manual_grace_period_active" in result.blockers
    assert result.selected_count == 1
    assert result.dispatched_count == 0
    assert stored is not None
    assert stored.status == "needs_approval"
    assert stored.approved_by is None


def test_validation_autopilot_blocks_openrouter_missing_before_mutation(monkeypatch, tmp_path):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    repo = SQLiteResearchRepository(tmp_path / "validation-autopilot-openrouter.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Autopilot live model candidate",
            task_type="expert_review",
            title="Expert review live model candidate",
            objective="Review the evidence packet.",
            rationale="OpenRouter must be ready before mutation.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review the evidence packet.",
            ),
            created_at=datetime.now(UTC) - timedelta(hours=2),
        )
    )

    result = service.run_validation_autopilot(
        ValidationAutopilotRequest(
            dry_run=False,
            model_profile="openrouter_required",
            minimum_queue_age_hours=0.0,
        )
    )
    stored = service.get_validation_request_queue_item(item.queue_item_id)

    assert "openrouter_api_key_missing" in result.blockers
    assert result.dispatched_count == 0
    assert stored is not None
    assert stored.status == "needs_approval"
    assert stored.approved_by is None
    assert stored.attempts == 0


def test_validation_gap_source_pack_builds_and_persists_targeted_queries(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-gap-source-pack.sqlite3", seed=False)
    service = HSAResearchService(repo)
    plan_id = uuid4()
    task_id = uuid4()
    origin_agent_run_id = uuid4()
    queue_item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=plan_id,
            task_id=task_id,
            brief_id=uuid4(),
            topic="Sorafenib VEGFR safety validation",
            task_type="expert_review",
            title="Review sorafenib safety in canine HSA",
            objective="Find direct canine sorafenib safety and PK/PD evidence before promotion.",
            rationale="Validation held because direct canine dosing evidence is missing.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                candidate_name="sorafenib",
                target_name="KDR",
                objective="Find direct canine sorafenib safety and PK/PD evidence before promotion.",
                require_approval=False,
            ),
            status="completed",
            metadata={
                "validation_agent_result": ValidationAgentResult(
                    queue_item_id=uuid4(),
                    plan_id=plan_id,
                    task_id=task_id,
                    task_type="expert_review",
                    validation_type="expert_review",
                    agent_name="evidence_review_validation_agent",
                    model_profile="openrouter_required",
                    decision="hold",
                    confidence=0.62,
                    summary="Hold pending direct canine dosing evidence.",
                    evidence_used=["C1"],
                    missing_evidence=["Direct canine sorafenib safety and dose-limiting toxicity evidence."],
                ).model_dump(mode="json"),
            },
        )
    )
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            identity_key=f"research_lead:validation_gap:{queue_item.queue_item_id}:safety",
            title="Safety signal: direct canine sorafenib DLT evidence",
            status="new",
            priority=10,
            source_key="pubmed",
            origin_source_key="validation_agent",
            origin_record_id=str(queue_item.queue_item_id),
            origin_review_id=queue_item.queue_item_id,
            origin_agent_run_id=origin_agent_run_id,
            reason="Direct canine sorafenib safety and dose-limiting toxicity evidence.",
            evidence_refs=[f"validation_queue:{queue_item.queue_item_id}"],
            topic_tags=["validation_gap", "safety_signal", "missing_evidence"],
            suggested_sources=["pubmed", "chembl", "openfda_animal_events"],
            metadata={
                "evidence_gap_resolver": {
                    "origin": "validation_agent_result",
                    "gap_type": "missing_evidence",
                    "lane": "safety_signal",
                    "gap_text": "Direct canine sorafenib safety and dose-limiting toxicity evidence.",
                    "queue_item_id": str(queue_item.queue_item_id),
                    "plan_id": str(plan_id),
                    "task_id": str(task_id),
                    "task_type": "expert_review",
                    "validation_type": "expert_review",
                    "decision": "hold",
                }
            },
        )
    )

    preview = service.build_validation_gap_source_pack(
        ValidationGapSourcePackRequest(
            lead_ids=[lead.lead_id],
            source_keys=["pubmed", "europe_pmc", "chembl", "openfda_animal_events"],
            max_queries_per_lane=5,
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="old_source_pack_query",
            query_text="stale sorafenib query",
            query_params={
                "source_pack_request": {"persist_queries": True},
                "lead_id": str(lead.lead_id),
                "lane": "safety_signal",
            },
            track="validation_gap",
            active=True,
        )
    )
    applied = service.build_validation_gap_source_pack(
        ValidationGapSourcePackRequest(
            lead_ids=[lead.lead_id],
            source_keys=["pubmed", "europe_pmc", "chembl", "openfda_animal_events"],
            max_queries_per_lane=5,
            persist_queries=True,
            dry_run=False,
        )
    )
    stored_queries = repo.list_source_queries(active_only=True)
    all_stored_queries = repo.list_source_queries(active_only=False)
    stale_query = next(query for query in all_stored_queries if query.query_name == "old_source_pack_query")

    assert isinstance(preview, ValidationGapSourcePackResult)
    assert preview.query_count == 4
    assert preview.persisted_query_count == 0
    assert {query.source_key for query in preview.queries} == {"pubmed", "europe_pmc", "chembl", "openfda_animal_events"}
    assert any("sorafenib" in query.query_text.lower() and "safety" in query.query_text.lower() for query in preview.queries)
    assert applied.persisted_query_count == 4
    assert len(stored_queries) == 4
    assert stale_query.active is False
    assert all(query.track == "validation_gap" for query in stored_queries)
    assert all(query.query_params["followup_lane"] == "agent_evaluator_followup" for query in stored_queries)
    assert all(query.query_params["comparative_policy"] == "disabled" for query in stored_queries)
    assert all(query.query_params["origin_review_id"] == str(queue_item.queue_item_id) for query in stored_queries)
    assert all(query.query_params["origin_agent_run_id"] == str(origin_agent_run_id) for query in stored_queries)
    europe_pmc_query = next(query for query in stored_queries if query.source_key == "europe_pmc")
    assert europe_pmc_query.query_params["fetch_full_text"] is True
    assert europe_pmc_query.query_params["full_text_time_budget_seconds"] == 20
    assert repo.list_agent_runs(agent_name="validation_gap_source_pack_agent", status="completed", limit=2)


def test_validation_gap_source_pack_compacts_long_candidate_and_target_terms(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-gap-source-pack-compact.sqlite3", seed=False)
    service = HSAResearchService(repo)
    plan_id = uuid4()
    task_id = uuid4()
    queue_item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=plan_id,
            task_id=task_id,
            brief_id=uuid4(),
            topic="Anti-PD-1 VEGFR-2 safety validation",
            task_type="expert_review",
            title="Review anti-PD-1 and VEGFR-2 safety in canine HSA",
            objective="Find canine anti-PD-1 monotherapy safety evidence.",
            rationale="Validation held because direct canine HSA evidence is missing.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                candidate_name=(
                    "Canine-specific anti-PD-1 monoclonal antibody (e.g., ca-4F12-E6 or next-generation "
                    "canine PD-1 inhibitor) / VEGFR-2 blocking antibody or small-molecule VEGFR-2 inhibitor "
                    "(toceranib phosphate as a clinically available canine-approved option) / Combination arm: "
                    "anti-PD-1 + VEGFR-2 inhibitor administered concurrently or sequentially following splenectomy"
                ),
                target_name="PD-1 / PD-L1 axis / VEGFR-2 (KDR) / VEGF (upstream ligand)",
                objective="Find canine anti-PD-1 monotherapy safety evidence.",
                require_approval=False,
            ),
            status="completed",
            metadata={},
        )
    )
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            identity_key=f"research_lead:validation_gap:{queue_item.queue_item_id}:safety",
            title="Safety signal: anti-PD-1 monotherapy in canine HSA",
            status="new",
            priority=10,
            origin_source_key="validation_agent",
            origin_record_id=str(queue_item.queue_item_id),
            origin_review_id=queue_item.queue_item_id,
            origin_agent_run_id=uuid4(),
            reason=(
                "Anti-PD-1 monotherapy efficacy and safety data in canine HSA; "
                "the ca-4F12-E6 precedent is melanoma-only."
            ),
            evidence_refs=[f"validation_queue:{queue_item.queue_item_id}"],
            topic_tags=["validation_gap", "safety_signal", "missing_evidence"],
            suggested_sources=["pubmed", "chembl", "openfda_animal_events"],
            metadata={
                "evidence_gap_resolver": {
                    "origin": "validation_agent_result",
                    "gap_type": "missing_evidence",
                    "lane": "safety_signal",
                    "gap_text": (
                        "Anti-PD-1 monotherapy efficacy and safety data in canine HSA; "
                        "the ca-4F12-E6 precedent is melanoma-only."
                    ),
                    "queue_item_id": str(queue_item.queue_item_id),
                    "plan_id": str(plan_id),
                    "task_id": str(task_id),
                    "task_type": "expert_review",
                    "validation_type": "expert_review",
                    "decision": "hold",
                }
            },
        )
    )

    result = service.build_validation_gap_source_pack(
        ValidationGapSourcePackRequest(
            lead_ids=[lead.lead_id],
            source_keys=["pubmed", "chembl", "openfda_animal_events"],
            max_queries_per_lane=5,
        )
    )
    query_text = " ".join(query.query_text for query in result.queries).lower()
    required_terms = [term for query in result.queries for term in query.required_terms]

    assert result.query_count == 3
    assert "combination arm" not in query_text
    assert "administered concurrently" not in query_text
    assert "anti-pd-1" in query_text
    assert "ca-4f12-e6" in query_text
    assert "toceranib" not in query_text
    assert "vegfr-2" not in query_text
    assert all(len(term) <= 80 for term in required_terms)


def test_validation_gap_source_pack_expands_frontier_modality_queries(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-gap-source-pack-frontier.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="ADC evidence gap for endothelial HSA antigen targeting",
            lead_type="unknown",
            status="followup",
            priority=25,
            reason="Find antibody-drug conjugate and ADC evidence for endothelial antigen targeting in canine HSA.",
            suggested_sources=["pubmed"],
            metadata={
                "evidence_gap_resolver": {
                    "origin": "frontier_moonshot_gap",
                    "lane": "chemistry",
                    "gap_text": "Antibody-drug conjugate ADC targeting CD31 or vascular antigens in canine HSA.",
                    "task_type": "frontier_modality_review",
                    "validation_type": "expert_review",
                }
            },
        )
    )

    result = service.build_validation_gap_source_pack(
        ValidationGapSourcePackRequest(lead_ids=[lead.lead_id], source_keys=["pubmed"], max_queries_per_lane=5)
    )

    assert result.query_count == 1
    query = result.queries[0]
    assert '"antibody-drug conjugate"' in query.query_text
    assert query.query_params["frontier_query_expansion"]
    assert "antibody_drug_conjugate" in query.query_params["matched_frontier_modalities"]


def test_validation_gap_ingest_runs_only_validation_gap_queries(monkeypatch):
    repo = InMemoryResearchRepository()
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="validation_gap_safety",
            query_text="sorafenib canine safety",
            query_params={
                "lane": "safety_signal",
                "validation_gap": True,
                "source_pack_request": {"dry_run": False},
                "require_policy_match": False,
                "mindate": "2026/01/01",
            },
            track="validation_gap",
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="starter_query",
            query_text="hemangiosarcoma",
            track="comparative_oncology",
        )
    )
    calls = []

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            calls.append((query.source_key, query.query_name, limit, persist_query, query.query_params))
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                raw_records=2,
                research_objects=2,
                document_chunks=2,
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    preview = HSAResearchService(repo).ingest_validation_gap_source_queries(
        ValidationGapSourceIngestRequest(source_keys=["pubmed"])
    )
    applied = HSAResearchService(repo).ingest_validation_gap_source_queries(
        ValidationGapSourceIngestRequest(source_keys=["pubmed"], dry_run=False, limit_per_query=3)
    )

    assert preview.dry_run is True
    assert preview.query_count == 1
    assert preview.attempted_query_count == 0
    assert applied.query_count == 1
    assert applied.completed_query_count == 1
    assert applied.raw_records == 2
    assert calls == [
        (
            "pubmed",
            "validation_gap_safety",
            3,
            False,
            {"require_policy_match": False, "mindate": "2026/01/01"},
        )
    ]


def test_validation_gap_ingest_can_select_omics_followup_track(monkeypatch):
    repo = InMemoryResearchRepository()
    selected = SourceQuery(
        source_key="pubmed",
        query_name="omics_followup_protein_expression",
        query_text="canine hemangiosarcoma vimentin immunohistochemistry",
        query_params={"task_type": "protein_expression"},
        track="omics_followup",
    )
    repo.upsert_source_query(selected)
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="validation_gap_safety",
            query_text="sorafenib canine safety",
            track="validation_gap",
        )
    )
    calls = []

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            calls.append((query.query_name, query.query_params))
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    default_preview = HSAResearchService(repo).ingest_validation_gap_source_queries(
        ValidationGapSourceIngestRequest(source_keys=["pubmed"])
    )
    omics_result = HSAResearchService(repo).ingest_validation_gap_source_queries(
        ValidationGapSourceIngestRequest(
            source_keys=["pubmed"],
            tracks=["omics_followup"],
            dry_run=False,
        )
    )

    assert [query.query_name for query in default_preview.source_queries] == ["validation_gap_safety"]
    assert omics_result.query_count == 1
    assert omics_result.source_queries == [selected]
    assert calls == [("omics_followup_protein_expression", {"task_type": "protein_expression"})]


def test_validation_gap_ingest_strips_internal_params_before_api_calls(monkeypatch):
    repo = InMemoryResearchRepository()
    repo.upsert_source_query(
        SourceQuery(
            source_key="openalex",
            query_name="validation_gap_openalex",
            query_text="sorafenib angiosarcoma",
            query_params={
                "lane": "clinical_response",
                "lead_id": "lead-1",
                "queue_item_id": "queue-1",
                "required_terms": ["sorafenib"],
                "source_pack_request": {"persist_queries": True},
                "validation_gap": True,
                "filter": "from_publication_date:2020-01-01",
                "sort": "cited_by_count:desc",
            },
            track="validation_gap",
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="clinicaltrials_gov",
            query_name="validation_gap_trials",
            query_text="angiosarcoma sorafenib",
            query_params={
                "lane": "clinical_response",
                "search_area": "term",
                "validation_gap": True,
            },
            track="validation_gap",
        )
    )
    calls = []

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            calls.append((query.source_key, query.query_params))
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    result = HSAResearchService(repo).ingest_validation_gap_source_queries(
        ValidationGapSourceIngestRequest(
            source_keys=["openalex", "clinicaltrials_gov"],
            dry_run=False,
        )
    )

    assert result.completed_query_count == 2
    assert calls == [
        ("clinicaltrials_gov", {"search_area": "term"}),
        ("openalex", {"filter": "from_publication_date:2020-01-01", "sort": "cited_by_count:desc"}),
    ]


def test_local_ingestion_sanitizes_validation_gap_query_params(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-gap-local-ingest.sqlite3", seed=False)
    calls = []

    class FakeHarvester:
        def fetch(self, query_text, limit=25, **params):
            calls.append((query_text, limit, params))
            return []

    monkeypatch.setattr(local_ingest_module, "get_harvester", lambda source_key: FakeHarvester())

    result = LocalIngestionPipeline(repo).ingest_query(
        SourceQuery(
            source_key="openalex",
            query_name="validation_gap_openalex",
            query_text="sorafenib angiosarcoma",
            query_params={
                "lane": "clinical_response",
                "lead_id": "lead-1",
                "validation_gap": True,
                "filter": "from_publication_date:2020-01-01",
            },
            track="validation_gap",
        ),
        limit=1,
        persist_query=True,
    )
    stored = repo.list_source_queries(source_key="openalex", active_only=True)[0]

    assert result.status == RunStatus.COMPLETED
    assert calls == [("sorafenib angiosarcoma", 1, {"filter": "from_publication_date:2020-01-01"})]
    assert stored.query_params["lane"] == "clinical_response"
    assert stored.query_params["validation_gap"] is True


def test_validation_planning_blocks_when_evaluation_is_not_ready(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "validation-planning-blocked.sqlite3", seed=False)
    brief = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="Uncited VEGF hypothesis",
            disease_scope="canine hemangiosarcoma",
            source_key="pubmed",
            review_mode="deterministic_only",
            final_brief="Uncited synthesis.",
            result_payload={
                "topic": "Uncited VEGF hypothesis",
                "disease_scope": "canine hemangiosarcoma",
                "final_brief": "Uncited synthesis.",
                "citations": [],
                "perspective_reports": [],
                "ranked_hypotheses": [],
                "unresolved_questions": [],
                "evidence": {},
                "errors": [],
            },
        )
    )
    evaluation = HSAResearchService(repo).evaluate_research_brief(
        ResearchBriefEvaluationRequest(brief_id=brief.brief_id)
    )

    result = HSAResearchService(repo).plan_validation(
        ValidationPlanRequest(evaluation_id=evaluation.evaluation_id)
    )

    assert result.status == "blocked"
    assert result.readiness == "needs_better_synthesis"
    assert result.hypothesis_drafts == []
    assert result.tasks[0].task_type == "expert_review"
    assert any("not ready" in error for error in result.errors)


def test_mcp_validation_gap_source_pack_tool_dumps_json_safe_payload(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-validation-gap-source-pack.sqlite3", seed=False)
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="PK/PD evidence: canine sorafenib exposure",
            status="new",
            priority=25,
            reason="Need canine sorafenib pharmacokinetic exposure and dose evidence.",
            topic_tags=["validation_gap", "pkpd"],
            suggested_sources=["pubmed", "chembl"],
            metadata={
                "evidence_gap_resolver": {
                    "lane": "pkpd",
                    "gap_text": "Need canine sorafenib pharmacokinetic exposure and dose evidence.",
                    "task_type": "expert_review",
                    "validation_type": "expert_review",
                }
            },
        )
    )

    payload = mcp_server.build_validation_gap_source_pack_tool(
        lead_ids=[str(lead.lead_id)],
        source_keys=["pubmed", "chembl"],
        max_queries_per_lane=5,
        dry_run=True,
    )

    assert payload["agent_run_id"]
    assert payload["query_count"] == 2
    assert {query["source_key"] for query in payload["queries"]} == {"pubmed", "chembl"}


def test_mcp_validation_plan_tools_dump_json_safe_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-validation-plans.sqlite3", seed=False)
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)
    object_id = uuid4()
    chunk_id = uuid4()
    citation = ResearchBriefCitation(
        citation_id="C1",
        chunk_id=chunk_id,
        research_object_id=object_id,
        source_key="pubmed",
        quote="VEGF target validation is discussed.",
    )
    finding = ResearchBriefFinding(
        claim="VEGF target validation should be prioritized in canine HSA.",
        stance="opportunity",
        citations=["C1"],
        evidence_strength="medium",
        reasoning="The cited evidence supports a testable translational target path.",
    )
    result = ResearchBriefResult(
        brief_id=uuid4(),
        topic="VEGF target validation in canine HSA",
        disease_scope="canine hemangiosarcoma",
        final_brief="VEGF target validation is a candidate path [C1].",
        ranked_hypotheses=[finding],
        citations=[citation],
    )
    brief_record = repo.upsert_research_brief(
        ResearchBriefRecord(
            brief_id=result.brief_id,
            topic=result.topic,
            disease_scope=result.disease_scope,
            source_key="pubmed",
            final_brief=result.final_brief,
            result_payload=result.model_dump(mode="json"),
            citation_count=1,
            hypothesis_count=1,
        )
    )
    evaluation = repo.upsert_research_brief_evaluation(
        ResearchBriefEvaluationRecord(
            brief_id=brief_record.brief_id,
            topic=brief_record.topic,
            source_key=brief_record.source_key,
            overall_score=0.85,
            passes_quality_bar=True,
            readiness="ready_for_hypothesis_review",
            result_payload={"readiness": "ready_for_hypothesis_review"},
        )
    )

    planned = mcp_server.plan_validation_tool(evaluation_id=str(evaluation.evaluation_id))
    fetched = mcp_server.get_validation_plan_tool(planned["plan_id"])
    listed = mcp_server.list_validation_plans_tool(readiness="ready_for_expert_review")
    queued = mcp_server.queue_validation_requests_tool(planned["plan_id"], dry_run=False)
    queue_item = queued["queue_items"][0]
    queue_fetched = mcp_server.get_validation_request_queue_item_tool(queue_item["queue_item_id"])
    queue_listed = mcp_server.list_validation_request_queue_tool(status="needs_approval")
    approved = mcp_server.approve_validation_request_tool(
        queue_item["queue_item_id"],
        approved_by="unit-test",
    )
    dispatched = mcp_server.dispatch_validation_request_tool(
        queue_item["queue_item_id"],
        model_profile="deterministic_only",
    )

    assert planned["agent_run_id"]
    assert planned["tasks"]
    assert fetched["plan_id"] == planned["plan_id"]
    assert listed[0]["plan_id"] == planned["plan_id"]
    assert queued["queued_count"] == len(queued["queue_items"])
    assert queued["queued_count"] >= 1
    assert queue_fetched["queue_item_id"] == queue_item["queue_item_id"]
    assert queue_listed[0]["queue_item_id"] == queue_item["queue_item_id"]
    assert approved["status"] == "approved"
    assert dispatched["status"] == "completed"
    assert dispatched["last_run_id"]
    assert dispatched["metadata"]["validation_agent_result"]["decision"] in {"promote", "hold", "demote"}
