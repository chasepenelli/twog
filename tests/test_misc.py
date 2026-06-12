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

def test_hypothesis_promotion_report_blocks_citation_repair_and_promotes_clean(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "promotion.sqlite3", seed=False)
    service = HSAResearchService(repo)
    dirty_brief, _dirty_eval = _seed_evaluated_brief(repo, duplicate_count=2)
    clean_brief, clean_eval = _seed_evaluated_brief(repo, topic="Clean toceranib VEGFR hypothesis", duplicate_count=0)
    clean_idea = TherapyIdea(
        title="Toceranib/KDR validation lane",
        hypothesis="Toceranib monotherapy should be reviewed against KDR/VEGFR direct canine HSA evidence.",
        rationale="The evidence packet has enough cited VEGFR context to plan a recommend-only validation review.",
        candidate_therapies=["toceranib"],
        targets=["KDR", "VEGFR"],
        biomarkers=["VEGFR2"],
        evidence_refs=["C1", "C2"],
        evidence_strength="medium",
        risks=["direct monotherapy response data may be sparse"],
        next_experiments=["Review direct canine monotherapy outcomes."],
        priority_score=0.72,
    )
    repo.upsert_therapy_idea(
        TherapyIdeaRecord(
            idea=clean_idea,
            source_brief_id=clean_brief.brief_id,
            source_evaluation_id=clean_eval.evaluation_id,
            topic=clean_brief.topic,
            status="ready_for_promotion",
            score=0.72,
        )
    )

    dirty_report = service.build_hypothesis_promotion_report(
        HypothesisPromotionReportRequest(brief_id=dirty_brief.brief_id)
    )
    clean_report = service.build_hypothesis_promotion_report(
        HypothesisPromotionReportRequest(therapy_idea_id=clean_idea.idea_id)
    )

    assert dirty_report.candidates
    assert {candidate.promotion_state for candidate in dirty_report.candidates} == {"needs_citation_repair"}
    assert clean_report.candidates[0].promotion_state == "ready_for_validation_plan"
    assert clean_report.candidates[0].matched_tools
    assert clean_report.candidates[0].matched_tools[0].tool.runner_status == "recommend_only"


def test_hypothesis_promotion_allows_successful_dedupe_metadata(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "promotion-dedupe.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief, _evaluation = _seed_evaluated_brief(
        repo,
        topic="Successfully deduped canine HSA citation packet",
        duplicate_count=3,
        evaluation_weaknesses=[],
    )

    report = service.build_hypothesis_promotion_report(
        HypothesisPromotionReportRequest(brief_id=brief.brief_id)
    )

    assert report.candidates
    assert {candidate.promotion_state for candidate in report.candidates} == {"ready_for_committee"}
    assert all("citation_repair_required" not in candidate.blockers for candidate in report.candidates)


def test_evidence_ref_repair_contract_rejects_invalid_status():
    item = EvidenceRefRepairItem(
        ref="C1",
        normalized_ref="C1",
        status="resolved",
        source_context="validation_packet:test",
        evidence_path="packets[0].evidence_refs[0]",
    )
    payload = item.model_dump(mode="json")
    payload["status"] = "bad"

    with pytest.raises(ValidationError):
        EvidenceRefRepairItem.model_validate(payload)


def test_evidence_ref_repair_report_resolves_and_blocks_stale_refs(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "evidence-ref-repair.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief, evaluation = _seed_evaluated_brief(repo, topic="Sorafenib VEGFR repair packet", duplicate_count=0)
    idea = TherapyIdea(
        title="Sorafenib VEGFR repair lane",
        hypothesis="Sorafenib should be reviewed in VEGFR-positive canine HSA.",
        rationale="The committee cited direct and analog VEGFR evidence.",
        candidate_therapies=["sorafenib"],
        targets=["KDR", "PDGFRB"],
        biomarkers=["VEGFR2"],
        evidence_refs=["C1", "C3"],
        evidence_strength="medium",
        risks=["C4 citation traceability remains unresolved for one risk annotation."],
        next_experiments=["Repair stale citation refs before validation."],
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

    report = service.build_evidence_ref_repair_report(
        EvidenceRefRepairRequest(therapy_idea_id=idea.idea_id, limit=1)
    )

    assert isinstance(report, EvidenceRefRepairReport)
    assert report.packet_count == 1
    assert report.item_count >= 2
    assert any(
        item.normalized_ref == "C1" and item.status == "resolved" and item.matched_title
        for item in report.items
    )
    assert any(item.normalized_ref == "C3" and item.status == "stale" for item in report.items)
    assert any(item.normalized_ref == "C4" and item.status == "stale" for item in report.items)
    assert report.blocker_count >= 2
    assert any("C3 unresolved" in blocker for blocker in report.unresolved_blockers)
    assert report.suggested_queries


def test_model_policy_uses_sonnet_for_operations_and_opus_for_big_ideas(monkeypatch):
    monkeypatch.delenv("HSA_DEFAULT_OPENROUTER_MODEL", raising=False)
    monkeypatch.delenv("HSA_BIG_IDEA_OPENROUTER_MODEL", raising=False)
    monkeypatch.delenv("HSA_THERAPY_COMMITTEE_MODEL", raising=False)
    monkeypatch.delenv("HSA_VALIDATION_AGENT_MODEL", raising=False)
    monkeypatch.delenv("HSA_RESEARCH_PROGRAM_BOARD_MODEL", raising=False)
    assert model_policy.default_openrouter_model() == "~anthropic/claude-sonnet-latest"
    assert model_policy.big_idea_openrouter_model() == "~anthropic/claude-opus-latest"
    assert therapy_committee._select_models(TherapyCommitteeRequest(topic="therapy", review_mode="openrouter_required")) == [
        "~anthropic/claude-sonnet-latest"
    ]
    assert validation_agents._model_name("openrouter_required") == "~anthropic/claude-sonnet-latest"
    assert research_program_board._select_models(ResearchProgramReviewRequest(review_mode="openrouter_required")) == [
        "~anthropic/claude-opus-latest"
    ]


def test_evidence_fit_assessment_contracts_validate_allowed_values():
    assessment = EvidenceFitAssessment(
        fit="strong",
        target_safety_fit="strong",
        disease_directness_fit="partial",
        actionability="strong",
        transfer_risk="moderate",
        overall_fit="strong",
        matched_terms=["sorafenib", "sorafenib"],
        missing_terms=[],
        required_terms=["sorafenib", "canine/dog/veterinary"],
        matched_required_count=2,
        total_required_count=2,
        source_keys=["PubMed"],
        chunk_count=3,
        reason="Matched the critical follow-up concepts.",
    )

    assert assessment.fit == "strong"
    assert assessment.target_safety_fit == "strong"
    assert assessment.disease_directness_fit == "partial"
    assert assessment.actionability == "strong"
    assert assessment.transfer_risk == "moderate"
    assert assessment.overall_fit == "strong"
    assert assessment.matched_terms == ["sorafenib"]
    assert assessment.source_keys == ["pubmed"]

    with pytest.raises(ValidationError):
        EvidenceFitAssessment(fit="great")
    with pytest.raises(ValidationError):
        EvidenceFitAssessment(transfer_risk="maybe")
    with pytest.raises(ValidationError):
        EvidenceFitAssessment(matched_required_count=2, total_required_count=1)


def test_evidence_fit_splits_translational_target_support_from_direct_disease_fit(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "evidence-fit-rubric.sqlite3", seed=False)
    fetch_run_id = repo.create_fetch_run("europe_pmc", "pd1-translational-support")
    raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="europe_pmc",
            source_record_id="33110170",
            content_hash="pd1-translational-support",
            raw_payload={"pmid": "33110170"},
        ),
        fetch_run_id=fetch_run_id,
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type=ResearchObjectType.PUBLICATION,
            title="A pilot clinical study of the therapeutic antibody against canine PD-1",
            abstract=(
                "CA-4F12-E6 is an anti-canine PD-1 therapeutic antibody evaluated in dogs "
                "with advanced spontaneous cancers. The pilot study reports safety, toxicity, "
                "and immune checkpoint inhibitor tolerability."
            ),
            source_key="europe_pmc",
            dedupe_key="pmid:33110170",
            identifiers={"pmid": "33110170"},
            raw_record_id=raw_id,
        ),
        raw_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "CA-4F12-E6 anti-canine PD-1 therapy was evaluated for safety and toxicity "
                "in canine advanced spontaneous cancer patients as an immune checkpoint inhibitor."
            ),
            content_hash="pd1-translational-support-chunk",
        )
    )
    lead = ResearchLeadRecord(
        title="Safety signal: CA-4F12-E6 anti-PD-1 evidence for canine hemangiosarcoma",
        status="followup",
        topic_tags=["ca-4f12-e6", "anti-pd-1", "canine", "hemangiosarcoma", "safety"],
    )
    query = SourceQuery(
        source_key="europe_pmc",
        query_name="pd1-translational-support",
        query_text="anti-canine pd-1 ca-4f12-e6 safety",
        query_params={
            "required_terms": [
                "ca-4f12-e6",
                "anti-pd-1",
                "pd-1",
                "canine",
                "hemangiosarcoma",
                "safety",
                "checkpoint inhibitor",
            ]
        },
        track="validation_gap",
    )
    ingest_result = ValidationGapSourceIngestResult(
        dry_run=False,
        source_keys=["europe_pmc"],
        query_count=1,
        attempted_query_count=1,
        completed_query_count=1,
        raw_records=1,
        research_objects=1,
        document_chunks=1,
        source_queries=[query],
        results=[
            IngestionResult(
                source_key="europe_pmc",
                query_name="pd1-translational-support",
                query_text=query.query_text,
                fetch_run_id=fetch_run_id,
                raw_records=1,
                research_objects=1,
                document_chunks=1,
                status=RunStatus.COMPLETED,
            )
        ],
    )

    assessment = evidence_fit.assess_research_followup_ingest_evidence_fit(repo, lead, ingest_result)

    assert assessment.fit == "strong"
    assert assessment.overall_fit == "strong"
    assert assessment.target_safety_fit == "strong"
    assert assessment.disease_directness_fit == "partial"
    assert assessment.actionability == "strong"
    assert assessment.transfer_risk == "moderate"
    assert "hemangiosarcoma/angiosarcoma" in assessment.missing_terms


def test_search_claims_uses_typed_contracts(tmp_path):
    service = make_service(tmp_path)

    results = service.search_claims(
        ClaimSearchRequest(query="propranolol", species="canine", min_confidence=0.1)
    )

    assert results.total == 1
    assert "Propranolol" in results.results[0].statement


def test_propose_hypothesis_defaults_to_draft(tmp_path):
    service = make_service(tmp_path)

    draft = service.propose_hypothesis(
        HypothesisProposalRequest(objective="propranolol in canine HSA", candidate_name="propranolol")
    )

    assert draft.status == "draft"
    assert draft.hypothesis_id is None
    assert draft.supporting_claim_ids


def test_commit_hypothesis_requires_explicit_call(tmp_path):
    service = make_service(tmp_path)
    draft = service.propose_hypothesis(
        HypothesisProposalRequest(objective="angiogenesis in canine HSA", target_name="VEGFA")
    )

    committed = service.commit_hypothesis(
        CommitHypothesisRequest(draft=draft, approved_by="test", approval_note="unit test")
    )

    assert committed.status == "approved"
    assert committed.hypothesis_id is not None
    assert committed.metadata["approved_by"] == "test"


def test_run_boltz_returns_approval_gated_handle(tmp_path):
    service = make_service(tmp_path)

    handle = service.run_boltz(
        BoltzRunRequest(target_name="cKDR", ligand_name="test ligand", ligand_smiles="CCO")
    )

    assert handle.status == "needs_approval"
    assert service.get_run_status(handle.run_id) == handle


def test_command_center_report_summarizes_operational_state(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center.sqlite3", seed=False)
    service = HSAResearchService(repo)
    queued = service.queue_research_brief(
        ResearchBriefQueueRequest(
            topic="VEGF synthesis queue item",
            source_key="pubmed",
            priority=10,
        )
    )
    repo.update_research_brief_queue_item(queued.queue_item_id, status="failed", last_error="model timeout")
    repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Angiosarcoma trial lead",
            lead_type="linked_article",
            status="new",
            priority=25,
            suggested_sources=["clinicaltrials_gov"],
        )
    )
    repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evidence-light linked article",
            lead_type="institutional_article",
            status="followup",
            priority=40,
            source_key="x_linked_article",
        )
    )
    repo.create_agent_run(
        AgentRunRecord(
            agent_name="research_synthesis_editor_agent",
            status=RunStatus.FAILED,
            errors=["failed synthesis smoke"],
        )
    )
    source_health_report = {
        "failed_sources": ["chembl"],
        "triage_sources": ["pubmed"],
        "watch_sources": ["openalex"],
        "embedding_missing_sources": ["pubmed"],
        "full_text_blocking_sources": ["pmc_oa"],
        "sources": [],
        "summary": {},
    }

    report = service.build_command_center_report(
        CommandCenterRequest(
            source_health_report=source_health_report,
            queue_limit=10,
            lead_limit=10,
            agent_run_limit=10,
        )
    )
    recommendation_areas = {item.area for item in report.recommendations}

    assert report.summary["brief_queue_failed"] == 1
    assert report.summary["research_leads_actionable"] == 1
    assert report.summary["research_leads_followup"] == 1
    assert report.summary["recent_agent_failures"] == 1
    assert report.summary["source_health_failed"] == 1
    assert report.summary["blocking_recommendations"] >= 2
    assert report.research_brief_queue["status_counts"]["failed"] == 1
    assert report.research_leads["status_counts"]["new"] == 1
    assert report.research_leads["status_counts"]["followup"] == 1
    assert report.source_health == source_health_report
    assert recommendation_areas >= {"brief_queue", "research_leads", "source_health", "embeddings", "full_text", "agents"}


def test_command_center_web_dispatch_reports_blockers(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-blocked.sqlite3", seed=False)
    service = HSAResearchService(repo)
    queue_item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            source_key="pubmed",
            topic="Structure validation plan",
            task_type="protein_structure",
            title="Run structure validation",
            objective="Run structure validation when target context is complete.",
            rationale="The generated plan lacks a target identity.",
            validation_request=ValidationRequest(
                validation_type="boltz",
                objective="Run structure validation when target context is complete.",
                assay_context=ValidationAssayContext(
                    disease_context="canine hemangiosarcoma and human angiosarcoma",
                    species=["canine", "human"],
                ),
            ),
            quality_gates=["target_identity_required"],
            priority=30,
        )
    )
    command_center_web.approve_validation_request_payload(
        service,
        str(queue_item.queue_item_id),
        {"approved_by": "operator"},
    )

    dispatched = command_center_web.dispatch_validation_request_payload(
        service,
        str(queue_item.queue_item_id),
        {"model_profile": "deterministic_only"},
    )

    assert dispatched["item"]["status"] == "blocked"
    assert "target_name_required" in dispatched["item"]["dispatch_blockers"]
    assert "model_system_required" in dispatched["item"]["dispatch_blockers"]


def test_command_center_web_dispatch_preflight_requires_openrouter_key(monkeypatch, tmp_path):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-openrouter-preflight.sqlite3", seed=False)
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
            priority=25,
        )
    )
    command_center_web.approve_validation_request_payload(
        service,
        str(queue_item.queue_item_id),
        {"approved_by": "operator"},
    )

    readiness = command_center_web.runtime_payload()["validation_dispatch"]
    with pytest.raises(RuntimeError, match="OPENROUTER_API_KEY"):
        command_center_web.dispatch_validation_request_payload(service, str(queue_item.queue_item_id))
    stored = service.get_validation_request_queue_item(queue_item.queue_item_id)

    assert readiness["dispatch_ready"] is False
    assert stored is not None
    assert stored.status == "approved"
    assert stored.attempts == 0
    assert stored.last_error is None


def test_command_center_web_lists_idea_records(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-ideas.sqlite3", seed=False)
    service = HSAResearchService(repo)
    idea = TherapyIdea(
        title="KDR mutation-gated VEGFR inhibition",
        hypothesis="KDR-altered canine HSA may reveal a translational VEGFR inhibition lane.",
        rationale="Comparative oncology evidence supports testing the conserved angiogenic pathway.",
        candidate_therapies=["pazopanib"],
        targets=["KDR"],
        biomarkers=["VEGFR2"],
        evidence_refs=["C1", "C2"],
        evidence_strength="medium",
        risks=["PK bridge is incomplete."],
        next_experiments=["Run canine/human KDR sequence conservation review."],
        priority_score=0.82,
    )
    committee_result = TherapyCommitteeResult(
        agent_run_id=uuid4(),
        topic="KDR therapy ideas",
        disease_scope="canine hemangiosarcoma and human angiosarcoma",
        ranked_ideas=[idea],
        decision_summary="Prioritize KDR validation.",
    )
    repo.create_agent_run(
        AgentRunRecord(
            agent_run_id=committee_result.agent_run_id,
            agent_name="therapy_committee_chair_agent",
            model_profile="therapy_committee",
            status=RunStatus.COMPLETED,
            output_payload=committee_result.model_dump(mode="json"),
        )
    )
    repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            source_key="pubmed",
            topic="KDR validation",
            task_type="expert_review",
            title="Expert review: KDR mutation-gated VEGFR inhibition",
            objective="Review the therapy idea.",
            rationale="Human review required.",
            validation_request=ValidationRequest(validation_type="expert_review", objective="Review the therapy idea."),
            metadata={"idea_id": str(idea.idea_id), "idea_title": idea.title},
        )
    )
    plan = repo.upsert_validation_plan(
        ValidationPlanRecord(
            brief_id=uuid4(),
            topic="VEGFR PK bridge",
            source_key="pubmed",
            result_payload={
                "hypothesis_drafts": [
                    {
                        "hypothesis_id": str(uuid4()),
                        "title": "PK bridge hypothesis",
                        "hypothesis": "Canine and human PK gaps should gate pazopanib translation.",
                        "rationale": "Validation agents need explicit exposure context.",
                        "status": "draft",
                        "confidence": 0.64,
                    }
                ]
            },
        )
    )

    payload = command_center_web.list_ideas_payload(service)
    therapy_payload = command_center_web.list_ideas_payload(service, {"kind": ["therapy_idea"]})
    query_payload = command_center_web.list_ideas_payload(service, {"query": ["pazopanib"]})

    assert payload["total"] == 2
    assert payload["kind_counts"] == {"therapy_idea": 1, "validation_hypothesis": 1}
    assert payload["status_counts"] == {"draft": 1, "needs_approval": 1}
    assert therapy_payload["visible"] == 1
    assert therapy_payload["items"][0]["idea_id"] == str(idea.idea_id)
    assert therapy_payload["items"][0]["validation_status_counts"] == {"needs_approval": 1}
    assert query_payload["visible"] == 2
    assert any(item.get("plan_id") == str(plan.plan_id) for item in payload["items"])


def test_command_center_web_action_items_and_research_lead_status_updates(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-actions.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Potential VEGF follow-up article",
            lead_type="linked_article",
            status="new",
            priority=20,
            source_key="x_linked_article",
            reason="Agent flagged a durable article link for follow-up.",
            topic_tags=["vegf", "therapy"],
            suggested_sources=["pubmed"],
        )
    )

    action_items = command_center_web.build_action_items_payload(service, {"limit": ["10"]})
    listed = command_center_web.list_research_leads_payload(service, {"status": ["new,watching,followup"]})
    promoted = command_center_web.update_research_lead_status_payload(
        service,
        str(lead.lead_id),
        {"status": "watching", "operator": "operator"},
    )
    demoted = command_center_web.update_research_lead_status_payload(
        service,
        str(lead.lead_id),
        {"status": "dismissed", "operator": "operator"},
    )

    assert any(item["kind"] == "research_lead" for item in action_items["items"])
    assert listed["visible"] == 1
    assert listed["items"][0]["lead_id"] == str(lead.lead_id)
    assert promoted["item"]["status"] == "watching"
    assert promoted["item"]["metadata"]["command_center"]["operator"] == "operator"
    assert demoted["item"]["status"] == "dismissed"


def test_command_center_web_action_items_surface_latest_evaluator_findings(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-agent-findings.sqlite3", seed=False)
    service = HSAResearchService(repo)
    resolved_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="research_followup_resolver_agent",
            status="completed",
            summary={"blocked": True, "unresolved_lead_ids": 1},
        )
    )
    stale_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="claim_curator_agent",
            status="completed",
            summary={"claims_reviewed": 5},
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=resolved_run.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="bad",
            feedback="Resolver completed with no usable lead work.",
            followup_actions=["rerun dry-run with explicit lead IDs", "inspect skip reasons"],
            metadata={"confidence": 0.82},
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=stale_run.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="needs_followup",
            feedback="Older evaluator concern.",
            created_at=datetime.now(UTC) - timedelta(minutes=5),
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=stale_run.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="useful",
            feedback="Latest evaluator cleared this run.",
            created_at=datetime.now(UTC),
        )
    )

    action_items = command_center_web.build_action_items_payload(service, {"limit": ["10"]})
    evaluator_items = [item for item in action_items["items"] if item["kind"] == "agent_evaluator_finding"]

    assert len(evaluator_items) == 1
    assert evaluator_items[0]["severity"] == "blocking"
    assert evaluator_items[0]["title"] == "research_followup_resolver_agent: Bad"
    assert "Inspect Skip Reasons" in evaluator_items[0]["description"]
    assert evaluator_items[0]["actions"] == ["escalate_agent_finding"]
    assert evaluator_items[0]["metadata"]["agent_run_id"] == str(resolved_run.agent_run_id)
    assert evaluator_items[0]["metadata"]["confidence"] == 0.82


def test_pubchem_smiles_resolver_accepts_smiles_field_variants(monkeypatch):
    def fake_fetch(url, timeout_seconds=45):
        assert "CanonicalSMILES,IsomericSMILES,ConnectivitySMILES" in url
        return json.dumps({"PropertyTable": {"Properties": [{"CID": 1, "IsomericSMILES": "C[C@H](O)N"}]}})

    monkeypatch.setattr(service_module, "_fetch_text_url", fake_fetch)

    assert service_module._fetch_pubchem_canonical_smiles("test compound") == "C[C@H](O)N"


def test_research_hunt_task_executor_extracts_claims_and_closes_task(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-hunt-task-executor.sqlite3", seed=False)
    service = HSAResearchService(repo)
    research_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Propranolol and VEGF in canine hemangiosarcoma",
            abstract="Canine hemangiosarcoma studies discuss propranolol with VEGF and angiogenesis.",
            source_key="pubmed",
            dedupe_key="pubmed:propranolol-hsa",
        )
    )
    chunk = repo.upsert_document_chunk(DocumentChunk(
        research_object_id=research_object_id,
        chunk_index=0,
        section_label="abstract",
        text_content="Canine hemangiosarcoma studies discuss propranolol with VEGF and angiogenesis.",
        content_hash="pubmed:propranolol-hsa:chunk",
    ))
    task_id = uuid4()
    duplicate_task_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Hunt task: extract propranolol HSA claims",
            status="watching",
            suggested_sources=["pubmed"],
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "hunting",
                    "open_task_count": 1,
                    "best_signal": {
                        "score": 100,
                        "evidence_refs": [f"chunk:{chunk.id}", f"research_object:{research_object_id}"],
                        "evidence_fit": {
                            "fit": "strong",
                            "matched_required_count": 4,
                            "total_required_count": 4,
                        },
                    },
                    "tasks": [
                        {
                            "task_id": str(task_id),
                            "identity_key": "claim_extract:test",
                            "status": "open",
                            "task_type": "claim_extract",
                            "action": f"Run claim extraction on chunk:{chunk.id}.",
                            "priority": 20,
                            "source_keys": ["pubmed"],
                            "created_at": datetime.now(UTC).isoformat(),
                            "updated_at": datetime.now(UTC).isoformat(),
                        },
                        {
                            "task_id": str(duplicate_task_id),
                            "identity_key": "safety_extract:test",
                            "status": "open",
                            "task_type": "safety_extract",
                            "action": f"Review safety details from chunk:{chunk.id}.",
                            "priority": 21,
                            "source_keys": ["pubmed"],
                            "created_at": datetime.now(UTC).isoformat(),
                            "updated_at": datetime.now(UTC).isoformat(),
                        }
                    ],
                }
            },
        )
    )

    result = service.run_research_hunt_tasks(
        ResearchHuntTaskRunRequest(
            lead_ids=[lead.lead_id],
            task_types=["claim_extract", "safety_extract"],
            dry_run=False,
            operator="operator",
        )
    )
    updated = repo.get_research_lead(lead.lead_id)
    hunt_state = updated.metadata["research_hunt"]
    claims = repo.search_claims(
        ClaimSearchRequest(query="propranolol", species="canine", min_confidence=0.1, include_drafts=True)
    )

    assert result.agent_run_id is not None
    assert result.selected_count == 2
    assert result.completed_count == 2
    assert result.claim_chunks_seen == 1
    assert result.claims_written >= 1
    assert claims
    assert hunt_state["coverage_status"] == "supported"
    assert hunt_state["open_task_count"] == 0
    assert hunt_state["tasks"][0]["status"] == "completed"
    assert hunt_state["tasks"][1]["status"] == "completed"
    assert hunt_state["tasks"][0]["last_execution"]["claims_written"] >= 1
    assert hunt_state["tasks"][1]["last_execution"]["reused_claim_extraction_from_task_id"] == str(task_id)
    assert hunt_state["tasks"][1]["last_execution"]["claims_written"] == 0


def test_research_hunt_task_executor_dry_run_does_not_mutate():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    task_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Hunt task: dry run",
            status="watching",
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "hunting",
                    "open_task_count": 1,
                    "best_signal": {"score": 100, "evidence_refs": [], "evidence_fit": {"fit": "strong"}},
                    "tasks": [
                        {
                            "task_id": str(task_id),
                            "status": "open",
                            "task_type": "claim_extract",
                            "action": "Run claim extraction.",
                            "priority": 20,
                        }
                    ],
                }
            },
        )
    )

    result = service.run_research_hunt_tasks(
        ResearchHuntTaskRunRequest(
            lead_ids=[lead.lead_id],
            dry_run=True,
            operator="operator",
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.selected_count == 1
    assert result.skipped_count == 1
    assert updated.metadata["research_hunt"]["tasks"][0]["status"] == "open"


def test_research_hunt_task_executor_skips_broad_tasks_by_default():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    broad_task_id = uuid4()
    concrete_task_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Hunt task: default selection guard",
            status="watching",
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "hunting",
                    "open_task_count": 2,
                    "best_signal": {"score": 100, "evidence_refs": [], "evidence_fit": {"fit": "strong"}},
                    "tasks": [
                        {
                            "task_id": str(broad_task_id),
                            "identity_key": "broaden_query:human-angiosarcoma-vegfr2",
                            "status": "open",
                            "task_type": "broaden_query",
                            "action": "Search additional human angiosarcoma VEGFR-2 safety data.",
                            "priority": 40,
                        },
                        {
                            "task_id": str(concrete_task_id),
                            "identity_key": "claim_extract:pmid:26062540",
                            "status": "open",
                            "task_type": "claim_extract",
                            "action": "Run claim extraction on PMID 26062540.",
                            "priority": 20,
                        },
                    ],
                }
            },
        )
    )

    default_result = service.run_research_hunt_tasks(
        ResearchHuntTaskRunRequest(lead_ids=[lead.lead_id], dry_run=True, operator="operator")
    )
    explicit_result = service.run_research_hunt_tasks(
        ResearchHuntTaskRunRequest(
            lead_ids=[lead.lead_id],
            task_types=["broaden_query"],
            dry_run=True,
            operator="operator",
        )
    )

    assert default_result.selected_count == 1
    assert default_result.items[0]["task_id"] == str(concrete_task_id)
    assert explicit_result.selected_count == 1
    assert explicit_result.items[0]["task_id"] == str(broad_task_id)


def test_research_hunt_broad_parent_suppresses_broad_fanout_but_keeps_concrete_tasks():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    parent_task_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Hunt task: broad fanout guard",
            status="watching",
            suggested_sources=["pubmed"],
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "hunting",
                    "open_task_count": 1,
                    "best_signal": {
                        "score": 95,
                        "evidence_refs": ["pmid:26062540"],
                        "evidence_fit": {"fit": "strong", "matched_required_count": 4, "total_required_count": 4},
                    },
                    "tasks": [
                        {
                            "task_id": str(parent_task_id),
                            "identity_key": "broaden_query:human-angiosarcoma-vegfr2",
                            "status": "open",
                            "task_type": "broaden_query",
                            "action": "Search human angiosarcoma VEGFR-2 toceranib safety data.",
                            "priority": 40,
                        }
                    ],
                }
            },
        )
    )
    loop_result = ResearchFollowupLoopResult(lead_id=lead.lead_id, dry_run=False)

    hunt_state = service._update_research_hunt_state(
        loop_result,
        ResearchFollowupLoopRequest(
            lead_id=lead.lead_id,
            dry_run=False,
            metadata={
                "research_hunt_task_id": str(parent_task_id),
                "research_hunt_parent_task_type": "broaden_query",
            },
        ),
        verdict="needs_followup",
        followup_actions=[
            "Consider additional searches for human angiosarcoma VEGFR-2 toceranib toxicity data.",
            "Run claim extraction on PMID 26062540.",
        ],
    )

    assert loop_result.hunt_tasks_created == 1
    assert loop_result.hunt_tasks_suppressed == 1
    assert loop_result.hunt_tasks[0]["task_type"] == "claim_extract"
    assert hunt_state["suppressed_task_count"] == 1
    assert hunt_state["suppressed_tasks"][0]["suppression_reason"] == "broad_child_fanout_without_new_evidence"


def test_research_hunt_completed_tasks_block_duplicate_recreation():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    action = "Run claim extraction on PMID 26062540."
    identity_key = service_module._research_hunt_task_identity_key("claim_extract", action)
    completed_task_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Hunt task: completed duplicate guard",
            status="watching",
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "supported",
                    "open_task_count": 0,
                    "best_signal": {
                        "score": 95,
                        "evidence_refs": ["pmid:26062540"],
                        "evidence_fit": {"fit": "strong", "matched_required_count": 4, "total_required_count": 4},
                    },
                    "tasks": [
                        {
                            "task_id": str(completed_task_id),
                            "identity_key": identity_key,
                            "status": "completed",
                            "task_type": "claim_extract",
                            "action": action,
                            "priority": 20,
                        }
                    ],
                }
            },
        )
    )
    loop_result = ResearchFollowupLoopResult(lead_id=lead.lead_id, dry_run=False)

    hunt_state = service._update_research_hunt_state(
        loop_result,
        ResearchFollowupLoopRequest(lead_id=lead.lead_id, dry_run=False),
        verdict="needs_followup",
        followup_actions=[action],
    )

    assert loop_result.hunt_tasks_created == 0
    assert loop_result.hunt_tasks_suppressed == 1
    assert hunt_state["open_task_count"] == 0
    assert hunt_state["coverage_status"] == "supported"
    assert hunt_state["suppressed_tasks"][0]["suppression_reason"] == "duplicate_existing_task"


def test_research_hunt_queue_report_classifies_tasks_and_actions():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    now = datetime.now(UTC)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Hunt queue report lead",
            status="watching",
            priority=5,
            suggested_sources=["pubmed"],
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "hunting",
                    "best_signal": {
                        "score": 95,
                        "evidence_fit": {"fit": "strong", "matched_required_count": 4, "total_required_count": 4},
                    },
                    "tasks": [
                        {
                            "task_id": str(uuid4()),
                            "identity_key": "claim_extract:pmid:26062540",
                            "status": "open",
                            "task_type": "claim_extract",
                            "action": "Run claim extraction on PMID 26062540.",
                            "priority": 20,
                            "created_at": (now - timedelta(hours=1)).isoformat(),
                        },
                        {
                            "task_id": str(uuid4()),
                            "identity_key": "broaden_query:human-angiosarcoma-vegfr2",
                            "status": "open",
                            "task_type": "broaden_query",
                            "action": "Search additional human angiosarcoma VEGFR-2 toxicity data.",
                            "priority": 40,
                            "created_at": (now - timedelta(hours=96)).isoformat(),
                        },
                        {
                            "task_id": str(uuid4()),
                            "identity_key": "research_followup:monitor",
                            "status": "open",
                            "task_type": "research_followup",
                            "action": "Monitor future publications for this topic.",
                            "priority": 60,
                            "created_at": (now - timedelta(hours=2)).isoformat(),
                        },
                        {
                            "task_id": str(uuid4()),
                            "identity_key": "claim_extract:done",
                            "status": "completed",
                            "task_type": "claim_extract",
                            "action": "Run claim extraction on prior chunks.",
                            "priority": 20,
                            "created_at": (now - timedelta(hours=2)).isoformat(),
                        },
                    ],
                    "suppressed_tasks": [
                        {
                            "task_id": str(uuid4()),
                            "identity_key": "broaden_query:duplicate",
                            "status": "suppressed",
                            "task_type": "broaden_query",
                            "action": "Search additional human angiosarcoma VEGFR-2 toxicity data.",
                            "suppression_reason": "broad_family_already_seen",
                            "created_at": (now - timedelta(hours=1)).isoformat(),
                        }
                    ],
                }
            },
        )
    )

    report = service.build_research_hunt_queue_report(
        ResearchHuntQueueReportRequest(lead_ids=[lead.lead_id], stale_after_hours=72)
    )
    task_by_identity = {task.identity_key: task for task in report.tasks}

    assert report.lead_count == 1
    assert report.executable_task_count == 1
    assert report.broad_task_count == 1
    assert report.passive_task_count == 1
    assert report.stale_task_count == 1
    assert report.suppressed_task_count == 1
    assert report.hunting_count == 1
    assert report.leads[0].control_status == "hunting"
    assert report.leads[0].recommended_action == "run_concrete_hunt_tasks"
    assert task_by_identity["claim_extract:pmid:26062540"].runnable_by_default is True
    assert task_by_identity["broaden_query:human-angiosarcoma-vegfr2"].recommended_action == "suppress_or_archive"
    assert task_by_identity["research_followup:monitor"].task_class == "passive"
    assert task_by_identity["broaden_query:duplicate"].recommended_action == "keep_suppressed"


def test_research_hunt_queue_report_marks_supported_lead_ready_for_synthesis():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Supported lead with optional broad work",
            status="watching",
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "hunting",
                    "best_signal": {
                        "score": 90,
                        "evidence_fit": {"fit": "strong", "matched_required_count": 4, "total_required_count": 4},
                    },
                    "tasks": [
                        {
                            "task_id": str(uuid4()),
                            "identity_key": "broaden_query:optional",
                            "status": "open",
                            "task_type": "broaden_query",
                            "action": "Search optional human angiosarcoma VEGFR-2 data.",
                            "priority": 40,
                            "created_at": datetime.now(UTC).isoformat(),
                        }
                    ],
                }
            },
        )
    )

    report = service.build_research_hunt_queue_report(ResearchHuntQueueReportRequest(lead_ids=[lead.lead_id]))

    assert report.executable_task_count == 0
    assert report.ready_for_synthesis_count == 1
    assert report.leads[0].control_status == "ready_for_synthesis"
    assert report.leads[0].recommended_action == "queue_synthesis"


def test_queue_ready_research_hunt_synthesis_dry_run_does_not_mutate():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Supported VEGFR lead",
            status="watching",
            priority=15,
            source_key="pubmed",
            topic_tags=["VEGFR", "angiosarcoma"],
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "supported",
                    "best_signal": {
                        "verdict": "useful",
                        "summary": "Durable signal across canine and human evidence.",
                        "evidence_fit": {"fit": "strong", "matched_required_count": 4, "total_required_count": 4},
                    },
                    "tasks": [
                        {
                            "task_id": str(uuid4()),
                            "identity_key": "broaden_query:optional",
                            "status": "open",
                            "task_type": "broaden_query",
                            "action": "Search optional comparator evidence.",
                            "created_at": datetime.now(UTC).isoformat(),
                        }
                    ],
                }
            },
        )
    )

    result = service.queue_ready_research_hunt_synthesis(
        ResearchHuntSynthesisQueueRequest(lead_ids=[lead.lead_id], dry_run=True, review_models=["anthropic/claude-sonnet-4.6"])
    )
    updated_lead = repo.get_research_lead(lead.lead_id)

    assert result.dry_run is True
    assert result.candidate_count == 1
    assert result.queued_count == 0
    assert result.preexisting_count == 0
    assert len(result.queue_items) == 1
    assert result.queue_items[0].status == "queued"
    assert result.queue_items[0].source_key is None
    assert "all-sources" in result.queue_items[0].identity_key
    assert result.queue_items[0].metadata["research_hunt_synthesis_queue"]["lead_id"] == str(lead.lead_id)
    assert repo.list_research_brief_queue_items(limit=None) == []
    assert updated_lead is not None
    assert updated_lead.status == "watching"


def test_queue_ready_research_hunt_synthesis_apply_queues_and_transitions_lead():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Supported PI3K lead",
            status="watching",
            priority=12,
            source_key="europe_pmc",
            reason="The hunt found strong translational PI3K evidence.",
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "supported",
                    "best_signal": {
                        "verdict": "useful",
                        "summary": "Strong durable evidence.",
                        "evidence_fit": {"fit": "strong", "matched_required_count": 4, "total_required_count": 4},
                    },
                    "tasks": [],
                }
            },
        )
    )

    result = service.queue_ready_research_hunt_synthesis(
        ResearchHuntSynthesisQueueRequest(
            lead_ids=[lead.lead_id],
            dry_run=False,
            priority=40,
            review_models=["anthropic/claude-sonnet-4.6"],
        )
    )
    persisted_items = repo.list_research_brief_queue_items(limit=None)
    updated_lead = repo.get_research_lead(lead.lead_id)

    assert result.candidate_count == 1
    assert result.queued_count == 1
    assert result.preexisting_count == 0
    assert len(persisted_items) == 1
    assert persisted_items[0].source_key is None
    assert persisted_items[0].priority == 12
    assert persisted_items[0].review_mode == "openrouter_required"
    assert persisted_items[0].review_models == ["anthropic/claude-sonnet-4.6"]
    assert persisted_items[0].metadata["research_hunt_synthesis_queue"]["control_status"] == "ready_for_synthesis"
    assert updated_lead is not None
    assert updated_lead.status == "queued"
    assert updated_lead.metadata["research_hunt_synthesis_queue"]["queue_item_id"] == str(persisted_items[0].queue_item_id)


def test_queue_ready_research_hunt_synthesis_skips_non_ready_lead():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Lead still requiring claim extraction",
            status="watching",
            priority=10,
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "best_signal": {
                        "verdict": "useful",
                        "evidence_fit": {"fit": "strong", "matched_required_count": 4, "total_required_count": 4},
                    },
                    "tasks": [
                        {
                            "task_id": str(uuid4()),
                            "identity_key": "claim_extract:pending",
                            "status": "open",
                            "task_type": "claim_extract",
                            "action": "Extract claims from the new article.",
                            "created_at": datetime.now(UTC).isoformat(),
                        }
                    ],
                }
            },
        )
    )

    result = service.queue_ready_research_hunt_synthesis(
        ResearchHuntSynthesisQueueRequest(lead_ids=[lead.lead_id], dry_run=False)
    )

    assert result.candidate_count == 0
    assert result.queued_count == 0
    assert result.skipped_count == 1
    assert result.skipped[0]["control_status"] == "hunting"
    assert repo.list_research_brief_queue_items(limit=None) == []


def test_queue_ready_research_hunt_synthesis_dedupes_preexisting_queue_item():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Supported mTOR lead",
            status="watching",
            priority=20,
            source_key="pubmed",
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "supported",
                    "best_signal": {
                        "verdict": "useful",
                        "evidence_fit": {"fit": "strong", "matched_required_count": 4, "total_required_count": 4},
                    },
                    "tasks": [],
                }
            },
        )
    )
    existing = service.queue_research_brief(
        ResearchBriefQueueRequest(
            topic="Review research lead: Supported mTOR lead",
            source_key=None,
            priority=20,
            max_chunks_per_perspective=10,
            max_claims=20,
            max_chunk_chars=2200,
            review_models=["anthropic/claude-sonnet-4.6"],
        )
    )

    result = service.queue_ready_research_hunt_synthesis(
        ResearchHuntSynthesisQueueRequest(
            lead_ids=[lead.lead_id],
            dry_run=False,
            review_models=["anthropic/claude-sonnet-4.6"],
        )
    )
    updated_lead = repo.get_research_lead(lead.lead_id)

    assert result.candidate_count == 1
    assert result.queued_count == 0
    assert result.preexisting_count == 1
    assert len(repo.list_research_brief_queue_items(limit=None)) == 1
    assert result.queue_items[0].queue_item_id == existing.queue_item_id
    assert updated_lead is not None
    assert updated_lead.status == "queued"
    assert updated_lead.metadata["research_hunt_synthesis_queue"]["preexisting"] is True


def test_research_hunt_synthesis_doc_contracts_validate():
    document = ResearchHuntSynthesisDocument(
        lead_id=uuid4(),
        title="Plain-language synthesis handoff",
        control_status="ready_for_synthesis",
        recommended_action="queue_synthesis",
        markdown="# Plain-language synthesis handoff\n",
        plain_language_summary="The lead is ready for a synthesis brief.",
    )
    result = ResearchHuntSynthesisDocResult(documents=[document])
    request = ResearchHuntSynthesisDocRequest(source_keys=["Europe PMC", "europe_pmc"], operator="  ")

    assert result.documents[0].control_status == "ready_for_synthesis"
    assert request.source_keys == ["europe_pmc"]
    assert request.operator == "research_hunt_synthesis_doc"


def test_create_ready_research_hunt_synthesis_doc_persists_plain_language_artifact():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    obj = ResearchObject(
        object_type=ResearchObjectType.PUBLICATION,
        title="Toceranib maintenance after splenectomy in canine hemangiosarcoma",
        source_key="europe_pmc",
        identifiers={"pmid": "26062540", "doi": "10.1186/s12917-015-0446-1"},
    )
    repo.research_objects[obj.id] = obj
    chunk = DocumentChunk(
        research_object_id=obj.id,
        chunk_index=0,
        section_label="Abstract",
        text_content="Dogs receiving toceranib after splenectomy and chemotherapy were evaluated for outcomes.",
        content_hash="toceranib-chunk",
    )
    repo.replace_document_chunks(obj.id, [chunk])
    repo.claims.append(
        ClaimSearchResult(
            claim_id=uuid4(),
            statement="Toceranib outcome evidence should distinguish maintenance therapy from monotherapy.",
            claim_type=ClaimType.COMPOUND_AFFECTS_OUTCOME,
            direction=ClaimDirection.UNKNOWN,
            confidence=0.84,
            evidence_level=EvidenceLevel.CANINE_CLINICAL,
            species="canine",
            source_object_id=obj.id,
            source_title=obj.title,
        )
    )
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Find toceranib/VEGFR inhibitor monotherapy outcomes in canine splenic HSA",
            status="watching",
            source_key="europe_pmc",
            evidence_refs=[f"chunk:{chunk.id}", f"research_object:{obj.id}"],
            metadata={
                "research_hunt": {
                    "signal_status": "supported",
                    "coverage_status": "supported",
                    "best_signal": {
                        "score": 100,
                        "summary": "The hunt found a useful toceranib/VEGFR signal.",
                        "evidence_fit": {"fit": "strong", "matched_required_count": 4, "total_required_count": 4},
                    },
                    "tasks": [
                        {
                            "task_id": str(uuid4()),
                            "identity_key": "claim_extract:done",
                            "status": "completed",
                            "task_type": "claim_extract",
                            "action": "Extract claims from newly ingested chunks.",
                            "created_at": datetime.now(UTC).isoformat(),
                        }
                    ],
                }
            },
        )
    )

    result = service.create_ready_research_hunt_synthesis_docs(
        ResearchHuntSynthesisDocRequest(lead_ids=[lead.lead_id], dry_run=False, max_claims=4)
    )
    updated_lead = repo.get_research_lead(lead.lead_id)
    artifact = repo.get_artifact(result.documents[0].artifact_id)

    assert result.candidate_count == 1
    assert result.document_count == 1
    assert result.artifact_count == 1
    assert result.documents[0].claim_count == 1
    assert "## Plain-language summary" in result.documents[0].markdown
    assert "## What this does not prove yet" in result.documents[0].markdown
    assert "## Technical footnotes" in result.documents[0].markdown
    assert "maintenance therapy from monotherapy" in result.documents[0].markdown
    assert artifact is not None
    assert artifact.artifact_type == "research_hunt_synthesis_handoff_markdown"
    assert artifact.metadata["markdown"] == result.documents[0].markdown
    assert updated_lead is not None
    assert updated_lead.metadata["research_hunt_synthesis_doc"]["artifact_id"] == str(artifact.artifact_id)


def test_queue_ready_research_hunt_synthesis_creates_handoff_doc_by_default():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Supported VEGFR synthesis handoff",
            status="watching",
            priority=15,
            metadata={
                "research_hunt": {
                    "signal_status": "supported",
                    "coverage_status": "supported",
                    "best_signal": {
                        "summary": "VEGFR evidence is ready to brief.",
                        "evidence_fit": {"fit": "strong", "matched_required_count": 4, "total_required_count": 4},
                    },
                    "tasks": [],
                }
            },
        )
    )

    result = service.queue_ready_research_hunt_synthesis(
        ResearchHuntSynthesisQueueRequest(lead_ids=[lead.lead_id], dry_run=False, review_mode="deterministic_only")
    )
    artifacts = repo.list_artifacts(artifact_type="research_hunt_synthesis_handoff_markdown", limit=None)

    assert result.queued_count == 1
    assert result.handoff_document_count == 1
    assert result.handoff_artifact_count == 1
    assert len(artifacts) == 1
    assert artifacts[0].metadata["lead_id"] == str(lead.lead_id)


def test_research_hunt_queue_maintenance_dry_run_does_not_mutate():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    now = datetime.now(UTC)
    stale_task_id = uuid4()
    passive_task_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Hunt queue maintenance dry run",
            status="watching",
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "hunting",
                    "best_signal": {
                        "score": 90,
                        "evidence_fit": {"fit": "strong", "matched_required_count": 4, "total_required_count": 4},
                    },
                    "tasks": [
                        {
                            "task_id": str(stale_task_id),
                            "status": "open",
                            "task_type": "broaden_query",
                            "action": "Search stale PubMed source coverage.",
                            "priority": 40,
                            "created_at": (now - timedelta(hours=96)).isoformat(),
                        },
                        {
                            "task_id": str(passive_task_id),
                            "status": "open",
                            "task_type": "research_followup",
                            "action": "Monitor future publications for this lead.",
                            "priority": 60,
                            "created_at": now.isoformat(),
                        },
                    ],
                    "suppressed_tasks": [],
                }
            },
        )
    )

    result = service.maintain_research_hunt_queue(
        ResearchHuntQueueMaintenanceRequest(lead_ids=[lead.lead_id], dry_run=True, stale_after_hours=72)
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.candidate_count == 2
    assert result.suppressed_count == 0
    assert {item.suppression_reason for item in result.items} == {
        "stale_broad_or_passive",
        "passive_monitoring_note",
    }
    assert [task["status"] for task in updated.metadata["research_hunt"]["tasks"]] == ["open", "open"]
    assert updated.metadata["research_hunt"]["suppressed_tasks"] == []


def test_research_hunt_queue_maintenance_suppresses_safe_tasks_and_recalculates_state():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    now = datetime.now(UTC)
    keep_task_id = uuid4()
    duplicate_task_id = uuid4()
    stale_task_id = uuid4()
    passive_task_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Hunt queue maintenance apply",
            status="watching",
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "hunting",
                    "best_signal": {
                        "score": 95,
                        "evidence_fit": {"fit": "strong", "matched_required_count": 4, "total_required_count": 4},
                    },
                    "tasks": [
                        {
                            "task_id": str(keep_task_id),
                            "status": "open",
                            "task_type": "research_followup",
                            "action": "Review NCT02979899 trial design for human angiosarcoma VEGFR parallels.",
                            "priority": 60,
                            "created_at": now.isoformat(),
                        },
                        {
                            "task_id": str(duplicate_task_id),
                            "status": "open",
                            "task_type": "research_followup",
                            "action": "Examine NCT02979899 for human angiosarcoma VEGFR outcome data.",
                            "priority": 60,
                            "created_at": now.isoformat(),
                        },
                        {
                            "task_id": str(stale_task_id),
                            "status": "open",
                            "task_type": "broaden_query",
                            "action": "Search stale PubMed source coverage.",
                            "priority": 40,
                            "created_at": (now - timedelta(hours=96)).isoformat(),
                        },
                        {
                            "task_id": str(passive_task_id),
                            "status": "open",
                            "task_type": "research_followup",
                            "action": "Monitor future publications for this lead.",
                            "priority": 60,
                            "created_at": now.isoformat(),
                        },
                    ],
                    "suppressed_tasks": [],
                }
            },
        )
    )

    result = service.maintain_research_hunt_queue(
        ResearchHuntQueueMaintenanceRequest(lead_ids=[lead.lead_id], dry_run=False, stale_after_hours=72)
    )
    updated = repo.get_research_lead(lead.lead_id)
    hunt_state = updated.metadata["research_hunt"]
    remaining_task_ids = {task["task_id"] for task in hunt_state["tasks"]}
    suppression_reasons = {task["suppression_reason"] for task in hunt_state["suppressed_tasks"]}

    assert result.candidate_count == 3
    assert result.suppressed_count == 3
    assert result.updated_lead_count == 1
    assert remaining_task_ids == {str(keep_task_id)}
    assert suppression_reasons == {
        "duplicate_broad_family",
        "stale_broad_or_passive",
        "passive_monitoring_note",
    }
    assert hunt_state["open_task_count"] == 1
    assert hunt_state["control_status"] == "ready_for_synthesis"
    assert hunt_state["coverage_status"] == "supported"


def test_pubtator_external_ids_normalize_vocabulary_identifiers():
    assert entity_resolution._pubtator_external_ids(  # noqa: SLF001
        {"infons": {"type": "Gene", "identifier": "NCBI Gene:7157"}}
    ) == {
        "pubtator_identifier": "NCBI Gene:7157",
        "ncbi_gene_id": "7157",
    }
    assert entity_resolution._pubtator_external_ids(  # noqa: SLF001
        {"infons": {"type": "Species", "identifier": "9606"}}
    ) == {
        "pubtator_identifier": "9606",
        "taxonomy_id": "9606",
    }
    assert entity_resolution._pubtator_external_ids(  # noqa: SLF001
        {"infons": {"type": "Disease", "identifier": "MESH:D012878|OMIM:614420"}}
    ) == {
        "pubtator_identifier": "MESH:D012878|OMIM:614420",
        "mesh_id": "D012878",
        "omim_id": "614420",
    }
    assert entity_resolution._pubtator_external_ids(  # noqa: SLF001
        {"infons": {"type": "Chemical", "identifier": "CHEBI:16236"}}
    ) == {
        "pubtator_identifier": "CHEBI:16236",
        "chebi_id": "CHEBI:16236",
    }
    assert normalize_entity_key("compound", "water", {"chebi_id": "CHEBI:16236"}) == "chebi_id:chebi:16236"


def test_pubtator_resolution_uses_external_vocab_ids_for_stable_keys():
    obj = ResearchObject(
        object_type="publication",
        source_key="pubmed",
        identifiers={"pmid": "1"},
    )
    chunk = DocumentChunk(
        research_object_id=obj.id,
        chunk_index=0,
        section_label="abstract",
        text_content="TP53 and Homo sapiens were annotated by PubTator.",
        content_hash="pubtator-entity-chunk",
    )

    mentions = entity_resolution.resolve_chunk_with_pubtator_annotations(
        chunk,
        obj,
        [
            {"text": "TP53", "infons": {"type": "Gene", "identifier": "7157"}},
            {"text": "Homo sapiens", "infons": {"type": "Species", "identifier": "NCBI Taxon:9606"}},
        ],
    )

    assert {mention.normalized_key for mention in mentions} == {
        "ncbi_gene_id:7157",
        "taxonomy_id:9606",
    }
    assert any(mention.external_ids.get("ncbi_gene_id") == "7157" for mention in mentions)
    assert any(mention.external_ids.get("taxonomy_id") == "9606" for mention in mentions)


def test_structured_pipeline_can_report_empty_selection(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)

    report = run_structured_sources_pipeline(repo, source_keys=[], initialize=False)

    assert report["source_keys"] == []
    assert report["sources"] == []
    assert report["totals"] == {
        "raw_records": 0,
        "research_objects": 0,
        "document_chunks": 0,
        "entity_mentions": 0,
        "claims": 0,
    }
    assert report["errors"] == []


def test_openalex_v2_normalizer_produces_raw_and_research_object():
    harvester = OpenAlexHarvesterV2()

    record = harvester.normalize(
        {
            "id": "https://openalex.org/W123",
            "doi": "https://doi.org/10.1234/example",
            "title": "Canine hemangiosarcoma example",
            "publication_year": 2026,
            "publication_date": "2026-01-02",
            "abstract_inverted_index": {"Canine": [0], "HSA": [1]},
            "ids": {"pmid": "https://pubmed.ncbi.nlm.nih.gov/123"},
            "primary_location": {
                "landing_page_url": "https://doi.org/10.1234/example",
                "source": {"display_name": "Example Journal"},
            },
        }
    )

    assert record.raw_record.source_key == "openalex"
    assert record.research_object.title == "Canine hemangiosarcoma example"
    assert record.research_object.identifiers["doi"] == "10.1234/example"
    assert record.research_object.abstract == "Canine HSA"
    assert record.research_object.metadata["harvester"] == "v2"


def test_scholarly_query_policy_always_includes_human_angiosarcoma():
    queries = build_scholarly_source_queries()
    pubmed_query = next(query for query in queries if query.source_key == "pubmed" and query.query_name == "comparative_hsa_required")
    pmc_query = next(query for query in queries if query.source_key == "pmc_oa")
    unpaywall_query = next(query for query in queries if query.source_key == "unpaywall")

    assert queries
    assert all("angiosarcoma" in query.query_text.lower() for query in queries)
    assert all("hemangiosarcoma" in query.query_text.lower() for query in queries)
    assert "angiosarcoma[tiab]" in pubmed_query.query_text
    assert "[tiab]" in pmc_query.query_text
    assert "comparative oncology" not in pmc_query.query_text.lower()
    assert unpaywall_query.query_params == {"is_oa": True}
    assert unpaywall_query.active is True


def test_comparative_scope_does_not_match_angiosarcoma_inside_hemangiosarcoma():
    policy = infer_comparative_scope(
        "Canine hemangiosarcoma angiogenesis",
        "Canine hemangiosarcoma studies discuss VEGF.",
    )

    assert policy["matched_concepts"] == ["canine_hsa"]


def test_pubmed_v2_normalizer_handles_nested_xml_text():
    article = ET.fromstring(
        """
        <PubmedArticle>
          <MedlineCitation>
            <PMID>123</PMID>
            <Article>
              <ArticleTitle>Canine <i>hemangiosarcoma</i> and human angiosarcoma</ArticleTitle>
              <Abstract>
                <AbstractText>Human <b>angiosarcoma</b> analog evidence.</AbstractText>
              </Abstract>
              <Journal>
                <Title>Example Journal</Title>
                <JournalIssue><PubDate><Year>2026</Year></PubDate></JournalIssue>
              </Journal>
            </Article>
          </MedlineCitation>
        </PubmedArticle>
        """
    )

    record = PubMedHarvesterV2().normalize(article)

    assert record.research_object.title == "Canine hemangiosarcoma and human angiosarcoma"
    assert record.research_object.abstract == "Human angiosarcoma analog evidence."
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == [
        "canine_hsa",
        "human_angiosarcoma",
    ]


def test_pubmed_v2_normalizer_uses_only_current_article_identifiers():
    article = ET.fromstring(
        """
        <PubmedArticle>
          <MedlineCitation>
            <PMID>36548371</PMID>
            <Article>
              <ArticleTitle>Pilot safety evaluation for canine splenic hemangiosarcoma</ArticleTitle>
              <Abstract>
                <AbstractText>Canine hemangiosarcoma immunotherapy safety study.</AbstractText>
              </Abstract>
              <Journal>
                <Title>PLOS One</Title>
                <JournalIssue><PubDate><Year>2022</Year></PubDate></JournalIssue>
              </Journal>
            </Article>
          </MedlineCitation>
          <PubmedData>
            <ArticleIdList>
              <ArticleId IdType="pubmed">36548371</ArticleId>
              <ArticleId IdType="doi">10.1371/journal.pone.0279594</ArticleId>
              <ArticleId IdType="pmc">PMC9778498</ArticleId>
            </ArticleIdList>
            <ReferenceList>
              <Reference>
                <ArticleIdList>
                  <ArticleId IdType="pubmed">20977336</ArticleId>
                  <ArticleId IdType="doi">10.1208/s12249-010-9526-5</ArticleId>
                  <ArticleId IdType="pmc">PMC3011075</ArticleId>
                </ArticleIdList>
              </Reference>
            </ReferenceList>
          </PubmedData>
        </PubmedArticle>
        """
    )

    record = PubMedHarvesterV2().normalize(article)

    assert record.research_object.identifiers["pmid"] == "36548371"
    assert record.research_object.identifiers["doi"] == "10.1371/journal.pone.0279594"
    assert record.research_object.identifiers["pmcid"] == "PMC9778498"
    assert record.research_object.dedupe_key == "pmid:36548371"


def test_pubmed_identifier_repair_updates_payload_links_and_dedupe_key(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "pubmed-identifier-repair.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="36548371",
            content_hash="pubmed-36548371-wrong",
            raw_payload={"pmid": "36548371"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type=ResearchObjectType.PUBLICATION,
            title="Pilot safety evaluation of doxorubicin chemotherapy combined with immunotherapy",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            dedupe_key="doi:10.1208/s12249-010-9526-5",
            identifiers={
                "pmid": "36548371",
                "doi": "10.1208/s12249-010-9526-5",
                "pmcid": "PMC3011075",
                "source_id": "36548371",
            },
        ),
        raw_record_id,
    )

    result = pubmed_identifier_repair.repair_pubmed_identifier_metadata(
        repo,
        PubMedIdentifierRepairRequest(pmids=["36548371"], dry_run=False),
        identifier_fetcher=lambda pmids: {
            "36548371": {
                "pmid": "36548371",
                "doi": "10.1371/journal.pone.0279594",
                "pmcid": "PMC9778498",
                "source_id": "36548371",
            }
        },
    )

    repaired = repo.get_research_object(object_id)
    identifier_links = repo.conn.execute(
        "select identifier_type, identifier_value from identifier_links where object_id = ? order by identifier_type",
        (str(object_id),),
    ).fetchall()

    assert result.repaired == 1
    assert result.items[0].old_dedupe_key == "doi:10.1208/s12249-010-9526-5"
    assert result.items[0].new_dedupe_key == "pmid:36548371"
    assert repaired is not None
    assert repaired.dedupe_key == "pmid:36548371"
    assert repaired.identifiers["doi"] == "10.1371/journal.pone.0279594"
    assert repaired.identifiers["pmcid"] == "PMC9778498"
    assert ("doi", "10.1208/s12249-010-9526-5") not in [
        (row["identifier_type"], row["identifier_value"]) for row in identifier_links
    ]
    assert ("doi", "10.1371/journal.pone.0279594") in [
        (row["identifier_type"], row["identifier_value"]) for row in identifier_links
    ]


def test_pubmed_identifier_repair_dry_run_does_not_update_object(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "pubmed-identifier-repair-dry-run.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type=ResearchObjectType.PUBLICATION,
            title="PubMed object with stale identifiers",
            source_key="pubmed",
            dedupe_key="doi:10.1208/s12249-010-9526-5",
            identifiers={
                "pmid": "36548371",
                "doi": "10.1208/s12249-010-9526-5",
                "pmcid": "PMC3011075",
                "source_id": "36548371",
            },
        )
    )

    result = pubmed_identifier_repair.repair_pubmed_identifier_metadata(
        repo,
        PubMedIdentifierRepairRequest(pmids=["36548371"]),
        identifier_fetcher=lambda pmids: {
            "36548371": {
                "pmid": "36548371",
                "doi": "10.1371/journal.pone.0279594",
                "pmcid": "PMC9778498",
                "source_id": "36548371",
            }
        },
    )
    unchanged = repo.get_research_object(object_id)

    assert result.would_repair == 1
    assert unchanged is not None
    assert unchanged.dedupe_key == "doi:10.1208/s12249-010-9526-5"
    assert unchanged.identifiers["doi"] == "10.1208/s12249-010-9526-5"


def test_europe_pmc_v2_normalizer_cleans_escaped_title_markup():
    record = EuropePMCHarvesterV2().normalize(
        {
            "id": "x1",
            "title": "Primary &lt;i&gt;Vaginal&lt;/i&gt; Angiosarcoma",
            "abstractText": "Human angiosarcoma case report.",
            "pubYear": "2026",
        }
    )

    assert record.research_object.title == "Primary Vaginal Angiosarcoma"
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["human_angiosarcoma"]


def test_europe_pmc_v2_fetch_keeps_body_only_policy_match(monkeypatch):
    def fake_get_json(url, params):
        assert url.endswith("/search")
        assert params["resultType"] == "core"
        return {
            "resultList": {
                "result": [
                    {
                        "id": "PMC123",
                        "pmcid": "PMC123",
                        "title": "Endothelial biology review",
                        "abstractText": "Sparse abstract.",
                        "isOpenAccess": "Y",
                    }
                ]
            }
        }

    def fake_get_text(url, params, **kwargs):
        assert url == "https://www.ebi.ac.uk/europepmc/webservices/rest/PMC123/fullTextXML"
        assert params == {}
        assert kwargs["timeout_seconds"] == harvesters_v2.FULL_TEXT_REQUEST_TIMEOUT_SECONDS
        assert kwargs["attempts"] == harvesters_v2.FULL_TEXT_REQUEST_ATTEMPTS
        return """
        <article xmlns="http://jats.nlm.nih.gov">
          <front><article-meta><article-id pub-id-type="pmc">PMC123</article-id></article-meta></front>
          <body><p>Canine hemangiosarcoma full text mentions VEGF and propranolol.</p></body>
        </article>
        """

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)
    monkeypatch.setattr(harvesters_v2, "_get_text", fake_get_text)

    records = EuropePMCHarvesterV2().fetch("hemangiosarcoma", limit=1, open_access=True, require_policy_match=True)

    assert len(records) == 1
    assert records[0].research_object.metadata["body_only_match"] is True
    assert records[0].research_object.metadata["body_ingestion_policy"]["matched_concepts"] == ["canine_hsa"]


def test_europe_pmc_v2_fetch_applies_publication_date_range(monkeypatch):
    def fake_get_json(url, params):
        assert url.endswith("/search")
        assert "FIRST_PDATE:[2026-04-27 TO 2026-04-27]" in params["query"]
        return {"resultList": {"result": []}}

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    records = EuropePMCHarvesterV2().fetch(
        "hemangiosarcoma",
        limit=1,
        open_access=True,
        fetch_full_text=False,
        require_policy_match=False,
        published_after="2026-04-27",
        published_before="2026-04-27",
    )

    assert records == []


def test_pmc_oa_v2_normalizer_splits_jats_body_sections():
    record = PMCOAHarvesterV2().normalize(
        """
        <article xmlns="http://jats.nlm.nih.gov">
          <front>
            <article-meta>
              <article-id pub-id-type="pmc">PMC3</article-id>
              <title-group><article-title>Canine hemangiosarcoma</article-title></title-group>
              <abstract><p>Short abstract.</p></abstract>
            </article-meta>
          </front>
          <body>
            <sec>
              <title>Materials and Methods</title>
              <p>Cells were profiled with a canine hemangiosarcoma assay.</p>
            </sec>
            <sec>
              <title>Results</title>
              <p>Human angiosarcoma VEGF signaling was compared.</p>
            </sec>
            <sec>
              <title>References</title>
              <p>Reference text should not become a body chunk.</p>
            </sec>
          </body>
        </article>
        """,
        oa_metadata={"oa_license": "CC BY"},
    )

    sections = PMCOAHarvesterV2().chunk_text_sections(record)

    assert [section_label for section_label, _text in sections] == [
        "title_abstract",
        "full_text:methods",
        "full_text:results",
    ]
    assert "canine hemangiosarcoma assay" in sections[1][1]
    assert "VEGF signaling" in sections[2][1]
    assert all("Reference text" not in text for _section_label, text in sections)


def test_pmc_oa_v2_normalizer_preserves_nested_jats_sections():
    record = PMCOAHarvesterV2().normalize(
        """
        <article xmlns="http://jats.nlm.nih.gov">
          <front>
            <article-meta>
              <article-id pub-id-type="pmc">PMC33</article-id>
              <title-group><article-title>Canine hemangiosarcoma nested sections</article-title></title-group>
              <abstract><p>Short abstract.</p></abstract>
            </article-meta>
          </front>
          <body>
            <sec>
              <title>Results</title>
              <p>Human angiosarcoma comparison was summarized.</p>
              <sec>
                <title>VEGF Signaling</title>
                <p>Nested VEGF signaling evidence should become its own searchable section.</p>
              </sec>
            </sec>
            <sec>
              <title>References</title>
              <sec>
                <title>Ignored Nested Reference</title>
                <p>Nested reference text should not become a body chunk.</p>
              </sec>
            </sec>
          </body>
        </article>
        """,
        oa_metadata={"oa_license": "CC BY"},
    )

    sections = PMCOAHarvesterV2().chunk_text_sections(record)

    assert [section_label for section_label, _text in sections] == [
        "title_abstract",
        "full_text:results",
        "full_text:vegf_signaling",
    ]
    assert "Human angiosarcoma comparison" in sections[1][1]
    assert "Nested VEGF signaling evidence" not in sections[1][1]
    assert "Nested VEGF signaling evidence" in sections[2][1]
    assert all("Nested reference text" not in text for _section_label, text in sections)


def test_pmc_oa_v2_fetch_keeps_body_only_policy_match(monkeypatch):
    xml = """
    <article xmlns="http://jats.nlm.nih.gov">
      <front>
        <article-meta>
          <article-id pub-id-type="pmc">PMC123456</article-id>
          <title-group><article-title>Open access endothelial biology review</article-title></title-group>
          <permissions>
            <license license-type="open-access">
              <license-p>Creative Commons Attribution License</license-p>
            </license>
          </permissions>
        </article-meta>
      </front>
      <body><p>Human angiosarcoma full text mentions VEGF and propranolol.</p></body>
    </article>
    """

    def fake_get_json(url, params):
        assert url.endswith("/esearch.fcgi")
        assert params["db"] == "pmc"
        return {"esearchresult": {"idlist": ["123456"]}}

    def fake_get_text(url, params, **kwargs):
        assert url == "https://pmc.ncbi.nlm.nih.gov/api/oai/v1/mh/"
        assert params["identifier"] == "oai:pubmedcentral.nih.gov:123456"
        assert kwargs["timeout_seconds"] == harvesters_v2.FULL_TEXT_REQUEST_TIMEOUT_SECONDS
        assert kwargs["attempts"] == harvesters_v2.FULL_TEXT_REQUEST_ATTEMPTS
        return xml

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)
    monkeypatch.setattr(harvesters_v2, "_get_text", fake_get_text)
    monkeypatch.setattr(
        harvesters_v2,
        "_pmc_oa_metadata",
        lambda pmcid, **kwargs: {"oa_license": "CC BY", "retracted": "no"},
    )
    monkeypatch.setattr(harvesters_v2.time, "sleep", lambda _seconds: None)

    records = PMCOAHarvesterV2().fetch("hemangiosarcoma", limit=1, require_policy_match=True)

    assert len(records) == 1
    assert records[0].research_object.metadata["body_only_match"] is True
    assert records[0].research_object.metadata["body_ingestion_policy"]["matched_concepts"] == [
        "human_angiosarcoma"
    ]


def test_pmc_oa_v2_fetch_applies_publication_date_params(monkeypatch):
    def fake_get_json(url, params):
        assert url.endswith("/esearch.fcgi")
        assert params["datetype"] == "pdat"
        assert params["mindate"] == "2026/04/27"
        assert params["maxdate"] == "2026/04/27"
        return {"esearchresult": {"idlist": []}}

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    records = PMCOAHarvesterV2().fetch(
        "hemangiosarcoma",
        limit=1,
        published_after="2026-04-27",
        published_before="2026-04-27",
    )

    assert records == []


def test_hosted_literature_smoke_includes_pmc_oa():
    assert "pmc_oa" in LITERATURE_CLINICAL_SMOKE_KEYS
    assert "pmc_oa" in HOSTED_API_REPORT_KEYS


def test_twitterapi_io_provider_requires_key(monkeypatch):
    monkeypatch.delenv("TWITTERAPI_IO_KEY", raising=False)

    with pytest.raises(ValueError):
        x_topic_monitor.TwitterApiIoProvider().search(
            x_topic_monitor.XTopicRequest(query='"canine hemangiosarcoma"')
        )


def test_clinicaltrials_gov_v2_normalizer_extracts_trial_fields():
    study = {
        "protocolSection": {
            "identificationModule": {
                "nctId": "NCT00000001",
                "orgStudyIdInfo": {"id": "ORG-1"},
                "briefTitle": "Pazopanib in Angiosarcoma",
                "officialTitle": "Pazopanib for Patients With Advanced Human Angiosarcoma",
            },
            "statusModule": {
                "overallStatus": "RECRUITING",
                "startDateStruct": {"date": "2026-01-01", "type": "ACTUAL"},
                "studyFirstPostDateStruct": {"date": "2026-02-01", "type": "ACTUAL"},
                "completionDateStruct": {"date": "2028-01", "type": "ESTIMATED"},
            },
            "descriptionModule": {
                "briefSummary": "This study tests pazopanib in human angiosarcoma.",
                "detailedDescription": "Participants receive pazopanib and undergo response assessment.",
            },
            "conditionsModule": {"conditions": ["Angiosarcoma", "Vascular Sarcoma"]},
            "designModule": {
                "studyType": "INTERVENTIONAL",
                "phases": ["PHASE2"],
                "enrollmentInfo": {"count": 42, "type": "ESTIMATED"},
            },
            "armsInterventionsModule": {
                "interventions": [
                    {"type": "DRUG", "name": "Pazopanib"},
                    {"type": "DRUG", "name": "Paclitaxel"},
                ]
            },
            "outcomesModule": {
                "primaryOutcomes": [{"measure": "Objective response rate"}],
                "secondaryOutcomes": [{"measure": "Progression-free survival"}],
            },
            "eligibilityModule": {
                "eligibilityCriteria": "Inclusion: measurable angiosarcoma.",
                "minimumAge": "18 Years",
                "sex": "ALL",
                "stdAges": ["ADULT", "OLDER_ADULT"],
            },
            "sponsorCollaboratorsModule": {
                "leadSponsor": {"name": "Example Cancer Center"},
                "collaborators": [{"name": "National Cancer Institute"}],
            },
            "contactsLocationsModule": {
                "locations": [
                    {
                        "facility": "Example Hospital",
                        "city": "Denver",
                        "state": "Colorado",
                        "country": "United States",
                        "status": "RECRUITING",
                    }
                ]
            },
        }
    }

    harvester = ClinicalTrialsGovHarvesterV2()
    record = harvester.normalize(study)

    assert record.raw_record.source_key == "clinicaltrials_gov"
    assert record.research_object.object_type == "clinical_trial"
    assert record.research_object.identifiers["nct_id"] == "NCT00000001"
    assert record.research_object.canonical_url == "https://clinicaltrials.gov/study/NCT00000001"
    assert record.research_object.metadata["overall_status"] == "RECRUITING"
    assert record.research_object.metadata["interventions"] == ["Pazopanib", "Paclitaxel"]
    assert record.research_object.metadata["primary_outcomes"] == ["Objective response rate"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == [
        "human_angiosarcoma",
        "vascular_sarcoma_analog",
    ]
    assert "Inclusion: measurable angiosarcoma." in harvester.text_for_chunking(record)


def test_avma_vctr_v2_normalizer_extracts_study_card_metadata():
    harvester = AVMAVCTRHarvesterV2()
    record = harvester.normalize(_avma_vctr_study_card(), source_query="hemangiosarcoma", source_total=27)

    assert record.raw_record.source_key == "avma_vctr"
    assert record.raw_record.source_record_id == "VCT16000189"
    assert record.research_object.object_type == "veterinary_trial"
    assert record.research_object.identifiers["vct_code"] == "VCT16000189"
    assert record.research_object.identifiers["avma_study_id"] == "12345"
    assert record.research_object.dedupe_key == "vct_code:vct16000189"
    assert record.research_object.canonical_url == "https://veterinaryclinicaltrials.org/s/antibody-therapy-hsa/"
    assert record.research_object.title == "Safety and efficacy of antibody therapy for dogs with splenic hemangiosarcoma"
    assert record.research_object.abstract == (
        "Dogs with splenic hemangiosarcoma receive antibody therapy after splenectomy."
    )
    assert record.research_object.metadata["status"] == "Recruiting"
    assert record.research_object.metadata["species"] == ["Dogs"]
    assert record.research_object.metadata["conditions"] == ["Hemangiosarcoma"]
    assert record.research_object.metadata["intervention_types"] == ["Biologic"]
    assert record.research_object.metadata["financial_incentives"] == ["Study costs covered"]
    assert record.research_object.metadata["visible_search_categories"]["Species"] == ["Dogs"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["canine_hsa"]
    assert record.research_object.metadata["source_total"] == 27
    assert "VCT16000189" in harvester.text_for_chunking(record)
    assert "Biologic" in harvester.text_for_chunking(record)


def test_avma_vctr_v2_normalizer_does_not_treat_late_reference_as_primary_hsa():
    study = _avma_vctr_study_card() | {
        "name": "The effect of a novel mushroom formula on canine oral malignant melanoma",
        "description": (
            "Oral malignant melanoma is an aggressive cancer in dogs. This study evaluates a mushroom "
            "supplement for melanoma. " + ("General oncology background. " * 40)
            + "A cited hemangiosarcoma study informed the dose."
        ),
        "tagline": "Evaluation of Medicinal Mushroom Supplementation in Canine Oral Malignant Melanoma",
        "visible_sc_items": [
            {"id": "species-dog", "tag_path": "/species/dogs", "label": "Canine", "parent_label": "Species"},
            {
                "id": "oncology-melanoma",
                "tag_path": "/primary-field/oncology/melanoma",
                "label": "Melanoma",
                "parent_label": "Oncology",
            },
        ],
        "vct_code": "VCT-MELANOMA",
    }

    record = AVMAVCTRHarvesterV2().normalize(study, source_query="hemangiosarcoma")

    assert record.research_object.metadata["conditions"] == ["Melanoma"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == []


def test_avma_vctr_v2_fetch_uses_public_search_endpoint(monkeypatch):
    captured = {}

    def fake_get_json(url, params):
        captured["url"] = url
        captured["params"] = params
        return {"total": 1, "studies": [_avma_vctr_study_card()]}

    monkeypatch.setattr("hsa_research.ingestion_bridge.harvesters_v2._get_json", fake_get_json)

    records = AVMAVCTRHarvesterV2().fetch("hemangiosarcoma", limit=5)

    assert len(records) == 1
    assert captured["url"] == "https://veterinaryclinicaltrials.org/avma/studies/search/json/"
    assert captured["params"]["search"] == "hemangiosarcoma"
    assert captured["params"]["skip"] == 0
    assert captured["params"]["take"] == 5
    assert captured["params"]["sort_by"] == "score"
    assert captured["params"]["skip_similar_studies"] == "true"
    assert captured["params"]["extra_aggregations"] == "[]"


def test_icdc_v2_normalizer_extracts_canine_case_metadata():
    case = {
        "case_id": "TCL01-DEN-HSA",
        "study_code": "TCL01",
        "study_type": "Genomics",
        "cohort": "Cell line",
        "breed": "Golden Retriever",
        "diagnosis": "Hemangiosarcoma",
        "disease_site": "Kidney",
        "primary_disease_site": "Kidney",
        "stage_of_disease": "Unknown",
        "age": 11.0,
        "sex": "Male",
        "response_to_treatment": "Not Applicable",
        "files": ["file-1", "file-2"],
        "treatment_data": "Yes",
        "follow_up_data": "No",
        "pathology_report": "No",
    }
    study = {
        "clinical_study_designation": "TCL01",
        "clinical_study_name": "Whole exome sequencing analysis of canine cancer cell lines",
        "clinical_study_description": "This study analyzes canine cancer cell lines including hemangiosarcoma.",
        "clinical_study_type": "Genomics",
        "accession_id": "000008",
        "dates_of_conduct": "2017-2019",
        "study_disposition": "Unrestricted",
    }

    harvester = ICDCHarvesterV2()
    record = harvester.normalize(case, study)

    assert record.raw_record.source_key == "icdc"
    assert record.research_object.object_type == "dataset"
    assert record.research_object.identifiers["icdc_case_id"] == "TCL01-DEN-HSA"
    assert record.research_object.identifiers["study_code"] == "TCL01"
    assert record.research_object.metadata["breed"] == "Golden Retriever"
    assert record.research_object.metadata["file_count"] == 2
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["canine_hsa"]
    assert "Diagnosis: Hemangiosarcoma" in harvester.text_for_chunking(record)


def test_geo_v2_normalizer_extracts_dataset_metadata():
    item = {
        "uid": "200310480",
        "accession": "GSE310480",
        "title": "MicroRNA biomarkers for canine visceral hemangiosarcoma",
        "summary": "Canine visceral hemangiosarcoma samples identify miRNA biomarkers.",
        "gse": "310480",
        "taxon": "Canis lupus familiaris",
        "entrytype": "GSE",
        "gdstype": "Non-coding RNA profiling by high throughput sequencing",
        "pdat": "2026/04/08",
        "suppfile": "TXT, XLSX",
        "samples": [{"accession": "GSM1", "title": "Cancer spleen 1"}],
        "n_samples": 36,
        "pubmedids": ["41924723"],
        "ftplink": "ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE310nnn/GSE310480/",
        "bioproject": "PRJNA1366394",
    }

    harvester = GEOHarvesterV2()
    record = harvester.normalize(item)

    assert record.raw_record.source_key == "geo"
    assert record.research_object.object_type == "dataset"
    assert record.research_object.identifiers["geo_accession"] == "GSE310480"
    assert record.research_object.identifiers["bioproject"] == "PRJNA1366394"
    assert record.research_object.metadata["sample_accessions"] == ["GSM1"]
    assert record.research_object.metadata["supplementary_file_types"] == ["TXT", "XLSX"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["canine_hsa"]
    assert "Cancer spleen 1" in harvester.text_for_chunking(record)


def test_geo_v2_exact_identifier_filters_substitutions(monkeypatch):
    calls = []

    def fake_get_json(url, params):
        calls.append((url, params))
        if "esearch.fcgi" in url:
            return {"esearchresult": {"idlist": ["1", "2"]}}
        return {
            "result": {
                "uids": ["1", "2"],
                "1": {
                    "uid": "1",
                    "accession": "GSE111111",
                    "title": "Nearby canine hemangiosarcoma dataset",
                    "summary": "Canine hemangiosarcoma RNA-seq.",
                },
                "2": {
                    "uid": "2",
                    "accession": "GSE222222",
                    "title": "Requested canine hemangiosarcoma dataset",
                    "summary": "Canine hemangiosarcoma RNA-seq.",
                },
            }
        }

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    records = GEOHarvesterV2().fetch(
        "GSE222222",
        limit=5,
        exact_identifier_type="geo",
        exact_identifier="GSE222222",
        require_policy_match=False,
    )

    assert [record.research_object.identifiers["geo_accession"] for record in records] == ["GSE222222"]
    assert calls[0][1]["term"] == "GSE222222"


def test_sra_v2_normalizer_extracts_run_metadata():
    item = {
        "uid": "42394755",
        "expxml": """
          <Summary>
            <Title>NM-BS23039 Canine hemangiosarcoma primary cell</Title>
            <Platform instrument_model="Illumina NovaSeq 6000">ILLUMINA</Platform>
            <Statistics total_runs="1" total_spots="26421116" total_bases="7926334800" total_size="2407693008"/>
          </Summary>
          <Submitter acc="SRA2311501" center_name="Tokyo University of Agriculture and Technology"/>
          <Experiment acc="SRX31723477" name="NM-BS23039 Canine hemangiosarcoma primary cell"/>
          <Study acc="SRP660537" name="Canine hemangiosarcoma primary cell RNA sequencing"/>
          <Organism taxid="9615" ScientificName="Canis lupus familiaris"/>
          <Sample acc="SRS27692090"/>
          <Library_descriptor>
            <LIBRARY_NAME>NM-BS23039_L1_1</LIBRARY_NAME>
            <LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY>
            <LIBRARY_SOURCE>TRANSCRIPTOMIC</LIBRARY_SOURCE>
            <LIBRARY_SELECTION>RANDOM</LIBRARY_SELECTION>
            <LIBRARY_LAYOUT><PAIRED/></LIBRARY_LAYOUT>
          </Library_descriptor>
          <Bioproject>PRJNA1399620</Bioproject>
          <Biosample>SAMN54501165</Biosample>
        """,
        "runs": '<Run acc="SRR36719144" total_spots="26421116" total_bases="7926334800" is_public="true"/>',
        "createdate": "2026/03/31",
        "updatedate": "2026/01/07",
    }

    harvester = SRAHarvesterV2()
    record = harvester.normalize(item)

    assert record.raw_record.source_key == "sra"
    assert record.research_object.object_type == "dataset"
    assert record.research_object.identifiers["sra_experiment"] == "SRX31723477"
    assert record.research_object.identifiers["sra_run"] == "SRR36719144"
    assert record.research_object.identifiers["bioproject"] == "PRJNA1399620"
    assert record.research_object.metadata["library_strategy"] == "RNA-Seq"
    assert record.research_object.metadata["library_layout"] == "PAIRED"
    assert record.research_object.metadata["statistics"]["total_spots"] == "26421116"
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["canine_hsa"]
    assert "SRR36719144" in harvester.text_for_chunking(record)


def test_sra_v2_exact_identifier_matches_runs_and_experiments(monkeypatch):
    def fake_get_json(url, params):
        if "esearch.fcgi" in url:
            return {"esearchresult": {"idlist": ["1"]}}
        return {
            "result": {
                "uids": ["1"],
                "1": {
                    "uid": "1",
                    "expxml": """
                      <Summary><Title>Canine hemangiosarcoma RNA-seq</Title></Summary>
                      <Experiment acc="SRX31723477" name="Canine hemangiosarcoma"/>
                      <Study acc="SRP660537" name="Canine hemangiosarcoma primary cell RNA sequencing"/>
                      <Organism taxid="9615" ScientificName="Canis lupus familiaris"/>
                      <Sample acc="SRS27692090"/>
                      <Library_descriptor><LIBRARY_STRATEGY>RNA-Seq</LIBRARY_STRATEGY></Library_descriptor>
                      <Bioproject>PRJNA1399620</Bioproject>
                    """,
                    "runs": '<Run acc="SRR36719144" total_spots="1"/>',
                    "createdate": "2026/03/31",
                    "updatedate": "2026/01/07",
                },
            }
        }

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    records = SRAHarvesterV2().fetch(
        "SRR36719144",
        limit=5,
        exact_identifier_type="sra",
        exact_identifier="SRR36719144",
        require_policy_match=False,
    )

    assert len(records) == 1
    assert records[0].research_object.identifiers["sra_run"] == "SRR36719144"


def test_pubchem_v2_normalizer_extracts_compound_metadata():
    payload = {
        "query_term": "propranolol",
        "properties": {
            "CID": 4946,
            "Title": "Propranolol",
            "MolecularFormula": "C16H21NO2",
            "MolecularWeight": 259.34,
            "CanonicalSMILES": "CC(C)NCC(COC1=CC=CC2=CC=CC=C21)O",
            "InChIKey": "AQHHHDLHHXJYJD-UHFFFAOYSA-N",
            "IUPACName": "1-naphthalen-1-yloxy-3-(propan-2-ylamino)propan-2-ol",
            "XLogP": 3.0,
            "TPSA": 41.5,
        },
        "synonyms": ["Propranolol", "Inderal"],
    }

    harvester = PubChemHarvesterV2()
    record = harvester.normalize(payload)

    assert record.raw_record.source_key == "pubchem"
    assert record.research_object.object_type == "compound_record"
    assert record.research_object.identifiers["pubchem_cid"] == "4946"
    assert record.research_object.identifiers["inchikey"] == "AQHHHDLHHXJYJD-UHFFFAOYSA-N"
    assert record.research_object.dedupe_key == "pubchem_cid:4946"
    assert record.research_object.canonical_url == "https://pubchem.ncbi.nlm.nih.gov/compound/4946"
    assert record.research_object.metadata["canonical_smiles"].startswith("CC(C)")
    assert "Inderal" in harvester.text_for_chunking(record)


def test_chembl_v2_normalizer_extracts_bioactivity_metadata():
    payload = {
        "query_term": "toceranib",
        "molecule": {
            "molecule_chembl_id": "CHEMBL13608",
            "pref_name": "TOCERANIB",
            "max_phase": 4,
            "molecule_type": "Small molecule",
        },
        "activity": {
            "activity_id": 123,
            "molecule_chembl_id": "CHEMBL13608",
            "target_chembl_id": "CHEMBL279",
            "target_pref_name": "Vascular endothelial growth factor receptor 2",
            "target_organism": "Homo sapiens",
            "assay_chembl_id": "CHEMBL-A",
            "document_chembl_id": "CHEMBL-D",
            "standard_type": "IC50",
            "standard_relation": "=",
            "standard_value": "5.0",
            "standard_units": "nM",
            "pchembl_value": "8.3",
            "assay_description": "Inhibition of VEGFR2 kinase activity.",
        },
    }

    harvester = ChEMBLHarvesterV2()
    record = harvester.normalize(payload)

    assert record.raw_record.source_key == "chembl"
    assert record.research_object.object_type == "bioactivity_assay"
    assert record.research_object.identifiers["chembl_activity_id"] == "123"
    assert record.research_object.identifiers["chembl_molecule_id"] == "CHEMBL13608"
    assert record.research_object.dedupe_key == "chembl_activity_id:123"
    assert record.research_object.metadata["standard_type"] == "IC50"
    assert record.research_object.metadata["target_pref_name"] == "Vascular endothelial growth factor receptor 2"
    assert record.research_object.metadata["target_gene"] == "KDR"
    assert record.research_object.metadata["target_category"] == "vegf_angiogenesis"
    assert record.research_object.metadata["pchembl_numeric"] == 8.3
    assert "Target gate: KDR (vegf_angiogenesis)" in harvester.text_for_chunking(record)
    assert "pChEMBL: 8.3" in harvester.text_for_chunking(record)


def test_chembl_v2_fetches_only_target_gated_relevant_bioactivities(monkeypatch):
    def fake_get_json(url, params):
        if url.endswith("/molecule.json"):
            if params.get("pref_name__iexact") == "toceranib":
                return {
                    "molecules": [
                        {
                            "molecule_chembl_id": "CHEMBL13608",
                            "pref_name": "TOCERANIB",
                            "max_phase": 2,
                            "molecule_type": "Small molecule",
                        }
                    ]
                }
            return {"molecules": []}
        if url.endswith("/activity.json"):
            assert params["target_chembl_id__in"] == "CHEMBL279"
            assert params["standard_type__in"] == "IC50"
            assert params["assay_type__in"] == "B"
            assert params["order_by"] == "-pchembl_value"
            return {
                "activities": [
                    {
                        "activity_id": 1,
                        "molecule_chembl_id": "CHEMBL13608",
                        "target_chembl_id": "CHEMBL279",
                        "target_pref_name": "Vascular endothelial growth factor receptor 2",
                        "target_organism": "Homo sapiens",
                        "assay_type": "B",
                        "standard_type": "IC50",
                        "standard_relation": "=",
                        "standard_value": "60.0",
                        "standard_units": "nM",
                        "pchembl_value": "7.22",
                        "assay_description": "Inhibition of VEGFR2.",
                    },
                    {
                        "activity_id": 2,
                        "target_chembl_id": "CHEMBL999",
                        "target_organism": "Homo sapiens",
                        "assay_type": "B",
                        "standard_type": "IC50",
                        "pchembl_value": "9.0",
                    },
                    {
                        "activity_id": 3,
                        "target_chembl_id": "CHEMBL279",
                        "target_organism": "Homo sapiens",
                        "assay_type": "B",
                        "standard_type": "IC50",
                        "pchembl_value": "3.5",
                    },
                ]
            }
        raise AssertionError(f"Unexpected ChEMBL URL: {url}")

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    harvester = ChEMBLHarvesterV2()
    records = harvester.fetch(
        "toceranib",
        limit=3,
        target_chembl_ids=["CHEMBL279"],
        target_organisms=["Homo sapiens"],
        standard_types=["IC50"],
        assay_types=["B"],
        min_pchembl=6.0,
        activities_per_molecule=3,
        include_cell_line_assays=False,
    )

    assert len(records) == 1
    assert records[0].research_object.identifiers["chembl_activity_id"] == "1"
    assert records[0].research_object.metadata["target_gene"] == "KDR"
    assert records[0].research_object.metadata["target_category"] == "vegf_angiogenesis"


def test_chembl_v2_cell_line_lane_requires_real_disease_term(monkeypatch):
    def fake_get_json(url, params):
        if url.endswith("/molecule.json"):
            if params.get("pref_name__iexact") == "paclitaxel":
                return {
                    "molecules": [
                        {
                            "molecule_chembl_id": "CHEMBL428647",
                            "pref_name": "PACLITAXEL",
                            "max_phase": 4,
                            "molecule_type": "Small molecule",
                        }
                    ]
                }
            return {"molecules": []}
        if url.endswith("/activity.json") and params.get("target_type") == "CELL-LINE":
            return {
                "activities": [
                    {
                        "activity_id": 10,
                        "target_chembl_id": "CHEMBL210",
                        "target_pref_name": "Beta-2 adrenergic receptor",
                        "target_organism": "Homo sapiens",
                        "assay_type": "F",
                        "standard_type": "IC50",
                        "pchembl_value": "9.2",
                        "assay_description": "Activity in endogenously expressing cells.",
                    },
                    {
                        "activity_id": 11,
                        "target_chembl_id": "CHEMBL613827",
                        "target_pref_name": "MES-SA/Dx5",
                        "target_organism": "Homo sapiens",
                        "assay_type": "F",
                        "standard_type": "IC50",
                        "pchembl_value": "10.4",
                        "assay_description": "Cytotoxic activity against uterine sarcoma cells.",
                    },
                ]
            }
        if url.endswith("/activity.json"):
            return {"activities": []}
        raise AssertionError(f"Unexpected ChEMBL URL: {url}")

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    harvester = ChEMBLHarvesterV2()
    records = harvester.fetch(
        "paclitaxel",
        limit=3,
        target_chembl_ids=["CHEMBL210"],
        target_organisms=["Homo sapiens"],
        include_cell_line_assays=True,
        cell_line_terms=["sarcoma", "dog"],
        cell_line_records_per_molecule=2,
    )

    assert len(records) == 1
    assert records[0].research_object.identifiers["chembl_activity_id"] == "11"
    assert records[0].research_object.metadata["target_category"] == "cell_cytotoxicity"
    assert records[0].research_object.metadata["matched_cell_line_term"] == "sarcoma"


def test_uniprot_v2_normalizer_extracts_target_metadata():
    entry = {
        "primaryAccession": "P35968",
        "uniProtKBId": "VGFR2_HUMAN",
        "entryType": "UniProtKB reviewed (Swiss-Prot)",
        "proteinDescription": {
            "recommendedName": {
                "fullName": {"value": "Vascular endothelial growth factor receptor 2"}
            }
        },
        "genes": [{"geneName": {"value": "KDR"}, "synonyms": [{"value": "VEGFR2"}]}],
        "organism": {"scientificName": "Homo sapiens", "taxonId": 9606},
        "sequence": {"length": 1356, "molWeight": 151527},
        "comments": [
            {
                "commentType": "FUNCTION",
                "texts": [{"value": "Tyrosine-protein kinase receptor for VEGFA."}],
            }
        ],
        "keywords": [{"name": "Angiogenesis"}],
        "uniProtKBCrossReferences": [{"database": "AlphaFoldDB", "id": "AF-P35968-F1"}],
    }

    harvester = UniProtHarvesterV2()
    record = harvester.normalize(entry, source_query="KDR")

    assert record.raw_record.source_key == "uniprot"
    assert record.research_object.object_type == "structure"
    assert record.research_object.identifiers["uniprot_accession"] == "P35968"
    assert record.research_object.identifiers["gene_symbol"] == "KDR"
    assert record.research_object.dedupe_key == "uniprot_accession:p35968"
    assert record.research_object.metadata["reviewed"] is True
    assert record.research_object.metadata["target_gene"] == "KDR"
    assert record.research_object.metadata["target_category"] == "vegf_angiogenesis"
    assert record.research_object.metadata["species_scope"] == "human"
    assert record.research_object.metadata["gene_match_verified"] is True
    assert record.research_object.metadata["alphafold_ids"] == ["AF-P35968-F1"]
    assert "AlphaFold IDs: AF-P35968-F1" in harvester.text_for_chunking(record)


def test_hgnc_v2_normalizer_extracts_gene_nomenclature_metadata():
    payload = {
        "hgnc_id": "HGNC:6307",
        "symbol": "KDR",
        "name": "kinase insert domain receptor",
        "alias_symbol": ["VEGFR2", "FLK1"],
        "entrez_id": "3791",
        "ensembl_gene_id": "ENSG00000128052",
        "uniprot_ids": ["P35968"],
        "taxonomy_id": "9606",
        "source_version": "2026.05",
    }

    record = HGNCHarvesterV2().normalize(payload)

    assert record.raw_record.source_key == "hgnc"
    assert record.research_object.object_type == "knowledge_entry"
    assert record.research_object.identifiers["hgnc_id"] == "HGNC:6307"
    assert record.research_object.metadata["aliases"] == ["VEGFR2", "FLK1"]
    assert record.research_object.metadata["cross_references"]["entrez_id"] == "3791"
    assert "Aliases: VEGFR2; FLK1" in HGNCHarvesterV2().text_for_chunking(record)


def test_ncbi_gene_v2_normalizer_extracts_gene_identifier_metadata():
    payload = {
        "uid": "3791",
        "name": "KDR",
        "description": "kinase insert domain receptor",
        "otheraliases": "VEGFR2, FLK1",
        "organism": {"scientificname": "Homo sapiens", "taxid": 9606},
    }

    record = NCBIGeneHarvesterV2().normalize(payload)

    assert record.raw_record.source_key == "ncbi_gene"
    assert record.research_object.identifiers["ncbi_gene_id"] == "3791"
    assert record.research_object.metadata["aliases"] == ["VEGFR2", "FLK1"]
    assert record.research_object.metadata["organism_taxid"] == "9606"


def test_reactome_and_ontology_normalizers_extract_primitive_metadata():
    pathway = ReactomeHarvesterV2().normalize(
        {
            "stId": "R-HSA-194138",
            "displayName": "VEGF ligand-receptor interactions",
            "query_term": "angiogenesis",
        }
    )
    disease = MONDOHarvesterV2().normalize(
        {
            "obo_id": "MONDO:0004992",
            "label": "angiosarcoma",
            "synonym": ["hemangiosarcoma"],
            "ontology_name": "mondo",
            "query_term": "angiosarcoma",
        }
    )

    assert pathway.research_object.identifiers["reactome_id"] == "R-HSA-194138"
    assert pathway.research_object.metadata["pathway_name"] == "VEGF ligand-receptor interactions"
    assert disease.research_object.identifiers["ontology_id"] == "MONDO:0004992"
    assert disease.research_object.metadata["synonyms"] == ["hemangiosarcoma"]


def test_rcsb_pdb_v2_normalizer_extracts_structure_metadata():
    payload = {
        "query_term": "KDR",
        "search_hit": {"identifier": "3VHE", "score": 42.0},
        "entry": {
            "rcsb_id": "3VHE",
            "struct": {"title": "Crystal structure of VEGFR2 kinase domain"},
            "exptl": [{"method": "X-RAY DIFFRACTION"}],
            "rcsb_accession_info": {
                "deposit_date": "2011-01-01",
                "initial_release_date": "2012-02-01",
                "revision_date": "2020-01-01",
                "has_released_experimental_data": True,
            },
            "citation": [{"pdbx_database_id_PubMed": 22212345}],
            "rcsb_entry_info": {"polymer_entity_count_protein": 1},
        },
    }

    harvester = RCSBPDBHarvesterV2()
    record = harvester.normalize(payload)

    assert record.raw_record.source_key == "rcsb_pdb"
    assert record.research_object.object_type == "structure"
    assert record.research_object.identifiers["pdb_id"] == "3VHE"
    assert record.research_object.identifiers["pmid"] == "22212345"
    assert record.research_object.dedupe_key == "pdb_id:3vhe"
    assert record.research_object.publication_year == 2012
    assert record.research_object.metadata["target_gene"] == "KDR"
    assert record.research_object.metadata["target_category"] == "vegf_angiogenesis"
    assert record.research_object.metadata["experimental_methods"] == ["X-RAY DIFFRACTION"]
    assert record.research_object.metadata["protein_entity_count"] == 1
    assert "PDB ID: 3VHE" in harvester.text_for_chunking(record)


def test_openfda_animal_events_v2_normalizer_extracts_safety_metadata():
    event = {
        "unique_aer_id_number": "US-FDA-CVM-2026-0001",
        "original_receive_date": "20260401",
        "animal": {"species": "Dog", "breed": "Golden Retriever", "gender": "Female", "age": {"unit": "Year", "value": "9"}},
        "drug": [
            {
                "brand_name": "Example Doxorubicin",
                "active_ingredients": [{"name": "doxorubicin"}],
            }
        ],
        "reaction": [{"veddra_term_name": "Vomiting", "veddra_term_code": "334"}, {"veddra_term_name": "Neutropenia"}],
        "outcome": "Recovered",
        "serious_ae": "true",
        "primary_reporter": "Veterinarian",
    }

    harvester = OpenFDAAnimalEventsHarvesterV2()
    record = harvester.normalize(event, source_query="doxorubicin", source_search='animal.species:"Dog"')

    assert record.raw_record.source_key == "openfda_animal_events"
    assert record.research_object.object_type == "safety_report"
    assert record.research_object.identifiers["openfda_report_id"] == "US-FDA-CVM-2026-0001"
    assert record.research_object.dedupe_key == "openfda_report_id:us-fda-cvm-2026-0001"
    assert record.research_object.publication_year == 2026
    assert record.research_object.metadata["species"] == "Dog"
    assert record.research_object.metadata["drug_names"] == ["Example Doxorubicin", "doxorubicin"]
    assert record.research_object.metadata["reaction_terms"] == ["Vomiting", "Neutropenia"]
    assert record.research_object.metadata["reaction_codes"] == ["334"]
    assert "Responsible use: signal_generation_only_not_clinical_decision_support" in harvester.text_for_chunking(record)


def test_openalex_v2_filters_unmatched_records_by_default():
    harvester = OpenAlexHarvesterV2()
    matched = harvester.normalize(
        {
            "id": "https://openalex.org/W1",
            "title": "Human angiosarcoma therapy",
            "publication_year": 2026,
            "abstract_inverted_index": {"Human": [0], "angiosarcoma": [1]},
            "primary_location": {"landing_page_url": "https://example.test/matched"},
        }
    )
    unmatched = harvester.normalize(
        {
            "id": "https://openalex.org/W2",
            "title": "Unrelated oncology therapy",
            "publication_year": 2026,
            "abstract_inverted_index": {"Unrelated": [0], "oncology": [1]},
            "primary_location": {"landing_page_url": "https://example.test/unmatched"},
        }
    )

    assert harvester.filter_relevant([matched, unmatched], {}) == [matched]


def test_local_deterministic_embedding_provider_is_repeatable():
    provider = LocalDeterministicEmbeddingProvider(dimensions=32)

    vector = provider.embed_text("VEGF signaling in canine hemangiosarcoma.")
    repeated = provider.embed_text("VEGF signaling in canine hemangiosarcoma.")
    fresh_provider_vector = LocalDeterministicEmbeddingProvider(dimensions=32).embed_text(
        "VEGF signaling in canine hemangiosarcoma."
    )
    different_vector = provider.embed_text("Doxorubicin toxicity monitoring in dogs.")

    assert vector == repeated
    assert vector == fresh_provider_vector
    assert vector != different_vector
    assert len(vector) == 32
    assert sum(value * value for value in vector) == pytest.approx(1.0)


def test_openrouter_embedding_provider_calls_embeddings_api(monkeypatch):
    calls = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def read(self):
            return json.dumps(
                {
                    "data": [
                        {"index": 0, "embedding": [0.1, 0.2, 0.3]},
                        {"index": 1, "embedding": [0.4, 0.5, 0.6]},
                    ]
                }
            ).encode("utf-8")

    def fake_urlopen(request, timeout):
        calls.append(
            {
                "url": request.full_url,
                "payload": json.loads(request.data.decode("utf-8")),
                "authorization": request.headers.get("Authorization"),
                "timeout": timeout,
            }
        )
        return FakeResponse()

    monkeypatch.setattr("hsa_research.ingestion_bridge.embeddings.urllib_request.urlopen", fake_urlopen)

    provider = OpenRouterEmbeddingProvider(
        embedding_model="openai/text-embedding-3-small",
        api_key="unit-test-key",
        timeout_seconds=12,
    )
    vectors = provider.embed_texts(["alpha", "beta"])

    assert provider.embedding_model == "openrouter:openai/text-embedding-3-small"
    assert provider.provider_name == "openrouter"
    assert provider.provider_model == "openai/text-embedding-3-small"
    assert provider.embedding_dimensions == 3
    assert vectors == [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]
    assert calls[0]["url"] == "https://openrouter.ai/api/v1/embeddings"
    assert calls[0]["payload"]["model"] == "openai/text-embedding-3-small"
    assert calls[0]["payload"]["input"] == ["alpha", "beta"]
    assert calls[0]["authorization"] == "Bearer unit-test-key"
    assert calls[0]["timeout"] == 12


def test_build_embedding_provider_selects_openrouter_and_local(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "unit-test-key")

    local_provider = build_embedding_provider("local-hash-test", dimensions=16)
    openrouter_provider = build_embedding_provider("openrouter:openai/text-embedding-3-small", dimensions=3)

    assert isinstance(local_provider, LocalDeterministicEmbeddingProvider)
    assert local_provider.embedding_dimensions == 16
    assert isinstance(openrouter_provider, OpenRouterEmbeddingProvider)
    assert openrouter_provider.embedding_model == "openrouter:openai/text-embedding-3-small"
    assert openrouter_provider.embedding_dimensions == 3


def test_embedding_model_selection_prefers_configured_then_openrouter(monkeypatch):
    models = {
        "local-hash-v1": 10,
        "openrouter:openai/text-embedding-3-small": 10,
        "openrouter:openai/text-embedding-3-large": 10,
    }

    monkeypatch.delenv("HSA_EMBEDDING_MODEL", raising=False)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    assert default_embedding_model_for_environment() == "local-hash-v1"
    assert select_embedding_model_from_coverage(models) == "local-hash-v1"

    monkeypatch.setenv("OPENROUTER_API_KEY", "unit-test-key")
    assert default_embedding_model_for_environment() == "openrouter:openai/text-embedding-3-large"
    assert select_embedding_model_from_coverage(models) == "openrouter:openai/text-embedding-3-large"

    monkeypatch.setenv("HSA_EMBEDDING_MODEL", "openrouter:openai/text-embedding-3-small")
    assert default_embedding_model_for_environment() == "openrouter:openai/text-embedding-3-small"
    assert select_embedding_model_from_coverage(models) == "openrouter:openai/text-embedding-3-small"


def test_text_embedding_contract_rejects_dimension_mismatch():
    with pytest.raises(ValueError, match="embedding_dimensions"):
        TextEmbedding(
            chunk_id=uuid4(),
            research_object_id=uuid4(),
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash="bad-dimensions",
            embedding_model="unit-embedding-v1",
            embedding_dimensions=2,
            embedding=[1.0],
        )


def test_embedding_maintenance_prunes_orphan_embeddings_and_reports_full_coverage(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Embedding maintenance example",
            source_key="pubmed",
            dedupe_key="pubmed:embedding-maintenance",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis is discussed in canine hemangiosarcoma.",
            content_hash="embedding-maintenance-live",
        )
    )
    live_embedding = repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunk.id,
            research_object_id=object_id,
            chunk_index=chunk.chunk_index,
            source_key="pubmed",
            object_type="publication",
            content_hash=chunk.content_hash,
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[1.0, 0.0, 0.0],
        )
    )
    repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=uuid4(),
            research_object_id=object_id,
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash="embedding-maintenance-orphan",
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[0.0, 1.0, 0.0],
        )
    )

    assert repo.coverage_summary()["text_embeddings"] == 2
    assert repo.count_orphan_text_embeddings(embedding_model="unit-embedding-v1") == 1

    result = maintain_embedding_index(repo, embedding_model="unit-embedding-v1")
    report = result.to_report()

    assert result.passes_minimum_bar is True
    assert report["passed"] is True
    assert report["orphan_embeddings"]["seen"] == 1
    assert report["orphan_embeddings"]["deleted"] == 1
    assert report["embedding_coverage"]["total_chunks"] == 1
    assert report["embedding_coverage"]["embedded_chunks"] == 1
    assert report["embedding_coverage"]["missing_chunks"] == 0
    assert repo.count_orphan_text_embeddings(embedding_model="unit-embedding-v1") == 0
    assert repo.list_text_embeddings(embedding_model="unit-embedding-v1") == [live_embedding]
    assert repo.coverage_summary()["text_embeddings"] == 1


def test_index_embeddings_rebuilds_on_content_hash_change_and_force(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Embedding rebuild example",
            source_key="pubmed",
            dedupe_key="pubmed:embedding-rebuild",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="KIT signaling appears in canine hemangiosarcoma.",
            content_hash="embedding-rebuild-original",
        )
    )

    first_result = index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    first_embedding = repo.list_text_embeddings(source_key="pubmed", embedding_model="local-hash-test")[0]
    repo.upsert_document_chunk(
        chunk.model_copy(
            update={
                "text_content": "MTOR signaling appears in canine hemangiosarcoma.",
                "content_hash": "embedding-rebuild-updated",
            }
        )
    )
    rebuild_result = index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    rebuilt_embedding = repo.list_text_embeddings(source_key="pubmed", embedding_model="local-hash-test")[0]
    unchanged_result = index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    forced_result = index_embeddings_for_repository(
        repo,
        source_key="pubmed",
        embedding_model="local-hash-test",
        force=True,
    )
    forced_embedding = repo.list_text_embeddings(source_key="pubmed", embedding_model="local-hash-test")[0]

    assert first_result.embeddings_created == 1
    assert rebuild_result.embeddings_updated == 1
    assert rebuild_result.embeddings_skipped == 0
    assert rebuilt_embedding.embedding_id == first_embedding.embedding_id
    assert rebuilt_embedding.content_hash != first_embedding.content_hash
    assert rebuilt_embedding.embedding != first_embedding.embedding
    assert rebuilt_embedding.metadata["chunk_content_hash"] == "embedding-rebuild-updated"
    assert unchanged_result.embeddings_skipped == 1
    assert forced_result.embeddings_updated == 1
    assert forced_embedding.embedding_id == first_embedding.embedding_id
    assert len(repo.list_text_embeddings(embedding_model="local-hash-test")) == 1


def test_embedding_bakeoff_scores_configured_models(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Sorafenib safety and coagulopathy in a dog",
            abstract="Sorafenib toxicity and dose monitoring in dogs.",
            source_key="pmc_oa",
            dedupe_key="pmc_oa:embedding-bakeoff",
        )
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Sorafenib dog safety toxicity coagulopathy evidence.",
            content_hash="embedding-bakeoff-chunk",
        )
    )
    index_embeddings_for_repository(repo, embedding_model="local-hash-test")

    report = run_embedding_bakeoff(
        repo,
        embedding_models=("local-hash-test",),
        benchmarks=(
            EmbeddingBenchmark(
                name="unit_sorafenib_dog_safety",
                query="sorafenib dog safety toxicity",
                expected_terms=("sorafenib", "dog", "safety", "toxicity"),
                preferred_source_keys=("pmc_oa",),
                expected_title_terms=("sorafenib", "dog"),
            ),
        ),
    )

    assert report["best_model"] == "local-hash-test"
    assert report["models"][0]["average_score"] > 0.8
    assert report["models"][0]["benchmarks"][0]["top_source_key"] == "pmc_oa"


def test_service_keyword_retrieval_scores_before_final_limit(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    generic_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="dataset",
            title="canine hemangiosarcoma generic dataset",
            source_key="geo",
            dedupe_key="geo:retrieval-keyword-limit-generic",
        )
    )
    for index in range(150):
        repo.upsert_document_chunk(
            DocumentChunk(
                research_object_id=generic_object_id,
                chunk_index=index,
                section_label="dataset",
                text_content="canine hemangiosarcoma cell line metadata",
                content_hash=f"retrieval-keyword-limit-generic-{index}",
            )
        )
    specific_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Sorafenib safety dose limiting toxicity in a dog",
            source_key="pmc_oa",
            dedupe_key="pmc_oa:retrieval-keyword-limit-specific",
        )
    )
    specific_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=specific_object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Sorafenib safety dose limiting toxicity in a dog with sarcoma.",
            content_hash="retrieval-keyword-limit-specific",
        )
    )

    results = service.search_research_chunks(
        ResearchChunkSearchRequest(
            query="canine hemangiosarcoma sorafenib safety dose limiting toxicity",
            limit=1,
        )
    )

    assert results.search_mode == "keyword"
    assert results.results[0].chunk.id == specific_chunk.id


def test_service_retrieval_smoke_chains_embedding_search_context_and_object(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Retrieval smoke example",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-smoke",
        )
    )
    target_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis in canine hemangiosarcoma retrieval smoke context.",
            content_hash="retrieval-smoke-target",
        )
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=1,
            section_label="methods",
            text_content="Doxorubicin dosing background.",
            content_hash="retrieval-smoke-other",
        )
    )
    index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")

    result = service.run_retrieval_smoke(
        RetrievalSmokeRequest(
            query="VEGFA angiogenesis",
            source_key="pubmed",
            embedding_model="local-hash-test",
            limit=2,
            require_embedding=True,
        )
    )

    assert result.passed is True
    assert result.errors == []
    assert result.search.search_mode == "embedding"
    assert result.selected_chunk_id == target_chunk.id
    assert result.selected_research_object_id == object_id
    assert result.chunk_context is not None
    assert result.chunk_context.chunk.id == target_chunk.id
    assert result.research_object is not None
    assert result.research_object.research_object.id == object_id


def test_service_retrieval_smoke_can_require_embeddings(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Retrieval smoke keyword fallback example",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-smoke-keyword",
        )
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis in canine hemangiosarcoma.",
            content_hash="retrieval-smoke-keyword",
        )
    )

    result = service.run_retrieval_smoke(
        RetrievalSmokeRequest(query="VEGFA angiogenesis", source_key="pubmed", require_embedding=True)
    )

    assert result.passed is False
    assert result.search.search_mode == "keyword"
    assert result.errors == ["expected embedding search, got keyword"]


def test_mcp_retrieval_tool_helpers_dump_bounded_read_results(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="MCP retrieval example",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-mcp",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Canine hemangiosarcoma retrieval context for MCP.",
            content_hash="retrieval-mcp-chunk",
        )
    )
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)

    search_payload = mcp_server.search_research_chunks_tool(
        query="hemangiosarcoma retrieval",
        source_key="pubmed",
        limit=1,
    )
    chunk_payload = mcp_server.get_chunk_context_tool(str(chunk.id), window=0)
    object_payload = mcp_server.get_research_object_tool(str(object_id), max_chunks=1)

    assert search_payload["search_mode"] == "keyword"
    assert search_payload["results"][0]["chunk"]["id"] == str(chunk.id)
    assert chunk_payload["chunk"]["id"] == str(chunk.id)
    assert object_payload["research_object"]["id"] == str(object_id)
    assert len(object_payload["chunks"]) == 1


def test_mcp_research_lead_tools_dump_json_safe_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-research-leads.sqlite3", seed=False)
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="HSA institutional article",
            url="https://example.edu/hsa",
            lead_type="institutional_article",
            source_key="x_linked_article",
        )
    )

    fetched = mcp_server.get_research_lead_tool(str(lead.lead_id))
    listed = mcp_server.list_research_leads_tool(status="new")

    assert fetched["lead_id"] == str(lead.lead_id)
    assert listed[0]["identity_key"] == lead.identity_key


def test_mcp_retrieval_smoke_helper_dumps_full_read_chain(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="MCP retrieval smoke example",
            source_key="pubmed",
            dedupe_key="pubmed:mcp-retrieval-smoke",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis retrieval smoke context for MCP.",
            content_hash="mcp-retrieval-smoke",
        )
    )
    index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)

    payload = mcp_server.run_retrieval_smoke_tool(
        query="VEGFA angiogenesis",
        source_key="pubmed",
        embedding_model="local-hash-test",
        require_embedding=True,
    )

    assert payload["passed"] is True
    assert payload["selected_chunk_id"] == str(chunk.id)
    assert payload["search"]["search_mode"] == "embedding"
    assert payload["chunk_context"]["chunk"]["id"] == str(chunk.id)
    assert payload["research_object"]["research_object"]["id"] == str(object_id)
