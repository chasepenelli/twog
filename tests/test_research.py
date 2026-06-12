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

def test_research_program_evidence_loop_queues_bounded_work():
    repo = InMemoryResearchRepository()
    program = repo.upsert_research_program(_research_program_fixture())
    result = HSAResearchService(repo).run_research_program_evidence_loop(
        ResearchProgramEvidenceLoopRequest(
            program_id=program.program_id,
            max_tasks=1,
            max_source_queries=2,
            priority=35,
            review_mode="deterministic_only",
        )
    )

    assert isinstance(result, ResearchProgramEvidenceLoopResult)
    assert result.blocked is False
    assert result.loop_count_before == 0
    assert result.loop_count_after == 1
    assert result.selected_task_count == 1
    assert result.research_lead_count == 1
    assert result.source_query_count == 2
    assert result.brief_queue_count == 1
    assert result.task_results[0].status_after == "queued"
    assert result.task_results[0].selected_source_keys == ["pubmed", "europe_pmc"]

    stored = repo.get_research_program(program.program_id)
    assert stored.evidence_loop_count == 1
    assert stored.status == "active"
    assert stored.evidence_tasks[0].status == "queued"
    assert stored.metadata["latest_evidence_loop"]["source_query_count"] == 2
    assert repo.list_research_brief_queue_items(topic_query="Coagulation evidence acquisition", limit=10)
    assert repo.list_research_leads(statuses=["followup"], limit=10)
    research_queries = [
        query
        for query in repo.list_source_queries(active_only=True)
        if query.track == "research_program_evidence"
    ]
    assert {
        query.source_key
        for query in research_queries
    } == {"pubmed", "europe_pmc"}
    assert all("frontier_query_expansion" in query.query_params for query in research_queries)
    assert any('"mRNA vaccine"' in query.query_text for query in research_queries)


def test_research_program_evidence_loop_dry_run_and_max_loop_block():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    program = repo.upsert_research_program(_research_program_fixture())
    dry_run = service.run_research_program_evidence_loop(
        ResearchProgramEvidenceLoopRequest(program_id=program.program_id, dry_run=True)
    )
    assert dry_run.loop_count_after == 0
    assert repo.get_research_program(program.program_id).evidence_loop_count == 0
    assert repo.list_research_brief_queue_items(limit=10) == []

    maxed = program.model_copy(update={"evidence_loop_count": program.max_evidence_loops})
    repo.upsert_research_program(maxed)
    blocked = service.run_research_program_evidence_loop(
        ResearchProgramEvidenceLoopRequest(program_id=program.program_id)
    )
    assert blocked.blocked is True
    assert "max_evidence_loops" in blocked.errors[0]


def test_research_program_board_deterministic_run_persists_bounded_program(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-program-board.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:vascular-program",
            content_hash="vascular-program-raw",
            raw_payload={"pmid": "vascular-program"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Coagulation and angiogenesis in canine hemangiosarcoma",
            abstract="Canine HSA evidence mentions coagulation, vascular injury, KDR, VEGF, and angiogenesis.",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            dedupe_key="pmid:vascular-program",
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma may involve coagulation, vascular injury, angiogenesis, "
                "KDR, VEGF, endothelial signaling, and human angiosarcoma analog biology."
            ),
            content_hash="vascular-program-chunk",
        )
    )

    result = HSAResearchService(repo).run_research_program_board(
        ResearchProgramReviewRequest(
            review_mode="deterministic_only",
            max_packets=0,
            thesis_topic="vascular injury coagulation angiogenesis HSA",
        )
    )

    assert isinstance(result, ResearchProgramReviewResult)
    assert result.program_count == 1
    assert result.persisted_count == 1
    program = result.programs[0]
    assert len(program.decisive_questions) == 2
    assert program.max_evidence_loops == 2
    assert program.gate_decision in {"needs_one_more_pass", "ready_for_therapy_ideas"}
    assert repo.list_research_programs(thesis_query="vascular", limit=10)
    agent_runs = repo.list_agent_runs(agent_name="research_program_board_agent", limit=10)
    assert agent_runs[0].status == RunStatus.COMPLETED


def test_research_program_board_openrouter_success_records_model_and_program(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-program-board-openrouter.sqlite3", seed=False)
    existing_program = repo.upsert_research_program(
        _research_program_fixture().model_copy(update={"evidence_loop_count": 1})
    )
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:program-openrouter",
            content_hash="program-openrouter-raw",
            raw_payload={"pmid": "program-openrouter"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Vascular program source",
            abstract="KDR VEGF coagulation and angiogenesis source.",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            dedupe_key="pmid:program-openrouter",
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="KDR VEGF coagulation vascular injury angiogenesis canine HSA human angiosarcoma.",
            content_hash="program-openrouter-chunk",
        )
    )
    brief = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="vascular ecology evidence loop brief",
            status="completed",
            final_brief="Two lanes passed quality, but safety and metastatic ecology need primary evidence [C1].",
            citation_count=1,
            hypothesis_count=1,
            evidence_limitation_count=1,
            result_payload={
                "ranked_hypotheses": [{"title": "vascular ecology", "citations": ["C1"]}],
                "evidence_limitations": ["Safety monitoring evidence remains indirect."],
                "citations": [{"citation_id": "C1", "title": "Vascular program source"}],
            },
        )
    )
    evaluation = repo.upsert_research_brief_evaluation(
        ResearchBriefEvaluationRecord(
            brief_id=brief.brief_id,
            topic=brief.topic,
            overall_score=0.71,
            passes_quality_bar=False,
            readiness="needs_more_evidence",
            summary={"verdict": "not ready"},
            result_payload={
                "weaknesses": ["Needs primary safety evidence."],
                "recommendations": ["Run one final bounded evidence pass."],
            },
        )
    )

    def fake_review_model(model_name, review_payload):
        assert model_name == "test/opus"
        evidence_payload = review_payload["evidence_payload"]
        assert evidence_payload["existing_program"]["program_id"] == str(existing_program.program_id)
        assert evidence_payload["existing_program"]["evidence_loop_count"] == 1
        assert evidence_payload["evaluated_briefs"][0]["brief_id"] == str(brief.brief_id)
        assert evidence_payload["evaluated_briefs"][0]["evaluation_id"] == str(evaluation.evaluation_id)
        assert evidence_payload["evaluated_briefs"][0]["evaluation"]["passes_quality_bar"] is False
        return {
            "text": json.dumps(
                {
                    "programs": [
                        {
                            "title": "Vascular-coagulation ecology",
                            "thesis": "HSA may expose a vascular-coagulation ecology vulnerability.",
                            "disease_model": "Endothelial signaling, coagulation, and angiogenesis may interact.",
                            "thesis_area": "vascular_coagulation_angiogenesis",
                            "therapy_families": ["vascular normalization"],
                            "modality_families": ["combination strategy"],
                            "decisive_questions": [
                                {
                                    "question": "Does coagulation biology affect HSA outcome?",
                                    "metric_plan": ["direct evidence count"],
                                    "tool_hints": ["literature_review"],
                                    "evidence_refs": ["evaluated_brief:1", "chunk:1"],
                                },
                                {
                                    "question": "Can KDR/VEGF biomarkers gate the program?",
                                    "metric_plan": ["pathway activity"],
                                    "tool_hints": ["omics_expression_review"],
                                    "evidence_refs": ["evaluated_brief:1"],
                                },
                            ],
                            "evidence_tasks": [
                                {
                                    "task_type": "literature_search",
                                    "title": "Acquire coagulation evidence",
                                    "objective": "Find direct and analog coagulation evidence.",
                                    "source_keys": ["pubmed"],
                                    "tool_hints": ["literature_review"],
                                    "metrics": ["unique source count"],
                                    "pass_values": ["three sources"],
                                    "fail_values": ["no direct evidence"],
                                    "evidence_refs": ["evaluated_brief:1"],
                                }
                            ],
                            "metric_plan": ["biological plausibility"],
                            "recommended_tools": ["literature_review"],
                            "stop_criteria": ["Archive if evidence is nonspecific after two loops."],
                            "downstream_therapy_opportunities": ["coagulation-aware vascular strategy"],
                            "status": "ready_for_therapy_ideas",
                            "gate_decision": "ready_for_therapy_ideas",
                            "biological_plausibility_score": 0.82,
                            "cross_species_support_score": 0.7,
                            "evidence_density_score": 0.66,
                            "novelty_score": 0.75,
                            "testability_score": 0.74,
                            "therapeutic_leverage_score": 0.72,
                            "failure_risk_score": 0.35,
                            "confidence_score": 0.73,
                            "review_summary": "Program is ready to spawn therapy ideas.",
                            "evidence_refs": ["evaluated_brief:1", "chunk:1"],
                        }
                    ],
                    "errors": [],
                }
            ),
            "metadata": {
                "requested_model": model_name,
                "model_name": "resolved/opus",
                "usage": {"cost": 0.02},
            },
        }

    monkeypatch.setattr(research_program_board, "_openrouter_review_model", fake_review_model)

    result = HSAResearchService(repo).run_research_program_board(
        ResearchProgramReviewRequest(
            program_id=existing_program.program_id,
            evaluation_ids=[evaluation.evaluation_id],
            review_mode="openrouter_required",
            review_models=["test/opus"],
            max_packets=0,
            thesis_topic="vascular coagulation angiogenesis HSA",
        )
    )

    assert result.persisted_count == 1
    assert result.programs[0].program_id == existing_program.program_id
    assert result.programs[0].evidence_loop_count == 1
    assert result.programs[0].evidence_refs[0] == "evaluated_brief:1"
    assert result.programs[0].metadata["requested_model"] == "test/opus"
    assert result.programs[0].metadata["model_name"] == "resolved/opus"
    assert result.programs[0].metadata["evidence"]["evaluated_brief_count"] == 1
    assert repo.list_research_programs(gate_decision="ready_for_therapy_ideas", limit=10)


def test_research_program_board_forces_gate_at_evidence_loop_cap(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-program-board-loop-cap.sqlite3", seed=False)
    existing_program = repo.upsert_research_program(
        _research_program_fixture().model_copy(update={"evidence_loop_count": 2, "max_evidence_loops": 2})
    )
    brief = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="loop-capped program evidence brief",
            status="completed",
            final_brief="The program remains plausible but still needs primary evidence [C1].",
            citation_count=1,
            hypothesis_count=1,
            evidence_limitation_count=1,
            result_payload={"citations": [{"citation_id": "C1", "title": "Loop capped evidence"}]},
        )
    )
    evaluation = repo.upsert_research_brief_evaluation(
        ResearchBriefEvaluationRecord(
            brief_id=brief.brief_id,
            topic=brief.topic,
            overall_score=0.61,
            passes_quality_bar=False,
            readiness="needs_more_evidence",
            summary={"verdict": "not ready"},
        )
    )

    def fake_review_model(model_name, review_payload):
        assert review_payload["evidence_payload"]["existing_program"]["evidence_loop_count"] == 2
        return {
            "text": json.dumps(
                {
                    "programs": [
                        {
                            "title": "Loop capped vascular program",
                            "thesis": "The program is plausible but should not request an endless loop.",
                            "disease_model": "Canine HSA vascular ecology.",
                            "thesis_area": "vascular_ecology",
                            "therapy_families": ["biomarker-stratified vascular therapy"],
                            "modality_families": ["combination strategy"],
                            "decisive_questions": [
                                {
                                    "question": "Does the signal justify child therapy ideas?",
                                    "metric_plan": ["confidence"],
                                    "tool_hints": ["expert_review"],
                                    "evidence_refs": ["evaluated_brief:1"],
                                },
                                {
                                    "question": "Is validation strategy ready?",
                                    "metric_plan": ["evidence density"],
                                    "tool_hints": ["safety_signal_review"],
                                    "evidence_refs": ["evaluated_brief:1"],
                                },
                            ],
                            "evidence_tasks": [
                                {
                                    "task_type": "literature_search",
                                    "title": "Do not persist another pass at cap",
                                    "objective": "This should be converted by the parser.",
                                    "source_keys": ["pubmed"],
                                    "tool_hints": ["literature_review"],
                                    "metrics": ["primary evidence"],
                                    "pass_values": ["direct evidence"],
                                    "fail_values": ["no direct evidence"],
                                    "evidence_refs": ["evaluated_brief:1"],
                                }
                            ],
                            "metric_plan": ["confidence"],
                            "recommended_tools": ["expert_review"],
                            "stop_criteria": ["Stop after two loops."],
                            "downstream_therapy_opportunities": ["narrow biomarker-stratified child ideas"],
                            "status": "active",
                            "gate_decision": "needs_one_more_pass",
                            "biological_plausibility_score": 0.75,
                            "cross_species_support_score": 0.7,
                            "evidence_density_score": 0.45,
                            "novelty_score": 0.6,
                            "testability_score": 0.75,
                            "therapeutic_leverage_score": 0.55,
                            "failure_risk_score": 0.6,
                            "confidence_score": 0.6,
                            "review_summary": "The model incorrectly requested another pass at the loop cap.",
                            "evidence_refs": ["evaluated_brief:1"],
                        }
                    ]
                }
            ),
            "metadata": {"requested_model": model_name, "model_name": "resolved/opus"},
        }

    monkeypatch.setattr(research_program_board, "_openrouter_review_model", fake_review_model)

    result = HSAResearchService(repo).run_research_program_board(
        ResearchProgramReviewRequest(
            program_id=existing_program.program_id,
            evaluation_ids=[evaluation.evaluation_id],
            review_mode="openrouter_required",
            review_models=["test/opus"],
            max_packets=0,
            max_chunks=0,
            max_evidence_loops=2,
        )
    )

    program = result.programs[0]
    assert program.evidence_loop_count == 2
    assert program.gate_decision == "ready_for_therapy_ideas"
    assert program.status == "ready_for_therapy_ideas"
    assert "max evidence loop cap" in program.errors[0]
    assert program.metadata["gate_override_reason"] == program.errors[0]


def test_research_program_board_invalid_openrouter_json_records_failed_run(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-program-board-invalid.sqlite3", seed=False)

    def fake_review_model(model_name, review_payload):
        return {"text": "not json", "metadata": {"requested_model": model_name}}

    monkeypatch.setattr(research_program_board, "_openrouter_review_model", fake_review_model)

    with pytest.raises(ValueError, match="Invalid research program board JSON"):
        HSAResearchService(repo).run_research_program_board(
            ResearchProgramReviewRequest(
                review_mode="openrouter_required",
                review_models=["test/opus"],
                max_packets=0,
                max_chunks=0,
                thesis_topic="vascular coagulation angiogenesis HSA",
            )
        )

    assert repo.list_research_programs(limit=10) == []
    failed = repo.list_agent_runs(agent_name="research_program_board_agent", status="failed", limit=10)
    assert failed
    assert "Invalid research program board JSON" in failed[0].errors[0]


def test_x_linked_article_followup_contracts_validate():
    request = XLinkedArticleFollowupRequest(
        urls=["https://cancer.ufl.edu/article"],
        approved_by="unit-test",
        max_urls=1,
    )
    result = XLinkedArticleFollowupResult(
        candidate_urls=request.urls,
        primary_source_links=[
            {
                "recommended_source_key": "crossref",
                "identifier_type": "doi",
                "identifier": "10.1234/test",
                "url": "https://doi.org/10.1234/test",
                "should_ingest": True,
            }
        ],
    )

    assert request.robots_policy == "reviewed"
    assert result.source_key == "x_linked_article"
    assert result.candidate_results == []
    assert result.primary_source_links[0]["recommended_source_key"] == "crossref"
    with pytest.raises(ValueError):
        XLinkedArticleFollowupRequest(max_urls=0)
    with pytest.raises(ValueError):
        XLinkedArticleFollowupRequest(robots_policy="bad")


def test_source_followup_and_linked_article_review_contracts_validate():
    item = SourceFollowupQueueItem(
        source_key="crossref",
        identifier_type="doi",
        identifier="10.1234/Test",
        origin_source_key="x_linked_article",
    )
    action = XLinkedArticleReviewAction(
        review_id=uuid4(),
        source_record_id="article-1",
        action="queue_primary_source_followup",
        severity="watch",
        reason="Primary DOI found.",
        followup_links=[
            XTopicLinkedSource(
                url="https://doi.org/10.1234/test",
                recommended_source_key="crossref",
                identifier_type="doi",
                identifier="10.1234/test",
                should_ingest=True,
                reason="DOI.",
            )
        ],
    )
    result = XLinkedArticleReviewResult(actions=[action])

    assert item.identifier == "10.1234/test"
    assert item.identity_key == "crossref:doi:10.1234/test"
    tracked_item = SourceFollowupQueueItem(
        source_key="crossref",
        identifier_type="doi",
        identifier="10.3389/fvets.2026.1778366?utm_source=twitter#section",
    )
    assert tracked_item.identifier == "10.3389/fvets.2026.1778366"
    assert SourceFollowupQueueRequest().source_key == "x_linked_article"
    assert SourceFollowupIngestRequest().statuses == ["queued", "approved"]
    assert XLinkedArticleReviewRequest().review_mode == "openrouter_required"
    assert result.actions[0].action == "queue_primary_source_followup"
    with pytest.raises(ValueError):
        SourceFollowupQueueItem(source_key="crossref", identifier_type="bad", identifier="x")
    with pytest.raises(ValueError):
        XLinkedArticleReviewAction(review_id=uuid4(), source_record_id="x", action="bad", severity="watch", reason="bad")


def test_research_followup_resolver_contracts_validate():
    lead_id = uuid4()
    request = ResearchFollowupResolverRequest(
        lead_ids=[lead_id],
        statuses=["followup"],
        search_source_keys=["pubmed"],
        limit=1,
        min_evidence_chunks=1,
    )
    lead_result = ResearchFollowupLeadResult(
        lead_id=lead_id,
        status_before="followup",
        status_after="watching",
        actions=["promoted_to_watching"],
        evidence_refs=["chunk:1"],
        durable_source_keys=["pubmed"],
        promoted=True,
    )
    result = ResearchFollowupResolverResult(
        leads_seen=1,
        promoted_leads=1,
        lead_results=[lead_result],
    )

    assert request.statuses == ["followup"]
    assert request.force_live_search is False
    assert request.inspect_evidence_refs is True
    assert result.lead_results[0].promoted is True
    with pytest.raises(ValueError):
        ResearchFollowupResolverRequest(limit=0)
    with pytest.raises(ValueError):
        ResearchFollowupLeadResult(
            lead_id=lead_id,
            status_before="followup",
            status_after="bad",
        )
    with pytest.raises(ValueError):
        ResearchFollowupLeadResult(
            lead_id=lead_id,
            status_before="followup",
            status_after="watching",
            actions=["bad"],
        )


def test_reward_event_contracts_validate_allowed_values():
    event = RewardEventRecord(
        event_source="operator_review",
        score=0.55,
        dimension_scores={"overall": 0.55, "operator_usefulness": 0.55},
        agent_run_id=uuid4(),
        verdict="needs_followup",
        agent_name="therapy_committee_chair_agent",
        outcome_bucket="actionable_followup",
        routing_recommendation="queue_targeted_followup",
        churn_risk_score=0.25,
    )
    row = RewardReportRow(group_type="agent_name", group_value="therapy_committee_chair_agent", event_count=1)

    assert event.identity_key.startswith("reward:operator_review:agent_run:")
    assert event.outcome_bucket == "actionable_followup"
    assert row.low_sample is False
    assert RewardEventSyncRequest().created_by == "reward_review_sync"
    assert RewardReportRequest().group_by == "agent_name"
    with pytest.raises(ValidationError):
        RewardEventRecord(event_source="bad_source", score=0.5)
    with pytest.raises(ValidationError):
        RewardEventRecord(event_source="operator_review", score=1.5)
    with pytest.raises(ValidationError):
        RewardEventRecord(event_source="operator_review", score=0.5, dimension_scores={"bad_dimension": 0.2})
    with pytest.raises(ValidationError):
        RewardEventRecord(event_source="operator_review", score=0.5, outcome_bucket="maybe")
    with pytest.raises(ValidationError):
        RewardEventRecord(event_source="operator_review", score=0.5, routing_recommendation="auto_mutate")


def test_reward_events_sync_reviews_and_report_are_idempotent(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "reward-events-sync.sqlite3", seed=False)
    service = HSAResearchService(repo)
    now = datetime.now(UTC)
    run_one = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            model_profile="openrouter_required",
            status=RunStatus.COMPLETED,
            source_key="pubmed",
            started_at=now - timedelta(minutes=2),
            metadata={"task_type": "therapy_committee"},
        )
    )
    run_two = repo.create_agent_run(
        AgentRunRecord(
            agent_name="omics_validation_agent",
            model_profile="openrouter_required",
            status=RunStatus.COMPLETED,
            source_key="geo",
            started_at=now - timedelta(minutes=1),
        )
    )
    review_one = repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run_one.agent_run_id,
            reviewer="operator",
            reviewer_type="operator",
            verdict="useful",
            feedback="Strong cited committee output.",
            created_at=now,
        )
    )
    review_two = repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run_two.agent_run_id,
            reviewer="validation_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="needs_followup",
            followup_actions=["queue_targeted_followup_for_geo_matrix_labels"],
            created_at=now,
            metadata={
                "agent_performance_evaluation": {
                    "rubric_scores": {"citation_quality": 4, "provenance_quality": 3, "actionability": 0.5}
                }
            },
        )
    )
    review_three = repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run_two.agent_run_id,
            reviewer="validation_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="bad",
            feedback="No evidence path and no concrete next action; operational dead end.",
            created_at=now + timedelta(seconds=1),
            metadata={"agent_performance_evaluation": {"rubric_scores": {"actionability": 0.1}}},
        )
    )

    first_sync = service.sync_reward_events_from_reviews(RewardEventSyncRequest(limit=10))
    second_sync = service.sync_reward_events_from_reviews(RewardEventSyncRequest(limit=10))
    report = service.build_reward_report(RewardReportRequest(limit=10, group_by="agent_name", min_sample_size=1))
    event = repo.list_reward_events(source_review_id=review_one.review_id, limit=1)[0]
    followup_event = repo.list_reward_events(source_review_id=review_two.review_id, limit=1)[0]
    churn_event = repo.list_reward_events(source_review_id=review_three.review_id, limit=1)[0]
    therapy_row = next(row for row in report.rows if row.group_value == "therapy_committee_chair_agent")
    omics_row = next(row for row in report.rows if row.group_value == "omics_validation_agent")

    assert first_sync.created_count == 3
    assert second_sync.created_count == 0
    assert second_sync.skipped_existing_count == 3
    assert event.score == 1.0
    assert event.outcome_bucket == "positive_signal"
    assert event.dimension_scores["operator_usefulness"] == 1.0
    assert followup_event.outcome_bucket == "actionable_followup"
    assert followup_event.routing_recommendation == "queue_targeted_followup"
    assert churn_event.outcome_bucket == "negative_signal"
    assert churn_event.routing_recommendation == "suppress_or_archive"
    assert report.event_count == 3
    assert report.reward_score == 52
    assert report.outcome_counts == {
        "actionable_followup": 1,
        "negative_signal": 1,
        "positive_signal": 1,
    }
    assert therapy_row.reward_score == 100
    assert therapy_row.low_sample is False
    assert omics_row.actionable_followup_count == 1
    assert omics_row.negative_signal_count == 1
    assert omics_row.actionable_followup_rate == 0.5


def test_research_followup_refinement_creates_refined_source_queries(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-refinement.sqlite3", seed=False)
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine DLT",
            status="followup",
            priority=5,
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["pubmed"],
            topic_tags=["sorafenib", "canine", "safety", "mtd", "dlt"],
            metadata={"created_by": "agent_finding_escalation_agent"},
        )
    )
    resolver_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="research_followup_resolver_agent",
            status=RunStatus.COMPLETED,
            source_key="pubmed",
            output_payload={
                "lead_results": [
                    {
                        "lead_id": str(lead.lead_id),
                        "title": lead.title,
                        "durable_source_keys": ["pubmed"],
                        "metadata": {"evidence_fit": {"fit": "weak", "missing_terms": ["sorafenib"]}},
                    }
                ]
            },
        )
    )
    evaluator_review = repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=resolver_run.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="needs_followup",
            feedback="Retry PubMed with more specific sorafenib canine DLT terms.",
            followup_actions=[
                "retry_pubmed_search_with_refined_terms:_'sorafenib_canine_dose-limiting_toxicity'_or_'sorafenib_dog_maximum_tolerated_dose'",
                "search_for_known_sorafenib_veterinary_oncology_studies_(e.g.,_robat_et_al._2012,_london_et_al.)_by_pmid_for_direct_ingestion",
                "increase_limit_per_source_to_at_least_5_and_add_source_keys_veterinary_databases_e.g._cab_abstracts_vetmed_resource",
                "check whether sorafenib is indexed under an alternative identifier and add that identifier to the next pass",
            ],
        )
    )

    result = service.refine_research_followups(
        ResearchFollowupRefinementRequest(lead_ids=[lead.lead_id], operator="operator")
    )
    queries = repo.list_source_queries(active_only=True)
    refinement_run = repo.get_agent_run(result.agent_run_id)

    assert result.scanned_count == 1
    assert result.lead_count == 1
    assert result.source_queries_created >= 2
    assert result.query_count >= 2
    assert any("sorafenib canine dose-limiting toxicity" in query.query_text for query in queries)
    assert any("robat" in query.query_text.lower() for query in queries)
    assert not any("increase limit" in query.query_text.lower() for query in queries)
    assert not any("check whether" in query.query_text.lower() for query in queries)
    assert any(skip["reason"] == "operational_recommendation_not_query" for skip in result.skipped)
    assert all(query.query_params["followup_lane"] == "agent_evaluator_followup" for query in queries)
    assert all(query.query_params["comparative_policy"] == "disabled" for query in queries)
    europe_pmc_queries = [query for query in queries if query.source_key == "europe_pmc"]
    if europe_pmc_queries:
        assert all(query.query_params["fetch_full_text"] is True for query in europe_pmc_queries)
        assert all(query.query_params["full_text_time_budget_seconds"] == 20 for query in europe_pmc_queries)
    assert all(query.query_params["origin_review_id"] == str(origin_review_id) for query in queries)
    assert all(query.query_params["origin_agent_run_id"] == str(origin_agent_run_id) for query in queries)
    assert all(query.query_params["origin_evaluator_review_id"] == str(evaluator_review.review_id) for query in queries)
    assert any("sorafenib" in query.query_params["required_terms"] for query in queries)
    safe_params = source_query_params.source_safe_query_params(queries[0])
    assert "origin_evaluator_review_id" not in safe_params
    assert "why_this_query_exists" not in safe_params
    assert refinement_run is not None
    assert refinement_run.summary["source_queries_created"] == result.source_queries_created


def test_research_followup_refinement_respects_explicit_source_filter_and_rejects_meta_queries(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-refinement-filter.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: CA-4F12-E6 canine safety",
            status="followup",
            priority=5,
            suggested_sources=["pubmed"],
            topic_tags=["canine", "safety", "hemangiosarcoma"],
        )
    )
    resolver_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="research_followup_resolver_agent",
            status=RunStatus.COMPLETED,
            output_payload={"lead_results": [{"lead_id": str(lead.lead_id), "title": lead.title}]},
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=resolver_run.agent_run_id,
            reviewer_type="llm_evaluator",
            verdict="needs_followup",
            followup_actions=[
                "retry with decomposed search terms: search 'canine anti-pd-1' and 'dog pd-1 checkpoint inhibitor hemangiosarcoma' separately across pubmed and europe pmc",
                "search clinicaltrials.gov and AVMA abstracts for CA-4F12-E6 sponsor pipeline disclosures",
                "check whether CA-4F12-E6 is indexed under an alternative identifier and add that identifier to the search",
                "reconsider lead promotion to watching until compound-specific safety data is confirmed",
            ],
        )
    )

    result = service.refine_research_followups(
        ResearchFollowupRefinementRequest(
            lead_ids=[lead.lead_id],
            source_keys=["pubmed", "europe_pmc"],
            max_queries_per_review=10,
            operator="operator",
        )
    )
    queries = repo.list_source_queries(active_only=True)

    assert result.query_count == 4
    assert {query.source_key for query in queries} == {"pubmed", "europe_pmc"}
    assert any("canine anti-pd-1" in query.query_text for query in queries)
    assert any("dog pd-1 checkpoint inhibitor hemangiosarcoma" in query.query_text for query in queries)
    assert not any(query.source_key == "clinicaltrials_gov" for query in queries)
    assert not any("check whether" in query.query_text.lower() for query in queries)
    assert not any("watching" in query.query_text.lower() for query in queries)
    assert any(skip["reason"] == "operational_recommendation_not_query" for skip in result.skipped)


def test_research_followup_refinement_handles_brief_quality_leads_without_review(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-refinement-brief-quality.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title=(
                "Strengthen citation provenance: mTOR inhibition in canine HSA "
                "and human angiosarcoma"
            ),
            status="followup",
            priority=20,
            source_key="pubmed",
            origin_source_key="research_brief_quality",
            origin_record_id=str(uuid4()),
            reason="Citations lack PMIDs, DOIs, and publication years.",
            summary="Strengthen provenance before promotion.",
            suggested_sources=["pubmed", "europe_pmc", "openalex", "crossref"],
            topic_tags=["citation_provenance_repair", "canine_hemangiosarcoma"],
            metadata={
                "research_followup_queue": {
                    "followup_kind": "citation_provenance_repair",
                    "requires_manual_research": False,
                    "topic": (
                        "Explicit negative or contradictory findings for mTOR inhibition "
                        "in canine HSA or human angiosarcoma."
                    ),
                    "feedback_items": [
                        {"source": "weakness", "text": "Citations lack PMIDs, DOIs, and publication years."},
                        {"source": "recommendation", "text": "Require citations to include PMIDs/DOIs/years."},
                    ],
                }
            },
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="agent_refine_old_bad_meta_query",
            query_text="citations lack pmids dois years require citations include pmids dois years",
            query_params={
                "followup_lane": "agent_evaluator_followup",
                "lead_id": str(lead.lead_id),
                "refinement_source": "llm_evaluator_followup_action",
            },
            track="validation_gap",
            object_type=ResearchObjectType.PUBLICATION,
            active=True,
        )
    )

    result = service.refine_research_followups(
        ResearchFollowupRefinementRequest(
            lead_ids=[lead.lead_id],
            source_keys=["pubmed", "europe_pmc"],
            max_queries_per_review=10,
            operator="operator",
        )
    )
    queries = repo.list_source_queries(active_only=True)

    assert result.scanned_count == 1
    assert result.lead_count == 1
    assert result.source_queries_created == 2
    assert result.source_queries_deactivated == 1
    assert {query.source_key for query in queries} == {"pubmed", "europe_pmc"}
    assert all("mtor" in query.query_text.lower() for query in queries)
    assert any("canine" in query.query_text.lower() for query in queries)
    assert all(query.query_params["origin_review_id"] for query in queries)
    assert all(query.query_params["origin_agent_run_id"] for query in queries)
    all_queries = repo.list_source_queries(active_only=False)
    old_query = next(query for query in all_queries if query.query_name == "agent_refine_old_bad_meta_query")
    assert old_query.active is False


def test_command_center_web_refines_research_followup_payload(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-refine-followup.sqlite3", seed=False)
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine DLT",
            status="followup",
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["pubmed"],
            topic_tags=["sorafenib", "canine", "safety", "mtd", "dlt"],
            metadata={
                "created_by": "agent_finding_escalation_agent",
                "research_followup_loop": {"verdict": "needs_followup"},
            },
        )
    )
    resolver_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="research_followup_resolver_agent",
            status=RunStatus.COMPLETED,
            output_payload={"lead_results": [{"lead_id": str(lead.lead_id), "title": lead.title}]},
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=resolver_run.agent_run_id,
            reviewer_type="llm_evaluator",
            verdict="needs_followup",
            followup_actions=["retry_pubmed_search_with_refined_terms:_'sorafenib_canine_dlt_mtd'"],
        )
    )

    payload = command_center_web.refine_research_followup_payload(
        service,
        str(lead.lead_id),
        {"operator": "operator"},
    )
    action_payload = command_center_web.build_action_items_payload(service, {"limit": ["5"]})
    lead_item = next(item for item in action_payload["items"] if item["item_id"] == str(lead.lead_id))

    assert payload["source_queries_created"] >= 1
    assert "create_refined_queries" in lead_item["actions"]


def test_research_brief_contracts_require_known_citations():
    citation = ResearchBriefCitation(
        citation_id="C1",
        chunk_id=uuid4(),
        research_object_id=uuid4(),
        quote="Canine hemangiosarcoma evidence quote.",
    )
    finding = ResearchBriefFinding(
        claim="VEGF biology is relevant enough to brief.",
        stance="supporting",
        citations=["C1"],
        evidence_strength="medium",
        reasoning="The finding is tied to a stored citation.",
    )

    report = ResearchBriefPerspectiveReport(
        perspective="evidence_scout",
        agent_name="evidence_scout_agent",
        summary="Evidence was found.",
        findings=[finding],
        citations=[citation],
    )
    result = ResearchBriefResult(
        topic="VEGF therapy",
        disease_scope="canine hemangiosarcoma",
        perspective_reports=[report],
        final_brief="The stored evidence supports review [C1].",
        ranked_hypotheses=[finding],
        citations=[citation],
    )

    assert result.final_brief.endswith("[C1].")
    brief_record = ResearchBriefRecord(
        agent_run_id=uuid4(),
        agent_run_ids=[uuid4()],
        topic="VEGF therapy",
        disease_scope="canine hemangiosarcoma",
        source_key="pubmed",
        review_mode="deterministic_only",
        final_brief=result.final_brief,
        result_payload=result.model_dump(mode="json"),
        citation_count=1,
        finding_count=1,
        hypothesis_count=1,
    )
    assert brief_record.status == "completed"
    assert brief_record.citation_count == 1
    assert brief_record.hard_error_count == 0
    assert brief_record.evidence_limitation_count == 0
    with pytest.raises(ValueError):
        ResearchBriefPerspectiveReport(
            perspective="evidence_scout",
            agent_name="evidence_scout_agent",
            summary="Bad citation.",
            findings=[finding.model_copy(update={"citations": ["C2"]})],
            citations=[citation],
        )
    with pytest.raises(ValueError):
        ResearchBriefResult(
            topic="VEGF therapy",
            disease_scope="canine hemangiosarcoma",
            final_brief="The stored evidence supports review.",
            citations=[citation],
        )


def test_research_brief_finding_truncates_model_citation_overflow():
    finding = ResearchBriefFinding(
        claim="A model may cite too many supporting snippets for one finding.",
        stance="supporting",
        citations=[f"C{index}" for index in range(1, 12)],
        evidence_strength="medium",
        reasoning="The contract should preserve the finding and keep a bounded citation set.",
    )

    assert finding.citations == [f"C{index}" for index in range(1, 11)]
    assert finding.metadata["citation_truncation"] == {
        "original_count": 11,
        "kept_count": 10,
        "dropped_citations": ["C11"],
    }


def test_research_brief_record_splits_legacy_errors_from_evidence_limitations():
    record = ResearchBriefRecord(
        topic="VEGF therapy",
        disease_scope="canine hemangiosarcoma",
        source_key="pubmed",
        review_mode="openrouter_required",
        final_brief="The stored evidence supports review [C1].",
        result_payload={
            "errors": [
                "No supplied citation directly addresses survival outcome; evidence is indirect.",
                "OpenRouter request failed: timeout",
            ]
        },
        error_count=2,
    )

    assert record.hard_error_count == 1
    assert record.evidence_limitation_count == 1


def test_research_brief_evaluation_contract_rejects_invalid_values():
    result = ResearchBriefEvaluationResult(
        brief_id=uuid4(),
        topic="VEGF therapy",
        overall_score=0.8,
        citation_coverage_score=0.8,
        perspective_balance_score=0.8,
        contradiction_handling_score=0.8,
        novelty_score=0.8,
        actionability_score=0.8,
        weakness_transparency_score=0.8,
        passes_quality_bar=True,
        readiness="ready_for_hypothesis_review",
    )
    record = ResearchBriefEvaluationRecord(
        evaluation_id=result.evaluation_id,
        brief_id=result.brief_id,
        topic=result.topic,
        overall_score=result.overall_score,
        passes_quality_bar=result.passes_quality_bar,
        readiness=result.readiness,
        result_payload=result.model_dump(mode="json"),
    )

    assert record.readiness == "ready_for_hypothesis_review"
    with pytest.raises(ValueError):
        ResearchBriefEvaluationResult(
            brief_id=uuid4(),
            topic="VEGF therapy",
            overall_score=1.2,
            citation_coverage_score=0.8,
            perspective_balance_score=0.8,
            contradiction_handling_score=0.8,
            novelty_score=0.8,
            actionability_score=0.8,
            weakness_transparency_score=0.8,
            readiness="ready_for_hypothesis_review",
        )
    with pytest.raises(ValueError):
        ResearchBriefEvaluationRecord(
            brief_id=uuid4(),
            topic="VEGF therapy",
            overall_score=0.5,
            readiness="not_real",
        )


def test_research_brief_evaluation_openrouter_judge_uses_model_payload(monkeypatch):
    citation = ResearchBriefCitation(
        citation_id="C1",
        chunk_id=uuid4(),
        research_object_id=uuid4(),
        source_key="pubmed",
        title="PD-1 and VEGFR-2 translational evidence",
        quote="PD-1 and VEGFR-2 evidence supports a testable translational hypothesis.",
    )
    finding = ResearchBriefFinding(
        claim="PD-1 plus VEGFR-2 blockade has a testable translational rationale.",
        stance="supporting",
        citations=["C1"],
        evidence_strength="medium",
        reasoning="The cited evidence connects immune checkpoint and angiogenic biology.",
    )
    brief_payload = ResearchBriefResult(
        topic="PD-1 plus VEGFR-2 in canine hemangiosarcoma",
        disease_scope="canine hemangiosarcoma and human angiosarcoma",
        final_brief="The synthesis is actionable and cited [C1].",
        citations=[citation],
        perspective_reports=[
            ResearchBriefPerspectiveReport(
                perspective="evidence_scout",
                agent_name="evidence_scout_agent",
                summary="Evidence scout found cited rationale.",
                findings=[finding],
                citations=[citation],
            ),
            ResearchBriefPerspectiveReport(
                perspective="translational_hypothesis",
                agent_name="translational_hypothesis_agent",
                summary="Translational hypothesis is testable.",
                findings=[finding],
                citations=[citation],
            ),
            ResearchBriefPerspectiveReport(
                perspective="skeptic_validation",
                agent_name="skeptic_validation_agent",
                summary="Skeptic view flagged no blocking contradiction.",
                findings=[finding.model_copy(update={"stance": "uncertain"})],
                citations=[citation],
            ),
        ],
        ranked_hypotheses=[finding],
        unresolved_questions=["Confirm effect size and assay readout."],
    )
    brief = ResearchBriefRecord(
        topic=brief_payload.topic,
        disease_scope=brief_payload.disease_scope,
        source_key="pubmed",
        final_brief=brief_payload.final_brief,
        citation_count=1,
        finding_count=3,
        hypothesis_count=1,
        result_payload=brief_payload.model_dump(mode="json"),
    )

    def fake_openrouter(model_name, review_payload):
        assert model_name == "test/model"
        assert review_payload["brief"]["brief_id"] == str(brief.brief_id)
        return {
            "text": json.dumps(
                {
                    "overall_score": 0.91,
                    "citation_coverage_score": 0.9,
                    "perspective_balance_score": 0.88,
                    "contradiction_handling_score": 0.82,
                    "novelty_score": 0.86,
                    "actionability_score": 0.93,
                    "weakness_transparency_score": 0.78,
                    "passes_quality_bar": True,
                    "readiness": "ready_for_hypothesis_review",
                    "strengths": ["Cited and actionable."],
                    "weaknesses": ["Needs assay confirmation."],
                    "recommendations": ["Promote into validation planning."],
                    "evidence": {"agent_review_summary": "Model judged the brief ready."},
                    "errors": [],
                }
            ),
            "metadata": {"provider": "openrouter", "requested_model": model_name},
        }

    monkeypatch.setattr(research_brief_evaluation, "_openrouter_review_model", fake_openrouter)

    result = research_brief_evaluation.evaluate_research_brief_synthesis(
        brief,
        ResearchBriefEvaluationRequest(
            brief_id=brief.brief_id,
            review_mode="openrouter_required",
            review_models=["test/model"],
        ),
    )

    assert result.overall_score == 0.91
    assert result.passes_quality_bar is True
    assert result.readiness == "ready_for_hypothesis_review"
    assert result.evidence["model_review"]["requested_model"] == "test/model"
    assert result.evidence["deterministic_floor"]["brief_id"] == str(brief.brief_id)


def test_research_brief_evaluation_requires_ready_readiness_to_pass_quality_bar():
    brief = ResearchBriefRecord(
        topic="Toceranib PK evidence",
        disease_scope="canine hemangiosarcoma and human angiosarcoma",
        source_key="pubmed",
        final_brief="Evidence is incomplete [C1].",
        citation_count=1,
        finding_count=1,
        hypothesis_count=1,
    )
    deterministic = ResearchBriefEvaluationResult(
        brief_id=brief.brief_id,
        topic=brief.topic,
        source_key=brief.source_key,
        overall_score=1.0,
        citation_coverage_score=1.0,
        perspective_balance_score=1.0,
        contradiction_handling_score=1.0,
        novelty_score=1.0,
        actionability_score=1.0,
        weakness_transparency_score=1.0,
        passes_quality_bar=True,
        readiness="ready_for_hypothesis_review",
    )

    result = research_brief_evaluation._evaluation_from_model(
        brief,
        ResearchBriefEvaluationRequest(brief_id=brief.brief_id, review_mode="openrouter_required"),
        deterministic,
        {
            "text": json.dumps(
                {
                    "overall_score": 0.76,
                    "citation_coverage_score": 0.7,
                    "perspective_balance_score": 0.7,
                    "contradiction_handling_score": 0.7,
                    "novelty_score": 0.7,
                    "actionability_score": 0.7,
                    "weakness_transparency_score": 0.7,
                    "passes_quality_bar": True,
                    "readiness": "needs_more_evidence",
                    "strengths": [],
                    "weaknesses": ["Primary question remains indirect."],
                    "recommendations": ["Run a narrower evidence search."],
                    "evidence": {},
                    "errors": [],
                }
            ),
            "metadata": {"provider": "openrouter"},
        },
    )

    assert result.readiness == "needs_more_evidence"
    assert result.passes_quality_bar is False
    assert "model_quality_bar_overridden" in result.evidence


def test_research_brief_followup_queue_contracts_validate():
    lead = ResearchLeadRecord(
        identity_key="research_lead:brief_followup:test",
        title="Follow up evidence limitation",
        status="followup",
        evidence_refs=["research_brief:abc"],
    )
    result = ResearchBriefFollowupQueueResult(
        candidate_brief_count=1,
        limitation_count=1,
        queued_count=1,
        followup_leads=[lead],
    )

    assert ResearchBriefFollowupQueueRequest(limit=25).max_limitations_per_brief == 20
    assert ResearchBriefFollowupQueueRequest(limit=25).force is False
    assert ResearchBriefFollowupQueueRequest(brief_ids=[lead.lead_id, lead.lead_id]).brief_ids == [lead.lead_id]
    assert result.followup_leads[0].status == "followup"
    with pytest.raises(ValueError):
        ResearchBriefFollowupQueueRequest(max_limitations_per_brief=0)


def test_research_brief_operator_doc_contracts_validate():
    brief_id = uuid4()
    request = ResearchBriefOperatorDocRequest(brief_ids=[brief_id, brief_id], operator=" ")
    assert request.brief_ids == [brief_id]
    assert request.operator == "research_brief_operator_doc"
    assert request.status == "completed"

    result = ResearchBriefOperatorDocResult(document_count=1, artifact_count=1)
    assert result.document_count == 1

    with pytest.raises(ValidationError):
        ResearchBriefOperatorDocRequest(max_hypotheses=0)


def test_research_brief_operator_doc_persists_plain_language_artifact(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    brief = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="Review research lead: Toceranib monotherapy in canine HSA | Reason: test",
            disease_scope="canine hemangiosarcoma",
            source_key="pubmed",
            final_brief="## Evidence Scout\nToceranib has not shown direct monotherapy outcome evidence. [C1]",
            result_payload={
                "ranked_hypotheses": [
                    {
                        "claim": "No direct toceranib monotherapy cohort was found for canine splenic HSA.",
                        "stance": "risk",
                        "citations": ["C1"],
                        "evidence_strength": "medium",
                        "reasoning": "The available record describes maintenance therapy, not monotherapy.",
                    }
                ],
                "unresolved_questions": ["Do gray-literature veterinary cohorts exist?"],
                "evidence_limitations": [
                    "No veterinary conference abstract or registry search is represented in the supplied evidence."
                ],
                "citations": [
                    {
                        "citation_id": "C1",
                        "title": "Toceranib maintenance study",
                        "source_key": "pubmed",
                        "source_url": "https://example.test/study",
                        "quote": "Toceranib maintenance did not improve outcomes.",
                    }
                ],
            },
            citation_count=1,
            finding_count=1,
            hypothesis_count=1,
            unresolved_question_count=1,
        )
    )

    result = HSAResearchService(repo).create_research_brief_operator_docs(
        ResearchBriefOperatorDocRequest(brief_ids=[brief.brief_id], dry_run=False)
    )

    assert result.document_count == 1
    assert result.artifact_count == 1
    document = result.documents[0]
    artifact = repo.get_artifact(result.artifacts[0].artifact_id)
    assert document.title == "Toceranib monotherapy in canine HSA"
    assert "## Plain-language summary" in document.markdown
    assert "No direct toceranib monotherapy cohort" in document.markdown
    assert artifact.artifact_type == "research_brief_operator_markdown"
    assert artifact.metadata["brief_id"] == str(brief.brief_id)
    assert artifact.metadata["technical_footnote_count"] >= 1


def test_research_brief_quality_report_joins_latest_evaluations(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "research-brief-quality.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        service = HSAResearchService(repo)
        ready_brief = repo.upsert_research_brief(
            ResearchBriefRecord(
                agent_run_id=uuid4(),
                topic="VEGF therapy in canine hemangiosarcoma",
                disease_scope="canine hemangiosarcoma and human angiosarcoma",
                source_key="pubmed",
                review_mode="openrouter_required",
                final_brief="Stored synthesis [C1].",
                citation_count=3,
                finding_count=2,
                hypothesis_count=1,
                result_payload={
                    "errors": [
                        "No supplied citation directly addresses clinical trial outcome; evidence is indirect."
                    ]
                },
                error_count=1,
                metadata={"review_models": ["anthropic/claude-sonnet-test"]},
            )
        )
        failed_brief = repo.upsert_research_brief(
            ResearchBriefRecord(
                topic="Evidence-light linked article",
                disease_scope="canine hemangiosarcoma and human angiosarcoma",
                source_key="x_linked_article",
                status="failed",
                review_mode="openrouter_required",
                error_count=4,
            )
        )
        followup_brief = repo.upsert_research_brief(
            ResearchBriefRecord(
                topic="Conference-only angiosarcoma lead",
                disease_scope="canine hemangiosarcoma and human angiosarcoma",
                source_key="x_linked_article",
                status="completed",
                review_mode="openrouter_required",
                result_payload={
                    "evidence_limitations": [
                        "Only a conference abstract was supplied; find durable peer-reviewed evidence."
                    ],
                    "errors": [],
                },
                evidence_limitation_count=1,
            )
        )
        repo.upsert_research_brief_evaluation(
            ResearchBriefEvaluationRecord(
                brief_id=ready_brief.brief_id,
                agent_run_id=uuid4(),
                topic=ready_brief.topic,
                source_key="pubmed",
                overall_score=0.88,
                passes_quality_bar=True,
                readiness="ready_for_hypothesis_review",
                summary={"overall_score": 0.88},
                result_payload={"recommendations": ["Promote to validation."]},
            )
        )

        report = service.build_research_brief_quality_report(
            ResearchBriefQualityReportRequest(limit=10)
        )
        rows_by_id = {row.brief_id: row for row in report.rows}

        assert report.brief_count == 3
        assert report.evaluated_count == 1
        assert report.ready_count == 1
        assert report.failed_count == 1
        assert report.followup_count == 1
        assert report.average_overall_score == pytest.approx(0.88)
        assert rows_by_id[ready_brief.brief_id].quality_status == "ready_for_validation"
        assert rows_by_id[ready_brief.brief_id].review_models == ["anthropic/claude-sonnet-test"]
        assert rows_by_id[ready_brief.brief_id].error_count == 0
        assert rows_by_id[ready_brief.brief_id].hard_error_count == 0
        assert rows_by_id[ready_brief.brief_id].evidence_limitation_count == 1
        assert rows_by_id[failed_brief.brief_id].quality_status == "brief_failed"
        assert rows_by_id[followup_brief.brief_id].quality_status == "needs_followup_research"


def test_research_brief_followup_queue_creates_idempotent_followup_leads(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-followup-queue.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="AACR angiosarcoma abstract",
            disease_scope="canine hemangiosarcoma and human angiosarcoma",
            source_key="x_linked_article",
            status="completed",
            review_mode="openrouter_required",
            result_payload={
                "evidence_limitations": [
                    "Only a conference abstract was supplied; find durable peer-reviewed evidence.",
                    "No PMID, DOI, PMCID, or NCT identifier was available.",
                ],
                "errors": [],
            },
            evidence_limitation_count=2,
        )
    )

    result = service.queue_research_brief_followups(
        ResearchBriefFollowupQueueRequest(limit=10)
    )
    rerun = service.queue_research_brief_followups(
        ResearchBriefFollowupQueueRequest(limit=10)
    )
    leads = repo.list_research_leads(status="followup", limit=10)

    assert result.candidate_brief_count == 1
    assert result.limitation_count == 2
    assert result.queued_count == 2
    assert result.existing_count == 0
    assert rerun.queued_count == 0
    assert rerun.existing_count == 2
    assert len(leads) == 2
    assert {lead.origin_record_id for lead in leads} == {str(brief.brief_id)}
    assert all(f"research_brief:{brief.brief_id}" in lead.evidence_refs for lead in leads)
    assert all(lead.metadata["research_followup_queue"]["origin"] == "research_brief_quality" for lead in leads)


def test_research_brief_followup_queue_force_routes_completed_unevaluated_gap(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-force-followup.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="Toceranib monotherapy in canine splenic HSA",
            disease_scope="canine hemangiosarcoma and human angiosarcoma",
            source_key="europe_pmc",
            status="completed",
            review_mode="openrouter_required",
            final_brief="Stored synthesis [C1].",
            citation_count=3,
            finding_count=2,
            hypothesis_count=1,
            result_payload={
                "evidence_limitations": [
                    "No prospective toceranib monotherapy cohort was found in supplied citations."
                ],
                "errors": [],
            },
            evidence_limitation_count=1,
        )
    )

    skipped = service.queue_research_brief_followups(
        ResearchBriefFollowupQueueRequest(brief_ids=[brief.brief_id], include_evaluations=False)
    )
    forced = service.queue_research_brief_followups(
        ResearchBriefFollowupQueueRequest(
            brief_ids=[brief.brief_id],
            include_evaluations=False,
            force=True,
        )
    )

    assert skipped.queued_count == 0
    assert skipped.skipped[0]["quality_status"] == "needs_evaluation"
    assert forced.queued_count == 1
    assert forced.followup_leads[0].reason.startswith("No prospective toceranib monotherapy cohort")


def test_research_brief_followup_queue_routes_failed_evaluation_feedback(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-eval-followup-queue.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief = repo.upsert_research_brief(
        ResearchBriefRecord(
            agent_run_id=uuid4(),
            topic="Toceranib monotherapy in canine splenic HSA",
            disease_scope="canine hemangiosarcoma and human angiosarcoma",
            source_key="pubmed",
            status="completed",
            review_mode="openrouter_required",
            final_brief="Stored synthesis [C1].",
            citation_count=4,
            finding_count=3,
            hypothesis_count=1,
            result_payload={"errors": []},
        )
    )
    evaluation = repo.upsert_research_brief_evaluation(
        ResearchBriefEvaluationRecord(
            brief_id=brief.brief_id,
            agent_run_id=uuid4(),
            topic=brief.topic,
            source_key="pubmed",
            overall_score=0.78,
            passes_quality_bar=False,
            readiness="needs_human_review",
            result_payload={
                "weaknesses": [
                    "C2 and C3 are duplicate citations and inflate apparent citation coverage.",
                    "No citation metadata (DOI, PMID, year) is available, making claim verification impossible.",
                ],
                "recommendations": [
                    "Create a focused evidence-acquisition plan for toceranib monotherapy cohorts in canine splenic HSA.",
                    "Require retrieval of at least one citation with PMID or DOI before promotion.",
                ],
                "evidence": {
                    "notable_risks": [
                        "Toceranib is a multi-kinase inhibitor; do not conflate it with selective VEGFR-2 blockade.",
                    ]
                },
            },
        )
    )

    result = service.queue_research_brief_followups(
        ResearchBriefFollowupQueueRequest(evaluation_ids=[evaluation.evaluation_id], max_limitations_per_brief=10)
    )
    rerun = service.queue_research_brief_followups(
        ResearchBriefFollowupQueueRequest(evaluation_ids=[evaluation.evaluation_id], max_limitations_per_brief=10)
    )
    leads = repo.list_research_leads(status="followup", limit=10)
    followup_kinds = {
        lead.metadata["research_followup_queue"]["followup_kind"]
        for lead in leads
    }

    assert result.candidate_brief_count == 1
    assert result.queued_count == 3
    assert rerun.queued_count == 0
    assert rerun.existing_count == 3
    assert followup_kinds == {
        "citation_dedupe_repair",
        "citation_provenance_repair",
        "focused_evidence_acquisition",
    }
    focused = next(
        lead
        for lead in leads
        if lead.metadata["research_followup_queue"]["followup_kind"] == "focused_evidence_acquisition"
    )
    assert "pubmed" in focused.suggested_sources
    assert "clinicaltrials_gov" in focused.suggested_sources
    assert f"research_brief_evaluation:{evaluation.evaluation_id}" in focused.evidence_refs


def test_research_brief_followup_queue_keeps_focused_evidence_topic_specific(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-vim-followup-queue.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief = repo.upsert_research_brief(
        ResearchBriefRecord(
            agent_run_id=uuid4(),
            topic="VIM/vimentin target availability in canine splenic HSA",
            disease_scope="canine hemangiosarcoma and human angiosarcoma",
            status="completed",
            review_mode="openrouter_required",
            final_brief="Stored synthesis [C1].",
            citation_count=4,
            finding_count=3,
            hypothesis_count=1,
            result_payload={"errors": []},
        )
    )
    evaluation = repo.upsert_research_brief_evaluation(
        ResearchBriefEvaluationRecord(
            brief_id=brief.brief_id,
            agent_run_id=uuid4(),
            topic=brief.topic,
            overall_score=0.52,
            passes_quality_bar=False,
            readiness="needs_more_evidence",
            result_payload={
                "recommendations": [
                    "Initiate targeted retrieval for primary VIM/vimentin expression studies in canine HSA.",
                    "Retrieve the full eVim vaccine trial publication and tumor-level vimentin expression data.",
                ],
            },
        )
    )

    result = service.queue_research_brief_followups(
        ResearchBriefFollowupQueueRequest(evaluation_ids=[evaluation.evaluation_id], max_limitations_per_brief=10)
    )
    lead = result.followup_leads[0]

    assert result.queued_count == 1
    assert lead.metadata["research_followup_queue"]["followup_kind"] == "focused_evidence_acquisition"
    assert "VIM/vimentin" in lead.title
    assert "toceranib" not in lead.title.lower()
    assert "vegfr" not in lead.summary.lower()


def test_research_brief_queue_controls_requeue_and_archive(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "research-brief-queue-controls.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        service = HSAResearchService(repo)
        queued = service.queue_research_brief(
            ResearchBriefQueueRequest(
                topic="Angiogenesis resistance patterns in canine hemangiosarcoma",
                source_key="pubmed",
                priority=50,
            )
        )
        failed = repo.update_research_brief_queue_item(
            queued.queue_item_id,
            status="failed",
            attempts=2,
            last_error="timeout",
        )

        assert failed is not None
        with pytest.raises(ValueError, match="priority must be between 0 and 1000"):
            service.requeue_research_brief_queue_item(failed.queue_item_id, priority=1001)

        requeued = service.requeue_research_brief_queue_item(failed.queue_item_id, priority=5)
        assert requeued is not None
        assert requeued.status == "queued"
        assert requeued.priority == 5
        assert requeued.attempts == 2
        assert requeued.last_error is None
        assert requeued.metadata["queue_control"]["last_action"] == "requeue"
        assert requeued.metadata["queue_control"]["previous_status"] == "failed"

        with pytest.raises(ValueError, match="only completed"):
            service.archive_research_brief_queue_item(requeued.queue_item_id)

        completed = repo.update_research_brief_queue_item(requeued.queue_item_id, status="completed")
        assert completed is not None
        archived = service.archive_research_brief_queue_item(completed.queue_item_id)
        assert archived is not None
        assert archived.status == "archived"
        assert archived.priority == 5
        assert archived.metadata["queue_control"]["last_action"] == "archive"
        assert archived.metadata["queue_control"]["previous_status"] == "completed"
        assert service.archive_research_brief_queue_item(archived.queue_item_id).status == "archived"
        assert service.requeue_research_brief_queue_item(uuid4()) is None


def test_research_brief_queue_maintenance_archives_stale_failed_items(tmp_path):
    for repo in (
        SQLiteResearchRepository(tmp_path / "research-brief-queue-maintenance.sqlite3", seed=False),
        InMemoryResearchRepository(),
    ):
        service = HSAResearchService(repo)
        stale_failed = service.queue_research_brief(
            ResearchBriefQueueRequest(
                topic="Stale linked article angiosarcoma review",
                source_key="x_linked_article",
                priority=80,
            )
        )
        fresh_failed = service.queue_research_brief(
            ResearchBriefQueueRequest(
                topic="Fresh PubMed angiosarcoma review",
                source_key="pubmed",
                priority=80,
            )
        )
        repo.update_research_brief_queue_item(
            stale_failed.queue_item_id,
            status="failed",
            attempts=2,
            last_error="old evidence-light item",
        )
        repo.update_research_brief_queue_item(
            fresh_failed.queue_item_id,
            status="failed",
            attempts=0,
            last_error="fresh failure",
        )

        dry_run = service.maintain_research_brief_queue(
            ResearchBriefQueueMaintenanceRequest(
                statuses=["failed"],
                source_key="x_linked_article",
                min_attempts=1,
                max_updated_age_hours=0,
                dry_run=True,
            )
        )
        assert dry_run.dry_run is True
        assert dry_run.candidate_count == 1
        assert dry_run.archived_count == 0
        assert dry_run.queue_items[0].queue_item_id == stale_failed.queue_item_id
        assert repo.get_research_brief_queue_item(stale_failed.queue_item_id).status == "failed"

        archived = service.maintain_research_brief_queue(
            ResearchBriefQueueMaintenanceRequest(
                statuses=["failed"],
                source_key="x_linked_article",
                min_attempts=1,
                max_updated_age_hours=0,
                dry_run=False,
                reason="superseded_by_pubmed_backed_synthesis",
            )
        )
        updated = repo.get_research_brief_queue_item(stale_failed.queue_item_id)
        untouched = repo.get_research_brief_queue_item(fresh_failed.queue_item_id)

        assert archived.archived_count == 1
        assert archived.queue_items[0].status == "archived"
        assert updated.status == "archived"
        assert updated.last_error == "old evidence-light item"
        assert updated.metadata["queue_control"]["last_action"] == "maintenance_archive"
        assert updated.metadata["queue_control"]["reason"] == "superseded_by_pubmed_backed_synthesis"
        assert untouched.status == "failed"


def test_research_brief_queue_maintenance_rejects_active_statuses():
    with pytest.raises(ValidationError, match="cannot target queued or running"):
        ResearchBriefQueueMaintenanceRequest(statuses=["queued"])


def test_research_brief_queue_batch_from_leads_and_source_health(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-queue-batch.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="VEGF resistance signal in canine hemangiosarcoma",
            lead_type="linked_article",
            status="new",
            priority=20,
            source_key="x_topic",
            origin_source_key="x_topic",
            reason="Agent flagged a durable article for synthesis follow-up.",
            summary="The linked article discusses VEGF resistance and translational therapy signals.",
            topic_tags=["VEGF", "resistance"],
            suggested_sources=["pubmed"],
        )
    )
    source_health_report = {
        "sources": [
            {
                "source_key": "chembl",
                "health_status": "failing",
                "document_chunks": 0,
                "passes_minimum_bar": False,
                "health_score": 0.2,
                "risks": ["No chunks are present."],
                "recommended_actions": ["Run structured source refresh."],
            },
            {
                "source_key": "pubmed",
                "health_status": "triage",
                "document_chunks": 4,
                "passes_minimum_bar": False,
                "health_score": 0.55,
                "risks": ["No promoted claims are present."],
                "recommended_actions": ["Inspect curator decisions."],
            },
        ]
    }

    result = service.queue_research_brief_batch(
        ResearchBriefQueueBatchRequest(
            mode="both",
            source_health_report=source_health_report,
            limit=5,
            priority=80,
        )
    )
    updated_lead = repo.get_research_lead(lead.lead_id)
    origins = {item.metadata["batch_queue"]["origin"] for item in result.queue_items}

    assert result.queued_count == 2
    assert result.lead_count == 1
    assert result.research_followup_count == 0
    assert result.source_health_count == 1
    assert result.skipped_count == 1
    assert result.skipped[0]["source_key"] == "chembl"
    assert origins == {"research_lead", "source_health"}
    assert updated_lead is not None
    assert updated_lead.status == "queued"
    assert updated_lead.metadata["research_brief_queue"]["queue_item_id"]
    assert any(item.source_key == "pubmed" for item in result.queue_items)
    assert any(item.priority == 20 for item in result.queue_items if item.metadata["batch_queue"]["origin"] == "research_lead")
    assert any(item.priority == 45 for item in result.queue_items if item.metadata["batch_queue"]["origin"] == "source_health")


def test_research_brief_queue_batch_routes_evidence_light_leads_to_followup(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-evidence-light-followup.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="UF angiosarcoma AACR report",
            url="https://cancer.ufl.edu/angiosarcoma-report",
            lead_type="institutional_article",
            status="new",
            priority=25,
            source_key="x_linked_article",
            origin_source_key="x_linked_article",
            reason="No DOI, PMID, PMCID, NCT, or suggested durable source was found.",
            topic_tags=["angiosarcoma", "therapy"],
        )
    )

    result = service.queue_research_brief_batch(
        ResearchBriefQueueBatchRequest(mode="research_leads", limit=5)
    )
    updated_lead = repo.get_research_lead(lead.lead_id)

    assert result.queued_count == 0
    assert result.lead_count == 0
    assert result.research_followup_count == 1
    assert result.skipped_count == 1
    assert result.skipped[0]["reason"] == "lead_needs_research_followup"
    assert result.skipped[0]["requires_manual_research"] is True
    assert updated_lead is not None
    assert updated_lead.status == "followup"
    assert updated_lead.metadata["research_followup_queue"]["requires_manual_research"] is True
    assert repo.list_source_followups(limit=10) == []


def test_research_brief_queue_batch_filters_research_leads_by_source_key(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-source-filter.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _seed_minimal_source_claim(repo, "pubmed")
    selected = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Resolved research brief quality follow-up",
            lead_type="unknown",
            status="watching",
            priority=25,
            source_key="pubmed",
            origin_source_key="research_brief_quality",
            suggested_sources=["pubmed"],
            reason="Durable chunks satisfy the prior evidence limitation.",
            evidence_refs=["chunk:pubmed:1"],
        )
    )
    ignored = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Unrelated watchlist lead",
            lead_type="unknown",
            status="watching",
            priority=10,
            source_key="x_linked_article",
            origin_source_key="x_linked_article",
            reason="Not part of the research brief quality follow-up lane.",
            evidence_refs=["chunk:pubmed:2"],
        )
    )

    result = service.queue_research_brief_batch(
        ResearchBriefQueueBatchRequest(
            mode="research_leads",
            lead_statuses=["watching"],
            source_keys=["research_brief_quality"],
            limit=10,
        )
    )

    updated_selected = repo.get_research_lead(selected.lead_id)
    updated_ignored = repo.get_research_lead(ignored.lead_id)

    assert result.queued_count == 1
    assert result.lead_count == 1
    assert result.queue_items[0].metadata["batch_queue"]["lead_id"] == str(selected.lead_id)
    assert updated_selected is not None
    assert updated_selected.status == "queued"
    assert updated_ignored is not None
    assert updated_ignored.status == "watching"


def test_research_brief_queue_batch_routes_identifier_leads_to_source_followup(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-identifier-followup.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Angiosarcoma DOI lead",
            lead_type="linked_article",
            status="new",
            priority=35,
            source_key="x_linked_article",
            origin_source_key="x_linked_article",
            identifiers={"doi": "10.1234/HSA.FOLLOWUP"},
            reason="Agent found a DOI that is not yet ingested.",
        )
    )

    result = service.queue_research_brief_batch(
        ResearchBriefQueueBatchRequest(mode="research_leads", limit=5)
    )
    updated_lead = repo.get_research_lead(lead.lead_id)
    followups = repo.list_source_followups(source_key="crossref", limit=10)

    assert result.queued_count == 0
    assert result.research_followup_count == 1
    assert result.skipped[0]["source_followup_source_key"] == "crossref"
    assert result.skipped[0]["source_followup_identifier_type"] == "doi"
    assert updated_lead is not None
    assert updated_lead.status == "followup"
    assert updated_lead.metadata["research_followup_queue"]["source_followup_source_key"] == "crossref"
    assert len(followups) == 1
    assert followups[0].identifier == "10.1234/hsa.followup"
    assert followups[0].metadata["followup_type"] == "research_lead_evidence_enrichment"


def test_research_followup_resolver_promotes_lead_with_ingested_source_followup(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-ingested.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Angiosarcoma DOI follow-up",
            lead_type="linked_article",
            status="followup",
            priority=20,
            source_key="x_linked_article",
            origin_source_key="x_linked_article",
            identifiers={"doi": "10.1234/HSA.RESOLVED"},
        )
    )
    fetch_run_id = uuid4()
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="crossref",
            source_record_id="10.1234/hsa.resolved",
            source_url="https://doi.org/10.1234/hsa.resolved",
            content_hash="crossref-hsa-resolved",
            raw_payload={"title": "Angiosarcoma durable DOI evidence"},
        ),
        fetch_run_id=fetch_run_id,
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Angiosarcoma durable DOI evidence",
            abstract="Durable source evidence for angiosarcoma follow-up resolution.",
            canonical_url="https://doi.org/10.1234/hsa.resolved",
            source_key="crossref",
            dedupe_key="crossref:10.1234/hsa.resolved",
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Angiosarcoma durable evidence for the DOI follow-up lead.",
            content_hash="crossref-hsa-resolved-chunk",
        )
    )
    followup = repo.upsert_source_followup(
        SourceFollowupQueueItem(
            source_key="crossref",
            identifier_type="doi",
            identifier="10.1234/hsa.resolved",
            status="ingested",
            metadata={
                "research_lead_id": str(lead.lead_id),
                "last_ingestion_report": {
                    "research_objects": 1,
                    "document_chunks": 2,
                    "fetch_run_id": str(fetch_run_id),
                },
            },
        )
    )

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            search_missing_identifiers=False,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.leads_seen == 1
    assert result.promoted_leads == 1
    assert result.lead_results[0].source_followup_ids == [followup.followup_id]
    assert result.lead_results[0].durable_source_keys == ["crossref"]
    assert updated is not None
    assert updated.status == "watching"
    assert updated.source_key == "crossref"
    assert updated.suggested_sources == ["crossref"]
    assert f"source_followup:{followup.followup_id}" in updated.evidence_refs


def test_research_followup_resolver_blocks_promotion_when_candidate_terms_are_missing(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-candidate-gate.sqlite3", seed=False)
    service = HSAResearchService(repo)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="pubmed:canine-hsa-safety",
            content_hash="pubmed-canine-hsa-safety-raw",
            raw_payload={"title": "Canine HSA safety study"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type=ResearchObjectType.PUBLICATION,
            title="Canine hemangiosarcoma safety study",
            abstract="Canine hemangiosarcoma safety and tolerability data.",
            source_key="pubmed",
            dedupe_key="pubmed:canine-hsa-safety",
            raw_record_id=raw_record_id,
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Canine hemangiosarcoma safety and tolerability data without checkpoint inhibitor evidence.",
            content_hash="pubmed-canine-hsa-safety-chunk",
        )
    )
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Safety signal: Anti-PD-1 CA-4F12-E6 safety data in canine HSA",
            lead_type="linked_article",
            status="followup",
            source_key="validation_agent",
            origin_source_key="validation_agent",
            topic_tags=["canine", "hemangiosarcoma", "safety"],
        )
    )
    fetch_run_id = repo.create_fetch_run("pubmed", "candidate-gate")
    repo.upsert_source_followup(
        SourceFollowupQueueItem(
            source_key="pubmed",
            identifier_type="pmid",
            identifier="123456",
            status="ingested",
            metadata={
                "research_lead_id": str(lead.lead_id),
                "last_ingestion_report": {
                    "research_objects": 1,
                    "document_chunks": 1,
                    "fetch_run_id": str(fetch_run_id),
                },
            },
        )
    )

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            ingest_source_followups=False,
            search_missing_identifiers=False,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)
    evidence_fit = result.lead_results[0].metadata["evidence_fit"]

    assert result.promoted_leads == 0
    assert result.kept_in_followup == 1
    assert evidence_fit["fit"] == "weak"
    assert "anti-pd-1" in evidence_fit["missing_terms"]
    assert "ca-4f12-e6" in evidence_fit["missing_terms"]
    assert updated is not None
    assert updated.status == "followup"


def test_research_followup_resolver_keeps_unresolved_lead_in_followup(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-manual.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="UF institutional article without primary source",
            lead_type="institutional_article",
            status="followup",
            source_key="x_linked_article",
            origin_source_key="x_linked_article",
        )
    )

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            search_missing_identifiers=False,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.promoted_leads == 0
    assert result.manual_research_required == 1
    assert "manual_research_required" in result.lead_results[0].actions
    assert result.lead_results[0].metadata["source_followup_ingest"] == {
        "status": "skipped",
        "reason": "no_source_followups_queued_or_linked",
        "source_followup_count": 0,
        "ingestable_count": 0,
        "statuses": [],
    }
    assert updated is not None
    assert updated.status == "followup"
    assert updated.metadata["research_followup_resolver"]["requires_manual_research"] is True


def test_research_followup_resolver_blocks_missing_explicit_lead_ids(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-missing.sqlite3", seed=False)
    service = HSAResearchService(repo)
    missing_lead_id = uuid4()

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(lead_ids=[missing_lead_id])
    )
    run = service.list_agent_runs(agent_name=research_followup_resolver.RESEARCH_FOLLOWUP_RESOLVER_AGENT_NAME, limit=1)[0]

    assert result.blocked is True
    assert result.leads_seen == 0
    assert result.skipped_leads == 1
    assert result.failed_leads == 1
    assert result.unresolved_lead_ids == [missing_lead_id]
    assert result.skip_reasons == [{"lead_id": str(missing_lead_id), "reason": "lead_not_found"}]
    assert "lead_not_found" in result.errors[0]
    assert run.summary["blocked"] is True
    assert run.summary["unresolved_lead_ids"] == 1


def test_research_followup_resolver_blocks_status_filtered_explicit_leads(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-status-filter.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Already promoted lead",
            lead_type="linked_article",
            status="watching",
            source_key="pubmed",
        )
    )

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(lead_ids=[lead.lead_id], statuses=["followup"])
    )

    assert result.blocked is True
    assert result.leads_seen == 0
    assert result.skipped_leads == 1
    assert result.failed_leads == 0
    assert result.unresolved_lead_ids == []
    assert result.skip_reasons[0]["reason"] == "status_not_allowed"
    assert result.skip_reasons[0]["status"] == "watching"
    assert "status_not_allowed" in result.errors[0]


def test_research_followup_resolver_dry_run_reports_planned_identifier_work(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-dry-run.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Dry run DOI follow-up",
            lead_type="linked_article",
            status="followup",
            source_key="x_linked_article",
            origin_source_key="x_linked_article",
            identifiers={"doi": "10.1234/HSA.DRYRUN"},
        )
    )

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            dry_run=True,
            search_missing_identifiers=False,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.dry_run is True
    assert result.source_followups_queued == 0
    assert result.source_followups_ingested == 0
    assert repo.list_source_followups(limit=None) == []
    assert result.lead_results[0].metadata["planned_source_followups"][0]["action"] == "would_queue_source_followup"
    assert result.lead_results[0].metadata["planned_source_followups"][0]["source_key"] == "crossref"
    assert result.lead_results[0].metadata["planned_action"] == "would_mark_manual_research_required"
    assert updated is not None
    assert updated.status == "followup"
    assert "research_followup_resolver" not in updated.metadata


def test_research_followup_resolver_dry_run_reports_planned_promotion(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-dry-run-promotion.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _seed_minimal_source_claim(repo, "pubmed")
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="PubMed source record canine HSA",
            lead_type="linked_article",
            status="followup",
            source_key="x_topic",
            origin_source_key="x_topic",
            topic_tags=["canine", "hemangiosarcoma"],
        )
    )

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            search_source_keys=["pubmed"],
            ingest_source_followups=False,
            search_missing_identifiers=False,
            dry_run=True,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.promoted_leads == 0
    assert result.kept_in_followup == 1
    assert result.lead_results[0].metadata["planned_action"] == "would_promote_to_watching"
    assert result.lead_results[0].durable_source_keys == ["pubmed"]
    assert updated is not None
    assert updated.status == "followup"


def test_research_followup_resolver_expands_safety_gap_query():
    lead = ResearchLeadRecord(
        title="Safety signal: Safety/tolerability profile of sorafenib in dogs at proposed doses (DLT, MTD data)",
        lead_type="unknown",
        status="watching",
        source_key="pubmed",
    )

    query = research_followup_resolver._lead_search_query(lead, max_terms=12)

    assert "sorafenib" in query
    assert "dlt" in query
    assert "mtd" in query
    assert "canine" in query
    assert "veterinary" in query
    assert "dose limiting" in query


def test_research_followup_resolver_removes_operational_words_from_query():
    lead = ResearchLeadRecord(
        title="Find toceranib/VEGFR inhibitor monotherapy outcomes in canine splenic HSA",
        summary=(
            "Run focused evidence acquisition for toceranib or VEGFR inhibitor monotherapy "
            "clinical outcomes in canine splenic hemangiosarcoma."
        ),
        reason="Create a focused evidence-acquisition plan before promotion.",
        lead_type="unknown",
        status="watching",
        source_key="pubmed",
        topic_tags=["research_brief", "evaluation_followup", "focused_evidence_acquisition"],
    )

    query = research_followup_resolver._lead_search_query(lead, max_terms=12)

    assert "toceranib" in query
    assert "vegfr" in query
    assert "monotherapy" in query
    assert "clinical" in query
    assert "canine" in query
    assert "hemangiosarcoma" in query
    assert " run " not in f" {query} "
    assert " focused " not in f" {query} "
    assert " acquisition " not in f" {query} "
    assert " promotion " not in f" {query} "
    assert " the " not in f" {query} "


def test_research_followup_resolver_normalizes_search_query_override():
    lead = ResearchLeadRecord(
        title="Fallback title that should not dominate query override",
        lead_type="unknown",
        status="watching",
        source_key="pubmed",
    )
    request = ResearchFollowupResolverRequest(
        search_query_text="Run focused THE toceranib canine hemangiosarcoma VEGFR monotherapy query",
        max_search_terms=8,
    )

    query = research_followup_resolver._resolver_search_query(lead, request)

    assert query == "toceranib canine hemangiosarcoma vegfr monotherapy query"


def test_research_followup_resolver_force_live_search_refreshes_existing_evidence(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-force-live.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _seed_minimal_source_claim(repo, "pubmed")
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Sorafenib canine toxicity follow-up",
            lead_type="linked_article",
            status="followup",
            source_key="x_topic",
            origin_source_key="x_topic",
            topic_tags=["sorafenib", "toxicity"],
        )
    )
    calls = []

    def fake_search(repository, lead_record, request):
        calls.append((lead_record.lead_id, request.force_live_search, request.search_missing_identifiers))
        _seed_minimal_source_claim(repository, "clinicaltrials_gov")
        return {
            "query_text": "sorafenib canine toxicity",
            "source_keys": request.search_source_keys,
            "limit_per_source": request.search_limit_per_source,
            "reports": [{"source_key": "clinicaltrials_gov", "document_chunks": 1}],
            "errors": [],
        }

    monkeypatch.setattr(research_followup_resolver, "_search_durable_sources", fake_search)

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            search_source_keys=["pubmed", "clinicaltrials_gov"],
            search_missing_identifiers=False,
            force_live_search=True,
            promote_ready_leads=False,
            min_evidence_chunks=1,
        )
    )
    lead_result = result.lead_results[0]

    assert calls == [(lead.lead_id, True, False)]
    assert result.force_live_search is True
    assert result.durable_source_searches == 1
    assert result.evidence_inspections == 1
    assert "searched_durable_sources" in lead_result.actions
    assert lead_result.metadata["durable_source_search"]["force_live_search"] is True
    assert lead_result.metadata["durable_source_search"]["evidence_refs_before_search"] >= 1
    assert set(lead_result.durable_source_keys) == {"pubmed", "clinicaltrials_gov"}
    inspected_sources = {
        record["source_key"]
        for record in lead_result.metadata["evidence_inspection"]["records"]
        if record.get("source_key")
    }
    assert {"pubmed", "clinicaltrials_gov"} <= inspected_sources


def test_research_followup_resolver_blocks_promotion_for_weak_evidence_fit(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-fit-gate.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Safety signal: Safety/tolerability profile of sorafenib in dogs at proposed doses (DLT, MTD data)",
            lead_type="unknown",
            status="followup",
            source_key="pubmed",
            origin_source_key="agent_evaluator",
            topic_tags=["sorafenib", "canine", "safety", "mtd", "dlt"],
        )
    )

    def fake_search(repository, lead_record, request):
        fetch_run_id = uuid4()
        raw_record_id = repository.upsert_raw_record(
            RawSourceRecord(
                source_key="pubmed",
                source_record_id="41900948",
                source_url="https://pubmed.ncbi.nlm.nih.gov/41900948/",
                content_hash="pubmed-41900948",
                raw_payload={"title": "Comparative oncology framework"},
            ),
            fetch_run_id=fetch_run_id,
        )
        object_id = repository.upsert_research_object(
            ResearchObject(
                object_type="publication",
                title="Comparative Cancer Genetics and Veterinary Therapeutics in Dogs and Cats",
                abstract=(
                    "Dogs and cats develop naturally occurring tumors that resemble human malignancies, "
                    "supporting a species-aware comparative oncology framework."
                ),
                canonical_url="https://pubmed.ncbi.nlm.nih.gov/41900948/",
                source_key="pubmed",
                identifiers={"pmid": "41900948"},
                dedupe_key="pubmed:41900948",
            ),
            raw_record_id,
        )
        repository.upsert_document_chunk(
            DocumentChunk(
                research_object_id=object_id,
                chunk_index=0,
                section_label="title_abstract",
                text_content=(
                    "Comparative oncology review covering dogs, cats, and cancer genetics across species-aware "
                    "therapeutic frameworks."
                ),
                content_hash="pubmed-41900948-chunk",
            )
        )
        return {
            "query_text": "sorafenib canine dlt mtd safety",
            "source_keys": ["pubmed"],
            "limit_per_source": request.search_limit_per_source,
            "reports": [
                {
                    "source_key": "pubmed",
                    "fetch_run_id": str(fetch_run_id),
                    "raw_records": 1,
                    "research_objects": 1,
                    "document_chunks": 1,
                }
            ],
            "errors": [],
        }

    monkeypatch.setattr(research_followup_resolver, "_search_durable_sources", fake_search)

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            search_source_keys=["pubmed"],
            search_missing_identifiers=False,
            force_live_search=True,
            promote_ready_leads=True,
            min_evidence_chunks=1,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)
    lead_result = result.lead_results[0]

    assert result.promoted_leads == 0
    assert result.kept_in_followup == 1
    assert lead_result.metadata["evidence_fit"]["fit"] == "weak"
    assert "sorafenib" in lead_result.metadata["evidence_fit"]["missing_terms"]
    assert "promoted_to_watching" not in lead_result.actions
    assert updated is not None
    assert updated.status == "followup"
    assert updated.metadata["research_followup_resolver"]["evidence_fit"]["fit"] == "weak"


def test_research_followup_resolver_ingests_only_linked_followup(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-resolver-ingest.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Angiosarcoma DOI follow-up",
            lead_type="linked_article",
            status="followup",
            source_key="x_linked_article",
            origin_source_key="x_linked_article",
            identifiers={"doi": "10.1234/HSA.INGEST"},
        )
    )
    unrelated = repo.upsert_source_followup(
        SourceFollowupQueueItem(
            source_key="crossref",
            identifier_type="doi",
            identifier="10.9999/unrelated",
            status="queued",
        )
    )

    def fake_ingest(repository, request):
        assert len(request.followup_ids) == 1
        assert unrelated.followup_id not in request.followup_ids
        item = repository.get_source_followup(request.followup_ids[0])
        assert item is not None
        fetch_run_id = uuid4()
        raw_record_id = repository.upsert_raw_record(
            RawSourceRecord(
                source_key="crossref",
                source_record_id="10.1234/hsa.ingest",
                source_url="https://doi.org/10.1234/hsa.ingest",
                content_hash="crossref-hsa-ingest",
                raw_payload={"title": "Angiosarcoma linked follow-up evidence"},
            ),
            fetch_run_id=fetch_run_id,
        )
        object_id = repository.upsert_research_object(
            ResearchObject(
                object_type="publication",
                title="Angiosarcoma linked follow-up evidence",
                abstract="Durable angiosarcoma evidence for the linked DOI follow-up lead.",
                canonical_url="https://doi.org/10.1234/hsa.ingest",
                source_key="crossref",
                dedupe_key="crossref:10.1234/hsa.ingest",
            ),
            raw_record_id,
        )
        repository.upsert_document_chunk(
            DocumentChunk(
                research_object_id=object_id,
                chunk_index=0,
                section_label="abstract",
                text_content="Angiosarcoma durable evidence for the linked follow-up lead.",
                content_hash="crossref-hsa-ingest-chunk",
            )
        )
        repository.update_source_followup(
            item.followup_id,
            status="ingested",
            attempts=item.attempts + 1,
            metadata={
                "research_lead_id": str(lead.lead_id),
                "last_ingestion_report": {
                    "research_objects": 1,
                    "document_chunks": 1,
                    "fetch_run_id": str(fetch_run_id),
                },
            },
        )

    class FakeIngestResult:
        ingested = 1

        def model_dump(self, mode):
            return {"ingested": 1, "items": []}

    def fake_ingest_result(repository, request):
        fake_ingest(repository, request)
        return FakeIngestResult()

    monkeypatch.setattr(research_followup_resolver, "ingest_source_followups", fake_ingest_result)

    result = service.resolve_research_followups(
        ResearchFollowupResolverRequest(
            lead_ids=[lead.lead_id],
            search_missing_identifiers=False,
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.source_followups_queued == 1
    assert result.source_followups_ingested == 1
    assert result.promoted_leads == 1
    assert repo.get_source_followup(unrelated.followup_id).status == "queued"
    assert updated is not None
    assert updated.status == "watching"


def test_command_center_web_lists_research_briefs_with_quality_state(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-briefs.sqlite3", seed=False)
    service = HSAResearchService(repo)
    brief = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="KDR translational synthesis",
            disease_scope="canine hemangiosarcoma and human angiosarcoma",
            source_key="pubmed",
            status="completed",
            final_brief="KDR/VEGFR2 translation should move into validation planning [C1].",
            citation_count=1,
            finding_count=1,
            hypothesis_count=1,
            result_payload={
                "citations": [
                    {
                        "citation_id": "C1",
                        "chunk_id": str(uuid4()),
                        "research_object_id": str(uuid4()),
                        "source_key": "pubmed",
                        "title": "VEGFR2 in angiosarcoma",
                        "source_url": "https://pubmed.ncbi.nlm.nih.gov/example",
                        "quote": "VEGFR2 signal.",
                    }
                ],
                "ranked_hypotheses": [
                    {
                        "claim": "KDR-altered tumors should be reviewed for VEGFR2 inhibition.",
                        "stance": "supports",
                        "citations": ["C1"],
                        "evidence_strength": "medium",
                        "reasoning": "Citation-backed rationale.",
                    }
                ],
                "evidence_limitations": ["Canine clinical-response bridge remains incomplete."],
            },
        )
    )
    repo.upsert_research_brief_evaluation(
        ResearchBriefEvaluationRecord(
            brief_id=brief.brief_id,
            topic=brief.topic,
            source_key="pubmed",
            overall_score=0.82,
            passes_quality_bar=True,
            readiness="ready_for_hypothesis_review",
        )
    )

    payload = command_center_web.list_research_briefs_payload(service)
    ready_payload = command_center_web.list_research_briefs_payload(
        service,
        {"quality_status": ["ready_for_validation"], "query": ["KDR"]},
    )

    assert payload["total"] == 1
    assert payload["ready_count"] == 1
    assert ready_payload["visible"] == 1
    assert ready_payload["items"][0]["brief_id"] == str(brief.brief_id)
    assert ready_payload["items"][0]["quality_status"] == "ready_for_validation"
    assert ready_payload["items"][0]["final_brief"].startswith("KDR/VEGFR2")
    assert ready_payload["items"][0]["citation_preview"][0]["citation_id"] == "C1"
    assert ready_payload["items"][0]["hypothesis_preview"][0]["evidence_strength"] == "medium"


def test_command_center_web_runs_research_followup_loop_payload(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-followup-loop.sqlite3", seed=False)
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine DLT",
            status="followup",
            priority=5,
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["pubmed"],
            metadata={"created_by": "agent_finding_escalation_agent"},
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="agent_eval_sorafenib",
            query_text="sorafenib canine maximum tolerated dose",
            query_params={
                "followup_lane": "agent_evaluator_followup",
                "origin_review_id": str(origin_review_id),
                "origin_agent_run_id": str(origin_agent_run_id),
            },
            track="validation_gap",
        )
    )

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            research_object = ResearchObject(
                object_type="publication",
                title="Sorafenib dose-limiting toxicity in dogs",
                abstract="Canine sorafenib maximum tolerated dose and dose-limiting toxicity data.",
                source_key=query.source_key,
                dedupe_key=f"{query.source_key}:{query.query_name}:strong",
            )
            object_id = self.repository.upsert_research_object(research_object)
            for index in range(2):
                self.repository.upsert_document_chunk(
                    DocumentChunk(
                        research_object_id=object_id,
                        chunk_index=index,
                        section_label="abstract",
                        text_content=(
                            "Sorafenib was evaluated in canine patients with safety, tolerability, "
                            "maximum tolerated dose, and dose-limiting toxicity endpoints."
                        ),
                        content_hash=f"{query.source_key}:{query.query_name}:strong:{index}",
                    )
                )
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                raw_records=1,
                research_objects=1,
                document_chunks=1,
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    payload = command_center_web.run_research_followup_loop_payload(
        service,
        str(lead.lead_id),
        {"ingest": True, "resolve": False, "evaluate": False, "operator": "operator"},
    )

    assert payload["lead_status_before"] == "followup"
    assert payload["lead_status_after"] == "watching"
    assert payload["query_count"] == 1
    assert payload["document_chunks"] == 1
    assert payload["evidence_fit"]["fit"] == "strong"
    assert payload["evidence_fit"]["target_safety_fit"] == "strong"
    assert payload["evidence_fit"]["actionability"] == "strong"
    assert payload["evidence_fit"]["overall_fit"] == "strong"


def test_research_brief_service_runs_three_perspectives_and_synthesis(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:brief",
            content_hash="brief-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/brief/",
            raw_payload={"pmid": "brief"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF therapy in canine hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/brief/",
            dedupe_key="pmid:brief",
            identifiers={"pmid": "brief"},
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
                "VEGF therapy, toxicity, species mismatch, target selection, translational "
                "biomarker work, and clinical evidence are discussed."
            ),
            content_hash="brief-chunk",
        )
    )
    repo.upsert_research_lead(
        ResearchLeadRecord(
            title="VEGF hemangiosarcoma conference abstract needs durable source review",
            url="https://www.abstractsonline.com/vegf-hsa",
            lead_type="conference_abstract",
            source_key="x_linked_article",
            reason="Agent found a credible but non-durable lead.",
            topic_tags=["hemangiosarcoma", "therapy"],
        )
    )

    result = HSAResearchService(repo).run_research_brief(
        ResearchBriefRequest(
            topic="VEGF therapy in canine hemangiosarcoma",
            review_mode="deterministic_only",
            max_chunks_per_perspective=3,
            max_claims=0,
        )
    )
    runs = repo.list_agent_runs(limit=10)

    assert len(result.perspective_reports) == 3
    assert result.final_brief.startswith("# Research Brief")
    assert "[C1]" in result.final_brief
    assert result.citations
    assert result.evidence["research_lead_count"] == 1
    assert result.ranked_hypotheses
    assert result.brief_id is not None
    assert result.agent_run_id is not None
    saved_brief = repo.get_research_brief(result.brief_id)
    assert saved_brief is not None
    assert saved_brief.agent_run_id == result.agent_run_id
    assert saved_brief.citation_count == len(result.citations)
    assert saved_brief.research_lead_count == 1
    assert saved_brief.result_payload["brief_id"] == str(result.brief_id)
    assert HSAResearchService(repo).list_research_briefs(topic_query="VEGF")[0].brief_id == result.brief_id
    assert {run.agent_name for run in runs} >= {
        "evidence_scout_agent",
        "translational_hypothesis_agent",
        "skeptic_validation_agent",
        "research_synthesis_editor_agent",
    }


def test_research_brief_evidence_dedupes_citations_by_identifier_with_provenance():
    repo = InMemoryResearchRepository()
    first_object = ResearchObject(
        object_type="publication",
        title="Toceranib therapy outcomes in canine hemangiosarcoma",
        source_key="pubmed",
        canonical_url="https://pubmed.ncbi.nlm.nih.gov/26062540/",
        dedupe_key="pmid:26062540",
        identifiers={"pmid": "26062540", "doi": "10.1186/S12917-015-0446-1"},
    )
    second_object = ResearchObject(
        object_type="publication",
        title="Toceranib therapy outcomes in canine hemangiosarcoma",
        source_key="europe_pmc",
        canonical_url="https://doi.org/10.1186/s12917-015-0446-1",
        dedupe_key="europe_pmc:26062540",
        identifiers={"doi": "https://doi.org/10.1186/s12917-015-0446-1", "pmcid": "PMC3837095"},
    )
    first_chunk = DocumentChunk(
        research_object_id=first_object.id,
        chunk_index=0,
        section_label="Abstract",
        text_content=(
            "Canine hemangiosarcoma toceranib therapy evidence discusses VEGF VEGFR inhibitor "
            "response, survival outcome, toxicity, and clinical relevance."
        ),
        content_hash="toceranib-primary",
    )
    second_chunk = DocumentChunk(
        research_object_id=second_object.id,
        chunk_index=0,
        section_label="Abstract",
        text_content=(
            "Toceranib therapy for canine hemangiosarcoma is a duplicate source discussing "
            "VEGF inhibition, survival outcome, clinical response, and toxicity."
        ),
        content_hash="toceranib-duplicate",
    )
    repo.research_objects[first_object.id] = first_object
    repo.research_objects[second_object.id] = second_object
    repo.document_chunks[first_chunk.id] = first_chunk
    repo.document_chunks[second_chunk.id] = second_chunk

    evidence = research_brief_agent.ResearchBriefAgent(repo).build_evidence(
        ResearchBriefRequest(
            topic="Toceranib therapy in canine hemangiosarcoma",
            review_mode="deterministic_only",
            max_chunks_per_perspective=4,
            max_claims=0,
        )
    )

    assert len(evidence.citations) == 1
    citation = evidence.citations[0]
    assert citation.metadata["identifiers"]["doi"] == "10.1186/s12917-015-0446-1"
    assert citation.metadata["dedupe"]["duplicate_count"] >= 1
    assert citation.metadata["dedupe"]["duplicate_citation_ids"] == ["C2"]
    assert set(citation.metadata["provenance"]["chunk_ids"]) == {str(first_chunk.id), str(second_chunk.id)}
    assert set(citation.metadata["provenance"]["source_keys"]) == {"pubmed", "europe_pmc"}
    assert "evidence_scout:" in (citation.relevance or "")
    assert "translational_hypothesis:" in (citation.relevance or "")
    assert "skeptic_validation:" in (citation.relevance or "")


def test_research_brief_perspective_queries_stay_within_chunk_search_contract():
    long_topic = " ".join(["follow-up research lead with verbose evidence limitation"] * 60)
    request = ResearchBriefRequest(
        topic=long_topic[:1000],
        disease_scope="canine hemangiosarcoma and human angiosarcoma",
        review_mode="deterministic_only",
    )

    queries = research_brief_agent._perspective_queries(request)

    for query_specs in queries.values():
        for query_spec in query_specs:
            assert 1 <= len(query_spec.query) <= 1000
            assert any(
                term in query_spec.query
                for term in ("biomarker", "mechanism", "comparative", "clinical", "negative", "inhibitor", "validation")
            )
            ResearchChunkSearchRequest(query=query_spec.query)


def test_research_brief_model_json_loader_repairs_common_llm_commas():
    payload = research_brief_agent._load_json_object(
        """
        ```json
        {
          "summary": "Model reviewed the cited evidence."
          "findings": [
            {
              "claim": "VEGF signaling is relevant."
              "stance": "supporting",
              "citations": ["C1"],
              "evidence_strength": "medium",
              "reasoning": "The citation discusses VEGF biology.",
              "open_questions": []
            }
            {
              "claim": "Translation remains uncertain.",
              "stance": "risk",
              "citations": ["C2"],
              "evidence_strength": "low",
              "reasoning": "The citation is indirect.",
              "open_questions": []
            },
          ]
          "errors": []
        }
        ```
        """
    )

    assert payload["summary"] == "Model reviewed the cited evidence."
    assert len(payload["findings"]) == 2
    assert payload["errors"] == []


def test_research_brief_openrouter_payload_includes_contract(monkeypatch):
    captured = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "id": "or-test",
                    "model": "anthropic/claude-sonnet-test",
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "summary": "Reviewed.",
                                        "findings": [],
                                        "errors": [],
                                    }
                                )
                            }
                        }
                    ],
                    "usage": {"total_tokens": 100},
                }
            ).encode("utf-8")

    def fake_urlopen(request, timeout):
        captured["timeout"] = timeout
        captured["payload"] = json.loads(request.data.decode("utf-8"))
        return FakeResponse()

    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    monkeypatch.delenv("HSA_RESEARCH_BRIEF_MAX_TOKENS", raising=False)
    monkeypatch.setattr(research_brief_agent.urllib.request, "urlopen", fake_urlopen)

    review = research_brief_agent._openrouter_review_model(
        "anthropic/claude-sonnet-test",
        {"topic": "VEGF therapy", "citations": [{"citation_id": "C1"}]},
    )
    user_payload = json.loads(captured["payload"]["messages"][1]["content"])

    assert captured["payload"]["max_tokens"] == 6000
    assert user_payload["response_contract"]["required"] == [
        "summary",
        "findings",
        "evidence_limitations",
        "errors",
    ]
    assert user_payload["evidence_payload"]["topic"] == "VEGF therapy"
    assert review["metadata"]["request_id"] == "or-test"


def test_research_brief_model_report_splits_limitations_from_errors():
    citation = ResearchBriefCitation(
        citation_id="C1",
        chunk_id=uuid4(),
        research_object_id=uuid4(),
        quote="VEGF therapy evidence was reviewed.",
        relevance="evidence_scout:direct_evidence",
    )
    evidence = research_brief_agent.ResearchBriefEvidenceBundle(
        citations=[citation],
        claims=[],
        research_leads=[],
        search_queries={},
        errors=["claim search failed: timeout"],
    )

    report = research_brief_agent._perspective_report_from_model(
        ResearchBriefRequest(topic="VEGF therapy"),
        "evidence_scout",
        evidence,
        {
            "metadata": {"model": "test-model"},
            "text": json.dumps(
                {
                    "summary": "Reviewed supplied evidence.",
                    "findings": [
                        {
                            "claim": "VEGF therapy has enough evidence for review.",
                            "stance": "supporting",
                            "citations": ["C1"],
                            "evidence_strength": "medium",
                            "reasoning": "The supplied citation discusses VEGF therapy.",
                            "open_questions": [],
                        }
                    ],
                    "evidence_limitations": ["No direct survival endpoint was supplied."],
                    "errors": [
                        "No supplied citation directly addresses dosing.",
                        "Invalid citation C99 was ignored.",
                    ],
                }
            ),
        },
    )

    assert report.errors == ["claim search failed: timeout", "Invalid citation C99 was ignored."]
    assert report.evidence_limitations == [
        "No direct survival endpoint was supplied.",
        "No supplied citation directly addresses dosing.",
    ]


def test_research_brief_evaluation_service_persists_ready_result(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-evaluation.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:evaluation-brief",
            content_hash="evaluation-brief-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/evaluation-brief/",
            raw_payload={"pmid": "evaluation-brief"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF therapy and translational validation in hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/evaluation-brief/",
            dedupe_key="pmid:evaluation-brief",
            identifiers={"pmid": "evaluation-brief"},
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
                "VEGF therapy, biomarker validation, species mismatch, toxicity, clinical "
                "translation, and target selection are all discussed as research needs."
            ),
            content_hash="evaluation-brief-chunk",
        )
    )
    service = HSAResearchService(repo)
    brief = service.run_research_brief(
        ResearchBriefRequest(
            topic="VEGF therapy in canine hemangiosarcoma",
            review_mode="deterministic_only",
            max_chunks_per_perspective=3,
            max_claims=0,
        )
    )

    result = service.evaluate_research_brief(
        ResearchBriefEvaluationRequest(brief_id=brief.brief_id)
    )
    saved = repo.get_research_brief_evaluation(result.evaluation_id)
    runs = repo.list_agent_runs(agent_name="research_brief_synthesis_evaluator_agent", status="completed")

    assert result.readiness == "ready_for_hypothesis_review"
    assert result.passes_quality_bar is True
    assert result.overall_score >= 0.7
    assert result.agent_run_id is not None
    assert saved is not None
    assert saved.brief_id == brief.brief_id
    assert runs[0].output_payload["evaluation_id"] == str(result.evaluation_id)


def test_research_brief_evaluation_service_blocks_uncited_record(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-evaluation-blocked.sqlite3", seed=False)
    record = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="VEGF therapy in canine hemangiosarcoma",
            disease_scope="canine hemangiosarcoma",
            source_key="pubmed",
            review_mode="deterministic_only",
            final_brief="Stored synthesis without citations.",
            result_payload={
                "topic": "VEGF therapy in canine hemangiosarcoma",
                "disease_scope": "canine hemangiosarcoma",
                "final_brief": "Stored synthesis without citations.",
                "citations": [],
                "perspective_reports": [],
                "ranked_hypotheses": [],
                "unresolved_questions": [],
                "evidence": {},
                "errors": [],
            },
        )
    )

    result = HSAResearchService(repo).evaluate_research_brief(
        ResearchBriefEvaluationRequest(brief_id=record.brief_id)
    )

    assert result.readiness == "blocked"
    assert result.passes_quality_bar is False
    assert result.citation_coverage_score == 0.0


def test_research_brief_evaluation_tracks_soft_evidence_limitations(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-evaluation-limitations.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:evaluation-limitations",
            content_hash="evaluation-limitations-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/evaluation-limitations/",
            raw_payload={"pmid": "evaluation-limitations"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF therapy and translational validation in hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/evaluation-limitations/",
            dedupe_key="pmid:evaluation-limitations",
            identifiers={"pmid": "evaluation-limitations"},
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
                "VEGF therapy, biomarker validation, species mismatch, toxicity, clinical "
                "translation, and target selection are all discussed as research needs."
            ),
            content_hash="evaluation-limitations-chunk",
        )
    )
    service = HSAResearchService(repo)
    brief = service.run_research_brief(
        ResearchBriefRequest(
            topic="VEGF therapy in canine hemangiosarcoma",
            review_mode="deterministic_only",
            max_chunks_per_perspective=3,
            max_claims=0,
        )
    )
    saved = repo.get_research_brief(brief.brief_id)
    assert saved is not None
    payload = dict(saved.result_payload)
    payload["errors"] = [
        "No supplied citation directly addresses a clinical trial outcome; evidence is indirect."
    ]
    repo.upsert_research_brief(
        saved.model_copy(update={"result_payload": payload, "error_count": 1})
    )

    result = service.evaluate_research_brief(ResearchBriefEvaluationRequest(brief_id=brief.brief_id))

    assert result.readiness == "ready_for_hypothesis_review"
    assert result.passes_quality_bar is True
    assert result.errors == []
    assert result.evidence["synthesis_limitation_count"] == 1
    assert any("follow-up research queue" in item for item in result.recommendations)


def test_research_brief_evaluation_blocks_explicit_insufficient_evidence(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-evaluation-insufficient.sqlite3", seed=False)
    citation = {
        "citation_id": "C1",
        "chunk_id": str(uuid4()),
        "research_object_id": str(uuid4()),
        "quote": "mTOR pathway validation is discussed.",
        "relevance": "evidence_scout:direct_evidence",
    }
    finding = {
        "claim": "The supplied evidence is insufficient to answer the mTOR inhibition question.",
        "stance": "uncertain",
        "citations": ["C1"],
        "evidence_strength": "low",
        "reasoning": "No primary clinical trial data were found in the supplied citations.",
        "open_questions": ["Which mTOR inhibitor trial provides direct response data?"],
    }
    record = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="mTOR evidence in canine hemangiosarcoma",
            disease_scope="canine hemangiosarcoma",
            source_key="pubmed",
            review_mode="deterministic_only",
            final_brief=(
                "The supplied evidence is insufficient to answer the mTOR inhibition "
                "question, so this should not be promoted yet [C1]."
            ),
            result_payload={
                "topic": "mTOR evidence in canine hemangiosarcoma",
                "disease_scope": "canine hemangiosarcoma",
                "brief_style": "technical",
                "model_profile": "research_brief",
                "final_brief": (
                    "The supplied evidence is insufficient to answer the mTOR inhibition "
                    "question, so this should not be promoted yet [C1]."
                ),
                "citations": [citation],
                "perspective_reports": [
                    {
                        "perspective": "evidence_scout",
                        "agent_name": "evidence_scout_agent",
                        "model_profile": "research_brief",
                        "summary": "Evidence was reviewed.",
                        "findings": [finding],
                        "citations": [citation],
                        "errors": [],
                    },
                    {
                        "perspective": "translational_hypothesis",
                        "agent_name": "translational_hypothesis_agent",
                        "model_profile": "research_brief",
                        "summary": "Translation was reviewed.",
                        "findings": [finding],
                        "citations": [citation],
                        "errors": [],
                    },
                    {
                        "perspective": "skeptic_validation",
                        "agent_name": "skeptic_validation_agent",
                        "model_profile": "research_brief",
                        "summary": "Skeptic validation found insufficient evidence for the focal question.",
                        "findings": [finding],
                        "citations": [citation],
                        "errors": [],
                    },
                ],
                "ranked_hypotheses": [finding],
                "unresolved_questions": ["Which mTOR inhibitor trial provides direct response data?"],
                "evidence": {},
                "errors": [],
            },
        )
    )

    result = HSAResearchService(repo).evaluate_research_brief(
        ResearchBriefEvaluationRequest(brief_id=record.brief_id)
    )

    assert result.readiness == "needs_more_evidence"
    assert result.passes_quality_bar is False
    assert result.evidence["insufficient_evidence_flag_count"] >= 1
    assert any("focused evidence acquisition" in item for item in result.recommendations)


def test_research_brief_evaluation_keeps_system_errors_hard(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-evaluation-hard-errors.sqlite3", seed=False)
    citation = {
        "citation_id": "C1",
        "chunk_id": str(uuid4()),
        "research_object_id": str(uuid4()),
        "quote": "VEGF therapy and validation are discussed.",
        "relevance": "evidence_scout:direct_evidence",
    }
    finding = {
        "claim": "VEGF therapy should be reviewed for validation.",
        "stance": "supporting",
        "citations": ["C1"],
        "evidence_strength": "medium",
        "reasoning": "The supplied citation discusses VEGF therapy and validation.",
        "open_questions": ["What validation experiment should run next?"],
    }
    record = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="VEGF therapy in canine hemangiosarcoma",
            disease_scope="canine hemangiosarcoma",
            source_key="pubmed",
            review_mode="deterministic_only",
            final_brief="VEGF therapy has evidence for validation planning [C1].",
            result_payload={
                "topic": "VEGF therapy in canine hemangiosarcoma",
                "disease_scope": "canine hemangiosarcoma",
                "brief_style": "technical",
                "model_profile": "research_brief",
                "final_brief": "VEGF therapy has evidence for validation planning [C1].",
                "citations": [citation],
                "perspective_reports": [
                    {
                        "perspective": "evidence_scout",
                        "agent_name": "evidence_scout_agent",
                        "model_profile": "research_brief",
                        "summary": "Evidence was reviewed.",
                        "findings": [finding],
                        "citations": [citation],
                        "errors": [],
                    },
                    {
                        "perspective": "translational_hypothesis",
                        "agent_name": "translational_hypothesis_agent",
                        "model_profile": "research_brief",
                        "summary": "Translation was reviewed.",
                        "findings": [finding],
                        "citations": [citation],
                        "errors": [],
                    },
                    {
                        "perspective": "skeptic_validation",
                        "agent_name": "skeptic_validation_agent",
                        "model_profile": "research_brief",
                        "summary": "Risks were reviewed.",
                        "findings": [finding],
                        "citations": [citation],
                        "errors": [],
                    },
                ],
                "ranked_hypotheses": [finding],
                "unresolved_questions": ["What validation experiment should run next?"],
                "evidence": {},
                "errors": ["chunk search failed: upstream timeout"],
            },
        )
    )

    result = HSAResearchService(repo).evaluate_research_brief(
        ResearchBriefEvaluationRequest(brief_id=record.brief_id)
    )

    assert result.readiness == "needs_human_review"
    assert result.passes_quality_bar is False
    assert result.errors == ["chunk search failed: upstream timeout"]


def test_evidence_gap_resolver_creates_research_leads_and_brief_queue_items(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "evidence-gap-resolver.sqlite3", seed=False)
    service = HSAResearchService(repo)
    queue_item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="KDR VEGFR2 validation",
            task_type="expert_review",
            title="Expert review: KDR mutation-gated TKI validation",
            objective="Review KDR mutation function and TKI response evidence.",
            rationale="The validation agent held the idea pending stronger evidence.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                target_name="KDR",
                candidate_name="sorafenib",
                objective="Review KDR mutation function and TKI response evidence.",
                require_approval=False,
                assay_context=ValidationAssayContext(
                    disease_context="canine hemangiosarcoma and human angiosarcoma",
                    species=["canine", "human"],
                    model_system="evidence packet",
                    assay_type="expert evidence review",
                    readout="go/no-go validation readiness",
                ),
            ),
            status="completed",
            last_run_id=uuid4(),
            metadata={
                "idea_id": str(uuid4()),
                "idea_title": "KDR mutation-gated TKI validation",
                "validation_agent_result": ValidationAgentResult(
                    agent_run_id=uuid4(),
                    queue_item_id=uuid4(),
                    plan_id=uuid4(),
                    task_id=uuid4(),
                    task_type="expert_review",
                    validation_type="expert_review",
                    agent_name="evidence_review_validation_agent",
                    model_profile="openrouter_required",
                    decision="hold",
                    confidence=0.67,
                    summary="Hold pending mutation function and clinical response evidence.",
                    evidence_used=["C1", "C2"],
                    missing_evidence=[
                        "Functional classification of KDR/FLT4 mutations as activating versus passenger.",
                        "Clinical response correlation between KDR mutation status and TKI response.",
                    ],
                    risks=["Sorafenib coagulopathy and hemorrhage safety risk."],
                    next_actions=["Validate phospho-VEGFR2 IHC assay as a pharmacodynamic readout."],
                ).model_dump(mode="json"),
            },
        )
    )

    preview = service.resolve_evidence_gaps(
        EvidenceGapResolverRequest(queue_item_ids=[queue_item.queue_item_id])
    )
    applied = service.resolve_evidence_gaps(
        EvidenceGapResolverRequest(
            queue_item_ids=[queue_item.queue_item_id],
            dry_run=False,
            queue_research_briefs=True,
        )
    )
    duplicate = service.resolve_evidence_gaps(
        EvidenceGapResolverRequest(queue_item_ids=[queue_item.queue_item_id], dry_run=False)
    )
    leads = repo.list_research_leads(status="new", limit=20)
    queued_briefs = repo.list_research_brief_queue_items(status="queued", limit=20)

    assert isinstance(preview, EvidenceGapResolverResult)
    assert preview.dry_run is True
    assert preview.gap_count == 4
    assert preview.leads_created == 0
    assert applied.queue_items_seen == 1
    assert applied.gap_count == 4
    assert applied.leads_created == 4
    assert applied.brief_queue_count == 4
    assert duplicate.existing_leads == 4
    assert len(leads) == 4
    assert len(queued_briefs) == 4
    assert {"mutation_function", "clinical_response", "safety_signal", "assay_protocol"}.issubset(
        {lead.metadata["evidence_gap_resolver"]["lane"] for lead in leads}
    )
    assert all("validation_gap" in lead.topic_tags for lead in leads)
    assert repo.list_agent_runs(agent_name="evidence_gap_resolver_agent", status="completed", limit=1)


def test_research_followup_loop_runs_search_and_updates_status(monkeypatch):
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine DLT",
            status="followup",
            priority=5,
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["pubmed"],
            metadata={"created_by": "agent_finding_escalation_agent"},
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="agent_eval_sorafenib",
            query_text="sorafenib canine maximum tolerated dose",
            query_params={
                "followup_lane": "agent_evaluator_followup",
                "origin_review_id": str(origin_review_id),
                "origin_agent_run_id": str(origin_agent_run_id),
            },
            track="validation_gap",
        )
    )

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            research_object = ResearchObject(
                object_type="publication",
                title="Sorafenib dose-limiting toxicity in dogs",
                abstract="Canine sorafenib maximum tolerated dose and dose-limiting toxicity data.",
                source_key=query.source_key,
                dedupe_key=f"{query.source_key}:{query.query_name}:strong",
            )
            self.repository.research_objects[research_object.id] = research_object
            for index in range(2):
                chunk = DocumentChunk(
                    research_object_id=research_object.id,
                    chunk_index=index,
                    section_label="abstract",
                    text_content=(
                        "Sorafenib was evaluated in canine patients with safety, tolerability, "
                        "maximum tolerated dose, and dose-limiting toxicity endpoints."
                    ),
                    content_hash=f"{query.source_key}:{query.query_name}:strong:{index}",
                )
                self.repository.document_chunks[chunk.id] = chunk
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                raw_records=1,
                research_objects=1,
                document_chunks=2,
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    result = service.run_research_followup_loop(
        ResearchFollowupLoopRequest(
            lead_id=lead.lead_id,
            dry_run=False,
            ingest=True,
            resolve=False,
            evaluate=False,
            operator="operator",
        )
    )
    updated = repo.get_research_lead(lead.lead_id)
    loop_runs = repo.list_agent_runs(agent_name="research_followup_loop_agent", status="completed", limit=5)

    assert isinstance(result, ResearchFollowupLoopResult)
    assert result.query_count == 1
    assert result.document_chunks == 2
    assert result.evidence_fit is not None
    assert result.evidence_fit.fit == "strong"
    assert result.evidence_fit.missing_terms == []
    assert result.lead_status_before == "followup"
    assert result.lead_status_after == "watching"
    assert [transition["to"] for transition in result.status_transitions] == ["queued", "watching"]
    assert updated.status == "watching"
    assert updated.metadata["research_followup_loop"]["document_chunks"] == 2
    assert updated.metadata["research_followup_loop"]["evidence_fit"]["fit"] == "strong"
    assert loop_runs
    assert loop_runs[0].summary["document_chunks"] == 2
    assert loop_runs[0].summary["evidence_fit"] == "strong"
    assert loop_runs[0].summary["target_safety_fit"] == "strong"
    assert loop_runs[0].summary["actionability"] == "strong"
    assert loop_runs[0].summary["overall_fit"] == "strong"


def test_research_followup_loop_queues_identifier_followups_and_records_claims(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-loop-identifiers.sqlite3", seed=False)
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine HSA safety",
            status="followup",
            priority=5,
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["europe_pmc"],
            topic_tags=["sorafenib", "canine", "hemangiosarcoma", "safety"],
            metadata={"created_by": "agent_finding_escalation_agent"},
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="europe_pmc",
            query_name="agent_eval_sorafenib_identifiers",
            query_text="sorafenib canine hemangiosarcoma safety",
            query_params={
                "followup_lane": "agent_evaluator_followup",
                "origin_review_id": str(origin_review_id),
                "origin_agent_run_id": str(origin_agent_run_id),
            },
            track="validation_gap",
        )
    )

    def persist_record(repository, source_key, query_name, title, identifiers, section_label):
        fetch_run_id = repository.create_fetch_run(source_key, query_name)
        raw_id = repository.upsert_raw_record(
            RawSourceRecord(
                source_key=source_key,
                source_record_id=f"{source_key}:{query_name}",
                content_hash=f"{source_key}:{query_name}",
                raw_payload={"identifiers": identifiers},
            ),
            fetch_run_id=fetch_run_id,
        )
        object_id = repository.upsert_research_object(
            ResearchObject(
                object_type="publication",
                title=title,
                abstract=(
                    "Sorafenib was evaluated in canine hemangiosarcoma with toxicity, "
                    "safety, tolerability, and dose-limiting endpoints."
                ),
                source_key=source_key,
                dedupe_key=f"{source_key}:{query_name}",
                identifiers=identifiers,
                raw_record_id=raw_id,
            ),
            raw_id,
        )
        repository.upsert_document_chunk(
            DocumentChunk(
                research_object_id=object_id,
                chunk_index=0,
                section_label=section_label,
                text_content=(
                    "Sorafenib canine hemangiosarcoma safety toxicity tolerability "
                    "dose-limiting evidence supports follow-up claim extraction."
                ),
                content_hash=f"{source_key}:{query_name}:chunk",
            )
        )
        return fetch_run_id

    class FakeValidationPipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            fetch_run_id = persist_record(
                self.repository,
                query.source_key,
                query.query_name,
                "Sorafenib safety in canine hemangiosarcoma",
                {"pmid": "33110170", "pmcid": "PMC7591904", "doi": "10.1038/s41598-020-75533-4"},
                "abstract",
            )
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=fetch_run_id,
                raw_records=1,
                research_objects=1,
                document_chunks=1,
                status=RunStatus.COMPLETED,
            )

    class FakeSourceFollowupPipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            identifier_type = str(query.query_params.get("exact_identifier_type") or "pmid").lower()
            identifier = str(query.query_params.get("exact_identifier") or "33110170")
            identifiers = {identifier_type: identifier}
            if identifier_type == "pmcid":
                identifiers["pmcid"] = identifier
            if identifier_type == "doi":
                identifiers["doi"] = identifier
            fetch_run_id = persist_record(
                self.repository,
                query.source_key,
                query.query_name,
                f"Identifier fallback {query.source_key}",
                identifiers,
                "full_text" if query.source_key == "pmc_oa" else "metadata",
            )
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=fetch_run_id,
                raw_records=1,
                research_objects=1,
                document_chunks=1,
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakeValidationPipeline)
    monkeypatch.setattr(source_followup, "LocalIngestionPipeline", FakeSourceFollowupPipeline)

    def fail_source_wide_enrichment(repository, source_keys, ingest_result):
        raise AssertionError("research follow-up loop should use run-scoped claim extraction")

    monkeypatch.setattr(source_followup, "_refresh_entity_claim_layers", fail_source_wide_enrichment)

    result = service.run_research_followup_loop(
        ResearchFollowupLoopRequest(
            lead_id=lead.lead_id,
            dry_run=False,
            ingest=True,
            resolve=False,
            evaluate=False,
            max_identifier_followups=3,
            operator="operator",
        )
    )
    updated = repo.get_research_lead(lead.lead_id)
    followups = repo.list_source_followups(limit=10)
    loop_runs = repo.list_agent_runs(agent_name="research_followup_loop_agent", status="completed", limit=5)

    assert result.source_followups_queued == 3
    assert result.source_followups_linked == 3
    assert result.source_followups_newly_queued == 3
    assert result.source_followups_preexisting == 0
    assert result.source_followups_already_ingested == 0
    assert result.source_followups_pending == 0
    assert result.source_followups_ingested == 3
    assert result.source_followups_ingested_this_run == 3
    assert result.source_followup_document_chunks == 3
    assert {item.source_key for item in followups} >= {"pmc_oa", "pubmed", "unpaywall"}
    assert result.claim_chunks_seen == 4
    assert result.claims_written > 0
    assert result.claim_extraction_errors == []
    assert updated is not None
    assert updated.metadata["research_followup_loop"]["source_followups_linked"] == 3
    assert updated.metadata["research_followup_loop"]["source_followups_queued"] == 3
    assert updated.metadata["research_followup_loop"]["source_followups_newly_queued"] == 3
    assert updated.metadata["research_followup_loop"]["source_followups_already_ingested"] == 0
    assert updated.metadata["research_followup_loop"]["source_followups_ingested"] == 3
    assert updated.metadata["research_followup_loop"]["source_followups_ingested_this_run"] == 3
    assert updated.metadata["research_followup_loop"]["claims_written"] == result.claims_written
    assert loop_runs
    assert loop_runs[0].summary["source_followups_linked"] == 3
    assert loop_runs[0].summary["source_followups_queued"] == 3
    assert loop_runs[0].summary["source_followups_newly_queued"] == 3
    assert loop_runs[0].summary["source_followups_already_ingested"] == 0
    assert loop_runs[0].summary["claims_written"] == result.claims_written

    repeat_result = service.run_research_followup_loop(
        ResearchFollowupLoopRequest(
            lead_id=lead.lead_id,
            dry_run=False,
            ingest=True,
            resolve=False,
            evaluate=False,
            max_identifier_followups=3,
            operator="operator",
        )
    )

    assert repeat_result.source_followups_linked == 3
    assert repeat_result.source_followups_queued == 0
    assert repeat_result.source_followups_newly_queued == 0
    assert repeat_result.source_followups_preexisting == 3
    assert repeat_result.source_followups_already_ingested == 3
    assert repeat_result.source_followups_ingested == 0
    assert repeat_result.source_followups_ingested_this_run == 0
    assert repeat_result.source_followup_result is None


def test_research_followup_loop_keeps_weak_evidence_in_followup(monkeypatch):
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine DLT",
            status="followup",
            priority=5,
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["pubmed"],
            metadata={"created_by": "agent_finding_escalation_agent"},
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="agent_eval_sorafenib",
            query_text="sorafenib canine maximum tolerated dose",
            query_params={
                "followup_lane": "agent_evaluator_followup",
                "origin_review_id": str(origin_review_id),
                "origin_agent_run_id": str(origin_agent_run_id),
            },
            track="validation_gap",
        )
    )

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            research_object = ResearchObject(
                object_type="publication",
                title="Comparative oncology framework in dogs and cats",
                abstract="Veterinary comparative oncology review covering canine and feline cancer models.",
                source_key=query.source_key,
                dedupe_key=f"{query.source_key}:{query.query_name}:weak",
            )
            self.repository.research_objects[research_object.id] = research_object
            chunk = DocumentChunk(
                research_object_id=research_object.id,
                chunk_index=0,
                section_label="abstract",
                text_content=(
                    "Dogs and cats can inform comparative oncology, but this review does not report "
                    "the requested drug-specific dosing or safety findings."
                ),
                content_hash=f"{query.source_key}:{query.query_name}:weak",
            )
            self.repository.document_chunks[chunk.id] = chunk
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                raw_records=1,
                research_objects=1,
                document_chunks=1,
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    result = service.run_research_followup_loop(
        ResearchFollowupLoopRequest(
            lead_id=lead.lead_id,
            dry_run=False,
            ingest=True,
            resolve=False,
            evaluate=False,
            operator="operator",
        )
    )
    updated = repo.get_research_lead(lead.lead_id)

    assert result.document_chunks == 1
    assert result.evidence_fit is not None
    assert result.evidence_fit.fit == "weak"
    assert "sorafenib" in result.evidence_fit.missing_terms
    assert result.lead_status_after == "followup"
    assert [transition["to"] for transition in result.status_transitions] == ["queued", "followup"]
    assert updated.status == "followup"
    assert updated.metadata["research_followup_loop"]["evidence_fit"]["fit"] == "weak"


def test_research_followup_loop_keeps_supported_signal_hunting_after_no_result(monkeypatch):
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine DLT",
            status="followup",
            priority=5,
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["pubmed"],
            metadata={"created_by": "agent_finding_escalation_agent"},
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="agent_eval_sorafenib",
            query_text="sorafenib canine maximum tolerated dose",
            query_params={
                "followup_lane": "agent_evaluator_followup",
                "origin_review_id": str(origin_review_id),
                "origin_agent_run_id": str(origin_agent_run_id),
            },
            track="validation_gap",
        )
    )
    ingest_calls = 0

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            nonlocal ingest_calls
            ingest_calls += 1
            if ingest_calls == 1:
                research_object = ResearchObject(
                    object_type="publication",
                    title="Sorafenib dose-limiting toxicity in dogs",
                    abstract="Canine sorafenib maximum tolerated dose and dose-limiting toxicity data.",
                    source_key=query.source_key,
                    dedupe_key=f"{query.source_key}:{query.query_name}:strong",
                )
                self.repository.research_objects[research_object.id] = research_object
                for index in range(2):
                    chunk = DocumentChunk(
                        research_object_id=research_object.id,
                        chunk_index=index,
                        section_label="abstract",
                        text_content=(
                            "Sorafenib was evaluated in canine patients with safety, tolerability, "
                            "maximum tolerated dose, and dose-limiting toxicity endpoints."
                        ),
                        content_hash=f"{query.source_key}:{query.query_name}:strong:{index}",
                    )
                    self.repository.document_chunks[chunk.id] = chunk
                return IngestionResult(
                    source_key=query.source_key,
                    query_name=query.query_name,
                    query_text=query.query_text,
                    fetch_run_id=uuid4(),
                    raw_records=1,
                    research_objects=1,
                    document_chunks=2,
                    status=RunStatus.COMPLETED,
                )
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    first_result = service.run_research_followup_loop(
        ResearchFollowupLoopRequest(
            lead_id=lead.lead_id,
            dry_run=False,
            ingest=True,
            resolve=False,
            evaluate=False,
            operator="operator",
        )
    )
    second_result = service.run_research_followup_loop(
        ResearchFollowupLoopRequest(
            lead_id=lead.lead_id,
            dry_run=False,
            ingest=True,
            resolve=False,
            evaluate=False,
            operator="operator",
        )
    )
    updated = repo.get_research_lead(lead.lead_id)
    hunt_state = updated.metadata["research_hunt"]

    assert first_result.lead_status_after == "watching"
    assert first_result.signal_status == "supported"
    assert first_result.coverage_status == "supported"
    assert first_result.best_signal is not None
    assert second_result.document_chunks == 0
    assert second_result.evidence_fit is not None
    assert second_result.evidence_fit.fit == "weak"
    assert second_result.lead_status_after == "watching"
    assert second_result.signal_status == "supported"
    assert second_result.coverage_status == "hunting"
    assert second_result.best_signal == first_result.best_signal
    assert second_result.hunt_tasks_created == 1
    assert second_result.hunt_tasks[0]["task_type"] == "broaden_query"
    assert updated.status == "watching"
    assert hunt_state["signal_status"] == "supported"
    assert hunt_state["coverage_status"] == "hunting"
    assert hunt_state["open_task_count"] == 1
    assert hunt_state["tasks"][0]["reason"] == "no_result_after_supported_signal"


def test_research_followup_loop_evaluator_followups_create_hunt_tasks(monkeypatch):
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine HSA safety",
            status="followup",
            priority=5,
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["pubmed"],
            topic_tags=["sorafenib", "canine", "hemangiosarcoma", "safety"],
            metadata={"created_by": "agent_finding_escalation_agent"},
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="agent_eval_sorafenib_hsa",
            query_text="sorafenib canine hemangiosarcoma safety",
            query_params={
                "followup_lane": "agent_evaluator_followup",
                "origin_review_id": str(origin_review_id),
                "origin_agent_run_id": str(origin_agent_run_id),
            },
            track="validation_gap",
        )
    )

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            research_object = ResearchObject(
                object_type="publication",
                title="Canine hemangiosarcoma safety case series",
                abstract=(
                    "Canine hemangiosarcoma cases reported toxicity, safety, tolerability, "
                    "and dose-limiting endpoints without the requested therapy detail."
                ),
                source_key=query.source_key,
                dedupe_key=f"{query.source_key}:{query.query_name}:weak",
            )
            self.repository.research_objects[research_object.id] = research_object
            chunk = DocumentChunk(
                research_object_id=research_object.id,
                chunk_index=0,
                section_label="abstract",
                text_content=(
                    "Canine hemangiosarcoma safety toxicity tolerability dose-limiting "
                    "evidence needs therapy-specific follow-up."
                ),
                content_hash=f"{query.source_key}:{query.query_name}:weak",
            )
            self.repository.document_chunks[chunk.id] = chunk
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                raw_records=1,
                research_objects=1,
                document_chunks=1,
                status=RunStatus.COMPLETED,
            )

    resolver_agent_run_id = uuid4()
    evaluator_agent_run_id = uuid4()
    resolver_evidence_fit = EvidenceFitAssessment(
        fit="strong",
        target_safety_fit="strong",
        disease_directness_fit="strong",
        actionability="strong",
        transfer_risk="low",
        overall_fit="strong",
        matched_terms=[
            "sorafenib",
            "canine/dog/veterinary",
            "hemangiosarcoma/angiosarcoma",
            "maximum tolerated dose/dlt/safety",
        ],
        required_terms=[
            "sorafenib",
            "canine/dog/veterinary",
            "hemangiosarcoma/angiosarcoma",
            "maximum tolerated dose/dlt/safety",
        ],
        matched_required_count=4,
        total_required_count=4,
        source_keys=["pubmed"],
        chunk_count=4,
        reason="Resolver-inspected evidence matched the specific therapy, disease, species, and safety terms.",
    )

    def fake_resolve_research_followups(request):
        return ResearchFollowupResolverResult(
            agent_run_id=resolver_agent_run_id,
            leads_seen=1,
            durable_source_searches=1,
            evidence_inspections=1,
            lead_results=[
                ResearchFollowupLeadResult(
                    lead_id=lead.lead_id,
                    title=lead.title,
                    status_before="watching",
                    status_after="watching",
                    actions=["searched_durable_sources"],
                    evidence_refs=["pmid:33110170", "doi:10.1038/s41598-020-75533-4"],
                    durable_source_keys=["pubmed"],
                    metadata={"evidence_fit": resolver_evidence_fit.model_dump(mode="json")},
                )
            ],
        )

    def fake_run_agent_performance_evaluation(request):
        return AgentPerformanceEvaluationResult(
            agent_run_id=evaluator_agent_run_id,
            reviewed_only=False,
            scanned_count=1,
            candidate_count=1,
            evaluated_count=1,
            review_created_count=1,
            evaluations=[
                {
                    "agent_run_id": str(resolver_agent_run_id),
                    "verdict": "needs_followup",
                    "confidence": 0.88,
                    "recommended_followup_actions": [
                        "Chase citations for PMID 33110170.",
                        "Run claim extraction on adverse event grading.",
                    ],
                }
            ],
        )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)
    monkeypatch.setattr(service, "resolve_research_followups", fake_resolve_research_followups)
    monkeypatch.setattr(service, "run_agent_performance_evaluation", fake_run_agent_performance_evaluation)

    result = service.run_research_followup_loop(
        ResearchFollowupLoopRequest(
            lead_id=lead.lead_id,
            dry_run=False,
            ingest=True,
            resolve=True,
            evaluate=True,
            operator="operator",
        )
    )
    updated = repo.get_research_lead(lead.lead_id)
    hunt_state = updated.metadata["research_hunt"]
    task_types = {task["task_type"] for task in hunt_state["tasks"]}

    assert result.evidence_fit is not None
    assert result.evidence_fit.fit == "weak"
    assert result.latest_evaluator_verdict == "needs_followup"
    assert result.lead_status_after == "watching"
    assert result.signal_status == "supported"
    assert result.coverage_status == "hunting"
    assert result.hunt_tasks_created == 2
    assert task_types == {"citation_chase", "claim_extract"}
    assert updated.status == "watching"
    assert hunt_state["best_signal"]["verdict"] == "needs_followup"
    assert hunt_state["best_signal"]["evidence_fit"]["fit"] == "strong"
    assert hunt_state["best_signal"]["score"] >= 80
    assert hunt_state["best_signal"]["evidence_refs"] == ["pmid:33110170", "doi:10.1038/s41598-020-75533-4"]


def test_research_hunt_source_followup_task_queues_identifier_followups(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-hunt-source-followups.sqlite3", seed=False)
    service = HSAResearchService(repo)
    fetch_run_id = uuid4()
    raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="26062540",
            content_hash="toceranib-maintenance-raw",
            raw_payload={"pmid": "26062540"},
        ),
        fetch_run_id=fetch_run_id,
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type=ResearchObjectType.PUBLICATION,
            title="Maintenance therapy with toceranib following doxorubicin-based chemotherapy",
            abstract="Canine splenic hemangiosarcoma maintenance therapy with toceranib.",
            source_key="pubmed",
            dedupe_key="pmid:26062540",
            identifiers={"pmid": "26062540", "pmcid": "PMC3837095", "doi": "10.1186/s12917-015-0446-1"},
            raw_record_id=raw_id,
        ),
        raw_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="title_abstract",
            text_content="Toceranib maintenance therapy in canine splenic hemangiosarcoma.",
            content_hash="toceranib-maintenance-chunk",
        )
    )
    task_id = uuid4()
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Find toceranib/VEGFR inhibitor monotherapy outcomes in canine splenic HSA",
            status="watching",
            metadata={
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "hunting",
                    "open_task_count": 1,
                    "best_signal": {
                        "score": 100,
                        "evidence_refs": [f"research_object:{object_id}"],
                        "evidence_fit": {"fit": "strong"},
                    },
                    "tasks": [
                        {
                            "task_id": str(task_id),
                            "identity_key": "source_followup_ingest:pmcid",
                            "status": "open",
                            "task_type": "source_followup_ingest",
                            "action": "Queue source follow-up ingestion for PMCIDs present in retrieved records.",
                            "priority": 10,
                        }
                    ],
                }
            },
        )
    )
    captured: dict[str, SourceFollowupIngestRequest] = {}

    def fake_ingest(self, request):
        captured["request"] = request
        return SourceFollowupIngestResult(
            queue_items_seen=len(request.followup_ids),
            attempted=len(request.followup_ids),
            ingested=len(request.followup_ids),
            document_chunks=2,
        )

    monkeypatch.setattr(HSAResearchService, "ingest_source_followups", fake_ingest)

    result = service.run_research_hunt_tasks(
        ResearchHuntTaskRunRequest(lead_ids=[lead.lead_id], task_ids=[task_id], dry_run=False, evaluate=False)
    )
    queued = repo.list_source_followups(limit=None)
    queued_identities = {item.identity_key for item in queued}

    assert result.completed_count == 1
    assert result.items[0]["source_followup"]["queued_count"] == 7
    assert len(captured["request"].followup_ids) == 7
    assert "pubmed:pmid:26062540" in queued_identities
    assert "europe_pmc:pmid:26062540" in queued_identities
    assert "pmc_oa:pmcid:pmc3837095" in queued_identities
    assert "europe_pmc:pmcid:pmc3837095" in queued_identities
    assert "crossref:doi:10.1186/s12917-015-0446-1" in queued_identities
    assert "europe_pmc:doi:10.1186/s12917-015-0446-1" in queued_identities
    assert "unpaywall:doi:10.1186/s12917-015-0446-1" in queued_identities


def test_research_hunt_queue_report_marks_resolved_followup_ready_for_synthesis():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Resolved mTOR evidence lead",
            status="watching",
            source_key="pubmed",
            metadata={
                "research_followup_resolver": {
                    "evidence_refs": [f"chunk:{uuid4()}", f"research_object:{uuid4()}"],
                    "evidence_fit": {
                        "fit": "strong",
                        "overall_fit": "strong",
                        "matched_required_count": 3,
                        "total_required_count": 3,
                    },
                }
            },
        )
    )

    report = service.build_research_hunt_queue_report(ResearchHuntQueueReportRequest(lead_ids=[lead.lead_id]))

    assert report.ready_for_synthesis_count == 1
    assert report.leads[0].signal_status == "supported"
    assert report.leads[0].coverage_status == "supported"
    assert report.leads[0].control_status == "ready_for_synthesis"
    assert report.leads[0].recommended_action == "queue_synthesis"


def test_queue_ready_research_hunt_synthesis_cleans_followup_topic():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title=(
                "Repair duplicate citations: Review research lead: Follow up research gap: "
                "Mutation function: PIK3CA/TP53 co-mutation and mTOR dependency"
            ),
            status="watching",
            priority=20,
            source_key="pubmed",
            origin_source_key="research_brief_quality",
            reason=(
                "Resolve the topic-string recursion and citation deduplication flags before "
                "re-synthesis. No supplied citation independently corroborates a PIK3CA/TP53 "
                "rapamycin survival signal."
            ),
            metadata={
                "research_followup_queue": {
                    "followup_kind": "citation_dedupe_repair",
                    "topic": (
                        "Review research lead: Repair duplicate citations: Review research lead: "
                        "Mutation function: Translational bridge evidence to human angiosarcoma "
                        "PIK3CA/TP53 co-mutation frequency and mTOR pathway dependency | "
                        "Reason: T | Tags: research_brief, evaluation_followup, pubmed"
                    ),
                },
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "supported",
                    "best_signal": {
                        "verdict": "useful",
                        "evidence_fit": {"fit": "strong", "matched_required_count": 4, "total_required_count": 4},
                    },
                    "tasks": [],
                },
            },
        )
    )

    result = service.queue_ready_research_hunt_synthesis(
        ResearchHuntSynthesisQueueRequest(
            lead_ids=[lead.lead_id],
            dry_run=True,
            review_models=["~anthropic/claude-opus-latest"],
        )
    )

    assert result.candidate_count == 1
    assert result.queue_items
    topic = result.queue_items[0].topic
    assert "Review research lead" not in topic
    assert "Reason:" not in topic
    assert "Tags:" not in topic
    assert "PIK3CA" in topic
    assert "TP53" in topic
    assert "mTOR" in topic
    assert "canine hemangiosarcoma and human angiosarcoma" in topic
    assert "independent-source dedupe" in topic


def test_queue_ready_research_hunt_synthesis_cleans_no_term_followup_topic():
    repo = InMemoryResearchRepository()
    service = HSAResearchService(repo)
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title=(
                "Repair duplicate citations: Review research lead: Omics context: "
                "Species-mismatch risk: canine splenic HSA survival benefit may not translate"
            ),
            status="watching",
            priority=20,
            source_key="pubmed",
            origin_source_key="research_brief_quality",
            reason="Duplicate-citation repair flagged but not executed before promotion.",
            metadata={
                "research_followup_queue": {
                    "followup_kind": "citation_dedupe_repair",
                    "topic": "Review research lead: Repair duplicate citations | Tags: research_brief, evaluation_followup",
                },
                "research_hunt": {
                    "version": "v1",
                    "signal_status": "supported",
                    "coverage_status": "supported",
                    "best_signal": {
                        "verdict": "useful",
                        "evidence_fit": {"fit": "strong", "matched_required_count": 3, "total_required_count": 3},
                    },
                    "tasks": [],
                },
            },
        )
    )

    result = service.queue_ready_research_hunt_synthesis(
        ResearchHuntSynthesisQueueRequest(
            lead_ids=[lead.lead_id],
            dry_run=True,
            review_models=["~anthropic/claude-opus-latest"],
        )
    )

    assert result.queue_items
    topic = result.queue_items[0].topic
    assert "Review research lead" not in topic
    assert "Tags:" not in topic
    assert topic.startswith("canine hemangiosarcoma and human angiosarcoma cross-species translation evidence")
    assert "independent-source dedupe" in topic


def test_research_followup_loop_evidence_fit_prefers_run_scoped_chunks(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "followup-run-scoped.sqlite3", seed=False)
    service = HSAResearchService(repo)
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    old_raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="old-strong",
            content_hash="old-strong",
            raw_payload={"title": "Old strong sorafenib evidence"},
        ),
        fetch_run_id=uuid4(),
    )
    old_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Old sorafenib canine maximum tolerated dose study",
            abstract="Sorafenib canine safety maximum tolerated dose and dose-limiting toxicity.",
            source_key="pubmed",
            dedupe_key="old-strong",
        ),
        old_raw_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=old_object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Sorafenib canine maximum tolerated dose and DLT evidence.",
            content_hash="old-strong-chunk",
        )
    )
    lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evaluator follow-up: sorafenib canine DLT",
            status="followup",
            priority=5,
            origin_review_id=origin_review_id,
            origin_agent_run_id=origin_agent_run_id,
            suggested_sources=["pubmed"],
            metadata={"created_by": "agent_finding_escalation_agent"},
        )
    )
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="agent_eval_sorafenib",
            query_text="sorafenib canine maximum tolerated dose",
            query_params={
                "followup_lane": "agent_evaluator_followup",
                "origin_review_id": str(origin_review_id),
                "origin_agent_run_id": str(origin_agent_run_id),
            },
            track="validation_gap",
        )
    )

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit, persist_query=True):
            fetch_run_id = uuid4()
            raw_id = self.repository.upsert_raw_record(
                RawSourceRecord(
                    source_key=query.source_key,
                    source_record_id="new-weak",
                    content_hash="new-weak",
                    raw_payload={"title": "New broad comparative oncology review"},
                ),
                fetch_run_id=fetch_run_id,
            )
            object_id = self.repository.upsert_research_object(
                ResearchObject(
                    object_type="publication",
                    title="Comparative oncology framework in dogs and cats",
                    abstract="Veterinary comparative oncology review covering canine and feline cancer models.",
                    source_key=query.source_key,
                    dedupe_key="new-weak",
                ),
                raw_id,
            )
            self.repository.upsert_document_chunk(
                DocumentChunk(
                    research_object_id=object_id,
                    chunk_index=0,
                    section_label="abstract",
                    text_content="Dogs and cats can inform comparative oncology broadly.",
                    content_hash="new-weak-chunk",
                )
            )
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=fetch_run_id,
                raw_records=1,
                research_objects=1,
                document_chunks=1,
                status=RunStatus.COMPLETED,
            )

    monkeypatch.setattr(validation_gap_ingest, "LocalIngestionPipeline", FakePipeline)

    result = service.run_research_followup_loop(
        ResearchFollowupLoopRequest(
            lead_id=lead.lead_id,
            dry_run=False,
            ingest=True,
            resolve=False,
            evaluate=False,
            operator="operator",
        )
    )

    assert result.evidence_fit is not None
    assert result.evidence_fit.fit == "weak"
    assert result.evidence_fit.chunk_count == 1
    assert result.lead_status_after == "followup"


def test_local_ingestion_sanitizes_research_followup_query_params(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-followup-local-ingest.sqlite3", seed=False)
    calls = []

    class FakeHarvester:
        def fetch(self, query_text, limit=25, **params):
            calls.append((query_text, limit, params))
            return []

    monkeypatch.setattr(local_ingest_module, "get_harvester", lambda source_key: FakeHarvester())

    result = LocalIngestionPipeline(repo).ingest_query(
        SourceQuery(
            source_key="openalex",
            query_name="research_followup_openalex",
            query_text="toceranib canine hemangiosarcoma",
            query_params={
                "comparative_policy": "enabled",
                "include_human_angiosarcoma": True,
                "require_policy_match": False,
                "filter": "from_publication_date:2015-01-01",
            },
            track="research_followup",
        ),
        limit=1,
        persist_query=False,
    )

    assert result.status == RunStatus.COMPLETED
    assert calls == [
        (
            "toceranib canine hemangiosarcoma",
            1,
            {
                "comparative_policy": "enabled",
                "require_policy_match": False,
                "filter": "from_publication_date:2015-01-01",
            },
        )
    ]


def test_research_brief_queue_runner_persists_brief_and_updates_queue(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-queue-runner.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:queue-brief",
            content_hash="queue-brief-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/queue-brief/",
            raw_payload={"pmid": "queue-brief"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF queue runner evidence in canine hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/queue-brief/",
            dedupe_key="pmid:queue-brief",
            identifiers={"pmid": "queue-brief"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma VEGF therapy evidence includes clinical outcome, "
                "toxicity, translational relevance, biomarker uncertainty, and target selection."
            ),
            content_hash="queue-brief-chunk",
        )
    )
    service = HSAResearchService(repo)
    queued = service.queue_research_brief(
        ResearchBriefQueueRequest(
            topic="VEGF therapy in canine hemangiosarcoma",
            source_key="pubmed",
            review_mode="deterministic_only",
            max_claims=0,
            max_chunks_per_perspective=2,
        )
    )

    result = service.run_next_research_brief_queue_item(
        ResearchBriefQueueRunRequest(source_key="pubmed")
    )
    updated = repo.get_research_brief_queue_item(queued.queue_item_id)

    assert result.ran is True
    assert result.brief is not None
    assert result.brief.brief_id is not None
    assert updated is not None
    assert updated.status == "completed"
    assert updated.last_brief_id == result.brief.brief_id
    saved_brief = repo.get_research_brief(result.brief.brief_id)
    assert saved_brief is not None
    assert saved_brief.status == "completed"


def test_research_brief_queue_runner_can_target_explicit_queue_ids(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-queue-runner-targeted.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:queue-brief-targeted",
            content_hash="queue-brief-targeted-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/queue-brief-targeted/",
            raw_payload={"pmid": "queue-brief-targeted"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF targeted queue runner evidence in canine hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/queue-brief-targeted/",
            dedupe_key="pmid:queue-brief-targeted",
            identifiers={"pmid": "queue-brief-targeted"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma VEGF therapy evidence includes clinical outcome, "
                "toxicity, translational relevance, biomarker uncertainty, and target selection."
            ),
            content_hash="queue-brief-targeted-chunk",
        )
    )
    service = HSAResearchService(repo)
    unselected = service.queue_research_brief(
        ResearchBriefQueueRequest(
            topic="KIT therapy in canine mast cell tumor",
            source_key="pubmed",
            priority=1,
            review_mode="deterministic_only",
            max_claims=0,
            max_chunks_per_perspective=2,
        )
    )
    selected = service.queue_research_brief(
        ResearchBriefQueueRequest(
            topic="VEGF therapy in canine hemangiosarcoma",
            source_key="pubmed",
            priority=100,
            review_mode="deterministic_only",
            max_claims=0,
            max_chunks_per_perspective=2,
        )
    )

    result = service.run_next_research_brief_queue_item(
        ResearchBriefQueueRunRequest(queue_item_ids=[selected.queue_item_id])
    )
    untouched = repo.get_research_brief_queue_item(unselected.queue_item_id)
    updated = repo.get_research_brief_queue_item(selected.queue_item_id)

    assert result.ran is True
    assert result.queue_item is not None
    assert result.queue_item.queue_item_id == selected.queue_item_id
    assert untouched is not None
    assert untouched.status == "queued"
    assert updated is not None
    assert updated.status == "completed"


def test_research_brief_queue_runner_fails_unusable_brief(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-queue-runner-fail.sqlite3", seed=False)
    service = HSAResearchService(repo)
    queued = service.queue_research_brief(
        ResearchBriefQueueRequest(
            topic="Unbacked linked article lead",
            source_key="x_linked_article",
            review_mode="deterministic_only",
            max_claims=0,
            max_chunks_per_perspective=2,
        )
    )

    result = service.run_next_research_brief_queue_item(
        ResearchBriefQueueRunRequest(source_key="x_linked_article")
    )
    updated = repo.get_research_brief_queue_item(queued.queue_item_id)

    assert result.ran is True
    assert result.brief is not None
    assert result.errors
    assert "did not meet completion bar" in result.errors[0]
    assert updated is not None
    assert updated.status == "failed"
    assert updated.last_brief_id == result.brief.brief_id
    assert updated.last_agent_run_id == result.brief.agent_run_id
    assert updated.last_error is not None
    assert "citations" in updated.last_error
    saved_brief = repo.get_research_brief(result.brief.brief_id)
    assert saved_brief is not None
    assert saved_brief.status == "failed"


def test_research_brief_skeptic_retrieval_prefers_clinical_outcome_evidence(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-skeptic.sqlite3", seed=False)
    weak_raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:mirna",
            content_hash="brief-mirna-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/mirna/",
            raw_payload={"pmid": "mirna"},
        )
    )
    weak_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="miRNA expression profiling in canine hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=weak_raw_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/mirna/",
            dedupe_key="pmid:mirna",
            identifiers={"pmid": "mirna"},
        ),
        weak_raw_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=weak_object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma miRNA expression profiles describe predicted "
                "VEGF pathway targets and biomarker hypotheses."
            ),
            content_hash="brief-mirna-chunk",
        )
    )
    clinical_raw_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:clinical",
            content_hash="brief-clinical-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/clinical/",
            raw_payload={"pmid": "clinical"},
        )
    )
    clinical_object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF inhibitor clinical outcomes in canine hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=clinical_raw_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/clinical/",
            dedupe_key="pmid:clinical",
            identifiers={"pmid": "clinical"},
        ),
        clinical_raw_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=clinical_object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "A canine hemangiosarcoma VEGF VEGFR inhibitor clinical trial reported "
                "response, survival outcome, toxicity, adverse events, and no clear "
                "progression-free benefit."
            ),
            content_hash="brief-clinical-chunk",
        )
    )

    result = HSAResearchService(repo).run_research_brief(
        ResearchBriefRequest(
            topic="VEGF therapy in canine hemangiosarcoma",
            review_mode="deterministic_only",
            max_chunks_per_perspective=1,
            max_claims=0,
        )
    )

    skeptic = next(report for report in result.perspective_reports if report.perspective == "skeptic_validation")
    assert skeptic.citations
    assert "clinical outcomes" in (skeptic.citations[0].title or "").lower()
    assert result.evidence["retrieval_strategy"] == "embedding_keyword_blended_perspective_rerank"


def test_research_brief_playground_pack_exports_prompt_contracts(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "research-brief-playground.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:playground",
            content_hash="brief-playground-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/playground/",
            raw_payload={"pmid": "playground"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGF inhibitor evidence in canine hemangiosarcoma",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            canonical_url="https://pubmed.ncbi.nlm.nih.gov/playground/",
            dedupe_key="pmid:playground",
            identifiers={"pmid": "playground"},
        ),
        raw_record_id,
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=(
                "Canine hemangiosarcoma VEGF VEGFR inhibitor evidence includes "
                "clinical outcome, toxicity, translational relevance, and biomarker limitations."
            ),
            content_hash="brief-playground-chunk",
        )
    )
    repo.upsert_research_lead(
        ResearchLeadRecord(
            title="VEGF hemangiosarcoma institutional article for review",
            url="https://example.edu/vegf-hsa-review",
            lead_type="institutional_article",
            source_key="x_topic_monitor",
            reason="Social monitoring found a credible non-durable source.",
            topic_tags=["hemangiosarcoma", "therapy"],
        )
    )

    pack = HSAResearchService(repo).build_research_brief_playground_pack(
        ResearchBriefRequest(
            topic="VEGF therapy in canine hemangiosarcoma",
            review_mode="external_required",
            max_chunks_per_perspective=2,
            max_claims=0,
        )
    )

    assert [prompt.perspective for prompt in pack.prompts] == [
        "evidence_scout",
        "translational_hypothesis",
        "skeptic_validation",
    ]
    assert pack.evidence["mode"] == "manual_playground_prompt_pack"
    assert pack.evidence["retrieval_strategy"] == "embedding_keyword_blended_perspective_rerank"
    assert pack.evidence["research_lead_count"] == 1
    assert pack.prompts[0].response_contract["required"] == [
        "summary",
        "findings",
        "evidence_limitations",
        "errors",
    ]
    assert "EVIDENCE_PAYLOAD_JSON" in pack.prompts[0].user_prompt
    assert "Return JSON only" in pack.prompts[0].user_prompt
    assert pack.prompts[0].prompt_payload["requirements"]["use_only_supplied_citation_ids"] is True
    assert pack.prompts[0].prompt_payload["requirements"]["research_leads_are_watchlist_context_not_citable_evidence"] is True
    assert pack.prompts[0].prompt_payload["research_leads"][0]["lead_type"] == "institutional_article"
    assert pack.prompts[2].prompt_payload["perspective"] == "skeptic_validation"
    assert any("clinical outcomes" in item for item in pack.prompts[2].evaluation_rubric)


def test_x_topic_review_queues_resolved_articles_for_scrape_followup(monkeypatch):
    resolved_url = (
        "https://cancer.ufl.edu/2026/04/20/"
        "researchers-characterize-genetic-landscape-of-angiosarcoma-opening-new-frontier-in-rare-cancer/"
    )
    monkeypatch.setattr(x_topic_review, "_follow_redirects", lambda url: resolved_url)

    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "quality_score": 0.7,
                    "durable_links": ["https://go.ufl.edu/r2uqpua"],
                }
            ],
        )
    )

    action = result.actions[0]
    link = action.ingestible_links[0]
    assert result.ingestion_candidate_count == 1
    assert action.action == "queue_source_followup"
    assert link.url == resolved_url
    assert link.recommended_source_key == "x_linked_article"
    assert link.should_ingest is False
    assert link.metadata["followup_type"] == "controlled_scrape_review"
    assert link.metadata["source_profile"] == "x_linked_article"
    assert link.metadata["original_url"] == "https://go.ufl.edu/r2uqpua"


def test_x_topic_review_queues_publisher_articles_for_scrape_followup():
    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "quality_score": 0.7,
                    "durable_links": ["https://www.nature.com/articles/s41586-026-00001"],
                }
            ],
        )
    )

    action = result.actions[0]
    link = action.ingestible_links[0]
    assert action.action == "queue_source_followup"
    assert link.recommended_source_key == "x_linked_article"
    assert link.metadata["followup_type"] == "controlled_scrape_review"
    assert link.metadata["resolution_status"] == "not_short_link"


def test_x_topic_review_queues_unresolved_short_links_for_followup(monkeypatch):
    def fake_follow_redirects(url):
        raise RuntimeError("timeout")

    monkeypatch.setattr(x_topic_review, "_follow_redirects", fake_follow_redirects)

    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "quality_score": 0.7,
                    "durable_links": ["https://t.co/source"],
                }
            ],
        )
    )

    action = result.actions[0]
    link = action.ingestible_links[0]
    assert action.action == "queue_source_followup"
    assert link.url == "https://t.co/source"
    assert link.recommended_source_key == "x_linked_article"
    assert link.metadata["resolution_status"] == "failed"
    assert link.metadata["fallback_reason"] == "unresolved_short_link"


def test_mcp_research_brief_tools_dump_json_safe_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-research-briefs.sqlite3", seed=False)
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)
    record = repo.upsert_research_brief(
        ResearchBriefRecord(
            agent_run_id=uuid4(),
            topic="VEGF therapy in canine hemangiosarcoma",
            disease_scope="canine hemangiosarcoma",
            source_key="pubmed",
            review_mode="deterministic_only",
            final_brief="Stored MCP synthesis [C1].",
            result_payload={"final_brief": "Stored MCP synthesis [C1]."},
            citation_count=1,
            finding_count=1,
        )
    )

    fetched = mcp_server.get_research_brief_tool(str(record.brief_id))
    listed = mcp_server.list_research_briefs_tool(topic_query="vegf")

    assert fetched["brief_id"] == str(record.brief_id)
    assert fetched["result_payload"]["final_brief"] == "Stored MCP synthesis [C1]."
    assert listed[0]["brief_id"] == str(record.brief_id)


def test_mcp_research_brief_evaluation_tools_dump_json_safe_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-research-brief-evaluations.sqlite3", seed=False)
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)
    record = repo.upsert_research_brief(
        ResearchBriefRecord(
            topic="VEGF therapy in canine hemangiosarcoma",
            disease_scope="canine hemangiosarcoma",
            source_key="pubmed",
            review_mode="deterministic_only",
            final_brief="Stored synthesis without citations.",
            result_payload={
                "topic": "VEGF therapy in canine hemangiosarcoma",
                "disease_scope": "canine hemangiosarcoma",
                "final_brief": "Stored synthesis without citations.",
                "citations": [],
                "perspective_reports": [],
                "ranked_hypotheses": [],
                "unresolved_questions": [],
                "evidence": {},
                "errors": [],
            },
        )
    )

    evaluated = mcp_server.evaluate_research_brief_tool(brief_id=str(record.brief_id))
    fetched = mcp_server.get_research_brief_evaluation_tool(evaluated["evaluation_id"])
    listed = mcp_server.list_research_brief_evaluations_tool(readiness="blocked")

    assert evaluated["brief_id"] == str(record.brief_id)
    assert evaluated["readiness"] == "blocked"
    assert fetched["evaluation_id"] == evaluated["evaluation_id"]
    assert listed[0]["evaluation_id"] == evaluated["evaluation_id"]


def test_mcp_research_brief_queue_tools_dump_json_safe_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-research-brief-queue.sqlite3", seed=False)
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)

    queued = mcp_server.queue_research_brief_tool(
        topic="VEGF therapy in canine hemangiosarcoma",
        source_key="pubmed",
        review_mode="deterministic_only",
    )
    fetched = mcp_server.get_research_brief_queue_item_tool(queued["queue_item_id"])
    listed = mcp_server.list_research_brief_queue_tool(status="queued")
    failed = repo.update_research_brief_queue_item(
        queued["queue_item_id"],
        status="failed",
        attempts=1,
        last_error="model timeout",
    )
    assert failed is not None
    requeued = mcp_server.requeue_research_brief_queue_item_tool(queued["queue_item_id"], priority=7)
    failed_again = repo.update_research_brief_queue_item(
        queued["queue_item_id"],
        status="failed",
        attempts=2,
        last_error="superseded",
    )
    assert failed_again is not None
    maintenance = mcp_server.maintain_research_brief_queue_tool(
        queue_item_ids=[queued["queue_item_id"]],
        statuses=["failed"],
        max_updated_age_hours=0,
        dry_run=True,
    )
    completed = repo.update_research_brief_queue_item(queued["queue_item_id"], status="completed")
    assert completed is not None
    archived = mcp_server.archive_research_brief_queue_item_tool(queued["queue_item_id"])
    repo.upsert_research_lead(
        ResearchLeadRecord(
            title="PDGF biomarker lead",
            lead_type="linked_article",
            status="new",
            priority=30,
            suggested_sources=["pubmed"],
        )
    )
    batch = mcp_server.queue_research_brief_batch_tool(mode="research_leads", limit=1)
    followup_lead = repo.upsert_research_lead(
        ResearchLeadRecord(
            title="Evidence light linked article",
            lead_type="linked_article",
            status="followup",
            source_key="x_linked_article",
        )
    )
    resolver = mcp_server.resolve_research_followups_tool(
        lead_ids=[str(followup_lead.lead_id)],
        search_missing_identifiers=False,
    )
    command_center = mcp_server.command_center_tool(include_source_health=False, queue_limit=5, lead_limit=5)

    assert queued["queue_item_id"]
    assert fetched["queue_item_id"] == queued["queue_item_id"]
    assert listed[0]["identity_key"] == queued["identity_key"]
    assert requeued["status"] == "queued"
    assert requeued["priority"] == 7
    assert requeued["last_error"] is None
    assert maintenance["candidate_count"] == 1
    assert maintenance["dry_run"] is True
    assert maintenance["queue_items"][0]["queue_item_id"] == queued["queue_item_id"]
    assert archived["status"] == "archived"
    assert archived["metadata"]["queue_control"]["previous_status"] == "completed"
    assert batch["queued_count"] == 1
    assert batch["queue_items"][0]["metadata"]["batch_queue"]["origin"] == "research_lead"
    assert resolver["leads_seen"] == 1
    assert resolver["manual_research_required"] == 1
    assert command_center["summary"]["brief_queue_total"] >= 1
    assert command_center["recommendations"]


def test_x_linked_article_followup_requires_fetch_approval(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)

    result = HSAResearchService(repo).run_x_linked_article_followup(
        XLinkedArticleFollowupRequest(urls=["https://cancer.ufl.edu/article"])
    )

    assert result.candidate_urls == ["https://cancer.ufl.edu/article"]
    assert result.requires_fetch_approval is True
    assert result.fetched_pages == 0
    assert result.candidate_results == [
        {
            "url": "https://cancer.ufl.edu/article",
            "status": "requires_fetch_approval",
            "reason": "Explicit approval is required before fetching X-linked article URLs.",
        }
    ]


def test_source_followup_ingest_dry_run_lists_queue_items(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "followups-dry-run.sqlite3", seed=False)
    repo.upsert_source_followup(
        SourceFollowupQueueItem(
            source_key="pubmed",
            identifier_type="pmid",
            identifier="12345678",
            origin_source_key="x_linked_article",
        )
    )

    result = HSAResearchService(repo).ingest_source_followups(SourceFollowupIngestRequest(dry_run=True))

    assert result.queue_items_seen == 1
    assert result.skipped == 1
    assert result.items[0].identifier == "12345678"
    assert repo.list_source_followups(source_key="pubmed")[0].status == "queued"


def test_source_followup_ingest_fails_closed_on_identifier_substitution(tmp_path, monkeypatch):
    repo = SQLiteResearchRepository(tmp_path / "followups-exact.sqlite3", seed=False)
    followup = repo.upsert_source_followup(
        SourceFollowupQueueItem(
            source_key="crossref",
            identifier_type="doi",
            identifier="10.1234/requested",
            origin_source_key="x_linked_article",
        )
    )

    class FakePipeline:
        def __init__(self, repository):
            self.repository = repository

        def ingest_query(self, query, limit=1, persist_query=False):
            raw_id = self.repository.upsert_raw_record(
                RawSourceRecord(
                    source_key="crossref",
                    source_record_id="10.1234/nearby",
                    content_hash="nearby",
                    raw_payload={"doi": "10.1234/nearby"},
                )
            )
            self.repository.upsert_research_object(
                ResearchObject(
                    object_type=ResearchObjectType.PUBLICATION,
                    title="Nearby substituted paper",
                    source_key="crossref",
                    dedupe_key="doi:10.1234/nearby",
                    identifiers={"doi": "10.1234/nearby"},
                ),
                raw_id,
            )
            return IngestionResult(
                source_key=query.source_key,
                query_name=query.query_name,
                query_text=query.query_text,
                fetch_run_id=uuid4(),
                raw_records=1,
                research_objects=1,
                document_chunks=0,
            )

    monkeypatch.setattr(source_followup, "LocalIngestionPipeline", FakePipeline)

    result = HSAResearchService(repo).ingest_source_followups(
        SourceFollowupIngestRequest(followup_ids=[followup.followup_id], run_claim_extraction=False)
    )

    updated = repo.get_source_followup(followup.followup_id)
    assert result.failed == 1
    assert updated is not None
    assert updated.status == "failed"
    assert "refusing substituted source" in (updated.last_error or "")
    assert updated.metadata["exact_identifier_match_count"] == 0


def test_source_followup_pmc_oa_fallback_queues_pubmed_and_crossref(tmp_path, monkeypatch):
    repo = SQLiteResearchRepository(tmp_path / "pmc-fallback.sqlite3", seed=False)
    item = repo.upsert_source_followup(
        SourceFollowupQueueItem(
            source_key="pmc_oa",
            identifier_type="pmcid",
            identifier="PMC5634767",
            origin_source_key="x_topic_monitor",
            metadata={"recommendation_source": "x_topic_review_agent"},
        )
    )
    monkeypatch.setattr(
        source_followup,
        "_pmc_idconv_metadata",
        lambda pmcid: {"pmcid": pmcid, "pmid": 28606972, "doi": "10.1634/theoncologist.2016-0429"},
    )

    fallback_items = source_followup._queue_pmc_metadata_fallbacks(
        repo,
        item,
        approved_by="unit-test",
    )
    rows = HSAResearchService(repo).list_source_followups(limit=10)

    assert {(row.source_key, row.identifier_type, row.identifier) for row in fallback_items} == {
        ("pubmed", "pmid", "28606972"),
        ("crossref", "doi", "10.1634/theoncologist.2016-0429"),
    }
    assert len(rows) == 3
    pubmed_row = next(row for row in rows if row.source_key == "pubmed")
    assert pubmed_row.metadata["fallback_type"] == "pmc_idconv_pubmed"
    assert pubmed_row.metadata["fallback_from_identifier"] == "PMC5634767"
    assert pubmed_row.metadata["approved_by"] == "unit-test"


def test_unpaywall_doi_followups_queue_only_doi_objects_idempotently(tmp_path):
    sqlite_repo = SQLiteResearchRepository(tmp_path / "doi-followups.sqlite3", seed=False)
    memory_repo = InMemoryResearchRepository()

    doi_object = ResearchObject(
        object_type=ResearchObjectType.PUBLICATION,
        title="Open access DOI candidate",
        source_key="crossref",
        identifiers={"doi": "https://doi.org/10.1234/HSA.OA"},
    )
    non_doi_object = ResearchObject(
        object_type=ResearchObjectType.PUBLICATION,
        title="No DOI candidate",
        source_key="pubmed",
        identifiers={"pmid": "12345678"},
    )

    sqlite_repo.upsert_research_object(doi_object)
    sqlite_repo.upsert_research_object(non_doi_object)
    memory_repo.research_objects[doi_object.id] = doi_object
    memory_repo.research_objects[non_doi_object.id] = non_doi_object

    for repo in (sqlite_repo, memory_repo):
        service = HSAResearchService(repo)
        queued = service.queue_unpaywall_doi_followups(DoiOpenAccessFollowupQueueRequest(limit=10))
        queued_again = service.queue_unpaywall_doi_followups(DoiOpenAccessFollowupQueueRequest(limit=10))
        rows = service.list_source_followups(source_key="unpaywall")

        assert queued.reviewed_records == 2
        assert queued.queued == 1
        assert queued.skipped_uningestible == 1
        assert queued_again.queued == 0
        assert queued_again.skipped_existing == 1
        assert len(rows) == 1
        assert rows[0].identifier_type == "doi"
        assert rows[0].identifier == "10.1234/hsa.oa"
        assert rows[0].metadata["followup_type"] == "doi_open_access_enrichment"
        assert rows[0].metadata["lookup_mode"] == "doi"
        assert rows[0].metadata["title_search"] is False


def test_source_followup_query_params_are_source_safe():
    crossref_query = source_followup._query_for_followup(
        SourceFollowupQueueItem(
            source_key="crossref",
            identifier_type="doi",
            identifier="10.1234/test",
            origin_source_key="x_linked_article",
        )
    )
    clinical_query = source_followup._query_for_followup(
        SourceFollowupQueueItem(
            source_key="clinicaltrials_gov",
            identifier_type="nct",
            identifier="NCT12345678",
            origin_source_key="x_linked_article",
        )
    )

    assert crossref_query.query_params == {
        "comparative_policy": "disabled",
        "exact_identifier": "10.1234/test",
        "exact_identifier_type": "doi",
        "require_policy_match": False,
    }
    assert clinical_query.query_params == {
        "exact_identifier": "NCT12345678",
        "exact_identifier_type": "nct",
        "require_policy_match": False,
    }

    unpaywall_query = source_followup._query_for_followup(
        SourceFollowupQueueItem(
            source_key="unpaywall",
            identifier_type="doi",
            identifier="10.1234/HSA.OA",
            origin_source_key="crossref",
        )
    )
    assert unpaywall_query.source_key == "unpaywall"
    assert unpaywall_query.query_text == "10.1234/hsa.oa"
    assert unpaywall_query.query_name == "source_followup_doi_10_1234_hsa_oa"
    assert unpaywall_query.query_params["exact_identifier"] == "10.1234/hsa.oa"


def test_followup_internal_policy_params_do_not_reach_external_apis(monkeypatch):
    crossref_calls = []

    def fake_crossref_get_json(url, params):
        crossref_calls.append((url, params))
        return {
            "message": {
                "DOI": "10.1234/test",
                "title": ["Follow-up article without local policy terms"],
                "URL": "https://doi.org/10.1234/test",
            }
        }

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_crossref_get_json)
    crossref_records = CrossrefHarvesterV2().fetch(
        "10.1234/test",
        limit=1,
        comparative_policy="disabled",
        exact_identifier_type="doi",
        exact_identifier="10.1234/test",
        require_policy_match=False,
    )

    assert len(crossref_records) == 1
    assert crossref_calls == [
        (
            "https://api.crossref.org/works/10.1234%2Ftest",
            {},
        )
    ]

    clinical_calls = []

    def fake_clinical_get_json(url, params):
        clinical_calls.append((url, params))
        return {"studies": []}

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_clinical_get_json)
    clinical_records = ClinicalTrialsGovHarvesterV2().fetch(
        "NCT12345678",
        limit=1,
        comparative_policy="disabled",
        require_policy_match=False,
    )

    assert clinical_records == []
    assert clinical_calls == [
        (
            "https://clinicaltrials.gov/api/v2/studies",
            {"query.term": "NCT12345678", "pageSize": 1, "format": "json"},
        )
    ]


def test_dagster_exposes_source_followup_jobs():
    assert dagster_asset_module.x_linked_article_review_job is not None
    assert dagster_asset_module.source_followup_queue_job is not None
    assert dagster_asset_module.source_followup_ingest_job is not None
    assert dagster_asset_module.command_center_job is not None
    assert dagster_asset_module.agent_performance_report_job is not None
    assert dagster_asset_module.agent_performance_evaluation_job is not None
    assert dagster_asset_module.pubmed_source_followup_ingest_job is not None
    assert dagster_asset_module.crossref_source_followup_ingest_job is not None
    assert dagster_asset_module.pmc_oa_source_followup_ingest_job is not None
    assert dagster_asset_module.clinicaltrials_gov_source_followup_ingest_job is not None
    assert dagster_asset_module.unpaywall_source_followup_ingest_job is not None
    assert dagster_asset_module.research_brief_agent_job is not None
    assert dagster_asset_module.research_brief_library_job is not None
    assert dagster_asset_module.research_brief_evaluation_job is not None
    assert dagster_asset_module.research_brief_evaluation_library_job is not None
    assert dagster_asset_module.research_brief_quality_job is not None
    assert dagster_asset_module.research_brief_followup_queue_job is not None
    assert dagster_asset_module.validation_plan_job is not None
    assert dagster_asset_module.validation_plan_library_job is not None
    assert dagster_asset_module.validation_request_queue_job is not None
    assert dagster_asset_module.validation_request_queue_library_job is not None
    assert dagster_asset_module.validation_autopilot_job is not None
    assert dagster_asset_module.research_brief_queue_job is not None
    assert dagster_asset_module.research_brief_queue_batch_job is not None
    assert dagster_asset_module.research_hunt_synthesis_queue_job is not None
    assert dagster_asset_module.research_hunt_synthesis_doc_job is not None
    assert dagster_asset_module.research_brief_queue_seed_job is not None
    assert dagster_asset_module.research_brief_queue_runner_job is not None
    assert dagster_asset_module.research_brief_queue_maintenance_job is not None
    assert dagster_asset_module.research_brief_playground_pack_job is not None
    assert dagster_asset_module.therapy_committee_validation_queue_job is not None
    assert dagster_asset_module.research_leads_job is not None
    assert dagster_asset_module.evidence_gap_resolver_job is not None
    assert dagster_asset_module.validation_gap_source_pack_job is not None
    assert dagster_asset_module.pubmed_identifier_repair_job is not None
    assert dagster_asset_module.validation_gap_source_ingest_job is not None
    assert dagster_asset_module.research_followup_resolver_job is not None
    assert dagster_asset_module.validation_autopilot_hourly_schedule is not None
    assert dagster_asset_module.validation_autopilot_hourly_schedule.cron_schedule == "0 * * * *"


def test_dagster_schedules_source_followup_lanes():
    assert dagster_asset_module.source_followup_queue_daily_schedule is not None
    assert dagster_asset_module.source_followup_queue_daily_schedule.cron_schedule == "5 3 * * *"
    assert dagster_asset_module.pubmed_source_followup_ingest_daily_schedule is not None
    assert dagster_asset_module.pubmed_source_followup_ingest_daily_schedule.cron_schedule == "20 3 * * *"
    assert dagster_asset_module.crossref_source_followup_ingest_daily_schedule is not None
    assert dagster_asset_module.crossref_source_followup_ingest_daily_schedule.cron_schedule == "35 3 * * *"
    assert dagster_asset_module.pmc_oa_source_followup_ingest_daily_schedule is not None
    assert dagster_asset_module.pmc_oa_source_followup_ingest_daily_schedule.cron_schedule == "50 3 * * *"
    assert dagster_asset_module.clinicaltrials_gov_source_followup_ingest_daily_schedule is not None
    assert dagster_asset_module.clinicaltrials_gov_source_followup_ingest_daily_schedule.cron_schedule == "5 4 * * *"
    assert dagster_asset_module.unpaywall_source_followup_ingest_daily_schedule is not None
    assert dagster_asset_module.unpaywall_source_followup_ingest_daily_schedule.cron_schedule == "20 4 * * *"
    assert dagster_asset_module.research_leads_daily_schedule is not None
    assert dagster_asset_module.research_leads_daily_schedule.cron_schedule == "35 4 * * *"
