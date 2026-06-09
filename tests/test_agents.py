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
    _md_runpod_input,
    _ready_for_therapy_ideas_program,
    _research_program_fixture,
    _seed_evaluated_brief,
    _seed_full_text_source_claim,
    _seed_minimal_source_claim,
    _seed_program_committee_corpus,
    _write_minimal_xlsx,
    _xlsx_column_name,
)

def test_agent_run_and_full_text_ops_contracts_validate():
    trace_id = uuid4()
    record = AgentRunRecord(
        trace_id=trace_id,
        agent_name="full_text_ops_agent",
        model_profile="reviewer",
        status=RunStatus.RUNNING,
        source_key="europe_pmc",
        input_payload={"source_keys": ["europe_pmc"]},
    )
    action = FullTextOpsAction(
        source_key="europe_pmc",
        action="run_source_date_partition",
        severity="watch",
        reason="Partition evidence is missing.",
        dagster_job_name="literature_full_text_source_date_job",
        partition_date="2026-04-27",
    )
    result = FullTextOpsResult(actions=[action], schedule_readiness="needs_partition_validation")
    manifest = RunManifestRecord(
        trace_id=trace_id,
        manifest_type="agent_run",
        status="running",
        title="full text ops review",
        agent_run_ids=[record.agent_run_id],
        model_profiles=["reviewer"],
        source_versions={"europe_pmc": "2026-05-21"},
    )

    assert record.status == "running"
    assert record.trace_id == trace_id
    assert result.actions[0].action == "run_source_date_partition"
    assert manifest.trace_id == trace_id
    assert manifest.source_versions["europe_pmc"] == "2026-05-21"
    with pytest.raises(ValueError):
        FullTextOpsAction(source_key="europe_pmc", action="bad", severity="watch", reason="bad")
    with pytest.raises(ValueError):
        FullTextOpsAction(source_key="europe_pmc", action="mark_clean", severity="bad", reason="bad")
    with pytest.raises(ValueError):
        AgentRunRecord(agent_name="x", status="bad")
    with pytest.raises(ValidationError):
        RunManifestRecord(manifest_type="bad", status="running", title="x")
    with pytest.raises(ValidationError):
        RunManifestRecord(manifest_type="agent_run", status="loop_forever", title="x")


def test_run_manifest_repository_roundtrip_memory_and_sqlite(tmp_path):
    trace_id = uuid4()
    agent_run_id = uuid4()
    manifest = RunManifestRecord(
        trace_id=trace_id,
        manifest_type="agent_run",
        status="completed",
        title="therapy committee run",
        agent_run_ids=[agent_run_id],
        model_profiles=["research_brief"],
        method_refs=["candidate-record-v1"],
        content_hashes={"payload": "abc123"},
    )

    for repo in (
        InMemoryResearchRepository(),
        SQLiteResearchRepository(tmp_path / "run-manifests.sqlite3", seed=False),
    ):
        repo.upsert_run_manifest(manifest)
        fetched = repo.get_run_manifest(manifest.manifest_id)
        listed = repo.list_run_manifests(trace_id=trace_id, manifest_type="agent_run", status="completed")

        assert fetched is not None
        assert fetched.trace_id == trace_id
        assert fetched.agent_run_ids == [agent_run_id]
        assert listed[0].manifest_id == manifest.manifest_id


def test_agent_runner_creates_trace_and_run_manifest():
    repo = InMemoryResearchRepository()
    trace_id = uuid4()
    runner = AgentRunner(repo)

    result = runner.run(
        agent_name="trace_smoke_agent",
        model_profile="deterministic",
        input_payload={"trace_id": str(trace_id), "topic": "vimentin"},
        execute=lambda: {"candidate_id": "twog-test", "created_count": 1},
        metadata={"trace_id": str(trace_id), "prompt_key": "trace-smoke-v1"},
    )
    run = repo.list_agent_runs(agent_name="trace_smoke_agent", limit=1)[0]
    manifest = repo.list_run_manifests(trace_id=trace_id, manifest_type="agent_run", limit=1)[0]

    assert result["candidate_id"] == "twog-test"
    assert run.trace_id == trace_id
    assert run.metadata["trace_id"] == str(trace_id)
    assert manifest.status == "completed"
    assert manifest.agent_run_ids == [run.agent_run_id]
    assert manifest.output_refs["candidate_id"] == "twog-test"


def test_agent_run_repository_roundtrip_sqlite_and_memory(tmp_path):
    sqlite_repo = SQLiteResearchRepository(tmp_path / "agent-runs.sqlite3", seed=False)
    memory_repo = InMemoryResearchRepository()

    for repo in (sqlite_repo, memory_repo):
        record = repo.create_agent_run(
            AgentRunRecord(
                agent_name="full_text_ops_agent",
                model_profile="reviewer",
                status=RunStatus.RUNNING,
                source_key="europe_pmc",
                partition_date="2026-04-27",
                input_payload={"source_keys": ["europe_pmc"]},
            )
        )
        finished = repo.finish_agent_run(
            record.agent_run_id,
            status="completed",
            output_payload={"schedule_readiness": "ready_to_enable"},
            summary={"actions": 1},
            errors=[],
        )

        assert finished is not None
        assert finished.status == "completed"
        assert finished.completed_at is not None
        assert repo.get_agent_run(record.agent_run_id).summary == {"actions": 1}
        assert repo.list_agent_runs(agent_name="full_text_ops_agent", status="completed", source_key="europe_pmc")
        assert repo.list_agent_runs(agent_name="source_scout_agent") == []


def test_agent_run_review_repository_roundtrip_sqlite_and_memory(tmp_path):
    sqlite_repo = SQLiteResearchRepository(tmp_path / "agent-run-reviews.sqlite3", seed=False)
    memory_repo = InMemoryResearchRepository()

    for repo in (sqlite_repo, memory_repo):
        run = repo.create_agent_run(AgentRunRecord(agent_name="therapy_committee_chair_agent"))
        review = repo.create_agent_run_review(
            AgentRunReviewRecord(
                agent_run_id=run.agent_run_id,
                reviewer=" operator ",
                reviewer_type="operator",
                verdict="needs_followup",
                feedback="  Need mutation-function evidence. ",
                tags=["KDR", "kdr", "omics"],
                followup_actions=["queue_research", "queue_research"],
            )
        )

        assert review.reviewer == "operator"
        assert review.reviewer_type == "operator"
        assert review.feedback == "Need mutation-function evidence."
        assert review.tags == ["kdr", "omics"]
        assert review.followup_actions == ["queue_research"]
        assert repo.get_agent_run_review(review.review_id).verdict == "needs_followup"
        assert repo.list_agent_run_reviews(agent_run_id=run.agent_run_id, verdict="needs_followup", reviewer="operator")
        assert repo.list_agent_run_reviews(verdict="bad") == []

    with pytest.raises(ValidationError):
        AgentRunReviewRecord(agent_run_id=uuid4(), verdict="wrong")
    with pytest.raises(ValidationError):
        AgentRunReviewRecord(agent_run_id=uuid4(), reviewer_type="robot", verdict="useful")


def test_agent_finding_escalation_contracts_validate_allowed_values():
    request = AgentFindingEscalationRequest(verdicts=["bad"], source_keys=["pubmed"], limit=5)
    result = AgentFindingEscalationResult(dry_run=True)

    assert request.verdicts == ["bad"]
    assert result.agent_name == "agent_finding_escalation_agent"

    with pytest.raises(ValidationError):
        AgentFindingEscalationRequest(verdicts=["wrong"])
    with pytest.raises(ValidationError):
        AgentFindingEscalationRequest(limit=0)


def test_agent_finding_escalation_creates_research_lead_and_source_queries(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-finding-escalation.sqlite3", seed=False)
    service = HSAResearchService(repo)
    lead_id = uuid4()
    run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="research_followup_resolver_agent",
            status=RunStatus.COMPLETED,
            source_key="pubmed",
            output_payload={
                "lead_results": [
                    {
                        "lead_id": str(lead_id),
                        "title": "Sorafenib canine dose escalation",
                        "durable_source_keys": ["pubmed", "clinicaltrials_gov"],
                        "evidence_refs": ["chunk:one"],
                    }
                ],
                "evidence_refs": ["chunk:one"],
            },
            summary={"blocked": True, "unresolved_lead_ids": 0},
        )
    )
    review = repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="bad",
            feedback="Retrieved records do not address canine sorafenib DLT/MTD.",
            followup_actions=[
                "rerun_search_with_refined_terms:_'sorafenib_canine_maximum_tolerated_dose',_'sorafenib_veterinary_phase_i_dog',_'robat_sorafenib_dog'",
                "manually_ingest_known_sorafenib_canine_dose-escalation_papers",
            ],
            metadata={"confidence": 0.9},
        )
    )

    result = service.escalate_agent_findings(
        AgentFindingEscalationRequest(review_ids=[review.review_id], operator="operator")
    )
    source_queries = repo.list_source_queries(active_only=True)
    source_keys = {query.source_key for query in source_queries}
    persisted_leads = repo.list_research_leads(status="followup", limit=10)
    escalation_run = repo.get_agent_run(result.agent_run_id)

    assert result.escalated_count == 1
    assert result.research_leads_created == 1
    assert result.source_queries_created >= 5
    assert persisted_leads[0].origin_review_id == review.review_id
    assert persisted_leads[0].origin_agent_run_id == run.agent_run_id
    assert persisted_leads[0].status == "followup"
    assert "sorafenib" in persisted_leads[0].topic_tags
    assert {"pubmed", "europe_pmc", "openalex", "clinicaltrials_gov", "icdc", "openfda_animal_events"}.issubset(source_keys)
    assert "avma_vctr" not in source_keys
    assert all(query.track == "validation_gap" for query in source_queries)
    assert all(query.query_params["followup_lane"] == "agent_evaluator_followup" for query in source_queries)
    assert any("sorafenib canine maximum tolerated dose" in query.query_text for query in source_queries)
    assert escalation_run is not None
    assert escalation_run.status == RunStatus.COMPLETED
    assert escalation_run.summary["research_leads_created"] == 1


def test_agent_finding_escalation_dry_run_does_not_persist(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-finding-escalation-dry-run.sqlite3", seed=False)
    service = HSAResearchService(repo)
    run = repo.create_agent_run(AgentRunRecord(agent_name="research_followup_resolver_agent", status=RunStatus.COMPLETED))
    review = repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="needs_followup",
            feedback="Needs a refined PubMed query.",
        )
    )

    result = service.escalate_agent_findings(
        AgentFindingEscalationRequest(review_ids=[review.review_id], dry_run=True)
    )

    assert result.escalated_count == 1
    assert result.research_leads_created == 0
    assert result.source_queries_created == 0
    assert result.research_leads
    assert result.source_queries
    assert repo.list_research_leads(limit=10) == []
    assert repo.list_source_queries(active_only=False) == []


def test_agent_performance_contracts_validate_allowed_values():
    row = AgentPerformanceRow(
        group_type="agent_name",
        group_value="therapy_committee_chair_agent",
        run_count=3,
        reviewed_run_count=2,
        performance_score=78,
    )
    result = AgentPerformanceReportResult(rows=[row], top_rows=[row], bottom_rows=[row])
    evaluation = AgentPerformanceEvaluationResult(evaluated_count=1, review_created_count=1)

    assert result.rows[0].group_type == "agent_name"
    assert evaluation.agent_name == "agent_performance_evaluator_agent"
    assert AgentPerformanceReportRequest().limit == 500
    assert AgentPerformanceEvaluationRequest().reviewed_only is True
    with pytest.raises(ValidationError):
        AgentPerformanceRow(group_type="wrong", group_value="bad")


def test_agent_performance_report_aggregates_latest_reviews_by_group(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-performance.sqlite3", seed=False)
    service = HSAResearchService(repo)
    now = datetime.now(UTC)

    run_one = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            agent_version="prompt-default",
            model_profile="openrouter_required",
            status=RunStatus.COMPLETED,
            source_key="pubmed",
            started_at=now - timedelta(minutes=4),
            metadata={"model_name": "anthropic/claude-sonnet-4.6", "prompt_version": "therapy-v2"},
        )
    )
    run_two = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            agent_version="prompt-default",
            model_profile="openrouter_required",
            status=RunStatus.COMPLETED,
            source_key="pubmed",
            started_at=now - timedelta(minutes=3),
            input_payload={"openrouter_model": "anthropic/claude-sonnet-4.6", "prompt_key": "therapy-v2"},
        )
    )
    run_three = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            agent_version="prompt-default",
            model_profile="openrouter_required",
            status=RunStatus.COMPLETED,
            source_key="pubmed",
            started_at=now - timedelta(minutes=2),
        )
    )
    run_four = repo.create_agent_run(
        AgentRunRecord(
            agent_name="full_text_ops_agent",
            model_profile="reviewer",
            status=RunStatus.COMPLETED,
            started_at=now - timedelta(minutes=1),
            output_payload={"evidence": {"selected_model": "anthropic/claude-sonnet-latest"}},
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run_one.agent_run_id,
            reviewer="operator",
            verdict="useful",
            created_at=now - timedelta(minutes=3),
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run_one.agent_run_id,
            reviewer="operator",
            verdict="needs_followup",
            created_at=now - timedelta(minutes=1),
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run_one.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="useful",
            created_at=now,
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run_two.agent_run_id,
            reviewer="operator",
            verdict="bad",
            created_at=now,
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run_four.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="useful",
            created_at=now,
        )
    )

    report = service.build_agent_performance_report(AgentPerformanceReportRequest(limit=10, min_sample_size=3))
    agent_row = next(row for row in report.rows if row.group_type == "agent_name" and row.group_value == "therapy_committee_chair_agent")
    model_row = next(row for row in report.rows if row.group_type == "model_key" and row.group_value == "anthropic/claude-sonnet-4.6")
    prompt_row = next(row for row in report.rows if row.group_type == "prompt_key" and row.group_value == "therapy-v2")

    assert report.agent_run_count == 4
    assert report.reviewed_run_count == 3
    assert report.unreviewed_run_count == 1
    assert report.operator_reviewed_count == 2
    assert report.evaluator_reviewed_count == 2
    assert report.disagreement_count == 1
    assert report.verdict_counts == {"bad": 1, "useful": 2}
    assert agent_row.run_count == 3
    assert agent_row.reviewed_run_count == 2
    assert agent_row.operator_reviewed_count == 2
    assert agent_row.evaluator_reviewed_count == 1
    assert agent_row.performance_score == 50
    assert agent_row.low_sample is True
    assert agent_row.disagreement_count == 1
    assert model_row.reviewed_run_count == 2
    assert prompt_row.reviewed_run_count == 2
    assert any(row.group_value == "full_text_ops_agent" for row in report.top_rows)


def test_agent_performance_evaluator_persists_specialist_reviews(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-performance-evaluator.sqlite3", seed=False)
    service = HSAResearchService(repo)
    agent_names = [
        "research_brief_synthesis_editor_agent",
        "validation_agent_omics",
        "full_text_ops_agent",
        "misc_agent",
    ]
    for agent_name in agent_names:
        run = repo.create_agent_run(
            AgentRunRecord(
                agent_name=agent_name,
                model_profile="openrouter_required",
                status=RunStatus.COMPLETED,
                input_payload={"topic": "KDR angiosarcoma"},
                output_payload={"summary": "Useful output."},
            )
        )
        repo.create_agent_run_review(
            AgentRunReviewRecord(
                agent_run_id=run.agent_run_id,
                reviewer="operator",
                verdict="useful",
            )
        )

    specialists = []

    def fake_openrouter(model_name, review_payload):
        specialists.append(review_payload["specialist"])
        assert review_payload["operator_review"]["verdict"] == "useful"
        return {
            "text": json.dumps(
                {
                    "verdict": "useful",
                    "confidence": 0.82,
                    "rationale": f"{review_payload['specialist']} evaluator agrees.",
                    "strengths": ["Clear next step."],
                    "failure_modes": [],
                    "recommended_followup_actions": ["keep_tracking"],
                    "rubric_scores": {"actionability": 0.8},
                }
            ),
            "metadata": {"provider": "openrouter", "model_name": model_name},
        }

    monkeypatch.setattr(agent_performance, "_openrouter_review_model", fake_openrouter)

    result = service.run_agent_performance_evaluation(
        AgentPerformanceEvaluationRequest(
            limit=4,
            review_models=["anthropic/claude-sonnet-4.6"],
        )
    )
    evaluator_reviews = repo.list_agent_run_reviews(reviewer="synthesis_openrouter_evaluator", limit=10)
    batch_runs = repo.list_agent_runs(agent_name="agent_performance_evaluator_agent", status="completed", limit=5)

    assert result.agent_run_id is not None
    assert result.evaluated_count == 4
    assert result.review_created_count == 4
    assert set(specialists) == {"synthesis", "validation", "ingestion", "general"}
    assert evaluator_reviews[0].reviewer_type == "llm_evaluator"
    assert evaluator_reviews[0].metadata["agent_performance_evaluation"]["model_name"] == "anthropic/claude-sonnet-4.6"
    assert batch_runs
    assert batch_runs[0].summary["review_created_count"] == 4


def test_agent_performance_evaluator_can_target_specific_agent_run(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-performance-targeted.sqlite3", seed=False)
    service = HSAResearchService(repo)
    target = repo.create_agent_run(AgentRunRecord(agent_name="research_followup_resolver_agent", status=RunStatus.COMPLETED))
    other = repo.create_agent_run(AgentRunRecord(agent_name="research_followup_resolver_agent", status=RunStatus.COMPLETED))

    def fake_openrouter(model_name, review_payload):
        assert review_payload["run"]["agent_run_id"] == str(target.agent_run_id)
        return {
            "text": json.dumps(
                {
                    "verdict": "useful",
                    "confidence": 0.8,
                    "rationale": "Targeted run is good.",
                    "strengths": [],
                    "failure_modes": [],
                    "recommended_followup_actions": [],
                    "rubric_scores": {},
                }
            ),
            "metadata": {"usage": {"cost": 0.01}},
        }

    monkeypatch.setattr(agent_performance, "_openrouter_review_model", fake_openrouter)

    result = service.run_agent_performance_evaluation(
        AgentPerformanceEvaluationRequest(
            agent_run_ids=[target.agent_run_id],
            status=None,
            reviewed_only=False,
            limit=1,
        )
    )

    assert result.evaluated_count == 1
    assert repo.list_agent_run_reviews(agent_run_id=target.agent_run_id, limit=10)
    assert repo.list_agent_run_reviews(agent_run_id=other.agent_run_id, limit=10) == []


def test_agent_performance_evaluator_payload_exposes_split_evidence_fit_policy():
    run = AgentRunRecord(
        agent_name="research_followup_loop_agent",
        model_profile="agent_performance_evaluator",
        status=RunStatus.COMPLETED,
        output_payload={
            "evidence_fit": {
                "fit": "strong",
                "target_safety_fit": "strong",
                "disease_directness_fit": "partial",
                "actionability": "strong",
                "transfer_risk": "moderate",
                "overall_fit": "strong",
            }
        },
    )

    payload = agent_performance._evaluation_payload(
        run=run,
        review_state={},
        specialist="ingestion",
        request=AgentPerformanceEvaluationRequest(limit=1),
    )

    assert payload["run"]["evidence_fit"]["target_safety_fit"] == "strong"
    assert payload["run"]["evidence_fit_interpretation"]["observed_dimensions"] == {
        "overall_fit": "strong",
        "target_safety_fit": "strong",
        "disease_directness_fit": "partial",
        "actionability": "strong",
        "transfer_risk": "moderate",
    }
    assert "split evidence-fit dimensions are interpreted correctly" in payload["rubric"]["criteria"]
    assert "Partial disease directness is not automatically bad" in agent_performance._AGENT_PERFORMANCE_SYSTEM_PROMPT


def test_agent_performance_specialist_routing_covers_agent_lanes():
    assert agent_performance._specialist_for_agent("research_synthesis_editor_agent") == "synthesis"
    assert agent_performance._specialist_for_agent("therapy_committee_chair_agent") == "synthesis"
    assert agent_performance._specialist_for_agent("validation_gap_source_pack_agent") == "ingestion"
    assert agent_performance._specialist_for_agent("research_followup_loop_agent") == "ingestion"
    assert agent_performance._specialist_for_agent("research_followup_resolver_agent") == "ingestion"
    assert agent_performance._specialist_for_agent("claim_curator_agent") == "ingestion"
    assert agent_performance._specialist_for_agent("evidence_scout_agent") == "ingestion"
    assert agent_performance._specialist_for_agent("omics_validation_agent") == "validation"
    assert agent_performance._specialist_for_agent("unknown_agent") == "general"


def test_agent_performance_evaluator_invalid_json_fails_batch_without_review(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-performance-evaluator-fail.sqlite3", seed=False)
    service = HSAResearchService(repo)
    run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            status=RunStatus.COMPLETED,
        )
    )
    repo.create_agent_run_review(AgentRunReviewRecord(agent_run_id=run.agent_run_id, reviewer="operator", verdict="useful"))

    def fake_openrouter(model_name, review_payload):
        return {"text": "not json", "metadata": {"provider": "openrouter", "model_name": model_name}}

    monkeypatch.setattr(agent_performance, "_openrouter_review_model", fake_openrouter)

    with pytest.raises(json.JSONDecodeError):
        service.run_agent_performance_evaluation(AgentPerformanceEvaluationRequest(limit=1))

    failed_runs = repo.list_agent_runs(agent_name="agent_performance_evaluator_agent", status="failed", limit=5)
    evaluator_reviews = [
        review for review in repo.list_agent_run_reviews(agent_run_id=run.agent_run_id, limit=10)
        if review.reviewer_type == "llm_evaluator"
    ]
    assert failed_runs
    assert evaluator_reviews == []


def test_model_review_summary_compacts_agent_run_payload():
    run = {
        "agent_run_id": "run-1",
        "agent_name": "full_text_ops_agent",
        "status": "completed",
        "source_key": "pmc_oa",
        "partition_date": "2026-04-27",
        "completed_at": "2026-04-28T16:51:49Z",
        "output_payload": {
            "schedule_readiness": "ready_to_enable",
            "should_block_schedule": False,
            "errors": [],
            "actions": [
                {"source_key": "all", "action": "ready_to_enable_schedule", "severity": "info", "reason": "clean"}
            ],
            "evidence": {
                "selected_model": "~anthropic/claude-sonnet-latest",
                "review_packet": {"large": "x" * 10000},
                "model_reviews": [
                    {
                        "model_name": "~anthropic/claude-sonnet-latest",
                        "status": "completed",
                        "metadata": {
                            "requested_model": "~anthropic/claude-sonnet-latest",
                            "model_name": "anthropic/claude-4.6-sonnet-20260217",
                            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15, "cost": 0.01},
                        },
                        "result": {
                            "schedule_readiness": "ready_to_enable",
                            "should_block_schedule": False,
                            "actions": [{"action": "ready_to_enable_schedule"}],
                        },
                    }
                ],
            },
        },
    }

    summary = cli_module._model_review_summary(run)

    assert summary["selected_model"] == "~anthropic/claude-sonnet-latest"
    assert summary["model_reviews"][0]["resolved_model"] == "anthropic/claude-4.6-sonnet-20260217"
    assert summary["model_reviews"][0]["usage"]["cost"] == 0.01
    assert not _contains_key(summary, "review_packet")


def test_dagster_agent_performance_assets_use_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def build_agent_performance_report(self, request):
            assert request.limit == 25
            return AgentPerformanceReportResult(
                agent_run_count=2,
                reviewed_run_count=1,
                operator_reviewed_count=1,
                rows=[
                    AgentPerformanceRow(
                        group_type="agent_name",
                        group_value="therapy_committee_chair_agent",
                        run_count=2,
                        reviewed_run_count=1,
                        performance_score=100,
                        low_sample=True,
                    )
                ],
            )

        def run_agent_performance_evaluation(self, request):
            assert request.dagster_run_id == "dagster-agent-performance-test"
            return AgentPerformanceEvaluationResult(
                evaluated_count=1,
                review_created_count=1,
                evaluations=[
                    {
                        "agent_run_id": str(uuid4()),
                        "agent_name": "therapy_committee_chair_agent",
                        "specialist": "synthesis",
                        "model_name": "test-model",
                        "verdict": "useful",
                        "confidence": 0.8,
                        "review_id": str(uuid4()),
                        "rationale": "Useful output.",
                    }
                ],
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    report_result = dagster_asset_module.agent_performance_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(op_config={"limit": 25, "min_sample_size": 3}),
        FakeRepositoryResource(),
    )
    evaluation_result = dagster_asset_module.agent_performance_evaluation_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "status": "completed",
                "limit": 1,
                "reviewed_only": True,
                "model_profile": "agent_performance_evaluator",
                "review_models": [],
            },
            run_id="dagster-agent-performance-test",
        ),
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository", "build_repository"]
    assert report_result.value["reviewed_run_count"] == 1
    assert report_result.metadata["reviewed_run_count"].value == 1
    assert evaluation_result.value["review_created_count"] == 1
    assert evaluation_result.metadata["review_created_count"].value == 1


def test_service_existing_agents_create_agent_run_ledger_rows(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-service.sqlite3", seed=True)
    _seed_full_text_source_claim(repo, "europe_pmc")
    _seed_minimal_source_claim(repo, "pubmed", curation_status="uncurated", extraction_status="draft")
    service = HSAResearchService(repo)

    triage = service.triage_full_text_issue(
        FullTextTriageRequest(
            source_key="europe_pmc",
            full_text_document_chunks=1,
            full_text_body_chars=250,
        )
    )
    scout = service.scout_sources(SourceScoutRequest(max_recommendations=2))
    curation = service.curate_claims(ClaimCurationRequest(limit=5, dry_run=True))

    assert triage.action == "no_action"
    assert scout.recommendations
    assert curation.claims_seen >= 1
    assert repo.list_agent_runs(agent_name="full_text_triage_agent", status="completed")
    assert repo.list_agent_runs(agent_name="source_scout_agent", status="completed")
    assert repo.list_agent_runs(agent_name="claim_curator_agent", status="completed")


def test_service_failed_agent_execution_records_failed_run(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-failure.sqlite3", seed=False)
    service = HSAResearchService(repo)

    def fail_run(self, request):
        raise RuntimeError("forced full-text ops failure")

    monkeypatch.setattr(full_text_ops.FullTextOpsAgent, "run", fail_run)

    with pytest.raises(RuntimeError, match="forced full-text ops failure"):
        service.run_full_text_ops(FullTextOpsRequest())

    runs = repo.list_agent_runs(agent_name="full_text_ops_agent", status="failed")
    assert len(runs) == 1
    assert runs[0].errors == ["forced full-text ops failure"]


def test_full_text_triage_agent_accepts_clean_body_chunks():
    result = FullTextTriageAgent().triage(
        FullTextTriageRequest(
            source_key="europe_pmc",
            stage="qa",
            raw_records=1,
            research_objects=1,
            document_chunks=2,
            full_text_document_chunks=1,
            full_text_body_chars=1000,
        )
    )

    assert result.action == "no_action"
    assert result.severity == "info"
    assert result.should_retry is False
    assert result.should_block_schedule is False


def test_full_text_triage_agent_reduces_batch_on_timeout():
    result = FullTextTriageAgent().triage(
        FullTextTriageRequest(
            source_key="europe_pmc",
            stage="dagster_run",
            error_message="Timed out after 2700 seconds",
            runtime_seconds=2700,
            timeout_seconds=2700,
            raw_records=10,
            research_objects=10,
            document_chunks=10,
        )
    )

    assert result.action == "reduce_batch_size"
    assert result.severity == "watch"
    assert result.should_retry is True
    assert result.should_block_schedule is True
    assert any("source/date partitioning" in action for action in result.recommended_next_actions)


def test_full_text_triage_agent_allows_empty_date_partition():
    result = FullTextTriageAgent().triage(
        FullTextTriageRequest(
            source_key="europe_pmc",
            stage="qa",
            raw_records=0,
            metadata={"allow_empty_current_run": True},
        )
    )

    assert result.action == "no_action"
    assert result.severity == "info"
    assert result.should_retry is False
    assert result.should_block_schedule is False


def test_full_text_triage_agent_flags_parser_fixture():
    result = FullTextTriageAgent().triage(
        FullTextTriageRequest(
            source_key="pmc_oa",
            stage="parse",
            error_message="XML parse error: unsupported JATS body shape",
            raw_records=1,
            research_objects=1,
            document_chunks=1,
            full_text_document_chunks=0,
        )
    )

    assert result.action == "needs_parser_fix"
    assert result.severity == "blocking"
    assert result.should_block_schedule is True
    assert any("fixture" in action for action in result.recommended_next_actions)


def test_command_center_web_lists_agent_runs_with_payloads(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-agent-runs.sqlite3", seed=False)
    service = HSAResearchService(repo)
    started_at = datetime.now(UTC) - timedelta(seconds=12)
    therapy_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            model_profile="openrouter_required",
            status=RunStatus.RUNNING,
            source_key="pubmed",
            started_at=started_at,
            input_payload={"topic": "KDR angiosarcoma therapy", "committee": "translational"},
            metadata={"estimated_cost_usd": 0.04},
        )
    )
    completed = repo.finish_agent_run(
        therapy_run.agent_run_id,
        status="completed",
        output_payload={"ranked_ideas": [{"title": "KDR/VEGFR2 validation lane"}]},
        summary={"topic": "KDR angiosarcoma therapy", "ideas": 1},
        errors=[],
    )
    assert completed is not None
    failed_seed = repo.create_agent_run(
        AgentRunRecord(
            agent_name="validation_planning_agent",
            model_profile="openrouter_required",
            status=RunStatus.RUNNING,
            source_key="x_topic_monitor",
            input_payload={"topic": "KIT mutation function"},
        )
    )
    failed = repo.finish_agent_run(
        failed_seed.agent_run_id,
        status="failed",
        output_payload={},
        summary={"topic": "KIT mutation function"},
        errors=["Missing mutation-function evidence."],
    )
    assert failed is not None
    review_payload = command_center_web.create_agent_run_review_payload(
        service,
        str(completed.agent_run_id),
        {
            "verdict": "useful",
            "feedback": "Good committee output.",
            "reviewer": "operator",
            "tags": ["KDR", "committee"],
        },
    )

    payload = command_center_web.list_agent_runs_payload(
        service,
        {"agent_name": ["therapy_committee_chair_agent"], "query": ["VEGFR2"]},
    )
    detail = command_center_web.get_agent_run_payload(service, str(completed.agent_run_id))
    failed_payload = command_center_web.list_agent_runs_payload(service, {"status": ["failed"]})

    assert payload["total"] == 1
    assert payload["visible"] == 1
    assert payload["status_counts"] == {"completed": 1}
    assert payload["agent_counts"] == {"therapy_committee_chair_agent": 1}
    assert payload["items"][0]["input_payload"]["topic"] == "KDR angiosarcoma therapy"
    assert payload["items"][0]["output_payload"]["ranked_ideas"][0]["title"] == "KDR/VEGFR2 validation lane"
    assert payload["items"][0]["duration_seconds"] is not None
    assert payload["items"][0]["review_count"] == 1
    assert payload["items"][0]["latest_review"]["verdict"] == "useful"
    assert payload["items"][0]["latest_review"]["reviewer_type"] == "operator"
    assert payload["items"][0]["latest_review"]["tags"] == ["kdr", "committee"]
    assert review_payload["item"]["feedback"] == "Good committee output."
    assert detail["item"]["agent_run_id"] == str(completed.agent_run_id)
    assert detail["item"]["summary"]["ideas"] == 1
    assert detail["item"]["latest_reviews"][0]["reviewer"] == "operator"
    assert failed_payload["visible"] == 1
    assert failed_payload["items"][0]["errors"] == ["Missing mutation-function evidence."]
    with pytest.raises(ValueError):
        command_center_web.get_agent_run_payload(service, "bad-id")
    with pytest.raises(LookupError):
        command_center_web.get_agent_run_payload(service, str(uuid4()))
    with pytest.raises(ValueError):
        command_center_web.create_agent_run_review_payload(service, str(completed.agent_run_id), {"verdict": "wrong"})
    with pytest.raises(LookupError):
        command_center_web.create_agent_run_review_payload(service, str(uuid4()), {"verdict": "bad"})


def test_command_center_web_agent_performance_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-agent-performance.sqlite3", seed=False)
    service = HSAResearchService(repo)
    run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="therapy_committee_chair_agent",
            model_profile="openrouter_required",
            status=RunStatus.COMPLETED,
            output_payload={"ranked_ideas": [{"title": "KDR validation"}]},
        )
    )
    repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run.agent_run_id,
            reviewer="operator",
            verdict="useful",
        )
    )

    def fake_openrouter(model_name, review_payload):
        assert review_payload["specialist"] == "synthesis"
        return {
            "text": json.dumps(
                {
                    "verdict": "useful",
                    "confidence": 0.9,
                    "rationale": "Clear committee output.",
                    "strengths": ["Specific validation idea."],
                    "failure_modes": [],
                    "recommended_followup_actions": ["keep"],
                    "rubric_scores": {"actionability": 0.9},
                }
            ),
            "metadata": {"provider": "openrouter", "model_name": model_name},
        }

    monkeypatch.setattr(agent_performance, "_openrouter_review_model", fake_openrouter)

    payload = command_center_web.agent_performance_payload(service)
    evaluation = command_center_web.run_agent_performance_evaluation_payload(
        service,
        {"limit": 1, "operator": "operator"},
    )
    updated = command_center_web.agent_performance_payload(service)

    assert payload["agent_run_count"] == 1
    assert payload["reviewed_run_count"] == 1
    assert payload["rows"][0]["performance_score"] == 100
    assert evaluation["review_created_count"] == 1
    assert updated["evaluator_reviewed_count"] == 1


def test_command_center_web_escalates_agent_finding_payload(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "command-center-web-agent-finding-escalation.sqlite3", seed=False)
    service = HSAResearchService(repo)
    run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="research_followup_resolver_agent",
            status="completed",
            output_payload={
                "lead_results": [
                    {
                        "title": "Sorafenib canine DLT gap",
                        "evidence_refs": ["chunk:gap"],
                    }
                ]
            },
        )
    )
    review = repo.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run.agent_run_id,
            reviewer="ingestion_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict="bad",
            feedback="Need a canine sorafenib DLT/MTD follow-up search.",
            followup_actions=["rerun_search_with_refined_terms:_'sorafenib_canine_maximum_tolerated_dose'"],
        )
    )

    payload = command_center_web.escalate_agent_findings_payload(
        service,
        {"review_id": str(review.review_id), "operator": "operator"},
    )

    assert payload["research_leads_created"] == 1
    assert payload["source_queries_created"] >= 3
    assert payload["research_leads"][0]["metadata"]["command_center"]["operator"] == "operator"
    assert repo.list_research_leads(status="followup", limit=10)
    assert repo.list_source_queries(active_only=True)
    action_items = command_center_web.build_action_items_payload(service, {"limit": ["10"]})
    assert all(item["item_id"] != f"agent-review:{review.review_id}" for item in action_items["items"])
    lead_items = [item for item in action_items["items"] if item["kind"] == "research_lead"]
    assert lead_items
    assert "run_followup_search" in lead_items[0]["actions"]
    assert "reevaluate_followup" in lead_items[0]["actions"]


def test_md_expert_agent_contract_rejects_invalid_decision():
    with pytest.raises(ValidationError):
        MDExpertAgentReviewResult(
            packet_id=uuid4(),
            packet_hash="a" * 64,
            decision="maybe",
            confidence=0.5,
            summary="Invalid decision.",
            model_profile="deterministic_only",
        )


def test_md_expert_agent_deterministic_review_persists_agent_approval(tmp_path, monkeypatch):
    repo = SQLiteResearchRepository(tmp_path / "md-expert-agent-deterministic.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(_md_queue_item())
    service.approve_validation_request_queue_item(item.queue_item_id, approved_by="unit-test")
    created = service.build_compute_job_report(
        ComputeJobReportRequest(
            queue_item_id=item.queue_item_id,
            create_from_queue_item=True,
            approved_by="unit-test",
        )
    ).created_job
    assert created is not None
    packet = service.create_md_expert_review_packet(created.compute_job_id)
    assert packet is not None

    result = service.run_md_expert_review_agent(
        MDExpertAgentReviewRequest(packet_id=packet.packet_id, model_profile="deterministic_only")
    )

    assert isinstance(result, MDExpertAgentReviewResult)
    assert result.decision == "approved"
    assert result.agent_run_id is not None
    assert result.approval_record is not None
    assert result.approval_record.reviewer_type == "md_expert_agent"
    assert result.approval_record.agent_run_id == result.agent_run_id
    approvals = repo.list_md_expert_approvals(packet_hash=packet.packet_hash, decision="approved")
    assert approvals[0].reviewer_type == "md_expert_agent"
    assert repo.get_md_expert_review_packet(packet.packet_id).status == "approved"

    requests_seen = []

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps({"id": "rp-md-agent-approved", "status": "IN_QUEUE"}).encode("utf-8")

    def fake_urlopen(request, timeout=60):
        requests_seen.append(json.loads(request.data.decode("utf-8")) if request.data else {})
        return FakeResponse()

    monkeypatch.setenv("RUNPOD_API_KEY", "temp-test-key")
    monkeypatch.setenv("HSA_RUNPOD_ENDPOINT_ID", "cbf4ffekmo36t9")
    monkeypatch.setattr(compute_runners.urllib.request, "urlopen", fake_urlopen)
    submitted = service.submit_compute_job(created.compute_job_id, dry_run=False)
    assert submitted is not None
    assert submitted.status == "submitted"
    assert submitted.runpod_job_id == "rp-md-agent-approved"
    assert submitted.metadata["md_expert_approval_id"] == str(result.approval_record.approval_id)
    assert submitted.metadata["md_expert_reviewer_type"] == "md_expert_agent"
    assert requests_seen[-1]["input"]["compound_smiles"] == "CCO"


def test_md_expert_agent_openrouter_review_persists_agent_approval(tmp_path, monkeypatch):
    repo = SQLiteResearchRepository(tmp_path / "md-expert-agent-openrouter.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(_md_queue_item())
    service.approve_validation_request_queue_item(item.queue_item_id, approved_by="unit-test")
    created = service.build_compute_job_report(
        ComputeJobReportRequest(
            queue_item_id=item.queue_item_id,
            create_from_queue_item=True,
            approved_by="unit-test",
        )
    ).created_job
    assert created is not None
    packet = service.create_md_expert_review_packet(created.compute_job_id)
    assert packet is not None

    def fake_openrouter(model_name, review_payload):
        assert model_name == "anthropic/claude-sonnet-test"
        assert review_payload["packet"]["packet_hash"] == packet.packet_hash
        return {
            "text": json.dumps(
                {
                    "decision": "approved",
                    "confidence": 0.82,
                    "summary": "Packet is acceptable for one smoke-scale worker contract test.",
                    "rationale": "Required inputs, provenance, endpoint, expected outputs, and cost bounds are present.",
                    "required_changes": [],
                    "checklist_assessment": ["Smoke-scale bounds are explicit."],
                    "risk_flags": ["Not an efficacy result."],
                }
            ),
            "metadata": {
                "provider": "openrouter",
                "model_name": "anthropic/claude-sonnet-test",
                "requested_model": "anthropic/claude-sonnet-test",
                "usage": {"cost": 0.01},
            },
        }

    monkeypatch.setattr(md_expert_agent, "_openrouter_review_model", fake_openrouter)
    result = service.run_md_expert_review_agent(
        MDExpertAgentReviewRequest(
            packet_id=packet.packet_id,
            model_profile="anthropic/claude-sonnet-test",
        )
    )

    assert result is not None
    assert result.decision == "approved"
    assert result.confidence == 0.82
    assert result.approval_record is not None
    assert result.approval_record.model_profile == "anthropic/claude-sonnet-test"
    assert result.approval_record.metadata["raw_response"]["provider_metadata"]["usage"]["cost"] == 0.01


def test_validation_request_queue_records_failed_live_agent_dispatch_without_key(monkeypatch, tmp_path):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    repo = SQLiteResearchRepository(tmp_path / "validation-agent-failure.sqlite3", seed=False)
    service = HSAResearchService(repo)
    item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="VEGF validation agent dispatch",
            task_type="expert_review",
            title="Expert review: VEGF translational signal",
            objective="Review whether the hypothesis has enough evidence for validation.",
            rationale="Human approval is required before dispatch.",
            validation_request=ValidationRequest(
                validation_type="expert_review",
                objective="Review whether the hypothesis has enough evidence for validation.",
                require_approval=True,
                assay_context=ValidationAssayContext(
                    disease_context="canine hemangiosarcoma and human angiosarcoma",
                    species=["canine", "human"],
                    model_system="human-reviewed evidence packet",
                    assay_type="expert evidence review",
                    readout="go/no-go validation readiness",
                ),
            ),
            quality_gates=["human_approval_required", "assay_context_present"],
        )
    )
    service.approve_validation_request_queue_item(item.queue_item_id, approved_by="unit-test")

    failed = service.dispatch_validation_request_queue_item(
        item.queue_item_id,
        model_profile="openrouter_required",
    )

    assert failed is not None
    assert failed.status == "failed"
    assert "OPENROUTER_API_KEY" in failed.last_error
    assert failed.metadata["validation_agent_model_profile"] == "openrouter_required"


def test_validation_gap_ingest_filters_agent_evaluator_followup_lane(monkeypatch):
    repo = InMemoryResearchRepository()
    origin_review_id = uuid4()
    origin_agent_run_id = uuid4()
    selected = SourceQuery(
        source_key="pubmed",
        query_name="agent_eval_selected",
        query_text="sorafenib canine maximum tolerated dose",
        query_params={
            "followup_lane": "agent_evaluator_followup",
            "origin_review_id": str(origin_review_id),
            "origin_agent_run_id": str(origin_agent_run_id),
        },
        track="validation_gap",
    )
    repo.upsert_source_query(selected)
    repo.upsert_source_query(
        SourceQuery(
            source_key="pubmed",
            query_name="validation_gap_other",
            query_text="sorafenib canine safety",
            query_params={"lane": "safety_signal"},
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

    result = HSAResearchService(repo).ingest_validation_gap_source_queries(
        ValidationGapSourceIngestRequest(
            source_keys=["pubmed"],
            followup_lane="agent_evaluator_followup",
            origin_review_ids=[origin_review_id],
            origin_agent_run_ids=[origin_agent_run_id],
            dry_run=False,
        )
    )

    assert result.query_count == 1
    assert result.source_queries == [selected]
    assert calls == [("agent_eval_selected", {})]


def test_x_topic_review_agent_flags_ingestible_links_and_skips_social_only():
    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_trial_monitoring",
                    "username": "vetonc",
                    "quality_score": 0.7,
                    "durable_links": ["https://pubmed.ncbi.nlm.nih.gov/123456/"],
                    "matched_terms": ["canine hemangiosarcoma"],
                },
                {
                    "post_id": "456",
                    "query_name": "x_disease_monitoring",
                    "username": "owner",
                    "quality_score": 0.4,
                    "durable_links": [],
                    "matched_terms": ["angiosarcoma"],
                },
            ],
        )
    )

    assert result.ingestion_candidate_count == 1
    assert result.rejected_count == 1
    assert result.actions[0].action == "flag_for_ingestion"
    assert result.actions[0].ingestible_links[0].recommended_source_key == "pubmed"
    assert result.actions[0].ingestible_links[0].identifier == "123456"
    assert result.actions[1].action == "skip_no_durable_source"


def test_service_x_topic_review_creates_agent_run(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "x-topic-agent.sqlite3", seed=False)

    result = HSAResearchService(repo).run_x_topic_review(
        XTopicReviewRequest(
            review_mode="deterministic_only",
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_trial_monitoring",
                    "quality_score": 0.7,
                    "durable_links": ["https://clinicaltrials.gov/study/NCT12345678"],
                }
            ],
        )
    )

    assert result.agent_run_id is not None
    assert result.actions[0].ingestible_links[0].recommended_source_key == "clinicaltrials_gov"
    runs = repo.list_agent_runs(agent_name="x_topic_review_agent", status="completed", source_key="x_topic_monitor")
    assert runs
    assert runs[0].summary["ingestion_candidate_count"] == 1


def test_mcp_agent_run_tools_dump_json_safe_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-agent-runs.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)

    payload = mcp_server.run_full_text_ops_tool(
        source_keys=["europe_pmc"],
        partition_date="2026-04-27",
        review_mode="deterministic_only",
    )
    run_payload = mcp_server.get_agent_run_tool(payload["agent_run_id"])
    runs_payload = mcp_server.list_agent_runs_tool(agent_name="full_text_ops_agent")

    assert payload["agent_run_id"]
    assert payload["actions"]
    assert run_payload["agent_run_id"] == payload["agent_run_id"]
    assert runs_payload[0]["agent_run_id"] == payload["agent_run_id"]


def test_mcp_agent_performance_tools_dump_json_safe_payloads(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "mcp-agent-performance.sqlite3", seed=False)
    service = HSAResearchService(repo)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)
    run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="full_text_ops_agent",
            status=RunStatus.COMPLETED,
            output_payload={"schedule_readiness": "keep_stopped"},
        )
    )
    repo.create_agent_run_review(AgentRunReviewRecord(agent_run_id=run.agent_run_id, verdict="needs_followup"))

    def fake_openrouter(model_name, review_payload):
        return {
            "text": json.dumps(
                {
                    "verdict": "needs_followup",
                    "confidence": 0.7,
                    "rationale": "Needs partition evidence.",
                    "strengths": ["Useful blocker."],
                    "failure_modes": [],
                    "recommended_followup_actions": ["run_source_date_partition"],
                    "rubric_scores": {"evidence_paths": 0.7},
                }
            ),
            "metadata": {"provider": "openrouter", "model_name": model_name},
        }

    monkeypatch.setattr(agent_performance, "_openrouter_review_model", fake_openrouter)

    report_payload = mcp_server.agent_performance_report_tool(limit=10)
    evaluation_payload = mcp_server.run_agent_performance_evaluation_tool(limit=1)

    assert report_payload["reviewed_run_count"] == 1
    assert report_payload["rows"]
    assert evaluation_payload["review_created_count"] == 1


def test_claim_curator_agent_promotes_supported_draft_claim(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    papers_path = tmp_path / "papers.json"
    papers_path.write_text(
        """
        [
          {
            "pmid": "123",
            "title": "VEGF in canine hemangiosarcoma",
            "abstract": "Canine hemangiosarcoma studies discuss VEGF and angiogenesis.",
            "journal": "Example Journal",
            "year": "2026",
            "source": "pubmed"
          },
          {
            "pmid": "124",
            "title": "Canine hemangiosarcoma angiogenesis",
            "abstract": "Canine hemangiosarcoma work again discusses VEGF and angiogenesis.",
            "journal": "Example Journal",
            "year": "2026",
            "source": "pubmed"
          }
        ]
        """
    )
    backfill_papers_json(repo, papers_path)
    extract_claims_for_repository(repo, source_key="current_papers")

    result = ClaimCuratorAgent(repo).curate(ClaimCurationRequest(limit=20, promote_threshold=0.5))
    visible_claims = repo.search_claims(ClaimSearchRequest(query="VEGFA", species="canine", min_confidence=0.1))

    assert result.claims_seen >= 2
    assert result.promoted >= 1
    assert result.merged_duplicates >= 1
    assert any(claim.metadata["curation_status"] == "promote" for claim in visible_claims)


def test_x_linked_article_followup_collects_agent_links_and_parses(tmp_path, monkeypatch):
    article_url = "https://cancer.ufl.edu/2026/04/20/angiosarcoma-frontier/"
    html = b"""
    <html>
      <head><title>Angiosarcoma frontier</title></head>
      <body>
        <a href="https://pubmed.ncbi.nlm.nih.gov/87654321/">Primary paper</a>
        DOI: 10.1158/0008-5472.CAN-26-0002
      </body>
    </html>
    """

    def fake_fetch_url(url):
        assert url == article_url
        return scraper_bridge.FetchedPage(url=url, status_code=200, mime_type="text/html", content=html)

    monkeypatch.setattr(scraper_bridge, "_fetch_url", fake_fetch_url)
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    repo.create_agent_run(
        AgentRunRecord(
            agent_name="x_topic_review_agent",
            status=RunStatus.COMPLETED,
            source_key="x_topic_monitor",
            output_payload={
                "actions": [
                    {
                        "source_record_id": "tweet-1",
                        "action": "queue_source_followup",
                        "ingestible_links": [
                            {
                                "url": article_url,
                                "recommended_source_key": "x_linked_article",
                                "identifier_type": "unknown",
                                "identifier": None,
                                "should_ingest": False,
                                "reason": "Controlled scraper follow-up.",
                            }
                        ],
                    }
                ]
            },
        )
    )

    result = HSAResearchService(repo).run_x_linked_article_followup(
        XLinkedArticleFollowupRequest(
            approved_by="unit-test",
            approval_note="robots reviewed",
            recent_run_limit=5,
        )
    )

    assert result.candidate_urls == [article_url]
    assert result.fetched_pages == 1
    assert result.parsed_records == 1
    assert result.review_ids
    assert result.candidate_results[0]["status"] == "parsed"
    assert result.candidate_results[0]["url"] == article_url
    assert result.candidate_results[0]["artifact_id"]
    assert result.candidate_results[0]["review_id"]
    assert result.candidate_results[0]["primary_source_link_count"] == 2
    assert {
        (link["recommended_source_key"], link["identifier_type"], link["identifier"])
        for link in result.primary_source_links
    } >= {
        ("pubmed", "pmid", "87654321"),
        ("crossref", "doi", "10.1158/0008-5472.CAN-26-0002"),
    }


def test_source_followup_queue_reads_linked_article_agent_recommendations(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "agent-followups.sqlite3", seed=False)
    review = repo.upsert_scrape_review(
        ScrapeReviewRecord(
            source_key="x_linked_article",
            artifact_id=uuid4(),
            source_record_id="article-context",
            title="Angiosarcoma context article",
            canonical_url="https://example.edu/article",
            parser_confidence=0.45,
            fields={
                "primary_source_links": [],
                "evidence_spans": [
                    {
                        "text": "The article mentions DOI 10.1158/0008-5472.CAN-26-0002.",
                        "matched_terms": ["doi"],
                    }
                ],
            },
        )
    )
    agent_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="x_linked_article_review_agent",
            status=RunStatus.COMPLETED,
            source_key="x_linked_article",
            output_payload={
                "actions": [
                    {
                        "review_id": str(review.review_id),
                        "source_record_id": review.source_record_id,
                        "action": "queue_primary_source_followup",
                        "reason": "Agent found a validated DOI in the article context.",
                        "followup_links": [
                            {
                                "url": "https://doi.org/10.1158/0008-5472.CAN-26-0002",
                                "recommended_source_key": "crossref",
                                "identifier_type": "doi",
                                "identifier": "10.1158/0008-5472.CAN-26-0002",
                                "should_ingest": True,
                                "reason": "Validated DOI from linked article review.",
                            }
                        ],
                    }
                ]
            },
        )
    )

    result = HSAResearchService(repo).queue_source_followups(
        SourceFollowupQueueRequest(review_ids=[review.review_id])
    )

    assert result.reviewed_records == 1
    assert result.agent_runs_seen == 1
    assert result.agent_recommendations_seen == 1
    assert result.queued == 1
    row = HSAResearchService(repo).list_source_followups(source_key="crossref")[0]
    assert row.identifier == "10.1158/0008-5472.can-26-0002"
    assert row.origin_agent_run_id == agent_run.agent_run_id
    assert row.metadata["recommendation_source"] == "linked_article_review_agent"
    assert row.metadata["agent_action_reason"] == "Agent found a validated DOI in the article context."


def test_source_followup_queue_reads_x_topic_agent_primary_source_flags(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "x-topic-followups.sqlite3", seed=False)
    agent_run = repo.create_agent_run(
        AgentRunRecord(
            agent_name="x_topic_review_agent",
            status=RunStatus.COMPLETED,
            source_key="x_topic_monitor",
            output_payload={
                "actions": [
                    {
                        "source_record_id": "tweet-1",
                        "query_name": "x_trial_monitoring",
                        "username": "vetonc",
                        "action": "flag_for_ingestion",
                        "reason": "Candidate links to a durable PubMed record.",
                        "ingestible_links": [
                            {
                                "url": "https://pubmed.ncbi.nlm.nih.gov/12345678/",
                                "recommended_source_key": "pubmed",
                                "identifier_type": "pmid",
                                "identifier": "12345678",
                                "should_ingest": True,
                                "reason": "PubMed link found in X topic review.",
                            },
                            {
                                "url": "https://example.edu/article",
                                "recommended_source_key": "x_linked_article",
                                "identifier_type": "unknown",
                                "identifier": None,
                                "should_ingest": False,
                                "reason": "Context article.",
                            },
                        ],
                    }
                ]
            },
        )
    )

    result = HSAResearchService(repo).queue_source_followups(
        SourceFollowupQueueRequest(source_key="x_topic_monitor")
    )

    assert result.reviewed_records == 0
    assert result.agent_runs_seen == 1
    assert result.agent_recommendations_seen == 2
    assert result.queued == 1
    assert result.skipped_uningestible == 1
    row = HSAResearchService(repo).list_source_followups(source_key="pubmed")[0]
    assert row.identifier == "12345678"
    assert row.origin_source_key == "x_topic_monitor"
    assert row.origin_agent_run_id == agent_run.agent_run_id
    assert row.metadata["recommendation_source"] == "x_topic_review_agent"
    assert row.metadata["source_record_id"] == "tweet-1"


def test_x_linked_article_review_agent_recommends_queue_and_ledgers(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "linked-review.sqlite3", seed=False)
    review = repo.upsert_scrape_review(
        ScrapeReviewRecord(
            source_key="x_linked_article",
            artifact_id=uuid4(),
            source_record_id="article-1",
            title="Angiosarcoma article",
            parser_confidence=0.7,
            fields={
                "primary_source_links": [
                    {
                        "url": "https://pubmed.ncbi.nlm.nih.gov/12345678/",
                        "recommended_source_key": "pubmed",
                        "identifier_type": "pmid",
                        "identifier": "12345678",
                        "should_ingest": True,
                        "reason": "PubMed link found.",
                    }
                ]
            },
        )
    )

    result = HSAResearchService(repo).run_x_linked_article_review(
        XLinkedArticleReviewRequest(review_ids=[review.review_id], review_mode="deterministic_only")
    )

    assert result.agent_run_id is not None
    assert result.queue_candidate_count == 1
    assert result.actions[0].action == "queue_primary_source_followup"
    assert result.actions[0].followup_links[0].recommended_source_key == "pubmed"
    runs = repo.list_agent_runs(agent_name="x_linked_article_review_agent", status="completed")
    assert runs
    assert runs[0].summary["queue_candidate_count"] == 1


def test_x_linked_article_review_agent_uses_context_without_queueing_it(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "linked-review-context.sqlite3", seed=False)
    review = repo.upsert_scrape_review(
        ScrapeReviewRecord(
            source_key="x_linked_article",
            artifact_id=uuid4(),
            source_record_id="article-context",
            title="Angiosarcoma conference article",
            canonical_url="https://cancer.ufl.edu/article",
            parser_confidence=0.45,
            fields={
                "primary_source_links": [],
                "context_links": [
                    {
                        "href": "https://www.abstractsonline.com/pp8/#!/21436/presentation/7856",
                        "text": "presented",
                        "host": "www.abstractsonline.com",
                        "reason": "conference_abstract_link",
                    }
                ],
                "evidence_spans": [
                    {
                        "text": "The work was presented at AACR and showed RAS plays a role.",
                        "matched_terms": ["presented", "RAS"],
                        "reason": "article_body_source_context",
                    }
                ],
                "article_text_preview": "The work was presented at AACR.",
            },
        )
    )

    result = HSAResearchService(repo).run_x_linked_article_review(
        XLinkedArticleReviewRequest(review_ids=[review.review_id], review_mode="deterministic_only")
    )

    action = result.actions[0]
    assert action.action == "needs_human_review"
    assert action.followup_links == []
    assert action.metadata["context_link_count"] == 1
    assert action.metadata["context_links"][0]["reason"] == "conference_abstract_link"
    assert result.queue_candidate_count == 0
    assert result.needs_human_review_count == 1
    leads = repo.list_research_leads(status="new", source_key="x_linked_article")
    assert len(leads) == 1
    assert leads[0].origin_review_id == review.review_id
    assert leads[0].url == "https://cancer.ufl.edu/article"
    assert "angiosarcoma" in leads[0].topic_tags

    collected = HSAResearchService(repo).collect_research_leads(ResearchLeadCollectRequest())
    assert collected.skipped_existing == 1
