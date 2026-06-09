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

def test_dagster_all_api_smoke_is_ingestion_only(monkeypatch):
    sentinel_repository = object()
    calls = []

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    def fail_full_pipeline(*args, **kwargs):
        raise AssertionError("all_api_smoke_job must stay ingestion-only")

    def fake_ingestion_pipeline(repository, **kwargs):
        assert repository is sentinel_repository
        assert kwargs == {
            "source_keys": dagster_asset_module.ALL_API_SMOKE_KEYS,
            "source_limits": {source_key: 1 for source_key in dagster_asset_module.ALL_API_SMOKE_KEYS},
        }
        return {
            "mode": "ingestion_only",
            "source_keys": kwargs["source_keys"],
            "sources": [
                {
                    "source_key": "unpaywall",
                    "qa": {
                        "raw_records": 1,
                        "research_objects": 1,
                        "document_chunks": 1,
                        "claims": 0,
                    },
                }
            ],
            "totals": {
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 1,
                "claims": 0,
            },
            "errors": [],
        }

    monkeypatch.setattr(structured_orchestration, "run_structured_sources_pipeline", fail_full_pipeline)
    monkeypatch.setattr(structured_orchestration, "run_structured_sources_ingestion_pipeline", fake_ingestion_pipeline)

    result = dagster_asset_module.all_api_smoke_report.node_def.compute_fn.decorated_fn(FakeRepositoryResource())
    check = dagster_asset_module.all_api_smoke_has_minimum_outputs.node_def.compute_fn.decorated_fn(result)

    assert calls == ["build_repository"]
    assert result["mode"] == "ingestion_only"
    assert check.passed is True
    assert check.metadata["mode"].value == "ingestion_only"


def test_dagster_literature_corpus_source_date_asset_uses_single_source_partition(monkeypatch):
    sentinel_repository = object()
    calls = []

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    def fake_pipeline(repository, **kwargs):
        assert repository is sentinel_repository
        return {
            **kwargs,
            "sources": [],
            "totals": {"raw_records": 0},
            "errors": [],
        }

    monkeypatch.setattr(structured_orchestration, "run_structured_sources_pipeline", fake_pipeline)

    partition_context = SimpleNamespace(
        multi_partition_key=dagster_asset_module.dg.MultiPartitionKey(
            {
                "source": "pubmed",
                "date": "2026-04-27",
            }
        )
    )
    result = dagster_asset_module.literature_corpus_source_date_report.node_def.compute_fn.decorated_fn(
        partition_context,
        FakeRepositoryResource(),
    )

    assert calls == ["build_repository"]
    assert isinstance(result, dagster_asset_module.dg.MaterializeResult)
    assert result.value["source_keys"] == ("pubmed",)
    assert result.value["source_limits"] == {"pubmed": 100}
    assert result.value["partition_date"] == "2026-04-27"
    assert result.value["mode"] == "source_date_partition"
    assert result.value["date_filter_status"] == "orchestration_metadata_only"
    assert result.metadata["source_key"] == "pubmed"
    assert result.metadata["partition_date"] == "2026-04-27"
    assert result.metadata["date_filter_status"] == "orchestration_metadata_only"


def test_dagster_literature_corpus_source_date_partitions_and_job_are_wired():
    assert dagster_asset_module.literature_corpus_source_date_job is not None
    assert (
        dagster_asset_module.literature_corpus_source_date_report.partitions_def
        is dagster_asset_module.LITERATURE_CORPUS_SOURCE_DATE_PARTITIONS
    )
    assert dagster_asset_module.LITERATURE_CORPUS_SOURCE_PARTITIONS.get_partition_keys() == list(
        dagster_asset_module.LITERATURE_CORPUS_SOURCE_KEYS
    )
    assert dagster_asset_module.LITERATURE_CORPUS_DATE_PARTITIONS.start.date().isoformat() == "2026-01-01"


def test_dagster_source_health_report_lives_in_control_panel_group():
    assert dagster_asset_module.source_health_report.group_names_by_key == {
        dagster_asset_module.dg.AssetKey(["source_health_report"]): "control_panel"
    }


def test_dagster_candidate_contribution_intake_report_lives_in_control_panel_group():
    assert dagster_asset_module.candidate_contribution_intake_report.group_names_by_key == {
        dagster_asset_module.dg.AssetKey(["candidate_contribution_intake_report"]): "control_panel"
    }


def test_dagster_candidate_contribution_intake_report_uses_report_builder(monkeypatch):
    calls = []

    def fake_report(**kwargs):
        calls.append(kwargs)
        return {
            "storage_configured": True,
            "table_available": True,
            "summary": {
                "row_count": 1,
                "queued_for_intake": 1,
                "triage_in_progress": 0,
                "needs_more_information": 0,
                "actionable_count": 1,
                "no_action_count": 0,
            },
            "status_counts": {"queued_for_intake": 1},
            "requested_action_counts": {"evidence_review": 1},
            "recommended_route_counts": {"accepted_for_evidence_review": 1},
            "rows": [
                {
                    "contribution_id": "11111111-1111-1111-1111-111111111111",
                    "display_id": "TWOG-15F50D",
                    "status": "queued_for_intake",
                    "contribution_type": "evidence",
                    "requested_system_action": "evidence_review",
                    "recommended_route": "accepted_for_evidence_review",
                    "evidence_count": 1,
                    "artifact_count": 0,
                    "created_at": "2026-05-19T00:00:00+00:00",
                    "route_reason": "Queue for citation/provenance review before synthesis.",
                }
            ],
            "errors": [],
        }

    monkeypatch.setattr(candidate_contribution_intake, "build_candidate_contribution_intake_report", fake_report)
    result = dagster_asset_module.candidate_contribution_intake_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "statuses": ["queued_for_intake"],
                "candidate_ids": ["twog-candidate-447eb8089965"],
                "limit": 10,
                "include_packet": False,
            }
        )
    )

    assert calls == [
        {
            "statuses": ["queued_for_intake"],
            "candidate_ids": ["twog-candidate-447eb8089965"],
            "limit": 10,
            "include_packet": False,
        }
    ]
    assert result.value["summary"]["row_count"] == 1
    assert result.metadata["row_count"].value == 1
    assert result.metadata["intake_rows"].records[0].data["display_id"] == "TWOG-15F50D"


def test_dagster_candidate_contribution_triage_report_lives_in_control_panel_group():
    assert dagster_asset_module.candidate_contribution_triage_report.group_names_by_key == {
        dagster_asset_module.dg.AssetKey(["candidate_contribution_triage_report"]): "control_panel"
    }
    assert dagster_asset_module.candidate_contribution_triage_job is not None


def test_dagster_candidate_contribution_triage_report_uses_triage_builder(monkeypatch):
    calls = []

    def fake_triage(**kwargs):
        calls.append(kwargs)
        return {
            "dry_run": True,
            "action": "start_triage",
            "target_status": "triage_in_progress",
            "operator": "dagster-test",
            "summary": {
                "requested_count": 1,
                "selected_count": 1,
                "missing_count": 0,
                "updated_count": 0,
            },
            "missing_contribution_ids": [],
            "rows": [
                {
                    "contribution_id": "11111111-1111-1111-1111-111111111111",
                    "display_id": "TWOG-15F50D",
                    "old_status": "queued_for_intake",
                    "new_status": "triage_in_progress",
                    "action": "start_triage",
                    "operator": "dagster-test",
                    "promoted_queue_id": None,
                    "would_update": True,
                }
            ],
            "errors": [],
        }

    monkeypatch.setattr(candidate_contribution_intake, "triage_candidate_contributions", fake_triage)
    result = dagster_asset_module.candidate_contribution_triage_report.node_def.compute_fn.decorated_fn(
        SimpleNamespace(
            op_config={
                "contribution_ids": ["11111111-1111-1111-1111-111111111111"],
                "action": "start_triage",
                "operator": "dagster-test",
                "review_notes": "Taking first look.",
                "dry_run": True,
            }
        )
    )

    assert calls == [
        {
            "contribution_ids": ["11111111-1111-1111-1111-111111111111"],
            "action": "start_triage",
            "operator": "dagster-test",
            "review_notes": "Taking first look.",
            "dry_run": True,
        }
    ]
    assert result.value["target_status"] == "triage_in_progress"
    assert result.metadata["dry_run"] is True
    assert result.metadata["selected_count"].value == 1
    assert result.metadata["triage_rows"].records[0].data["display_id"] == "TWOG-15F50D"


def test_dagster_metadata_table_rows_encode_nested_values():
    rows = dagster_asset_module._compact_table_rows(
        [
            {
                "source_key": "pubchem",
                "raw_records": 1,
                "claim_status": {"promote": 1},
                "sample_claims": [{"statement": "Propranolol has PubChem identity CID 4946."}],
                "passes_minimum_bar": True,
            }
        ],
        columns=("source_key", "raw_records", "claim_status", "sample_claims", "passes_minimum_bar"),
    )

    assert rows[0]["source_key"] == "pubchem"
    assert rows[0]["raw_records"] == 1
    assert rows[0]["passes_minimum_bar"] is True
    assert json.loads(rows[0]["claim_status"]) == {"promote": 1}
    assert json.loads(rows[0]["sample_claims"]) == [
        {"statement": "Propranolol has PubChem identity CID 4946."}
    ]
    assert all(value is None or isinstance(value, str | int | float | bool) for value in rows[0].values())


def test_dagster_count_report_asset_returns_materialize_result_with_report_value(monkeypatch):
    sentinel_repository = object()
    calls = []
    report = {
        "source_keys": ["pubchem"],
        "sources": [
            {
                "source_key": "pubchem",
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 1,
                "entity_mentions": 0,
                "claims": 1,
                "passes_minimum_bar": True,
                "claim_status": {"promote": 1},
                "claim_types": {"other": 1},
            }
        ],
        "totals": {
            "raw_records": 1,
            "research_objects": 1,
            "document_chunks": 1,
            "entity_mentions": 0,
            "claims": 1,
        },
        "failed_sources": [],
        "passes_minimum_bar": True,
        "minimum_bar": {"require_claims": True},
        "coverage": {"claims": 1},
    }

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    def fake_count_report(repository, **kwargs):
        assert repository is sentinel_repository
        assert kwargs == {
            "source_keys": dagster_asset_module.HOSTED_API_REPORT_KEYS,
            "sample_limit": 3,
            "require_claims": True,
        }
        return report

    monkeypatch.setattr(structured_orchestration, "build_structured_source_count_report", fake_count_report)

    result = dagster_asset_module.structured_source_count_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == ["build_repository"]
    assert isinstance(result, dagster_asset_module.dg.MaterializeResult)
    assert result.value is report
    assert result.metadata["source_count"] == 1
    assert result.metadata["passes_minimum_bar"] is True
    table_row = result.metadata["source_count_table"].records[0].data
    assert json.loads(table_row["claim_status"]) == {"promote": 1}
    assert json.loads(table_row["claim_types"]) == {"other": 1}


def test_dagster_embedding_index_check_requires_embedding_when_chunks_exist():
    failing_result = dagster_asset_module.embedding_index_has_minimum_outputs.node_def.compute_fn.decorated_fn(
        {
            "errors": [],
            "totals": {"chunks_seen": 1},
            "embedding_coverage": {"total_chunks": 1, "embedded_chunks": 0},
        }
    )
    empty_store_result = dagster_asset_module.embedding_index_has_minimum_outputs.node_def.compute_fn.decorated_fn(
        {
            "errors": [],
            "totals": {"chunks_seen": 0},
            "embedding_coverage": {"total_chunks": 0, "embedded_chunks": 0},
        }
    )
    populated_store_result = dagster_asset_module.embedding_index_has_minimum_outputs.node_def.compute_fn.decorated_fn(
        {
            "errors": [],
            "totals": {"chunks_seen": 3},
            "embedding_coverage": {"total_chunks": 3, "embedded_chunks": 3},
        }
    )

    assert failing_result.passed is False
    assert empty_store_result.passed is True
    assert populated_store_result.passed is True


def test_dagster_embedding_maintenance_check_requires_full_active_model_coverage():
    failing_result = dagster_asset_module.embedding_maintenance_has_clean_coverage.node_def.compute_fn.decorated_fn(
        {
            "errors": [],
            "orphan_embeddings": {"seen": 0, "deleted": 0},
            "embedding_coverage": {"total_chunks": 3, "embedded_chunks": 2, "missing_chunks": 1},
        }
    )
    empty_store_result = dagster_asset_module.embedding_maintenance_has_clean_coverage.node_def.compute_fn.decorated_fn(
        {
            "errors": [],
            "orphan_embeddings": {"seen": 0, "deleted": 0},
            "embedding_coverage": {"total_chunks": 0, "embedded_chunks": 0, "missing_chunks": 0},
        }
    )
    populated_store_result = dagster_asset_module.embedding_maintenance_has_clean_coverage.node_def.compute_fn.decorated_fn(
        {
            "errors": [],
            "orphan_embeddings": {"seen": 1, "deleted": 1},
            "embedding_coverage": {"total_chunks": 3, "embedded_chunks": 3, "missing_chunks": 0},
        }
    )

    assert failing_result.passed is False
    assert empty_store_result.passed is True
    assert populated_store_result.passed is True


def test_dagster_full_text_check_requires_full_text_body_chunks():
    report = {
        "source_keys": ["europe_pmc"],
        "sources": [
            {
                "source_key": "europe_pmc",
                "qa": {
                    "raw_records": 1,
                    "research_objects": 1,
                    "document_chunks": 1,
                    "claims": 1,
                },
                "full_text_qa": {
                    "passes_full_text_bar": False,
                    "full_text_document_chunks": 0,
                    "triage": {
                        "action": "needs_parser_fix",
                        "severity": "blocking",
                        "should_retry": False,
                        "should_block_schedule": True,
                        "reasons": ["Records were persisted but no full-text body chunks were written."],
                        "recommended_next_actions": ["Inspect chunk_text_sections."],
                    },
                },
            }
        ],
        "errors": [],
        "totals": {"raw_records": 1, "research_objects": 1, "document_chunks": 1, "claims": 1},
    }
    annotated = dagster_asset_module._annotate_full_text_report(report, mode="refresh")

    result = dagster_asset_module.literature_full_text_refresh_has_outputs.node_def.compute_fn.decorated_fn(annotated)

    assert result.passed is False
    assert result.metadata["failed_sources"].data == ["europe_pmc"]
    assert result.metadata["full_text_blocking_sources"].data == ["europe_pmc"]
    assert "full_text_triage" in result.metadata


def test_dagster_full_text_partition_check_allows_empty_partition():
    report = {
        "mode": "source_date_partition",
        "partition_date": "2026-04-27",
        "source_keys": ["europe_pmc"],
        "sources": [
            {
                "source_key": "europe_pmc",
                "qa": {
                    "raw_records": 0,
                    "research_objects": 0,
                    "document_chunks": 0,
                    "claims": 0,
                },
                "full_text_qa": {
                    "passes_full_text_bar": True,
                    "current_empty_passes": True,
                    "triage": {
                        "action": "no_action",
                        "severity": "info",
                        "should_retry": False,
                        "should_block_schedule": False,
                        "reasons": ["The date-partitioned source run completed with no records."],
                        "recommended_next_actions": ["Mark the partition clean."],
                    },
                },
            }
        ],
        "errors": [],
        "full_text_triage": [],
        "totals": {"raw_records": 0, "research_objects": 0, "document_chunks": 0, "claims": 0},
    }

    result = dagster_asset_module.literature_full_text_source_date_has_outputs.node_def.compute_fn.decorated_fn(
        report
    )

    assert result.passed is True
    assert result.metadata["empty_sources"].data == ["europe_pmc"]


def test_dagster_x_topic_monitor_review_asset_uses_twitterapi_io(monkeypatch):
    calls = []
    repo = InMemoryResearchRepository()

    class FakeTwitterApiIoProvider:
        def search(self, request):
            calls.append(request.query_name)
            assert request.max_results == 10
            return x_topic_monitor.XTopicProviderResult(
                provider="twitterapi_io",
                query_name=request.query_name,
                raw_tweet_count=2,
                candidates=[
                    x_topic_monitor.XTopicReviewCandidate(
                        source_record_id="123",
                        canonical_url="https://x.com/vetonc/status/123",
                        username="vetonc",
                        matched_query_name=request.query_name,
                        matched_terms=["canine hemangiosarcoma", "trial"],
                        durable_links=["https://pubmed.ncbi.nlm.nih.gov/123456/"],
                        quality_score=0.7,
                    )
                ],
            )

    class FakeRepositoryResource:
        def build_repository(self):
            return repo

    monkeypatch.setenv("HSA_X_TOPIC_QUERY_NAME", "x_trial_monitoring")
    monkeypatch.setenv("HSA_X_TOPIC_MAX_RESULTS", "10")
    monkeypatch.setenv("HSA_X_TOPIC_REVIEW_MODE", "deterministic_only")
    monkeypatch.setattr(x_topic_monitor, "TwitterApiIoProvider", FakeTwitterApiIoProvider)

    result = dagster_asset_module.x_topic_monitor_review_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == ["x_trial_monitoring"]
    assert result.value["provider"] == "twitterapi_io"
    assert result.value["raw_tweet_count"] == 2
    assert result.value["candidate_count"] == 1
    assert result.value["candidates"][0]["post_id"] == "123"
    assert result.value["agent_review"]["ingestion_candidate_count"] == 1
    assert result.value["agent_review"]["actions"][0]["action"] == "flag_for_ingestion"
    assert result.value["manual_review_required"] is True
    assert repo.list_agent_runs(agent_name="x_topic_review_agent", status="completed")
    assert dagster_asset_module.x_topic_monitor_review_job is not None


def test_dagster_x_topic_monitor_review_asset_paces_twitterapi_io_queries(monkeypatch):
    calls = []
    sleeps = []
    repo = InMemoryResearchRepository()
    queries = [
        SourceQuery(
            source_key=x_topic_monitor.X_TOPIC_SOURCE_KEY,
            query_name="first_query",
            query_text='"canine hemangiosarcoma"',
            object_type=ResearchObjectType.KNOWLEDGE_ENTRY,
        ),
        SourceQuery(
            source_key=x_topic_monitor.X_TOPIC_SOURCE_KEY,
            query_name="second_query",
            query_text='"angiosarcoma" "dog"',
            object_type=ResearchObjectType.KNOWLEDGE_ENTRY,
        ),
    ]

    class FakeTwitterApiIoProvider:
        def search(self, request):
            calls.append(request.query_name)
            return x_topic_monitor.XTopicProviderResult(
                provider="twitterapi_io",
                query_name=request.query_name,
                raw_tweet_count=0,
                candidates=[],
            )

    class FakeRepositoryResource:
        def build_repository(self):
            return repo

    monkeypatch.delenv("HSA_X_TOPIC_QUERY_NAME", raising=False)
    monkeypatch.setenv("HSA_X_TOPIC_MAX_RESULTS", "10")
    monkeypatch.setenv("HSA_X_TOPIC_QUERY_DELAY_SECONDS", "0.25")
    monkeypatch.setenv("HSA_X_TOPIC_REVIEW_MODE", "deterministic_only")
    monkeypatch.setattr(x_topic_monitor, "build_default_source_queries", lambda: queries)
    monkeypatch.setattr(x_topic_monitor, "TwitterApiIoProvider", FakeTwitterApiIoProvider)
    monkeypatch.setattr(dagster_asset_module.time, "sleep", lambda seconds: sleeps.append(seconds))

    result = dagster_asset_module.x_topic_monitor_review_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == ["first_query", "second_query"]
    assert sleeps == [0.25]
    assert result.value["query_delay_seconds"] == 0.25
    assert result.value["raw_tweet_count"] == 0
