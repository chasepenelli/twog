import json
from types import SimpleNamespace
import xml.etree.ElementTree as ET
from uuid import uuid4

import pytest

from hsa_research.ingestion_bridge import dagster_assets as dagster_asset_module
from hsa_research.ingestion_bridge import cli as cli_module
from hsa_research.ingestion_bridge import storage, structured_orchestration
from hsa_research.ingestion_bridge.contracts import (
    AgentRunRecord,
    BoltzRunRequest,
    CandidateDossierRequest,
    ChunkContextRequest,
    ClaimDirection,
    ClaimSearchRequest,
    ClaimSearchResult,
    ClaimType,
    CommitHypothesisRequest,
    DocumentChunk,
    EmbeddingCoverageSummary,
    EntityMention,
    EvidenceLevel,
    FullTextTriageRequest,
    FullTextOpsAction,
    FullTextOpsRequest,
    FullTextOpsResult,
    HypothesisProposalRequest,
    RawSourceRecord,
    ResearchChunkSearchRequest,
    ResearchObject,
    ResearchObjectType,
    ResearchObjectReadRequest,
    RetrievalSmokeRequest,
    ScrapeFetchRequest,
    ScrapeIngestRequest,
    ScrapeManifestFetchRequest,
    ScrapeManifestRequest,
    ScrapeProfileReviewRequest,
    ScrapeReviewRequest,
    ScrapeSourceProfile,
    SourceQuery,
    TextEmbedding,
    TextEmbeddingSearchRequest,
    ValidationRequest,
    ClaimCurationRequest,
    SourceScoutRequest,
    RunStatus,
    XTopicLinkedSource,
    XTopicReviewAction,
    XTopicReviewRequest,
    XTopicReviewResult,
)
from hsa_research.ingestion_bridge.backfill import backfill_deep_dives, backfill_papers_json
from hsa_research.ingestion_bridge.claim_curator import ClaimCuratorAgent
from hsa_research.ingestion_bridge.claim_extractor import LocalRuleClaimExtractor, extract_claims_for_repository
from hsa_research.ingestion_bridge.chunker import chunk_text
from hsa_research.ingestion_bridge.full_text_triage import FullTextTriageAgent
from hsa_research.ingestion_bridge import full_text_ops
from hsa_research.ingestion_bridge.full_text_ops import FullTextOpsAgent
from hsa_research.ingestion_bridge.dagster_assets import (
    ALL_API_SMOKE_KEYS,
    HOSTED_API_REPORT_KEYS,
    LITERATURE_CLINICAL_SMOKE_KEYS,
    LITERATURE_CORPUS_SOURCE_KEYS,
    LITERATURE_CORPUS_SOURCE_LIMITS,
    LITERATURE_FULL_TEXT_SOURCE_KEYS,
    LITERATURE_FULL_TEXT_SOURCE_LIMITS,
)
from hsa_research.ingestion_bridge.dagster_resources import ResearchRepositoryResource
from hsa_research.ingestion_bridge.entity_resolution import normalize_entity_key, resolve_entities_for_repository
from hsa_research.ingestion_bridge.embeddings import (
    EmbeddingIndexResult,
    EmbeddingMaintenanceResult,
    LocalDeterministicEmbeddingProvider,
    build_chunk_embedding_text,
    index_embeddings_for_repository,
    maintain_embedding_index,
)
from hsa_research.ingestion_bridge import harvesters_v2
from hsa_research.ingestion_bridge.harvesters_v2 import (
    AVMAVCTRHarvesterV2,
    ChEMBLHarvesterV2,
    ClinicalTrialsGovHarvesterV2,
    EuropePMCHarvesterV2,
    GEOHarvesterV2,
    HARVESTERS_V2,
    ICDCHarvesterV2,
    OpenAlexHarvesterV2,
    OpenFDAAnimalEventsHarvesterV2,
    PMCOAHarvesterV2,
    PubChemHarvesterV2,
    PubMedHarvesterV2,
    RCSBPDBHarvesterV2,
    SRAHarvesterV2,
    UniProtHarvesterV2,
    UnpaywallHarvesterV2,
)
from hsa_research.ingestion_bridge.local_ingest import LocalIngestionPipeline
from hsa_research.ingestion_bridge.local_store import SQLiteResearchRepository
from hsa_research.ingestion_bridge import mcp_server
from hsa_research.ingestion_bridge.query_policy import build_scholarly_source_queries, infer_comparative_scope
from hsa_research.ingestion_bridge.repository import InMemoryResearchRepository
from hsa_research.ingestion_bridge import scraper_bridge
from hsa_research.ingestion_bridge import x_topic_monitor
from hsa_research.ingestion_bridge import x_topic_review
from hsa_research.ingestion_bridge.scraper_bridge import ScrapeBridge, list_scrape_profiles
from hsa_research.ingestion_bridge import service as service_module
from hsa_research.ingestion_bridge.service import HSAResearchService
from hsa_research.ingestion_bridge.source_scout import SourceScoutAgent
from hsa_research.ingestion_bridge.source_health import build_source_health_report
from hsa_research.ingestion_bridge.structured_orchestration import (
    build_structured_source_count_report,
    full_text_source_qa,
    run_structured_sources_pipeline,
    structured_source_qa,
)


def make_service(tmp_path):
    return HSAResearchService(SQLiteResearchRepository(tmp_path / "hsa.sqlite3"))


def _contains_key(value, key: str) -> bool:
    if isinstance(value, dict):
        return key in value or any(_contains_key(item, key) for item in value.values())
    if isinstance(value, list):
        return any(_contains_key(item, key) for item in value)
    return False


def test_research_repository_resource_uses_hsa_sqlite_path(monkeypatch, tmp_path):
    db_path = tmp_path / "dagster-resource.sqlite3"
    monkeypatch.setenv("HSA_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("HSA_SQLITE_PATH", str(db_path))
    monkeypatch.delenv("HSA_DATABASE_URL", raising=False)

    repo = ResearchRepositoryResource().build_repository()
    try:
        assert isinstance(repo, SQLiteResearchRepository)
        assert repo.db_path == db_path
        assert repo.coverage_summary()["claims"] >= 1
    finally:
        repo.conn.close()


def test_research_repository_resource_rejects_memory_backend():
    with pytest.raises(RuntimeError, match="sqlite or postgres storage"):
        ResearchRepositoryResource(storage_backend="memory").build_repository()


def test_agent_run_and_full_text_ops_contracts_validate():
    record = AgentRunRecord(
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

    assert record.status == "running"
    assert result.actions[0].action == "run_source_date_partition"
    with pytest.raises(ValueError):
        FullTextOpsAction(source_key="europe_pmc", action="bad", severity="watch", reason="bad")
    with pytest.raises(ValueError):
        FullTextOpsAction(source_key="europe_pmc", action="mark_clean", severity="bad", reason="bad")
    with pytest.raises(ValueError):
        AgentRunRecord(agent_name="x", status="bad")


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


def test_dagster_structured_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    def fail_direct_factory():
        raise AssertionError("dagster assets must use the injected repository resource")

    def fake_pipeline(repository, **kwargs):
        assert repository is sentinel_repository
        return {"repository": "injected", "source_keys": kwargs["source_keys"]}

    monkeypatch.setattr(storage, "build_sql_repository", fail_direct_factory)
    monkeypatch.setattr(structured_orchestration, "run_structured_sources_pipeline", fake_pipeline)

    result = dagster_asset_module.structured_source_smoke_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == ["build_repository"]
    assert result == {"repository": "injected", "source_keys": dagster_asset_module.STRUCTURED_SOURCE_SMOKE_KEYS}


def test_dagster_full_text_ops_asset_uses_injected_repository(monkeypatch):
    sentinel_repository = object()
    calls = []

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append("build_repository")
            return sentinel_repository

    class FakeService:
        def __init__(self, repository):
            assert repository is sentinel_repository

        def run_full_text_ops(self, request):
            assert isinstance(request, FullTextOpsRequest)
            return FullTextOpsResult(
                agent_run_id=uuid4(),
                actions=[
                    FullTextOpsAction(
                        source_key="europe_pmc",
                        action="run_source_date_partition",
                        severity="watch",
                        reason="Partition evidence is missing.",
                    )
                ],
                schedule_readiness="needs_partition_validation",
            )

    monkeypatch.setattr(service_module, "HSAResearchService", FakeService)

    result = dagster_asset_module.full_text_ops_agent_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == ["build_repository"]
    assert result.value["agent_name"] == "full_text_ops_agent"
    assert result.metadata["action_count"] == 1


def test_dagster_full_text_source_specific_assets_use_injected_repository(monkeypatch):
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
            "totals": {},
            "errors": [],
        }

    monkeypatch.setattr(structured_orchestration, "run_structured_sources_pipeline", fake_pipeline)
    monkeypatch.setattr(structured_orchestration, "run_structured_sources_ingestion_pipeline", fake_pipeline)

    europe_pmc_report = dagster_asset_module.europe_pmc_full_text_refresh_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    pmc_oa_report = dagster_asset_module.pmc_oa_full_text_refresh_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    ingest_smoke_report = dagster_asset_module.literature_full_text_ingest_smoke_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    europe_pmc_ingest_report = dagster_asset_module.europe_pmc_full_text_ingest_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    pmc_oa_ingest_report = dagster_asset_module.pmc_oa_full_text_ingest_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    smoke_report = dagster_asset_module.literature_full_text_smoke_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )
    partition_context = SimpleNamespace(
        multi_partition_key=dagster_asset_module.dg.MultiPartitionKey(
            {
                "source": "europe_pmc",
                "date": "2026-04-27",
            }
        )
    )
    partition_result = (
        dagster_asset_module.literature_full_text_source_date_report.node_def.compute_fn.decorated_fn(
            partition_context,
            FakeRepositoryResource(),
        )
    )

    assert calls == [
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
        "build_repository",
    ]
    assert europe_pmc_report["source_keys"] == ("europe_pmc",)
    assert europe_pmc_report["source_limits"] == {"europe_pmc": 10}
    assert europe_pmc_report["mode"] == "refresh"
    assert pmc_oa_report["source_keys"] == ("pmc_oa",)
    assert pmc_oa_report["source_limits"] == {"pmc_oa": 3}
    assert pmc_oa_report["mode"] == "refresh"
    assert ingest_smoke_report["source_keys"] == dagster_asset_module.LITERATURE_FULL_TEXT_SOURCE_KEYS
    assert ingest_smoke_report["source_limits"] == {"europe_pmc": 1, "pmc_oa": 1}
    assert ingest_smoke_report["mode"] == "ingestion_only"
    assert europe_pmc_ingest_report["source_keys"] == ("europe_pmc",)
    assert europe_pmc_ingest_report["source_limits"] == {"europe_pmc": 10}
    assert europe_pmc_ingest_report["mode"] == "ingestion_only"
    assert pmc_oa_ingest_report["source_keys"] == ("pmc_oa",)
    assert pmc_oa_ingest_report["source_limits"] == {"pmc_oa": 3}
    assert pmc_oa_ingest_report["mode"] == "ingestion_only"
    assert smoke_report["source_keys"] == dagster_asset_module.LITERATURE_FULL_TEXT_SOURCE_KEYS
    assert smoke_report["source_limits"] == {"europe_pmc": 1, "pmc_oa": 1}
    assert smoke_report["mode"] == "refresh"
    assert partition_result.value["source_keys"] == ("europe_pmc",)
    assert partition_result.value["source_limits"] == {"europe_pmc": 10}
    assert partition_result.value["partition_date"] == "2026-04-27"
    assert partition_result.value["mode"] == "source_date_partition"


def test_full_text_ingestion_pipeline_skips_downstream_work(monkeypatch):
    calls = []

    class FakeIngestionResult:
        def model_dump(self, mode):
            assert mode == "json"
            return {
                "source_key": "europe_pmc",
                "query_name": "licensed_full_text_hsa",
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 2,
                "full_text_research_objects": 1,
                "section_chunk_counts": {"title_abstract": 1, "full_text": 1},
                "status": "completed",
                "errors": [],
            }

    class FakePipeline:
        def __init__(self, repository):
            calls.append(("init", repository))

        def initialize(self):
            calls.append(("initialize",))

        def ingest_source(self, source_key, limit):
            calls.append(("ingest_source", source_key, limit))
            return [FakeIngestionResult()]

    class FakeRepository:
        def source_runtime_summary(self, source_key, sample_limit=5):
            assert source_key == "europe_pmc"
            return {
                "source_key": source_key,
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 2,
                "entity_mentions": 0,
                "claims": 0,
            }

        def list_research_objects(self, source_key=None):
            assert source_key == "europe_pmc"
            return [SimpleNamespace(metadata={"full_text_available": True})]

        def list_document_chunks(self, source_key=None):
            assert source_key == "europe_pmc"
            return [
                SimpleNamespace(section_label="title_abstract", text_content="title"),
                SimpleNamespace(section_label="full_text", text_content="body"),
            ]

        def coverage_summary(self):
            return {"document_chunks": 2}

    def fail_downstream(*args, **kwargs):
        raise AssertionError("ingestion-only pipeline must not run downstream claim work")

    monkeypatch.setattr(structured_orchestration, "LocalIngestionPipeline", FakePipeline)
    monkeypatch.setattr(structured_orchestration, "resolve_entities_for_repository", fail_downstream)
    monkeypatch.setattr(structured_orchestration, "extract_claims_for_repository", fail_downstream)
    monkeypatch.setattr(structured_orchestration, "curate_claims_for_repository", fail_downstream)

    report = structured_orchestration.run_structured_sources_ingestion_pipeline(
        FakeRepository(),
        source_keys=("europe_pmc",),
        source_limits={"europe_pmc": 1},
    )

    assert calls[-1] == ("ingest_source", "europe_pmc", 1)
    assert report["mode"] == "ingestion_only"
    assert report["totals"]["document_chunks"] == 2
    source_report = report["sources"][0]
    assert source_report["entity_resolution"]["status"] == "skipped"
    assert source_report["extraction"]["status"] == "skipped"
    assert source_report["curation"]["status"] == "skipped"
    assert source_report["full_text_qa"]["passes_full_text_bar"] is True
    assert source_report["full_text_qa"]["triage"]["action"] == "no_action"
    assert source_report["full_text_triage_action"] == "no_action"


def test_full_text_partition_allows_empty_current_day(monkeypatch):
    calls = []

    class EmptyIngestionResult:
        def model_dump(self, mode):
            assert mode == "json"
            return {
                "source_key": "europe_pmc",
                "query_name": "comparative_hsa_open_access:partition_2026-04-27",
                "raw_records": 0,
                "research_objects": 0,
                "document_chunks": 0,
                "full_text_research_objects": 0,
                "section_chunk_counts": {},
                "status": "completed",
                "errors": [],
            }

    class FakePipeline:
        def __init__(self, repository):
            calls.append(("init", repository))

        def initialize(self):
            calls.append(("initialize",))

        def ingest_source(self, source_key, limit=25, **kwargs):
            calls.append(("ingest_source", source_key, limit, kwargs))
            return [EmptyIngestionResult()]

    class FakeRepository:
        def source_runtime_summary(self, source_key, sample_limit=5):
            return {
                "source_key": source_key,
                "raw_records": 0,
                "research_objects": 0,
                "document_chunks": 0,
                "entity_mentions": 0,
                "claims": 0,
            }

        def list_research_objects(self, source_key=None):
            return []

        def list_document_chunks(self, source_key=None):
            return []

        def coverage_summary(self):
            return {}

    monkeypatch.setattr(structured_orchestration, "LocalIngestionPipeline", FakePipeline)

    report = structured_orchestration.run_structured_sources_ingestion_pipeline(
        FakeRepository(),
        source_keys=("europe_pmc",),
        source_limits={"europe_pmc": 1},
        partition_date="2026-04-27",
    )

    source_report = report["sources"][0]
    assert calls[-1] == (
        "ingest_source",
        "europe_pmc",
        1,
        {
            "query_param_overrides": {
                "published_after": "2026-04-27",
                "published_before": "2026-04-27",
            },
            "query_name_suffix": "partition_2026-04-27",
        },
    )
    assert report["partition_date"] == "2026-04-27"
    assert source_report["full_text_qa"]["current_empty_passes"] is True
    assert source_report["full_text_qa"]["passes_full_text_bar"] is True
    assert source_report["full_text_qa"]["triage"]["action"] == "no_action"


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


def test_dagster_embedding_index_asset_uses_injected_repository(monkeypatch):
    calls = []

    class FakeRepository:
        def embedding_coverage(self, *, embedding_model=None, **kwargs):
            calls.append(("embedding_coverage", embedding_model, kwargs))
            return EmbeddingCoverageSummary(
                embedding_model=embedding_model,
                total_chunks=1,
                embedded_chunks=1,
                missing_chunks=0,
                coverage_ratio=1.0,
                embedding_models={embedding_model: 1},
            )

        def coverage_summary(self):
            calls.append(("coverage_summary",))
            return {"document_chunks": 1, "text_embeddings": 1}

    sentinel_repository = FakeRepository()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append(("build_repository",))
            return sentinel_repository

    def fake_index_embeddings_for_repository(repository, **kwargs):
        assert repository is sentinel_repository
        assert kwargs == {}
        calls.append(("index_embeddings_for_repository",))
        return EmbeddingIndexResult(
            embedding_model="local-hash-v1",
            chunks_seen=1,
            embeddings_created=1,
        )

    monkeypatch.setattr(
        "hsa_research.ingestion_bridge.embeddings.index_embeddings_for_repository",
        fake_index_embeddings_for_repository,
    )

    result = dagster_asset_module.embedding_index_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == [
        ("build_repository",),
        ("index_embeddings_for_repository",),
        ("embedding_coverage", "local-hash-v1", {}),
        ("coverage_summary",),
    ]
    assert isinstance(result, dagster_asset_module.dg.MaterializeResult)
    assert result.value["embedding_model"] == "local-hash-v1"
    assert result.value["totals"]["chunks_seen"] == 1
    assert result.value["totals"]["embeddings_created"] == 1
    assert result.value["embedding_coverage"]["embedded_chunks"] == 1
    assert result.value["passes_minimum_bar"] is True
    assert result.metadata["embedded_chunks"] == 1
    assert result.metadata["passes_minimum_bar"] is True


def test_dagster_embedding_maintenance_asset_uses_injected_repository(monkeypatch):
    calls = []

    class FakeRepository:
        pass

    sentinel_repository = FakeRepository()

    class FakeRepositoryResource:
        def build_repository(self):
            calls.append(("build_repository",))
            return sentinel_repository

    def fake_maintain_embedding_index(repository, **kwargs):
        assert repository is sentinel_repository
        assert kwargs == {}
        calls.append(("maintain_embedding_index",))
        return EmbeddingMaintenanceResult(
            embedding_model="local-hash-v1",
            prune_embedding_model=None,
            source_key=None,
            object_type=None,
            orphan_embeddings_seen=2,
            orphan_embeddings_deleted=2,
            prune_enabled=True,
            embedding_coverage=EmbeddingCoverageSummary(
                embedding_model="local-hash-v1",
                total_chunks=3,
                embedded_chunks=3,
                missing_chunks=0,
                coverage_ratio=1.0,
                embedding_models={"local-hash-v1": 3},
            ),
            coverage={"document_chunks": 3, "text_embeddings": 3},
        )

    monkeypatch.setattr(
        "hsa_research.ingestion_bridge.embeddings.maintain_embedding_index",
        fake_maintain_embedding_index,
    )

    result = dagster_asset_module.embedding_maintenance_report.node_def.compute_fn.decorated_fn(
        FakeRepositoryResource()
    )

    assert calls == [("build_repository",), ("maintain_embedding_index",)]
    assert isinstance(result, dagster_asset_module.dg.MaterializeResult)
    assert result.value["orphan_embeddings"]["seen"] == 2
    assert result.value["orphan_embeddings"]["deleted"] == 2
    assert result.value["embedding_coverage"]["missing_chunks"] == 0
    assert result.value["passes_minimum_bar"] is True
    assert result.metadata["orphan_embeddings_deleted"] == 2
    assert result.metadata["embedded_chunks"] == 3
    assert result.metadata["passes_minimum_bar"] is True


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


def _seed_minimal_source_claim(
    repo: SQLiteResearchRepository,
    source_key: str,
    *,
    curation_status: str = "promote",
    extraction_status: str = "typed",
) -> None:
    raw_record = RawSourceRecord(
        source_key=source_key,
        source_record_id=f"{source_key}:1",
        content_hash=f"{source_key}-raw",
        source_url=f"https://example.org/{source_key}/1",
        raw_payload={"source_key": source_key},
    )
    raw_record_id = repo.upsert_raw_record(raw_record)
    research_object = ResearchObject(
        object_type="publication",
        title=f"{source_key} source record",
        canonical_url=f"https://example.org/{source_key}/1",
        source_key=source_key,
        raw_record_id=raw_record_id,
        dedupe_key=f"{source_key}:1",
    )
    object_id = repo.upsert_research_object(research_object, raw_record_id)
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content=f"{source_key} mentions canine hemangiosarcoma and human angiosarcoma context.",
            content_hash=f"{source_key}-chunk",
        )
    )
    repo.upsert_claim(
        ClaimSearchResult(
            claim_id=uuid4(),
            statement=f"{source_key} provides source context for canine HSA research.",
            claim_type=ClaimType.OTHER,
            direction=ClaimDirection.NEUTRAL,
            confidence=0.7,
            evidence_level=EvidenceLevel.REVIEW,
            source_object_id=object_id,
            source_title=f"{source_key} source record",
            source_url=f"https://example.org/{source_key}/1",
            support_count=1,
            metadata={"curation_status": curation_status, "extraction_status": extraction_status},
        )
    )


def _seed_full_text_source_claim(repo: SQLiteResearchRepository, source_key: str = "europe_pmc") -> None:
    raw_record = RawSourceRecord(
        source_key=source_key,
        source_record_id=f"{source_key}:full-text",
        content_hash=f"{source_key}-full-text-raw",
        source_url=f"https://example.org/{source_key}/full-text",
        raw_payload={"source_key": source_key, "full_text": "Full text body mentions canine hemangiosarcoma."},
    )
    raw_record_id = repo.upsert_raw_record(raw_record)
    research_object = ResearchObject(
        object_type="publication",
        title=f"{source_key} full text source record",
        canonical_url=f"https://example.org/{source_key}/full-text",
        source_key=source_key,
        raw_record_id=raw_record_id,
        dedupe_key=f"{source_key}:full-text",
        metadata={"full_text_available": True},
    )
    object_id = repo.upsert_research_object(research_object, raw_record_id)
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="full_text",
            text_content="Full text body mentions canine hemangiosarcoma and human angiosarcoma context.",
            content_hash=f"{source_key}-full-text-chunk",
        )
    )
    repo.upsert_claim(
        ClaimSearchResult(
            claim_id=uuid4(),
            statement=f"{source_key} provides full-text source context for canine HSA research.",
            claim_type=ClaimType.OTHER,
            direction=ClaimDirection.NEUTRAL,
            confidence=0.7,
            evidence_level=EvidenceLevel.REVIEW,
            source_object_id=object_id,
            source_title=f"{source_key} full text source record",
            source_url=f"https://example.org/{source_key}/full-text",
            support_count=1,
            metadata={"curation_status": "promote", "extraction_status": "typed"},
        )
    )


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


def test_full_text_ops_ready_when_health_and_partition_are_clean(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-ready.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    _seed_full_text_source_claim(repo, "pmc_oa")
    partition_report = {
        "mode": "source_date_partition",
        "partition_date": "2026-04-27",
        "sources": [
            {"source_key": "europe_pmc", "full_text_qa": {"passes_full_text_bar": True}},
            {"source_key": "pmc_oa", "full_text_qa": {"current_empty_passes": True}},
        ],
        "errors": [],
    }

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            partition_date="2026-04-27",
            full_text_report=partition_report,
            review_mode="deterministic_only",
        )
    )

    assert result.schedule_readiness == "ready_to_enable"
    assert result.should_block_schedule is False
    assert any(action.action == "ready_to_enable_schedule" for action in result.actions)
    assert {action.action for action in result.actions if action.source_key != "all"} == {"mark_clean"}


def test_full_text_ops_external_review_packet_includes_deterministic_guardrail(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-external-review.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    partition_report = {
        "mode": "source_date_partition",
        "partition_date": "2026-04-27",
        "sources": [
            {"source_key": "europe_pmc", "full_text_qa": {"passes_full_text_bar": True}},
        ],
        "errors": [],
    }

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            source_keys=["europe_pmc"],
            partition_date="2026-04-27",
            full_text_report=partition_report,
        )
    )

    assert result.schedule_readiness == "keep_stopped"
    assert result.should_block_schedule is True
    assert any(action.action == "needs_human_review" for action in result.actions)
    assert result.evidence["external_reviewer"]["provider"] == "openai_chatgpt_pro"
    assert result.evidence["review_packet"]["deterministic_guardrail_result"]["schedule_readiness"] == "ready_to_enable"


def test_full_text_ops_openrouter_compare_records_each_model(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-openrouter.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    repo.create_agent_run(
        AgentRunRecord(
            agent_name="full_text_ops_agent",
            status=RunStatus.COMPLETED,
            source_key="europe_pmc",
            partition_date="2026-04-26",
            output_payload={
                "schedule_readiness": "keep_stopped",
                "should_block_schedule": True,
                "actions": [{"source_key": "all", "action": "needs_human_review", "severity": "watch"}],
                "evidence": {
                    "review_packet": {"large_nested_payload": "x" * 10000},
                    "model_reviews": [{"model_name": "old", "status": "failed", "error": "old failure"}],
                },
            },
            summary={"actions": 1},
            errors=["old failure"],
        )
    )
    partition_report = {
        "mode": "source_date_partition",
        "partition_date": "2026-04-27",
        "sources": [
            {"source_key": "europe_pmc", "full_text_qa": {"passes_full_text_bar": True}},
        ],
        "errors": [],
    }

    def fake_review_model(model_name, review_payload):
        assert review_payload["deterministic_guardrail_result"]["schedule_readiness"] == "ready_to_enable"
        assert not _contains_key(review_payload, "output_payload")
        assert not _contains_key(review_payload, "review_packet")
        assert review_payload["recent_agent_runs"][0]["output"]["model_review_statuses"][0]["model_name"] == "old"
        return {
            "text": json.dumps(
                {
                    "agent_name": "full_text_ops_agent",
                    "model_profile": "reviewer",
                    "schedule_readiness": "ready_to_enable",
                    "should_block_schedule": False,
                    "actions": [
                        {
                            "source_key": "all",
                            "action": "ready_to_enable_schedule",
                            "severity": "info",
                            "reason": f"{model_name} agrees the evidence is clean.",
                            "evidence_refs": ["deterministic_guardrail_result"],
                        }
                    ],
                    "evidence": {"model_name": model_name},
                    "errors": [],
                }
            ),
            "metadata": {"provider": "openrouter", "model_name": model_name},
        }

    monkeypatch.setattr(full_text_ops, "_openrouter_review_model", fake_review_model)

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            source_keys=["europe_pmc"],
            partition_date="2026-04-27",
            full_text_report=partition_report,
            review_mode="openrouter_compare",
            review_models=["openai/gpt-5.1", "anthropic/claude-sonnet-4.5"],
        )
    )

    assert result.schedule_readiness == "ready_to_enable"
    assert result.evidence["selected_model"] == "openai/gpt-5.1"
    assert [review["model_name"] for review in result.evidence["model_reviews"]] == [
        "openai/gpt-5.1",
        "anthropic/claude-sonnet-4.5",
    ]
    assert all(review["status"] == "completed" for review in result.evidence["model_reviews"])


def test_full_text_ops_openrouter_defaults_to_sonnet_latest(monkeypatch):
    monkeypatch.delenv("HSA_FULL_TEXT_OPS_MODEL", raising=False)
    monkeypatch.delenv("HSA_FULL_TEXT_OPS_REVIEW_MODELS", raising=False)

    assert full_text_ops._review_models(FullTextOpsRequest(review_mode="openrouter_required")) == [
        "~anthropic/claude-sonnet-latest"
    ]
    assert full_text_ops._review_models(FullTextOpsRequest(review_mode="openrouter_compare")) == [
        "~anthropic/claude-sonnet-latest"
    ]


def test_full_text_ops_openrouter_compare_persists_model_failures(monkeypatch, tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-openrouter-failures.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    partition_report = {
        "mode": "source_date_partition",
        "partition_date": "2026-04-27",
        "sources": [
            {"source_key": "europe_pmc", "full_text_qa": {"passes_full_text_bar": True}},
        ],
        "errors": [],
    }

    def failing_review_model(model_name, review_payload):
        assert review_payload["deterministic_guardrail_result"]["schedule_readiness"] == "ready_to_enable"
        raise RuntimeError(f"{model_name} unavailable")

    monkeypatch.setattr(full_text_ops, "_openrouter_review_model", failing_review_model)

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            source_keys=["europe_pmc"],
            partition_date="2026-04-27",
            full_text_report=partition_report,
            review_mode="openrouter_compare",
            review_models=["openai/gpt-5.1", "anthropic/claude-sonnet-4.5"],
        )
    )

    assert result.schedule_readiness == "keep_stopped"
    assert result.should_block_schedule is True
    assert result.evidence["openrouter_all_models_failed"] is True
    assert result.evidence["selected_model"] is None
    assert [review["status"] for review in result.evidence["model_reviews"]] == ["failed", "failed"]
    assert any("openai/gpt-5.1 unavailable" in error for error in result.errors)
    assert any(action.action == "needs_human_review" for action in result.actions)


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


def test_full_text_ops_maps_triage_actions_to_recommendations(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-triage.sqlite3", seed=False)
    report = {
        "source_keys": ["europe_pmc", "pmc_oa"],
        "sources": [
            {
                "source_key": "europe_pmc",
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 1,
                "full_text_qa": {
                    "passes_full_text_bar": False,
                    "triage": {"action": "needs_parser_fix", "severity": "blocking"},
                },
            },
            {
                "source_key": "pmc_oa",
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 1,
                "full_text_qa": {
                    "passes_full_text_bar": False,
                    "triage": {"action": "needs_license_review", "severity": "blocking"},
                },
            },
        ],
    }

    result = FullTextOpsAgent(repo).run(
        FullTextOpsRequest(
            source_keys=["europe_pmc", "pmc_oa"],
            source_health_report=report,
            review_mode="deterministic_only",
        )
    )

    assert result.schedule_readiness == "blocked"
    assert result.should_block_schedule is True
    assert {action.action for action in result.actions} >= {"inspect_parser", "inspect_license"}


def test_full_text_ops_service_is_recommend_only(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "full-text-ops-recommend-only.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")
    claims_before = len(repo.list_claims(source_key="europe_pmc", include_seed_claims=True))
    source_queries_before = repo.list_source_queries(source_key="europe_pmc")

    result = HSAResearchService(repo).run_full_text_ops(
        FullTextOpsRequest(
            source_keys=["europe_pmc"],
            partition_date="2026-04-27",
            review_mode="deterministic_only",
        )
    )

    assert result.agent_run_id is not None
    assert len(repo.list_claims(source_key="europe_pmc", include_seed_claims=True)) == claims_before
    assert repo.list_source_queries(source_key="europe_pmc") == source_queries_before
    assert repo.list_agent_runs(agent_name="full_text_ops_agent", status="completed")


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


def test_local_pipeline_initializes_sources_and_queries(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)

    output = pipeline.initialize()
    coverage = pipeline.coverage()

    assert output["sources"] >= 4
    assert coverage["sources"] >= 4
    assert coverage["source_queries"] >= 4
    assert any(query.query_name == "licensed_full_text_hsa" for query in repo.list_source_queries("pmc_oa"))
    unpaywall_queries = repo.list_source_queries("unpaywall", active_only=False)
    assert any(query.query_name == "oa_discovery_hsa_titles" and not query.active for query in unpaywall_queries)
    assert any(
        query.query_name == "human_vascular_sarcoma_trials"
        for query in repo.list_source_queries("clinicaltrials_gov")
    )
    assert any(query.query_name == "canine_hsa_trials" for query in repo.list_source_queries("avma_vctr"))
    assert any(query.query_name == "canine_hsa_cases" for query in repo.list_source_queries("icdc"))
    assert any(query.query_name == "canine_hsa_expression" for query in repo.list_source_queries("geo"))
    assert any(query.query_name == "canine_hsa_sequence_runs" for query in repo.list_source_queries("sra"))
    assert any(query.query_name == "priority_compounds" for query in repo.list_source_queries("pubchem"))
    assert any(query.query_name == "priority_compound_bioactivities" for query in repo.list_source_queries("chembl"))
    chembl_query = next(query for query in repo.list_source_queries("chembl") if query.query_name == "priority_compound_bioactivities")
    assert "CHEMBL279" in chembl_query.query_params["target_chembl_ids"]
    assert chembl_query.query_params["target_organisms"] == ["Homo sapiens", "Canis lupus familiaris"]
    assert chembl_query.query_params["include_cell_line_assays"] is True
    assert "sarcoma" in chembl_query.query_params["cell_line_terms"]
    pubchem_query = next(query for query in repo.list_source_queries("pubchem") if query.query_name == "priority_compounds")
    assert pubchem_query.query_params["require_exact_match"] is True
    assert any(query.query_name == "canine_human_priority_targets" for query in repo.list_source_queries("uniprot"))
    assert any(query.query_name == "priority_target_structures" for query in repo.list_source_queries("rcsb_pdb"))
    assert any(query.query_name == "priority_drug_safety" for query in repo.list_source_queries("openfda_animal_events"))


def test_structured_source_qa_reports_source_scoped_counts(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    raw_record = RawSourceRecord(
        source_key="pubchem",
        source_record_id="CID:4946",
        content_hash="pubchem-4946",
        source_url="https://pubchem.ncbi.nlm.nih.gov/compound/4946",
        raw_payload={"cid": 4946},
    )
    raw_record_id = repo.upsert_raw_record(raw_record)
    research_object = ResearchObject(
        object_type="compound_record",
        title="Propranolol",
        canonical_url="https://pubchem.ncbi.nlm.nih.gov/compound/4946",
        source_key="pubchem",
        raw_record_id=raw_record_id,
        dedupe_key="pubchem:4946",
        identifiers={"cid": "4946"},
    )
    object_id = repo.upsert_research_object(research_object, raw_record_id)
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="pubchem_identity",
            text_content="Propranolol has PubChem CID 4946.",
            content_hash="chunk-pubchem-4946",
        )
    )
    repo.upsert_claim(
        ClaimSearchResult(
            claim_id=uuid4(),
            statement="Propranolol has PubChem identity CID 4946.",
            claim_type=ClaimType.OTHER,
            direction=ClaimDirection.NEUTRAL,
            confidence=0.82,
            evidence_level=EvidenceLevel.IN_SILICO,
            source_object_id=object_id,
            source_title="Propranolol",
            source_url="https://pubchem.ncbi.nlm.nih.gov/compound/4946",
            support_count=1,
            metadata={"curation_status": "promote"},
        )
    )

    qa = structured_source_qa(repo, "pubchem")

    assert qa["raw_records"] == 1
    assert qa["research_objects"] == 1
    assert qa["document_chunks"] == 1
    assert qa["claims"] == 1
    assert qa["claim_status"] == {"promote": 1}
    assert qa["claim_types"] == {"other": 1}
    assert qa["passes_minimum_bar"] is True
    assert qa["sample_claims"][0]["curation_status"] == "promote"

    report = build_structured_source_count_report(repo, source_keys=["pubchem", "chembl"], sample_limit=1)

    assert report["source_keys"] == ["pubchem", "chembl"]
    assert report["totals"] == {
        "raw_records": 1,
        "research_objects": 1,
        "document_chunks": 1,
        "entity_mentions": 0,
        "claims": 1,
    }
    assert report["failed_sources"] == ["chembl"]
    assert report["passes_minimum_bar"] is False
    assert report["minimum_bar"] == {"require_claims": True}
    assert report["sources"][0]["sample_claims"][0]["statement"] == "Propranolol has PubChem identity CID 4946."

    source_health_report = build_structured_source_count_report(
        repo,
        source_keys=["pubchem"],
        sample_limit=1,
        require_claims=False,
    )

    assert source_health_report["failed_sources"] == []
    assert source_health_report["minimum_bar"] == {"require_claims": False}


def test_source_health_report_separates_failed_and_watch_sources(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_minimal_source_claim(
        repo,
        "pubchem",
        curation_status="needs_review",
        extraction_status="source_context",
    )

    report = build_source_health_report(repo, source_keys=["pubchem", "chembl"], sample_limit=1)
    pubchem = next(source for source in report["sources"] if source["source_key"] == "pubchem")
    chembl = next(source for source in report["sources"] if source["source_key"] == "chembl")

    assert report["source_keys"] == ["pubchem", "chembl"]
    assert report["summary"] == {"sources": 2, "healthy": 0, "triage": 0, "watch": 1, "failing": 1}
    assert report["failed_sources"] == ["chembl"]
    assert report["watch_sources"] == ["pubchem"]
    assert report["triage_sources"] == []
    assert pubchem["health_status"] == "watch"
    assert pubchem["source_role"] == "evidence"
    assert pubchem["health_score"] >= report["minimum_bar"]["min_health_score"]
    assert pubchem["passes_minimum_bar"] is True
    assert pubchem["claim_metadata"]["extraction_status"] == {"source_context": 1}
    assert any("source-context" in risk for risk in pubchem["risks"])
    assert chembl["health_status"] == "failing"
    assert chembl["passes_minimum_bar"] is False


def test_source_health_report_marks_expected_triage_sources(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_minimal_source_claim(
        repo,
        "sra",
        curation_status="needs_review",
        extraction_status="source_context",
    )

    report = build_source_health_report(repo, source_keys=["sra"], sample_limit=1)
    sra = report["sources"][0]

    assert report["summary"] == {"sources": 1, "healthy": 0, "triage": 1, "watch": 0, "failing": 0}
    assert report["failed_sources"] == []
    assert report["triage_sources"] == ["sra"]
    assert report["watch_sources"] == []
    assert sra["source_role"] == "triage"
    assert sra["health_status"] == "triage"
    assert sra["passes_minimum_bar"] is True
    assert "triage_only_source" in sra["signals"]
    assert any("specialized triage agent" in action for action in sra["recommended_actions"])


def test_full_text_source_health_requires_body_chunks(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_minimal_source_claim(repo, "europe_pmc")

    report = build_source_health_report(repo, source_keys=["europe_pmc"], sample_limit=1)
    europe_pmc = report["sources"][0]

    assert report["failed_sources"] == ["europe_pmc"]
    assert report["passes_minimum_bar"] is False
    assert europe_pmc["full_text_qa"]["passes_full_text_bar"] is False
    assert europe_pmc["full_text_qa"]["triage"]["action"] == "needs_parser_fix"
    assert europe_pmc["full_text_triage_action"] == "needs_parser_fix"
    assert europe_pmc["minimum_bar"]["full_text_required_passes"] is False
    assert report["full_text_blocking_sources"] == ["europe_pmc"]
    assert any("Full-text source lacks" in risk for risk in europe_pmc["risks"])
    assert any("Full-text triage action: needs_parser_fix" in risk for risk in europe_pmc["risks"])


def test_full_text_source_count_report_passes_with_body_chunks(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    _seed_full_text_source_claim(repo, "europe_pmc")

    report = build_structured_source_count_report(repo, source_keys=["europe_pmc"], sample_limit=1)
    qa = full_text_source_qa(repo, "europe_pmc")

    assert report["failed_sources"] == []
    assert report["passes_minimum_bar"] is True
    assert report["sources"][0]["full_text_qa"]["passes_full_text_bar"] is True
    assert report["sources"][0]["full_text_triage_action"] == "no_action"
    assert qa["full_text_research_objects"] == 1
    assert qa["full_text_document_chunks"] == 1
    assert qa["triage"]["action"] == "no_action"
    current_run_qa = full_text_source_qa(repo, "europe_pmc", ingestion_results=[])
    assert current_run_qa["passes_full_text_bar"] is False
    assert current_run_qa["triage"]["action"] == "retry_later"


def test_full_text_qa_uses_body_chunks_as_persisted_gate(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    raw_record = RawSourceRecord(
        source_key="pmc_oa",
        source_record_id="pmc_oa:metadata-gap",
        content_hash="pmc-oa-metadata-gap-raw",
        source_url="https://example.org/pmc_oa/metadata-gap",
        raw_payload={"source_key": "pmc_oa", "full_text": "Full text body is persisted."},
    )
    raw_record_id = repo.upsert_raw_record(raw_record)
    research_object = ResearchObject(
        object_type="publication",
        title="PMC OA full text with missing object flag",
        canonical_url="https://example.org/pmc_oa/metadata-gap",
        source_key="pmc_oa",
        raw_record_id=raw_record_id,
        dedupe_key="pmc_oa:metadata-gap",
        metadata={},
    )
    object_id = repo.upsert_research_object(research_object, raw_record_id)
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="full_text:results",
            text_content="Full text body mentions canine hemangiosarcoma.",
            content_hash="pmc-oa-metadata-gap-chunk",
        )
    )

    qa = full_text_source_qa(repo, "pmc_oa")
    current_qa = full_text_source_qa(
        repo,
        "pmc_oa",
        ingestion_results=[
            {
                "source_key": "pmc_oa",
                "query_name": "metadata_gap_current_run",
                "raw_records": 1,
                "research_objects": 1,
                "document_chunks": 1,
                "full_text_research_objects": 0,
                "section_chunk_counts": {"full_text:results": 1},
                "status": "completed",
                "errors": [],
            }
        ],
    )

    assert qa["full_text_research_objects"] == 0
    assert qa["full_text_document_chunks"] == 1
    assert qa["passes_persisted_full_text_bar"] is True
    assert qa["passes_full_text_bar"] is True
    assert current_qa["passes_current_full_text_bar"] is True
    assert current_qa["triage"]["action"] == "no_action"


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


def test_service_triages_full_text_issue(tmp_path):
    service = make_service(tmp_path)

    result = service.triage_full_text_issue(
        FullTextTriageRequest(
            source_key="pmc_oa",
            stage="fetch",
            error_message="429 too many requests",
            http_status=429,
        )
    )

    assert result.action == "retry_later"
    assert result.should_retry is True
    assert result.should_block_schedule is False


def test_entity_resolution_persists_entities_aliases_and_mentions_idempotently(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    raw_record_id = repo.upsert_raw_record(
        RawSourceRecord(
            source_key="pubmed",
            source_record_id="PMID:1",
            content_hash="entity-resolution-raw",
            source_url="https://pubmed.ncbi.nlm.nih.gov/1/",
            raw_payload={"pmid": "1"},
        )
    )
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Entity resolution example",
            source_key="pubmed",
            raw_record_id=raw_record_id,
            dedupe_key="pmid:1",
            identifiers={"pmid": "1"},
        ),
        raw_record_id,
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Adriamycin is discussed with VEGF receptor 2 and KIT, but not kitchen, in human angiosarcoma.",
            content_hash="entity-resolution-chunk",
        )
    )

    result = resolve_entities_for_repository(repo, source_key="pubmed")
    second_result = resolve_entities_for_repository(repo, source_key="pubmed")
    entities = repo.list_entities()
    mentions = repo.list_entity_mentions(source_key="pubmed")

    assert result.errors == []
    assert result.chunks_seen == 1
    assert result.mentions_upserted >= 4
    assert second_result.errors == []
    assert {entity.canonical_name for entity in entities} >= {"doxorubicin", "KDR", "KIT"}
    assert len(mentions) == len({mention.mention_id for mention in mentions})
    assert len(mentions) == len(repo.list_entity_mentions(chunk_id=chunk.id))
    assert sum(1 for mention in mentions if mention.canonical_name == "KIT") == 1
    assert normalize_entity_key("compound", "propranolol", {"pubchem_cid": "4946"}) == "pubchem_cid:4946"
    coverage = repo.coverage_summary()
    assert coverage["entity_aliases"] >= 1
    assert coverage["entity_mentions"] == len(mentions)
    assert repo.source_runtime_summary("pubmed")["entity_mentions"] == len(mentions)


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


def test_unpaywall_v2_normalizer_preserves_oa_location_metadata():
    record = UnpaywallHarvesterV2().normalize(
        {
            "score": 0.42,
            "snippet": "<b>Angiosarcoma</b> open access title match",
            "response": {
                "doi": "https://doi.org/10.1234/HSA.OA",
                "doi_url": "https://doi.org/10.1234/HSA.OA",
                "title": "Human angiosarcoma open access review",
                "year": 2026,
                "published_date": "2026-04-01",
                "publisher": "Example Publisher",
                "journal_name": "Example Journal",
                "genre": "journal-article",
                "is_oa": True,
                "oa_status": "gold",
                "journal_is_in_doaj": True,
                "best_oa_location": {
                    "url_for_landing_page": "https://example.org/article",
                    "url_for_pdf": "https://example.org/article.pdf",
                    "license": "cc-by",
                    "host_type": "publisher",
                    "version": "publishedVersion",
                },
                "oa_locations": [
                    {
                        "url_for_landing_page": "https://example.org/article",
                        "license": "cc-by",
                        "host_type": "publisher",
                    }
                ],
                "z_authors": [{"given": "Ada", "family": "Lovelace"}],
            },
        }
    )

    assert record.raw_record.source_key == "unpaywall"
    assert record.research_object.identifiers["doi"] == "10.1234/hsa.oa"
    assert record.research_object.canonical_url == "https://example.org/article"
    assert record.research_object.metadata["best_oa_location"]["url_for_pdf"] == "https://example.org/article.pdf"
    assert record.research_object.metadata["authors"] == ["Ada Lovelace"]
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == ["human_angiosarcoma"]
    assert UnpaywallHarvesterV2().chunk_section_label(record) == "oa_discovery_metadata"


def test_unpaywall_v2_fetch_uses_title_search_endpoint(monkeypatch):
    calls = []

    def fake_get_json(url, params, **kwargs):
        calls.append((url, params, kwargs))
        return {
            "results": [
                {
                    "score": 1.0,
                    "response": {
                        "doi": "10.1234/example",
                        "title": "Canine hemangiosarcoma open access paper",
                        "is_oa": True,
                        "best_oa_location": {"url_for_landing_page": "https://example.org/paper"},
                    },
                }
            ]
        }

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    records = UnpaywallHarvesterV2().fetch(
        '"canine hemangiosarcoma"',
        limit=1,
        email="contact@example.org",
        is_oa=True,
    )

    assert len(records) == 1
    assert calls == [
        (
            "https://api.unpaywall.org/v2/search/",
            {"query": '"canine hemangiosarcoma"', "is_oa": "true", "email": "contact@example.org"},
            {"timeout_seconds": harvesters_v2.DEFAULT_REQUEST_TIMEOUT_SECONDS, "attempts": harvesters_v2.DEFAULT_REQUEST_ATTEMPTS},
        )
    ]


def test_unpaywall_v2_fetch_uses_doi_endpoint_for_doi_queries(monkeypatch):
    calls = []

    def fake_get_json(url, params, **kwargs):
        calls.append((url, params, kwargs))
        return {
            "doi": "10.1234/example",
            "title": "Human angiosarcoma open access DOI record",
            "is_oa": True,
            "best_oa_location": {"url_for_landing_page": "https://example.org/doi-record"},
        }

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)

    records = UnpaywallHarvesterV2().fetch(
        "https://doi.org/10.1234/example",
        limit=1,
        email="contact@example.org",
    )

    assert len(records) == 1
    assert records[0].research_object.identifiers["doi"] == "10.1234/example"
    assert calls == [
        (
            "https://api.unpaywall.org/v2/10.1234%2Fexample",
            {"email": "contact@example.org"},
            {"timeout_seconds": harvesters_v2.DEFAULT_REQUEST_TIMEOUT_SECONDS, "attempts": harvesters_v2.DEFAULT_REQUEST_ATTEMPTS},
        )
    ]


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
    assert unpaywall_query.active is False


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


def test_europe_pmc_v2_normalizer_can_store_licensed_full_text():
    record = EuropePMCHarvesterV2().normalize(
        {
            "id": "PMC123",
            "pmcid": "PMC123",
            "title": "Endothelial biology review",
            "abstractText": "Sparse abstract.",
            "isOpenAccess": "Y",
        },
        full_text_xml="""
        <article xmlns="http://jats.nlm.nih.gov">
          <front>
            <article-meta>
              <article-id pub-id-type="pmc">PMC123</article-id>
            </article-meta>
          </front>
          <body>
            <sec>
              <title>Results</title>
              <p>Human angiosarcoma full text mentions VEGF and propranolol.</p>
            </sec>
          </body>
        </article>
        """,
    )
    harvester = EuropePMCHarvesterV2()

    assert record.raw_record.raw_payload["full_text"] == "Results Human angiosarcoma full text mentions VEGF and propranolol."
    assert record.raw_record.raw_payload["full_text_sections"] == [
        {
            "section_label": "full_text:results",
            "title": "Results",
            "text": "Results Human angiosarcoma full text mentions VEGF and propranolol.",
        }
    ]
    assert record.research_object.metadata["full_text_available"] is True
    assert record.research_object.metadata["body_only_match"] is True
    assert record.research_object.metadata["body_ingestion_policy"]["matched_concepts"] == ["human_angiosarcoma"]
    assert harvester.chunk_section_label(record) == "full_text"
    assert "full text mentions VEGF" in harvester.text_for_chunking(record)
    sections = harvester.chunk_text_sections(record)
    assert [section_label for section_label, _text in sections] == ["title_abstract", "full_text:results"]
    assert "Sparse abstract" in sections[0][1]
    assert "Sparse abstract" not in sections[1][1]
    assert "full text mentions VEGF" in sections[1][1]


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


def test_pmc_oa_v2_normalizer_extracts_license_and_full_text():
    xml = """
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <GetRecord>
        <record>
          <metadata>
            <article xmlns="http://jats.nlm.nih.gov" xmlns:xlink="http://www.w3.org/1999/xlink" article-type="research-article">
              <front>
                <journal-meta>
                  <journal-title-group><journal-title>Example Journal</journal-title></journal-title-group>
                </journal-meta>
                <article-meta>
                  <article-id pub-id-type="pmid">12345</article-id>
                  <article-id pub-id-type="pmc">PMC999999</article-id>
                  <article-id pub-id-type="doi">10.1234/PMC.TEST</article-id>
                  <title-group>
                    <article-title>Canine <italic>hemangiosarcoma</italic> and human angiosarcoma</article-title>
                  </title-group>
                  <pub-date pub-type="epub"><year>2026</year><month>04</month><day>01</day></pub-date>
                  <permissions>
                    <license license-type="open-access" xlink:href="https://creativecommons.org/licenses/by/4.0/">
                      <license-p>Creative Commons Attribution License</license-p>
                    </license>
                  </permissions>
                  <abstract><p>Human angiosarcoma analog evidence.</p></abstract>
                </article-meta>
              </front>
              <body>
                <sec>
                  <title>Results</title>
                  <p>Canine hemangiosarcoma full text mentions VEGF and propranolol.</p>
                </sec>
              </body>
            </article>
          </metadata>
        </record>
      </GetRecord>
    </OAI-PMH>
    """

    record = PMCOAHarvesterV2().normalize(
        xml,
        oa_metadata={"oa_license": "CC BY", "links": [{"format": "tgz", "href": "ftp://example.test/a.tgz"}]},
        source_query="hemangiosarcoma",
    )

    assert record.raw_record.source_key == "pmc_oa"
    assert record.raw_record.raw_payload["full_text"] == "Results Canine hemangiosarcoma full text mentions VEGF and propranolol."
    assert record.raw_record.raw_payload["full_text_sections"] == [
        {
            "section_label": "full_text:results",
            "title": "Results",
            "text": "Results Canine hemangiosarcoma full text mentions VEGF and propranolol.",
        }
    ]
    assert record.research_object.identifiers["pmcid"] == "PMC999999"
    assert record.research_object.identifiers["doi"] == "10.1234/pmc.test"
    assert record.research_object.metadata["journal"] == "Example Journal"
    assert record.research_object.metadata["license"]["oa_license"] == "CC BY"
    assert record.research_object.metadata["license"]["jats_license_url"] == "https://creativecommons.org/licenses/by/4.0/"
    assert record.research_object.metadata["full_text_available"] is True
    assert record.research_object.metadata["ingestion_policy"]["matched_concepts"] == [
        "canine_hsa",
        "human_angiosarcoma",
    ]


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


def test_pmc_oa_v2_chunks_full_text_not_just_abstract():
    record = PMCOAHarvesterV2().normalize(
        """
        <article xmlns="http://jats.nlm.nih.gov">
          <front>
            <article-meta>
              <article-id pub-id-type="pmc">PMC1</article-id>
              <title-group><article-title>Canine hemangiosarcoma</article-title></title-group>
              <abstract><p>Short abstract.</p></abstract>
            </article-meta>
          </front>
          <body><p>Full text body with human angiosarcoma comparative evidence.</p></body>
        </article>
        """,
        oa_metadata={"oa_license": "CC BY"},
    )
    harvester = PMCOAHarvesterV2()

    assert harvester.chunk_section_label(record) == "full_text"
    assert "Full text body" in harvester.text_for_chunking(record)
    sections = harvester.chunk_text_sections(record)
    assert [section_label for section_label, _text in sections] == ["title_abstract", "full_text"]
    assert "Short abstract" in sections[0][1]
    assert "Short abstract" not in sections[1][1]
    assert sections[1][1] == "Full text body with human angiosarcoma comparative evidence."


def test_pmc_oa_v2_does_not_label_abstract_only_records_as_full_text():
    record = PMCOAHarvesterV2().normalize(
        """
        <article xmlns="http://jats.nlm.nih.gov">
          <front>
            <article-meta>
              <article-id pub-id-type="pmc">PMC2</article-id>
              <title-group><article-title>Canine hemangiosarcoma</article-title></title-group>
              <abstract><p>Short abstract.</p></abstract>
            </article-meta>
          </front>
        </article>
        """,
        oa_metadata={"oa_license": "CC BY"},
    )
    harvester = PMCOAHarvesterV2()

    assert record.research_object.metadata["full_text_available"] is False
    assert harvester.chunk_section_label(record) == "title_abstract"
    assert harvester.chunk_text_sections(record) == [("title_abstract", "Canine hemangiosarcoma\n\nShort abstract.")]


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


def test_pmc_oa_v2_fetch_caps_candidate_metadata_scans(monkeypatch):
    metadata_calls = []

    def fake_get_json(url, params):
        assert url.endswith("/esearch.fcgi")
        assert params["retmax"] == 4
        return {"esearchresult": {"idlist": [str(index) for index in range(1, 20)]}}

    def fake_metadata(pmcid, **kwargs):
        metadata_calls.append((pmcid, kwargs))
        return None

    monkeypatch.setattr(harvesters_v2, "_get_json", fake_get_json)
    monkeypatch.setattr(harvesters_v2, "_pmc_oa_metadata", fake_metadata)

    records = PMCOAHarvesterV2().fetch(
        "hemangiosarcoma",
        limit=3,
        max_candidate_records=4,
    )

    assert records == []
    assert [pmcid for pmcid, _kwargs in metadata_calls] == ["PMC1", "PMC2", "PMC3", "PMC4"]
    assert all(
        kwargs["timeout_seconds"] == harvesters_v2.FULL_TEXT_REQUEST_TIMEOUT_SECONDS
        and kwargs["attempts"] == harvesters_v2.FULL_TEXT_REQUEST_ATTEMPTS
        for _pmcid, kwargs in metadata_calls
    )


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


def test_local_ingestion_replaces_full_text_chunks_by_section(tmp_path, monkeypatch):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    pipeline = LocalIngestionPipeline(repo)
    query = SourceQuery(
        source_key="europe_pmc",
        query_name="licensed_full_text_test",
        query_text="hemangiosarcoma",
    )
    full_text_record = EuropePMCHarvesterV2().normalize(
        {
            "id": "PMC123",
            "pmcid": "PMC123",
            "title": "Endothelial biology review",
            "abstractText": "Sparse abstract.",
            "isOpenAccess": "Y",
        },
        full_text_xml="""
        <article xmlns="http://jats.nlm.nih.gov">
          <body><p>Canine hemangiosarcoma full text body mentions VEGF and propranolol.</p></body>
        </article>
        """,
    )
    abstract_only_record = EuropePMCHarvesterV2().normalize(
        {
            "id": "PMC123",
            "pmcid": "PMC123",
            "title": "Endothelial biology review",
            "abstractText": "Sparse abstract.",
            "isOpenAccess": "Y",
        }
    )

    class FakeEuropePMCHarvester(EuropePMCHarvesterV2):
        records = [full_text_record]

        def fetch(self, query_text, limit=25, **params):
            return self.records

    monkeypatch.setitem(harvesters_v2.HARVESTERS_V2, "europe_pmc", FakeEuropePMCHarvester)

    result = pipeline.ingest_query(query, limit=1)
    chunks = repo.list_document_chunks(source_key="europe_pmc")

    assert result.document_chunks == 2
    assert result.full_text_research_objects == 1
    assert result.section_chunk_counts == {"title_abstract": 1, "full_text": 1}
    assert [chunk.section_label for chunk in chunks] == ["title_abstract", "full_text"]
    assert "Sparse abstract" not in chunks[1].text_content

    FakeEuropePMCHarvester.records = [abstract_only_record]
    refreshed = pipeline.ingest_query(query, limit=1)
    refreshed_chunks = repo.list_document_chunks(source_key="europe_pmc")

    assert refreshed.document_chunks == 1
    assert refreshed.full_text_research_objects == 0
    assert refreshed.section_chunk_counts == {"title_abstract": 1}
    assert [chunk.section_label for chunk in refreshed_chunks] == ["title_abstract"]


def test_pmc_oa_v2_is_registered_harvester():
    assert HARVESTERS_V2["pmc_oa"] is PMCOAHarvesterV2


def test_hosted_literature_smoke_includes_pmc_oa():
    assert "pmc_oa" in LITERATURE_CLINICAL_SMOKE_KEYS
    assert "pmc_oa" in HOSTED_API_REPORT_KEYS


def test_unpaywall_is_registered_for_manual_oa_discovery():
    assert HARVESTERS_V2["unpaywall"] is UnpaywallHarvesterV2
    assert "unpaywall" not in HOSTED_API_REPORT_KEYS


def test_literature_corpus_harvest_targets_hundreds_of_papers():
    assert LITERATURE_CORPUS_SOURCE_KEYS == (
        "openalex",
        "pubmed",
        "crossref",
    )
    assert sum(LITERATURE_CORPUS_SOURCE_LIMITS.values()) >= 300
    assert set(LITERATURE_CORPUS_SOURCE_KEYS).isdisjoint(LITERATURE_FULL_TEXT_SOURCE_KEYS)


def test_full_text_refresh_keeps_heavy_sources_bounded():
    assert LITERATURE_FULL_TEXT_SOURCE_KEYS == ("europe_pmc", "pmc_oa")
    assert LITERATURE_FULL_TEXT_SOURCE_LIMITS["europe_pmc"] <= 10
    assert LITERATURE_FULL_TEXT_SOURCE_LIMITS["pmc_oa"] <= 3


def test_all_api_smoke_covers_every_hosted_report_source():
    assert ALL_API_SMOKE_KEYS == HOSTED_API_REPORT_KEYS
    assert set(ALL_API_SMOKE_KEYS) == {
        "pubchem",
        "chembl",
        "uniprot",
        "rcsb_pdb",
        "openfda_animal_events",
        "icdc",
        "geo",
        "sra",
        "openalex",
        "pubmed",
        "europe_pmc",
        "crossref",
        "pmc_oa",
        "clinicaltrials_gov",
    }


def test_x_topic_monitor_builds_official_api_request_only():
    request = x_topic_monitor.build_recent_search_request(
        x_topic_monitor.XTopicRequest(
            query='"canine hemangiosarcoma"',
            query_name="x_disease_monitoring",
            max_results=10,
        )
    )

    assert request.method == "GET"
    assert request.url.startswith("https://api.x.com/2/tweets/search/recent?")
    assert request.params["query"] == '"canine hemangiosarcoma" lang:en -is:retweet'
    assert request.headers["Authorization"] == "Bearer <X_BEARER_TOKEN>"
    assert request.billable is True
    assert any("Official X API" in note for note in request.notes)


def test_x_topic_monitor_builds_twitterapi_io_request():
    request = x_topic_monitor.build_twitterapi_io_search_request(
        x_topic_monitor.XTopicRequest(
            query='"canine hemangiosarcoma"',
            query_name="x_disease_monitoring",
            max_results=10,
        )
    )

    assert request.method == "GET"
    assert request.url.startswith("https://api.twitterapi.io/twitter/tweet/advanced_search?")
    assert request.params["query"] == '"canine hemangiosarcoma" lang:en -filter:retweets'
    assert request.params["queryType"] == "Latest"
    assert request.headers["x-api-key"] == "<TWITTERAPI_IO_KEY>"
    assert request.billable is True


def test_twitterapi_io_provider_searches_and_normalizes_candidates():
    calls = []

    def fake_transport(url, params, headers, timeout_seconds):
        calls.append((url, params, headers, timeout_seconds))
        return {
            "tweets": [
                {
                    "id": "123",
                    "url": "https://x.com/vetonc/status/123",
                    "text": "New canine hemangiosarcoma trial links to PubMed.",
                    "createdAt": "2026-04-28T10:00:00Z",
                    "lang": "en",
                    "conversationId": "789",
                    "retweetCount": 1,
                    "replyCount": 2,
                    "likeCount": 3,
                    "quoteCount": 4,
                    "viewCount": 500,
                    "author": {"id": "456", "userName": "vetonc", "name": "Vet Onc"},
                    "entities": {
                        "urls": [
                            {"expanded_url": "https://pubmed.ncbi.nlm.nih.gov/123456/"},
                        ]
                    },
                }
            ],
            "has_next_page": False,
            "next_cursor": "",
        }

    result = x_topic_monitor.TwitterApiIoProvider(
        api_key="test-key",
        transport=fake_transport,
        timeout_seconds=12.0,
    ).search(
        x_topic_monitor.XTopicRequest(
            query='"canine hemangiosarcoma"',
            query_name="x_trial_monitoring",
            max_results=10,
        )
    )

    assert calls == [
        (
            "https://api.twitterapi.io/twitter/tweet/advanced_search",
            {"query": '"canine hemangiosarcoma" lang:en -filter:retweets', "queryType": "Latest", "cursor": ""},
            {"x-api-key": "test-key"},
            12.0,
        )
    ]
    assert result.provider == "twitterapi_io"
    assert result.raw_tweet_count == 1
    assert len(result.candidates) == 1
    assert result.candidates[0].canonical_url == "https://x.com/vetonc/status/123"
    assert result.candidates[0].username == "vetonc"
    assert result.candidates[0].durable_links == ["https://pubmed.ncbi.nlm.nih.gov/123456/"]
    assert result.candidates[0].metadata["provider_payload"]["provider"] == "twitterapi_io"


def test_twitterapi_io_provider_requires_key(monkeypatch):
    monkeypatch.delenv("TWITTERAPI_IO_KEY", raising=False)

    with pytest.raises(ValueError):
        x_topic_monitor.TwitterApiIoProvider().search(
            x_topic_monitor.XTopicRequest(query='"canine hemangiosarcoma"')
        )


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


def test_x_topic_monitor_normalizes_post_for_manual_review():
    candidate = x_topic_monitor.normalize_post_payload(
        {
            "id": "123",
            "author_id": "456",
            "author": {"username": "vetonc"},
            "conversation_id": "789",
            "created_at": "2026-04-28T10:00:00Z",
            "lang": "en",
            "text": "New canine hemangiosarcoma trial links to PubMed.",
            "entities": {
                "urls": [
                    {"expanded_url": "https://pubmed.ncbi.nlm.nih.gov/123456/"},
                ]
            },
        },
        query_name="x_trial_monitoring",
    )

    assert candidate.source_record_id == "123"
    assert candidate.canonical_url == "https://x.com/vetonc/status/123"
    assert candidate.review_status == x_topic_monitor.XReviewStatus.NEEDS_REVIEW
    assert "canine hemangiosarcoma" in candidate.matched_terms
    assert candidate.durable_links == ["https://pubmed.ncbi.nlm.nih.gov/123456/"]
    assert candidate.quality_score > 0.5


def test_x_topic_review_contracts_validate():
    linked_source = XTopicLinkedSource(
        url="https://pubmed.ncbi.nlm.nih.gov/123456/",
        recommended_source_key="pubmed",
        identifier_type="pmid",
        identifier="123456",
        should_ingest=True,
        reason="PubMed record.",
    )
    action = XTopicReviewAction(
        source_record_id="123",
        query_name="x_trial_monitoring",
        action="flag_for_ingestion",
        severity="watch",
        reason="Linked PubMed record should be harvested.",
        ingestible_links=[linked_source],
    )
    result = XTopicReviewResult(actions=[action])

    assert XTopicReviewRequest().review_mode == "openrouter_required"
    assert result.actions[0].ingestible_links[0].identifier_type == "pmid"
    with pytest.raises(ValueError):
        XTopicReviewAction(source_record_id="123", action="bad", severity="watch", reason="bad")
    with pytest.raises(ValueError):
        XTopicLinkedSource(url="https://example.com", identifier_type="bad", reason="bad")


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


def test_x_topic_review_openrouter_preserves_deterministic_ingestion_guardrail(monkeypatch):
    def fake_review_model(model_name, review_payload):
        assert model_name == "anthropic/claude-sonnet-test"
        assert review_payload["candidates"][0]["durable_links"] == [
            "https://doi.org/10.7717/peerj.4375"
        ]
        return {
            "text": json.dumps(
                {
                    "actions": [
                        {
                            "source_record_id": "123",
                            "query_name": "x_disease_monitoring",
                            "username": "vetonc",
                            "action": "needs_human_review",
                            "severity": "watch",
                            "reason": "Model wants a human pass.",
                            "ingestible_links": [],
                            "evidence_refs": ["candidate:123"],
                            "metadata": {},
                        }
                    ],
                    "evidence": {"review_summary": "reviewed"},
                    "errors": [],
                }
            ),
            "metadata": {"model_name": "anthropic/claude-sonnet-test", "usage": {"cost": 0.01}},
        }

    monkeypatch.setattr(x_topic_review, "_openrouter_review_model", fake_review_model)

    result = x_topic_review.XTopicReviewAgent().run(
        XTopicReviewRequest(
            review_mode="openrouter_required",
            review_models=["anthropic/claude-sonnet-test"],
            candidates=[
                {
                    "post_id": "123",
                    "query_name": "x_disease_monitoring",
                    "username": "vetonc",
                    "quality_score": 0.7,
                    "durable_links": ["https://doi.org/10.7717/peerj.4375"],
                }
            ],
        )
    )

    assert result.ingestion_candidate_count == 1
    assert [action.action for action in result.actions] == ["needs_human_review", "flag_for_ingestion"]
    assert result.actions[1].ingestible_links[0].recommended_source_key == "crossref"
    assert result.evidence["model_reviews"][0]["status"] == "completed"


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


def test_x_topic_monitor_requires_review_before_storage_contracts():
    payload = {
        "id": "123",
        "author_id": "456",
        "author": {"username": "vetonc"},
        "created_at": "2026-04-28T10:00:00Z",
        "lang": "en",
        "text": "Canine hemangiosarcoma signal.",
    }
    candidate = x_topic_monitor.normalize_post_payload(payload, query_name="x_disease_monitoring")

    with pytest.raises(ValueError):
        x_topic_monitor.to_research_record(candidate, payload, accepted_by="operator")

    accepted = candidate.model_copy(update={"review_status": x_topic_monitor.XReviewStatus.ACCEPTED_SIGNAL})
    record = x_topic_monitor.to_research_record(accepted, payload, accepted_by="operator")

    assert record.raw_record.source_key == "x_topic_monitor"
    assert record.research_object.object_type == ResearchObjectType.KNOWLEDGE_ENTRY
    assert record.research_object.dedupe_key == "x_topic_monitor:post:123"
    assert record.document_chunk.section_label == "x_topic_signal"
    assert "text retention mode=store_metadata_only" in record.document_chunk.text_content


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


def test_clinicaltrials_gov_v2_is_registered_harvester():
    assert HARVESTERS_V2["clinicaltrials_gov"] is ClinicalTrialsGovHarvesterV2


def _avma_vctr_study_card():
    return {
        "id": 12345,
        "is_similar": False,
        "description": "<p>Dogs with splenic hemangiosarcoma receive antibody therapy after splenectomy.</p>",
        "absolute_url": "https://veterinaryclinicaltrials.org/s/antibody-therapy-hsa/",
        "tagline": "Dogs with splenic hemangiosarcoma after splenectomy",
        "name": "Safety and efficacy of antibody therapy for dogs with splenic hemangiosarcoma",
        "visible_sc_items": [
            {"id": "species-dog", "tag_path": "/species/dogs", "label": "Dogs", "parent_label": "Species"},
            {
                "id": "oncology-hsa",
                "tag_path": "/primary-field/oncology/hemangiosarcoma",
                "label": "Hemangiosarcoma",
                "parent_label": "Oncology",
            },
            {
                "id": "intervention-biologic",
                "tag_path": "/intervention-type/biologic",
                "label": "Biologic",
                "parent_label": "Intervention type",
            },
            {
                "id": "financial-covered",
                "tag_path": "/financial-incentive/study-costs-covered",
                "label": "Study costs covered",
                "parent_label": "Financial incentive",
            },
        ],
        "thumbnail": "https://veterinaryclinicaltrials.org/media/cache/thumb.jpg",
        "study_type": "Interventional",
        "target_gender": "All",
        "age_range": "1 year and older",
        "top_investigator": "Claire Lemons, DVM",
        "status": "Recruiting",
        "status_color": "green",
        "distance_to_location": None,
        "vct_code": "VCT16000189",
    }


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


def test_avma_vctr_v2_is_registered_harvester():
    assert HARVESTERS_V2["avma_vctr"] is AVMAVCTRHarvesterV2


def test_avma_vctr_claim_extractor_requires_primary_hsa_scope():
    extractor = LocalRuleClaimExtractor()
    melanoma = ResearchObject(
        object_type="veterinary_trial",
        title="The effect of a novel mushroom formula on canine oral malignant melanoma",
        abstract=(
            "This canine melanoma study evaluates immune activity and angiogenesis. "
            "A cited hemangiosarcoma paper informed dose selection."
        ),
        source_key="avma_vctr",
        metadata={"conditions": ["Melanoma"]},
    )
    melanoma_chunk = DocumentChunk(
        research_object_id=melanoma.id,
        chunk_index=0,
        section_label="veterinary_trial_record",
        text_content="Dogs receive an immune supplement. Angiogenesis and macrophage activity are monitored.",
        content_hash="melanoma",
    )
    hsa = ResearchObject(
        object_type="veterinary_trial",
        title="Combination therapy for dogs with hemangiosarcoma",
        abstract="Dogs with splenic hemangiosarcoma receive doxorubicin.",
        source_key="avma_vctr",
        metadata={"conditions": ["Hemangiosarcoma"]},
    )
    hsa_chunk = DocumentChunk(
        research_object_id=hsa.id,
        chunk_index=0,
        section_label="veterinary_trial_record",
        text_content="Dogs with hemangiosarcoma receive doxorubicin chemotherapy in this trial.",
        content_hash="hsa",
    )

    assert extractor.extract_chunk(melanoma_chunk, melanoma) == []
    claims = extractor.extract_chunk(hsa_chunk, hsa)
    assert any(claim.statement.startswith("doxorubicin is discussed") for claim in claims)


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


def test_icdc_v2_is_registered_harvester():
    assert HARVESTERS_V2["icdc"] is ICDCHarvesterV2


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


def test_geo_and_sra_v2_are_registered_harvesters():
    assert HARVESTERS_V2["geo"] is GEOHarvesterV2
    assert HARVESTERS_V2["sra"] is SRAHarvesterV2


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


def test_phase_three_api_harvesters_are_registered():
    assert HARVESTERS_V2["pubchem"] is PubChemHarvesterV2
    assert HARVESTERS_V2["chembl"] is ChEMBLHarvesterV2
    assert HARVESTERS_V2["uniprot"] is UniProtHarvesterV2
    assert HARVESTERS_V2["rcsb_pdb"] is RCSBPDBHarvesterV2
    assert HARVESTERS_V2["openfda_animal_events"] is OpenFDAAnimalEventsHarvesterV2


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


def test_local_store_persists_raw_and_research_object(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    record = OpenAlexHarvesterV2().normalize(
        {
            "id": "https://openalex.org/W456",
            "doi": "https://doi.org/10.1234/local",
            "title": "Local persistence example",
            "publication_year": 2026,
            "abstract_inverted_index": {"Local": [0], "object": [1]},
            "primary_location": {"landing_page_url": "https://doi.org/10.1234/local"},
        }
    )

    fetch_run_id = repo.create_fetch_run("openalex", "unit_test")
    raw_id = repo.upsert_raw_record(record.raw_record, fetch_run_id)
    object_id = repo.upsert_research_object(record.research_object, raw_id)
    saved = repo.get_research_object(object_id)

    assert saved is not None
    assert saved.title == "Local persistence example"
    assert repo.coverage_summary()["research_objects"] == 1


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


def test_build_chunk_embedding_text_includes_canonical_entity_context():
    object_id = uuid4()
    chunk_id = uuid4()
    research_object = ResearchObject(
        id=object_id,
        object_type="publication",
        title="Canonical entity embedding example",
        abstract="Canine HSA angiogenesis context.",
        source_key="pubmed",
        identifiers={"pmid": "123"},
    )
    chunk = DocumentChunk(
        id=chunk_id,
        research_object_id=object_id,
        chunk_index=0,
        section_label="abstract",
        text_content="KDR is also called VEGF receptor 2 in canine hemangiosarcoma.",
        content_hash="embedding-text-context",
    )
    mentions = [
        EntityMention(
            research_object_id=object_id,
            chunk_id=chunk_id,
            chunk_index=0,
            section_label="abstract",
            source_key="pubmed",
            entity_type="target",
            canonical_name="KDR",
            normalized_key="target:kdr",
            matched_text="VEGF receptor 2",
            matched_alias="VEGF receptor 2",
            chunk_char_start=19,
            chunk_char_end=34,
            external_ids={"chembl_id": "CHEMBL279"},
            resolver_name="unit",
            resolver_version="1",
            match_rule="unit",
        ),
        EntityMention(
            research_object_id=object_id,
            chunk_id=chunk_id,
            chunk_index=0,
            section_label="abstract",
            source_key="pubmed",
            entity_type="disease",
            canonical_name="canine hemangiosarcoma",
            normalized_key="disease:canine_hsa",
            matched_text="canine hemangiosarcoma",
            matched_alias="canine hemangiosarcoma",
            chunk_char_start=38,
            chunk_char_end=61,
            resolver_name="unit",
            resolver_version="1",
            match_rule="unit",
        ),
    ]

    text = build_chunk_embedding_text(chunk, research_object, mentions)

    assert "title: Canonical entity embedding example" in text
    assert "chunk_text: KDR is also called VEGF receptor 2" in text
    assert "canonical_entities:" in text
    assert "target: KDR [target:kdr] (chembl_id=CHEMBL279)" in text
    assert "disease: canine hemangiosarcoma [disease:canine_hsa]" in text


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


def test_sqlite_text_embeddings_persist_and_report_coverage(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Embedding persistence example",
            source_key="pubmed",
            dedupe_key="pubmed:embedding-persistence",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGF signaling is discussed in canine hemangiosarcoma.",
            content_hash="embedding-chunk-1",
        )
    )

    saved = repo.upsert_text_embedding(
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
            text_preview=chunk.text_content,
        )
    )

    fetched = repo.get_text_embedding(saved.embedding_id)
    listed = repo.list_text_embeddings(source_key="pubmed", embedding_model="unit-embedding-v1")
    coverage = repo.embedding_coverage(source_key="pubmed", embedding_model="unit-embedding-v1")

    assert fetched == saved
    assert listed == [saved]
    assert repo.coverage_summary()["text_embeddings"] == 1
    assert coverage.total_chunks == 1
    assert coverage.embedded_chunks == 1
    assert coverage.missing_chunks == 0
    assert coverage.coverage_ratio == 1.0
    assert coverage.embedding_models == {"unit-embedding-v1": 1}


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


def test_index_embeddings_for_repository_is_idempotent_and_includes_entities(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Embedding index example",
            source_key="pubmed",
            dedupe_key="pubmed:embedding-index",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGF receptor 2 appears in canine hemangiosarcoma literature.",
            content_hash="embedding-index-chunk-1",
        )
    )
    repo.upsert_entity_mention(
        EntityMention(
            research_object_id=object_id,
            chunk_id=chunk.id,
            chunk_index=chunk.chunk_index,
            section_label=chunk.section_label,
            source_key="pubmed",
            entity_type="target",
            canonical_name="KDR",
            normalized_key="target:kdr",
            matched_text="VEGF receptor 2",
            matched_alias="VEGF receptor 2",
            chunk_char_start=0,
            chunk_char_end=15,
            resolver_name="unit",
            resolver_version="1",
            match_rule="unit",
        )
    )

    first_result = index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    first_embedding = repo.list_text_embeddings(source_key="pubmed", embedding_model="local-hash-test")[0]
    second_result = index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    second_embedding = repo.list_text_embeddings(source_key="pubmed", embedding_model="local-hash-test")[0]

    assert first_result.errors == ()
    assert first_result.chunks_seen == 1
    assert first_result.embeddings_created == 1
    assert second_result.embeddings_skipped == 1
    assert second_result.embeddings_created == 0
    assert second_embedding.embedding_id == first_embedding.embedding_id
    assert second_embedding.embedding == first_embedding.embedding
    assert len(repo.list_text_embeddings(embedding_model="local-hash-test")) == 1
    assert "canonical_entities: target: KDR [target:kdr]" in second_embedding.text_preview
    assert second_embedding.metadata["chunk_content_hash"] == chunk.content_hash
    assert second_embedding.metadata["canonical_entity_count"] == 1


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


def test_sqlite_text_embedding_search_uses_json_vectors_and_upserts_by_chunk_model(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Embedding search example",
            source_key="pubmed",
            dedupe_key="pubmed:embedding-search",
        )
    )
    chunks = [
        repo.upsert_document_chunk(
            DocumentChunk(
                research_object_id=object_id,
                chunk_index=index,
                section_label="abstract",
                text_content=text,
                content_hash=f"embedding-search-{index}",
            )
        )
        for index, text in enumerate(
            [
                "VEGF signaling and angiogenesis in hemangiosarcoma.",
                "Chemotherapy toxicity monitoring in dogs.",
            ]
        )
    ]
    first = repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunks[0].id,
            research_object_id=object_id,
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash=chunks[0].content_hash,
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[1.0, 0.0, 0.0],
        )
    )
    repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunks[1].id,
            research_object_id=object_id,
            chunk_index=1,
            source_key="pubmed",
            object_type="publication",
            content_hash=chunks[1].content_hash,
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[0.0, 1.0, 0.0],
        )
    )

    results = repo.search_text_embeddings(
        TextEmbeddingSearchRequest(
            query_embedding=[0.95, 0.05, 0.0],
            embedding_model="unit-embedding-v1",
            source_key="pubmed",
            limit=2,
        )
    )

    updated = repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=chunks[0].id,
            research_object_id=object_id,
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash="embedding-search-0-updated",
            embedding_model="unit-embedding-v1",
            embedding_dimensions=3,
            embedding=[0.0, 0.0, 1.0],
        )
    )
    updated_results = repo.search_text_embeddings(
        TextEmbeddingSearchRequest(query_embedding=[0.0, 0.0, 1.0], embedding_model="unit-embedding-v1")
    )

    assert [result.embedding.chunk_id for result in results] == [chunks[0].id, chunks[1].id]
    assert results[0].score > results[1].score
    assert updated.embedding_id == first.embedding_id
    assert updated.content_hash == "embedding-search-0-updated"
    assert len(repo.list_text_embeddings(embedding_model="unit-embedding-v1")) == 2
    assert updated_results[0].embedding.embedding_id == first.embedding_id


def test_service_search_research_chunks_uses_embeddings_without_returning_vectors(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGFA angiogenesis in canine hemangiosarcoma",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-embedding",
        )
    )
    vegf_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis VEGFA angiogenesis in canine hemangiosarcoma.",
            content_hash="retrieval-embedding-vegf",
        )
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=1,
            section_label="methods",
            text_content="Doxorubicin cardiotoxicity monitoring in dogs.",
            content_hash="retrieval-embedding-dox",
        )
    )
    index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")

    results = service.search_research_chunks(
        ResearchChunkSearchRequest(
            query="VEGFA angiogenesis",
            source_key="pubmed",
            embedding_model="local-hash-test",
            limit=2,
        )
    )
    payload = results.model_dump(mode="json")

    assert results.search_mode == "embedding"
    assert results.results[0].match_type == "embedding"
    assert results.results[0].chunk.id == vegf_chunk.id
    assert not _contains_key(payload, "embedding")


def test_service_search_research_chunks_overfetches_stale_embedding_hits(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="VEGFA angiogenesis overfetch example",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-overfetch",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis in canine hemangiosarcoma.",
            content_hash="retrieval-overfetch-valid",
        )
    )
    provider = LocalDeterministicEmbeddingProvider(embedding_model="local-hash-test")
    stale_vector = provider.embed_text("VEGFA angiogenesis")
    repo.upsert_text_embedding(
        TextEmbedding(
            chunk_id=uuid4(),
            research_object_id=object_id,
            chunk_index=0,
            source_key="pubmed",
            object_type="publication",
            content_hash="retrieval-overfetch-stale",
            embedding_model="local-hash-test",
            embedding_dimensions=provider.embedding_dimensions,
            embedding=stale_vector,
        )
    )
    index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")

    results = service.search_research_chunks(
        ResearchChunkSearchRequest(
            query="VEGFA angiogenesis",
            source_key="pubmed",
            embedding_model="local-hash-test",
            limit=1,
        )
    )

    assert results.search_mode == "embedding"
    assert results.results[0].chunk.id == chunk.id


def test_service_search_research_chunks_falls_back_to_keyword_and_bounds_text(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Keyword fallback retrieval example",
            abstract="VEGFA angiogenesis context for canine HSA.",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-keyword",
        )
    )
    chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis " + ("canine hemangiosarcoma " * 20),
            content_hash="retrieval-keyword-vegf",
        )
    )
    repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=1,
            section_label="methods",
            text_content="Chemotherapy dosing schedule.",
            content_hash="retrieval-keyword-other",
        )
    )

    results = service.search_research_chunks(
        ResearchChunkSearchRequest(
            query="VEGFA angiogenesis",
            source_key="pubmed",
            limit=5,
            max_chunk_chars=220,
        )
    )

    assert results.search_mode == "keyword"
    assert results.results[0].match_type == "keyword"
    assert results.results[0].chunk.id == chunk.id
    assert len(results.results[0].chunk.text_content) == 220
    assert results.results[0].text_truncated is True


def test_service_get_chunk_context_and_research_object_are_bounded(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Chunk context example",
            source_key="pubmed",
            dedupe_key="pubmed:retrieval-context",
        )
    )
    chunks = [
        repo.upsert_document_chunk(
            DocumentChunk(
                research_object_id=object_id,
                chunk_index=index,
                section_label=f"section-{index}",
                text_content=text,
                content_hash=f"retrieval-context-{index}",
            )
        )
        for index, text in enumerate(
            [
                "Background chunk about canine hemangiosarcoma.",
                "Middle chunk mentions VEGFA receptor signaling in canine hemangiosarcoma.",
                "Follow-up chunk about translational validation.",
            ]
        )
    ]
    repo.upsert_entity_mention(
        EntityMention(
            research_object_id=object_id,
            chunk_id=chunks[1].id,
            chunk_index=1,
            section_label="section-1",
            source_key="pubmed",
            entity_type="target",
            canonical_name="VEGFA",
            normalized_key="target:vegfa",
            matched_text="VEGFA",
            matched_alias="VEGFA",
            chunk_char_start=22,
            chunk_char_end=27,
            resolver_name="unit",
            resolver_version="1",
            match_rule="unit",
        )
    )

    context = service.get_chunk_context(
        ChunkContextRequest(chunk_id=chunks[1].id, window=1, max_chunk_chars=220)
    )
    object_result = service.get_research_object(
        ResearchObjectReadRequest(research_object_id=object_id, max_chunks=2, max_chunk_chars=220)
    )

    assert context is not None
    assert context.chunk.id == chunks[1].id
    assert [chunk.id for chunk in context.before_chunks] == [chunks[0].id]
    assert [chunk.id for chunk in context.after_chunks] == [chunks[2].id]
    assert context.entity_mentions[0].canonical_name == "VEGFA"
    assert object_result is not None
    assert object_result.research_object.id == object_id
    assert [chunk.id for chunk in object_result.chunks] == [chunks[0].id, chunks[1].id]


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


def test_document_chunk_upsert_preserves_readable_stable_chunk_id(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    service = HSAResearchService(repo)
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Stable chunk id retrieval example",
            source_key="pubmed",
            dedupe_key="pubmed:stable-chunk-id",
        )
    )
    original_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="Original text.",
            content_hash="stable-chunk-original",
        )
    )
    replacement_chunk = repo.upsert_document_chunk(
        DocumentChunk(
            research_object_id=object_id,
            chunk_index=0,
            section_label="abstract",
            text_content="VEGFA angiogenesis stable chunk context.",
            content_hash="stable-chunk-replacement",
        )
    )

    search = service.search_research_chunks(
        ResearchChunkSearchRequest(query="VEGFA angiogenesis", source_key="pubmed", limit=1)
    )
    index_embeddings_for_repository(repo, source_key="pubmed", embedding_model="local-hash-test")
    smoke = service.run_retrieval_smoke(
        RetrievalSmokeRequest(
            query="VEGFA angiogenesis",
            source_key="pubmed",
            embedding_model="local-hash-test",
            require_embedding=True,
        )
    )

    assert replacement_chunk.id == original_chunk.id
    assert repo.get_document_chunk(replacement_chunk.id) is not None
    assert search.results[0].chunk.id == original_chunk.id
    assert smoke.passed is True
    assert smoke.selected_chunk_id == original_chunk.id
    assert smoke.chunk_context is not None
    assert smoke.chunk_context.chunk.content_hash == "stable-chunk-replacement"


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


def test_mcp_full_text_triage_helper_dumps_action(monkeypatch, tmp_path):
    service = make_service(tmp_path)
    monkeypatch.setattr(mcp_server, "get_service", lambda: service)

    payload = mcp_server.triage_full_text_issue_tool(
        source_key="europe_pmc",
        stage="dagster_run",
        error_message="Timed out while running hosted refresh",
        runtime_seconds=2700,
        timeout_seconds=2700,
        raw_records=10,
        research_objects=10,
    )

    assert payload["triage_name"] == "full_text_triage_agent"
    assert payload["action"] == "reduce_batch_size"
    assert payload["should_retry"] is True
    assert payload["should_block_schedule"] is True


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


def test_backfill_papers_json_creates_object_and_chunk(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    papers_path = tmp_path / "papers.json"
    papers_path.write_text(
        """
        [
          {
            "pmid": "123",
            "doi": "10.1234/hsa",
            "title": "Canine hemangiosarcoma backfill",
            "abstract": "This abstract mentions canine hemangiosarcoma.",
            "journal": "Example Journal",
            "year": "2026",
            "source": "pubmed",
            "url": "https://pubmed.ncbi.nlm.nih.gov/123/"
          }
        ]
        """
    )

    result = backfill_papers_json(repo, papers_path)
    coverage = repo.coverage_summary()

    assert result.raw_records == 1
    assert result.research_objects == 1
    assert result.document_chunks == 1
    assert coverage["document_chunks"] == 1


def test_backfill_deep_dives_creates_knowledge_entry_chunks(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    deep_dives = tmp_path / "deep_dives"
    deep_dives.mkdir()
    (deep_dives / "treatment_example.md").write_text(
        "# Treatment Example\n\n## TL;DR\n\nThis is a local knowledge entry.\n\n## Detail\n\nMore text."
    )

    result = backfill_deep_dives(repo, deep_dives)
    objects = repo.list_research_objects(object_type="knowledge_entry")

    assert result.raw_records == 1
    assert result.research_objects == 1
    assert result.document_chunks == 1
    assert objects[0].metadata["track"] == "treatment"


def test_local_claim_extractor_creates_draft_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()
    papers_path = tmp_path / "papers.json"
    papers_path.write_text(
        """
        [
          {
            "pmid": "123",
            "title": "Propranolol and VEGF in canine hemangiosarcoma",
            "abstract": "Canine hemangiosarcoma studies discuss propranolol with VEGF and angiogenesis.",
            "journal": "Example Journal",
            "year": "2026",
            "source": "pubmed"
          }
        ]
        """
    )
    backfill_papers_json(repo, papers_path)

    result = extract_claims_for_repository(repo, source_key="current_papers")
    claims = repo.search_claims(
        ClaimSearchRequest(query="propranolol", species="canine", min_confidence=0.1, include_drafts=True)
    )

    assert result.chunks_seen == 1
    assert result.claims_written >= 1
    assert any(claim.metadata.get("extraction_status") == "draft" for claim in claims)


def test_local_claim_extractor_attaches_persisted_entity_mentions(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3", seed=False)
    title = "Propranolol and VEGF in canine hemangiosarcoma"
    obj_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title=title,
            abstract="Canine hemangiosarcoma studies discuss propranolol with VEGF and angiogenesis.",
            source_key="pubmed",
        )
    )
    for chunk in chunk_text(obj_id, title, section_label="title_abstract"):
        repo.upsert_document_chunk(chunk)
    resolved = resolve_entities_for_repository(repo, source_key="pubmed")
    mentions = repo.list_entity_mentions(source_key="pubmed")

    result = extract_claims_for_repository(repo, source_key="pubmed")
    claims = repo.search_claims(
        ClaimSearchRequest(query="propranolol", species="canine", min_confidence=0.1, include_drafts=True)
    )

    assert resolved.mentions_upserted >= 2
    assert result.claims_written >= 1
    assert claims
    assert set(claims[0].metadata["source_entity_mention_ids"]) == {str(mention.mention_id) for mention in mentions}
    assert set(claims[0].metadata["source_entity_canonical_names"]) >= {"propranolol", "VEGFA"}
    assert set(claims[0].metadata["source_entity_types"]) >= {"compound", "target"}


def test_local_claim_extractor_handles_human_angiosarcoma_analogs(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    title = "Paclitaxel targets VEGF signaling in human angiosarcoma"
    obj_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title=title,
            abstract="Human angiosarcoma studies discuss paclitaxel with VEGF and angiogenesis.",
            source_key="pubmed",
        )
    )
    for chunk in chunk_text(obj_id, title, section_label="title_abstract"):
        repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, source_key="pubmed")
    claims = repo.search_claims(
        ClaimSearchRequest(query="paclitaxel", species="human", min_confidence=0.1, include_drafts=True)
    )

    assert result.claims_written >= 1
    assert any(claim.metadata.get("context_key") == "human_angiosarcoma_analog" for claim in claims)


def test_local_claim_extractor_creates_sparse_scholarly_context_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    europe_pmc_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Primary vaginal angiosarcoma case report",
            source_key="europe_pmc",
        )
    )
    crossref_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Vascular sarcoma clinical series",
            source_key="crossref",
        )
    )
    for object_id, text in (
        (europe_pmc_id, "Primary vaginal angiosarcoma case report."),
        (crossref_id, "Vascular sarcoma clinical series."),
    ):
        for chunk in chunk_text(object_id, text, section_label="title_abstract"):
            repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, limit=10)
    claims = repo.search_claims(ClaimSearchRequest(query="source context", min_confidence=0.1, include_drafts=True, limit=10))
    statements = [claim.statement for claim in claims]

    assert result.claims_written == 2
    assert any("Europe PMC record provides human angiosarcoma" in statement for statement in statements)
    assert any("Crossref record provides human angiosarcoma" in statement for statement in statements)


def test_local_claim_extractor_creates_dataset_source_context_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    geo_id = repo.upsert_research_object(
        ResearchObject(
            object_type="dataset",
            title="Canine hemangiosarcoma expression dataset",
            source_key="geo",
        )
    )
    sra_id = repo.upsert_research_object(
        ResearchObject(
            object_type="dataset",
            title="Dog hemangiosarcoma sequence runs",
            source_key="sra",
        )
    )
    icdc_id = repo.upsert_research_object(
        ResearchObject(
            object_type="dataset",
            title="ICDC canine case CASE-1: Hemangiosarcoma",
            source_key="icdc",
        )
    )
    for object_id, text in (
        (geo_id, "Canine hemangiosarcoma expression dataset."),
        (sra_id, "Dog hemangiosarcoma sequence runs."),
        (icdc_id, "Diagnosis: Hemangiosarcoma. Species: canine."),
    ):
        for chunk in chunk_text(object_id, text, section_label="dataset_metadata"):
            repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, limit=10)
    claims = repo.search_claims(ClaimSearchRequest(query="source context", min_confidence=0.1, include_drafts=True, limit=10))
    statements = [claim.statement for claim in claims]

    assert result.claims_written == 3
    assert any("GEO record provides canine HSA source context" in statement for statement in statements)
    assert any("SRA record provides canine HSA source context" in statement for statement in statements)
    assert any("ICDC record provides canine HSA source context" in statement for statement in statements)


def test_local_claim_extractor_creates_structured_chembl_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    title = "TOCERANIB IC50 against Vascular endothelial growth factor receptor 2"
    obj_id = repo.upsert_research_object(
        ResearchObject(
            object_type="bioactivity_assay",
            title=title,
            abstract="Inhibition of Vascular endothelial growth factor receptor 2.",
            source_key="chembl",
            identifiers={"chembl_activity_id": "726668", "chembl_target_id": "CHEMBL279"},
            metadata={
                "query_term": "toceranib",
                "molecule_pref_name": "TOCERANIB",
                "target_pref_name": "Vascular endothelial growth factor receptor 2",
                "target_gene": "KDR",
                "target_category": "vegf_angiogenesis",
                "target_organism": "Homo sapiens",
                "assay_type": "B",
                "standard_type": "IC50",
                "standard_relation": "=",
                "standard_value": "60.0",
                "standard_units": "nM",
                "pchembl_value": "7.22",
                "pchembl_numeric": 7.22,
            },
        )
    )
    for chunk in chunk_text(obj_id, title, section_label="bioactivity_assay"):
        repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, source_key="chembl")
    claims = repo.search_claims(
        ClaimSearchRequest(query="toceranib", species="human", min_confidence=0.1, include_drafts=True)
    )

    assert result.chunks_seen == 1
    assert result.claims_written == 1
    assert claims[0].claim_type == "compound_modulates_target"
    assert claims[0].evidence_level == "in_vitro"
    assert claims[0].metadata["context_key"] == "chembl_target_bioactivity"
    assert "pChEMBL 7.22" in claims[0].statement


def test_local_claim_extractor_creates_structured_source_claims(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    pubchem_id = repo.upsert_research_object(
        ResearchObject(
            object_type="compound_record",
            title="Propranolol",
            source_key="pubchem",
            identifiers={"pubchem_cid": "4946", "inchikey": "AQHHHDLHHXJYJD-UHFFFAOYSA-N"},
            metadata={"identity_match": {"identity_verified": True}},
        )
    )
    uniprot_id = repo.upsert_research_object(
        ResearchObject(
            object_type="structure",
            title="KDR Vascular endothelial growth factor receptor 2",
            source_key="uniprot",
            identifiers={"uniprot_accession": "P35968"},
            metadata={
                "target_gene": "KDR",
                "organism": "Homo sapiens",
                "species_scope": "human",
                "reviewed": True,
                "alphafold_ids": ["AF-P35968-F1"],
            },
        )
    )
    rcsb_id = repo.upsert_research_object(
        ResearchObject(
            object_type="structure",
            title="Human VEGFR2 kinase domain",
            source_key="rcsb_pdb",
            identifiers={"pdb_id": "3VHE"},
            metadata={"target_gene": "KDR", "experimental_methods": ["X-RAY DIFFRACTION"]},
        )
    )
    openfda_id = repo.upsert_research_object(
        ResearchObject(
            object_type="safety_report",
            title="openFDA animal adverse event for Doxorubicin in Dog",
            source_key="openfda_animal_events",
            identifiers={"openfda_report_id": "US-FDA-CVM-2026-0001"},
            metadata={
                "matched_drug_name": "Doxorubicin",
                "species": "Dog",
                "reaction_terms": ["Vomiting", "Neutropenia"],
                "serious_ae": "true",
            },
        )
    )
    for object_id, label in (
        (pubchem_id, "compound_metadata"),
        (uniprot_id, "protein_target_metadata"),
        (rcsb_id, "structure_metadata"),
        (openfda_id, "safety_report_metadata"),
    ):
        for chunk in chunk_text(object_id, "structured source text", section_label=label):
            repo.upsert_document_chunk(chunk)

    result = extract_claims_for_repository(repo, limit=10)
    claims = repo.search_claims(ClaimSearchRequest(min_confidence=0.1, include_drafts=True, limit=20))
    statements = [claim.statement for claim in claims]

    assert result.claims_written == 4
    assert any("PubChem compound identity CID 4946" in statement for statement in statements)
    assert any("UniProtKB target metadata for Homo sapiens" in statement for statement in statements)
    assert any("RCSB PDB contains experimental structure 3VHE" in statement for statement in statements)
    assert any("openFDA animal adverse event signal reports in Dog" in statement for statement in statements)


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


def test_claim_curator_keeps_pmc_oa_source_context_review_only(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Licensed full text source context",
            source_key="pmc_oa",
        )
    )
    text = "Human angiosarcoma source context. " + ("Licensed full text background. " * 12)
    for index in range(6):
        repo.upsert_document_chunk(
            DocumentChunk(
                research_object_id=object_id,
                chunk_index=index,
                section_label="full_text",
                text_content=text,
                content_hash=f"pmc-oa-source-context-{index}",
            )
        )

    extract_claims_for_repository(repo, source_key="pmc_oa")
    result = ClaimCuratorAgent(repo).curate(ClaimCurationRequest(source_key="pmc_oa", limit=20, promote_threshold=0.5))
    review_decisions = [item for item in result.decisions if item.decision == "needs_review"]

    assert result.promoted == 0
    assert result.needs_review == 1
    assert result.merged_duplicates == 5
    assert review_decisions
    assert "source-context triage claim is review-only" in review_decisions[0].reasons
    assert "licensed full-text chunk has substantive snippet" in review_decisions[0].reasons


def test_claim_curator_downgrades_stale_source_context_promotions(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    object_id = repo.upsert_research_object(
        ResearchObject(
            object_type="publication",
            title="Stale source context",
            source_key="crossref",
        )
    )
    claim_id = uuid4()
    repo.upsert_claim(
        ClaimSearchResult(
            claim_id=claim_id,
            statement="Crossref record provides canine-human comparative angiosarcoma/HSA source context relevant to HSA evidence triage.",
            claim_type=ClaimType.OTHER,
            direction=ClaimDirection.NEUTRAL,
            confidence=0.7,
            evidence_level=EvidenceLevel.UNKNOWN,
            source_object_id=object_id,
            support_count=1,
            metadata={
                "curation_status": "promote",
                "curation_score": 0.7,
                "extraction_status": "curated",
                "rule_key": "source-context:canine_human_comparative",
                "context_key": "canine_human_comparative",
                "source_chunk_id": str(uuid4()),
            },
        )
    )

    result = ClaimCuratorAgent(repo).curate(ClaimCurationRequest(source_key="crossref", limit=20))
    updated = repo.get_claim(claim_id)

    assert result.needs_review == 1
    assert updated is not None
    assert updated.metadata["curation_status"] == "needs_review"
    assert updated.metadata["extraction_status"] == "draft"
    assert updated.confidence == 0.49


def test_source_scout_prioritizes_zero_coverage_bridges(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    pipeline = LocalIngestionPipeline(repo)
    pipeline.initialize()

    result = SourceScoutAgent(repo).scout(SourceScoutRequest(max_recommendations=5))
    keys = [recommendation.source_key for recommendation in result.recommendations]

    assert "pubmed" in keys
    assert "europe_pmc" in keys
    assert result.recommendations[0].status == "coverage_gap"
    assert result.next_actions


def test_scrape_profiles_keep_avma_approval_gated():
    profiles = {profile.source_key: profile for profile in list_scrape_profiles()}

    assert "avma_vctr" in profiles
    assert profiles["avma_vctr"].approval_required is True
    assert profiles["avma_vctr"].enabled is False
    assert profiles["avma_vctr"].robots_policy == "unknown"
    assert profiles["avma_vctr"].parser == "avma_vctr"


def test_scrape_bridge_refuses_approval_gated_fetch_without_approval(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    result = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts").fetch(
        ScrapeFetchRequest(
            source_key="avma_vctr",
            urls=["https://veterinaryclinicaltrials.org/"],
        )
    )

    assert result.fetched_pages == 0
    assert result.artifact_ids == []
    assert "requires explicit approval" in result.errors[0]


def test_disabled_scrape_source_requires_profile_review_before_approved_fetch(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    result = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts").fetch(
        ScrapeFetchRequest(
            source_key="avma_vctr",
            urls=["https://veterinaryclinicaltrials.org/"],
            approved_by="unit-test",
        )
    )

    assert result.fetched_pages == 0
    assert "requires source profile review" in result.errors[0]


def test_scrape_profile_review_is_persisted(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    review = bridge.review_profile(
        ScrapeProfileReviewRequest(
            source_key="avma_vctr",
            robots_policy="reviewed",
            approved_for_fetch=True,
            reviewed_by="unit-test",
            review_note="robots and storage policy reviewed",
        )
    )

    assert review.approved_for_fetch is True
    assert review.robots_policy == "reviewed"
    assert repo.get_scrape_profile_review("avma_vctr").reviewed_by == "unit-test"


def test_scrape_bridge_stores_snapshot_and_parses_generic_html(tmp_path, monkeypatch):
    html_path = tmp_path / "trial.html"
    html_path.write_text(
        """
        <html>
          <head><title>Canine Hemangiosarcoma Trial</title></head>
          <body><a href="/trial/1">Trial detail</a></body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="test_scraper",
        display_name="Test Scraper",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="generic_html",
        storage_policy="metadata_only",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    fetch = bridge.fetch(ScrapeFetchRequest(source_key="test_scraper", urls=[html_path.as_uri()]))
    artifact = repo.get_artifact(fetch.artifact_ids[0])
    parse = bridge.parse("test_scraper")

    assert fetch.fetched_pages == 1
    assert artifact is not None
    assert artifact.artifact_type == "scrape_snapshot"
    assert artifact.metadata["source_key"] == "test_scraper"
    assert artifact.metadata["requires_review"] is True
    assert parse.artifacts_seen == 1
    assert parse.parsed_records == 1
    assert len(parse.review_ids) == 1
    assert parse.records[0].title == "Canine Hemangiosarcoma Trial"
    assert parse.records[0].record_type == "veterinary_trial"
    assert parse.records[0].review_status == "needs_review"
    reviews = repo.list_scrape_reviews(source_key="test_scraper", review_status="needs_review")
    assert len(reviews) == 1
    assert reviews[0].title == "Canine Hemangiosarcoma Trial"


def test_scrape_bridge_ingest_requires_approval(tmp_path, monkeypatch):
    profile = ScrapeSourceProfile(
        source_key="test_scraper",
        display_name="Test Scraper",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    result = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts").ingest(
        ScrapeIngestRequest(source_key="test_scraper")
    )

    assert result.promoted_records == 0
    assert "requires explicit approval" in result.errors[0]


def test_scrape_bridge_promotes_snapshot_after_review_approval(tmp_path, monkeypatch):
    html_path = tmp_path / "trial.html"
    html_path.write_text(
        """
        <html>
          <head><title>Canine Hemangiosarcoma Trial</title></head>
          <body><a href="/trial/1">Trial detail</a></body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="test_scraper",
        display_name="Test Scraper",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="generic_html",
        storage_policy="metadata_only",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")
    fetch = bridge.fetch(ScrapeFetchRequest(source_key="test_scraper", urls=[html_path.as_uri()]))
    parse = bridge.parse("test_scraper")
    review = bridge.review(
        ScrapeReviewRequest(
            source_key="test_scraper",
            review_ids=parse.review_ids,
            decision="accepted",
            reviewed_by="unit-test",
            review_note="fields look valid",
        )
    )

    ingest = bridge.ingest(
        ScrapeIngestRequest(
            source_key="test_scraper",
            review_ids=[record.review_id for record in review.records],
            approved_by="unit-test",
            approval_note="reviewed parsed fields",
        )
    )
    objects = repo.list_research_objects(source_key="test_scraper")

    assert fetch.fetched_pages == 1
    assert review.reviewed_records == 1
    assert ingest.promoted_records == 1
    assert ingest.review_records_seen == 1
    assert ingest.raw_records == 1
    assert ingest.research_objects == 1
    assert ingest.document_chunks == 1
    assert objects[0].title == "Canine Hemangiosarcoma Trial"
    assert objects[0].object_type == "veterinary_trial"
    assert objects[0].metadata["review_status"] == "accepted"
    assert objects[0].metadata["approved_by"] == "unit-test"
    assert objects[0].metadata["review_id"] == str(review.records[0].review_id)


def test_scrape_review_queue_preserves_review_decision_on_reparse(tmp_path, monkeypatch):
    html_path = tmp_path / "trial.html"
    html_path.write_text("<html><head><title>Reviewed Trial</title></head><body></body></html>", encoding="utf-8")
    profile = ScrapeSourceProfile(
        source_key="test_scraper",
        display_name="Test Scraper",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        parser="generic_html",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")
    bridge.fetch(ScrapeFetchRequest(source_key="test_scraper", urls=[html_path.as_uri()]))
    first_parse = bridge.parse("test_scraper")
    bridge.review(
        ScrapeReviewRequest(
            source_key="test_scraper",
            review_ids=first_parse.review_ids,
            decision="rejected",
            reviewed_by="unit-test",
            review_note="not a target source",
        )
    )

    second_parse = bridge.parse("test_scraper")
    reviews = repo.list_scrape_reviews(source_key="test_scraper")

    assert second_parse.review_ids == first_parse.review_ids
    assert second_parse.records[0].review_status == "rejected"
    assert len(reviews) == 1
    assert reviews[0].review_status == "rejected"
    assert reviews[0].reviewer == "unit-test"


def test_avma_vctr_parser_extracts_trial_fields(tmp_path, monkeypatch):
    html_path = tmp_path / "avma.html"
    html_path.write_text(
        """
        <html>
          <head>
            <meta property="og:title" content="Evaluation of a combination of three drugs in dogs with hemangiosarcoma">
            <meta name="description" content="Combination therapy for dogs with Hemangiosarcoma.">
          </head>
          <body>
            <h1>Evaluation of a combination of three drugs in dogs with hemangiosarcoma</h1>
            <p>The objective of this study is to investigate doxorubicin or carboplatin and temozolomide with propranolol in dogs with hemangiosarcoma.</p>
            <dl>
              <dt>Condition</dt><dd>Hemangiosarcoma</dd>
              <dt>Species</dt><dd>Canine</dd>
              <dt>Study Type</dt><dd>Drug</dd>
              <dt>Funding</dt><dd>Unfunded</dd>
              <dt>Status</dt><dd>Recruiting</dd>
              <dt>Investigator</dt><dd>Claire Lemons, DVM</dd>
            </dl>
            <a href="/s/combination-therapy-hsa-123456/">Learn More</a>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="avma_vctr_test",
        display_name="AVMA Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="avma_vctr",
        storage_policy="link_and_registry_metadata",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    bridge.fetch(ScrapeFetchRequest(source_key="avma_vctr_test", urls=[html_path.as_uri()]))
    parse = bridge.parse("avma_vctr_test")
    record = parse.records[0]

    assert parse.parsed_records == 1
    assert record.title == "Evaluation of a combination of three drugs in dogs with hemangiosarcoma"
    assert record.source_record_id.endswith("/avma.html")
    assert record.record_type == "veterinary_trial"
    assert record.fields["condition"] == "Hemangiosarcoma"
    assert record.fields["species"] == "Canine"
    assert record.fields["study_type"] == "Drug"
    assert record.fields["funding"] == "Unfunded"
    assert record.fields["status"] == "Recruiting"
    assert record.fields["investigator"] == "Claire Lemons, DVM"
    assert record.parser_confidence >= 0.3


def test_avma_vctr_parser_extracts_embedded_study_json(tmp_path, monkeypatch):
    html_path = tmp_path / "embedded.html"
    html_path.write_text(
        """
        <html>
          <head><meta property="og:title" content="Antibody therapy for dogs with splenic hemangiosarcoma"></head>
          <body>
            <script id="d_study_keywords" type="application/json">["hemangiosarcoma", "VEGF"]</script>
            <script id="d_avma_study_data" type="application/json">
              {"vct_code": "VCT16000189", "patients_randomly_assigned": true}
            </script>
            <script id="d_avma_studycontent_data" type="application/json">
              {
                "diagnosis": "Hemangiosarcoma",
                "inclusion_criteria": "<p>Splenic hemangiosarcoma after splenectomy.</p>",
                "exclusion_criteria": "Metastatic disease at screening.",
                "intervention_name": "Anti-VEGF antibody",
                "potential_benefits": "Increased time to progression",
                "potential_risks": "Elevated blood pressure",
                "pri_outcome_name": "Safety",
                "pri_outcome_measure": "Blood pressure measurement",
                "pri_outcome_endpoint": "Safety",
                "sec_outcome1_name": "Overall survival",
                "sec_outcome1_measure": "Survival tracking",
                "sec_outcome1_endpoint": "Death or euthanasia",
                "funding_source_institution": true
              }
            </script>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="avma_vctr_test",
        display_name="AVMA Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        parser="avma_vctr",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    bridge.fetch(ScrapeFetchRequest(source_key="avma_vctr_test", urls=[html_path.as_uri()]))
    record = bridge.parse("avma_vctr_test").records[0]

    assert record.fields["vct_code"] == "VCT16000189"
    assert record.fields["condition"] == "Hemangiosarcoma"
    assert record.fields["keywords"] == ["hemangiosarcoma", "VEGF"]
    assert record.fields["intervention"] == "Anti-VEGF antibody"
    assert record.fields["eligibility"] == "Splenic hemangiosarcoma after splenectomy."
    assert record.fields["primary_outcome"]["measure"] == "Blood pressure measurement"
    assert record.fields["secondary_outcomes"][0]["name"] == "Overall survival"
    assert record.fields["funding_sources"] == ["institution"]
    assert record.parser_confidence >= 0.65


def test_avma_vctr_parser_keeps_sparse_pages_low_confidence(tmp_path, monkeypatch):
    html_path = tmp_path / "sparse.html"
    html_path.write_text("<html><head><title>Unknown Veterinary Page</title></head><body></body></html>", encoding="utf-8")
    profile = ScrapeSourceProfile(
        source_key="avma_vctr_test",
        display_name="AVMA Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        parser="avma_vctr",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")

    bridge.fetch(ScrapeFetchRequest(source_key="avma_vctr_test", urls=[html_path.as_uri()]))
    parse = bridge.parse("avma_vctr_test")
    record = parse.records[0]

    assert record.title == "Unknown Veterinary Page"
    assert "condition" not in record.fields
    assert "species" not in record.fields
    assert record.parser_confidence < 0.3


def test_scrape_manifest_discovers_avma_candidate_urls_from_stored_seed_page(tmp_path, monkeypatch):
    seed_path = tmp_path / "seed.html"
    detail_dir = tmp_path / "s"
    detail_dir.mkdir()
    detail_path = detail_dir / "combination-therapy-hsa-123456.html"
    detail_path.write_text(
        "<html><head><title>Combination therapy in canine hemangiosarcoma</title></head><body></body></html>",
        encoding="utf-8",
    )
    seed_path.write_text(
        f"""
        <html>
          <body>
            <a href="{detail_path.as_uri()}">Hemangiosarcoma clinical trial</a>
            <a href="{(tmp_path / "about.html").as_uri()}">About</a>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    profile = ScrapeSourceProfile(
        source_key="avma_vctr_test",
        display_name="AVMA Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="avma_vctr",
        storage_policy="link_and_registry_metadata",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")
    bridge.fetch(ScrapeFetchRequest(source_key="avma_vctr_test", urls=[seed_path.as_uri()]))

    manifest = bridge.build_manifest(ScrapeManifestRequest(source_key="avma_vctr_test"))
    manifest_artifact = repo.get_artifact(manifest.manifest_artifact_id)

    assert manifest.seed_artifacts_seen == 1
    assert len(manifest.candidate_urls) == 1
    assert manifest.candidate_urls[0].url == detail_path.as_uri()
    assert manifest.candidate_urls[0].confidence == 0.8
    assert manifest_artifact.artifact_type == "scrape_manifest"
    assert manifest_artifact.metadata["candidate_count"] == 1


def test_fetch_scrape_manifest_fetches_manifest_candidate_pages(tmp_path, monkeypatch):
    seed_path = tmp_path / "seed.html"
    detail_dir = tmp_path / "s"
    detail_dir.mkdir()
    detail_path = detail_dir / "solid-tumor-study.html"
    detail_path.write_text(
        "<html><head><title>Solid tumor study</title></head><body>Canine solid tumor trial.</body></html>",
        encoding="utf-8",
    )
    seed_path.write_text(f'<html><body><a href="{detail_path.as_uri()}">Solid tumor study</a></body></html>', encoding="utf-8")
    profile = ScrapeSourceProfile(
        source_key="avma_vctr_test",
        display_name="AVMA Test",
        base_url=tmp_path.as_uri(),
        allowed_url_patterns=[f"{tmp_path.as_uri()}/*"],
        robots_policy="reviewed",
        rate_limit_per_minute=120,
        parser="avma_vctr",
        storage_policy="link_and_registry_metadata",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")
    bridge = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts")
    bridge.fetch(ScrapeFetchRequest(source_key="avma_vctr_test", urls=[seed_path.as_uri()]))
    manifest = bridge.build_manifest(ScrapeManifestRequest(source_key="avma_vctr_test"))

    fetch = bridge.fetch_manifest(
        ScrapeManifestFetchRequest(
            source_key="avma_vctr_test",
            manifest_artifact_id=manifest.manifest_artifact_id,
            max_pages=1,
        )
    )

    assert fetch.fetched_pages == 1
    assert len(fetch.artifact_ids) == 1
    assert repo.get_artifact(fetch.artifact_ids[0]).metadata["source_url"] == detail_path.as_uri()


def test_scrape_bridge_skips_urls_outside_profile_allowlist(tmp_path, monkeypatch):
    profile = ScrapeSourceProfile(
        source_key="test_scraper",
        display_name="Test Scraper",
        base_url="file:///allowed",
        allowed_url_patterns=["file:///allowed/*"],
        robots_policy="reviewed",
        approval_required=False,
        enabled=True,
    )
    monkeypatch.setattr(scraper_bridge, "SCRAPE_SOURCE_PROFILES", (profile,))
    repo = SQLiteResearchRepository(tmp_path / "hsa.sqlite3")

    result = ScrapeBridge(repo, artifact_root=tmp_path / "artifacts").fetch(
        ScrapeFetchRequest(source_key="test_scraper", urls=[(tmp_path / "outside.html").as_uri()])
    )

    assert result.fetched_pages == 0
    assert result.skipped_pages == 1
    assert "outside allowed patterns" in result.errors[0]
