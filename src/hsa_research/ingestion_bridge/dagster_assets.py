"""Dagster asset scaffold for Ingestion Bridge v2.

These assets define the durable graph before each harvester is implemented.
They are intentionally lightweight and deterministic.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
import os
from typing import Any

from .contracts import ClaimCurationRequest, ClaimSearchRequest, ResearchObject, SourceQuery, SourceScoutRequest
from .query_policy import (
    build_canine_data_source_queries,
    build_chemistry_source_queries,
    build_clinical_trial_source_queries,
    build_omics_source_queries,
    build_safety_source_queries,
    build_scholarly_source_queries,
    build_target_structure_source_queries,
)
from .source_registry import get_initial_sources
from .source_sets import (
    ALL_API_SOURCE_KEYS,
    CANINE_DATA_OMICS_SOURCE_KEYS,
    LITERATURE_CLINICAL_SOURCE_KEYS,
    LITERATURE_CORPUS_SOURCE_KEYS,
    LITERATURE_CORPUS_SOURCE_LIMITS,
    LITERATURE_FULL_TEXT_SOURCE_KEYS,
    LITERATURE_FULL_TEXT_SOURCE_LIMITS,
    STRUCTURED_SOURCE_KEYS,
)

try:
    import dagster as dg
except ImportError:  # pragma: no cover - Dagster is optional until the orchestration env is installed
    dg = None  # type: ignore[assignment]


STRUCTURED_SOURCE_SMOKE_KEYS = ("pubchem",)
STRUCTURED_SOURCE_MULTISOURCE_SMOKE_KEYS = STRUCTURED_SOURCE_KEYS
CANINE_DATA_OMICS_SMOKE_KEYS = CANINE_DATA_OMICS_SOURCE_KEYS
LITERATURE_CLINICAL_SMOKE_KEYS = LITERATURE_CLINICAL_SOURCE_KEYS
ALL_API_SMOKE_KEYS = ALL_API_SOURCE_KEYS
HOSTED_API_REPORT_KEYS = ALL_API_SMOKE_KEYS
LITERATURE_FULL_TEXT_SMOKE_LIMITS = {
    source_key: 1 for source_key in LITERATURE_FULL_TEXT_SOURCE_KEYS
}
SCHEDULE_TIMEZONE = "America/Denver"

_STRUCTURED_SOURCE_COUNT_TABLE_COLUMNS = (
    "source_key",
    "raw_records",
    "research_objects",
    "document_chunks",
    "entity_mentions",
    "claims",
    "passes_minimum_bar",
    "claim_status",
    "claim_types",
    "full_text_triage_action",
    "full_text_triage_severity",
    "full_text_triage_should_retry",
    "full_text_triage_should_block_schedule",
    "full_text_qa",
)
_SOURCE_HEALTH_TABLE_COLUMNS = (
    "source_key",
    "source_role",
    "health_status",
    "health_score",
    "raw_records",
    "research_objects",
    "document_chunks",
    "entity_mentions",
    "claims",
    "passes_minimum_bar",
    "signals",
    "risks",
    "recommended_actions",
    "claim_metadata",
    "full_text_triage_action",
    "full_text_triage_severity",
    "full_text_triage_should_retry",
    "full_text_triage_should_block_schedule",
    "full_text_qa",
)
_FULL_TEXT_TRIAGE_TABLE_COLUMNS = (
    "source_key",
    "mode",
    "passes_full_text_bar",
    "triage_action",
    "triage_severity",
    "should_retry",
    "should_block_schedule",
    "reasons",
    "recommended_next_actions",
)
_ENTITY_RESOLUTION_TABLE_COLUMNS = (
    "source_key",
    "chunks_seen",
    "chunks_with_mentions",
    "entities_upserted",
    "aliases_upserted",
    "mentions_upserted",
    "entity_mentions",
    "claims",
    "passes_minimum_bar",
    "errors",
)


def _metadata_table_scalar(value: Any) -> str | int | float | bool | None:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return json.dumps(value, sort_keys=True, default=str)


def _compact_table_rows(
    rows: Sequence[Mapping[str, Any]],
    columns: Sequence[str],
) -> list[dict[str, str | int | float | bool | None]]:
    return [
        {column: _metadata_table_scalar(row.get(column)) for column in columns}
        for row in rows
    ]


def build_source_queries() -> list[SourceQuery]:
    """Create the starter query set for implemented ingestion sources."""

    return [
        *build_scholarly_source_queries(),
        *build_clinical_trial_source_queries(),
        *build_canine_data_source_queries(),
        *build_omics_source_queries(),
        *build_chemistry_source_queries(),
        *build_target_structure_source_queries(),
        *build_safety_source_queries(),
    ]


if dg is not None:
    from .dagster_resources import ResearchRepositoryResource

    def _metadata_table(
        rows: Sequence[Mapping[str, Any]],
        columns: Sequence[str],
    ) -> dg.TableMetadataValue:
        compact_rows = _compact_table_rows(rows, columns)
        records = [dg.TableRecord(data=row) for row in compact_rows]
        if records:
            return dg.MetadataValue.table(records=records)
        return dg.MetadataValue.table(
            records=[],
            schema=dg.TableSchema(columns=[dg.TableColumn(name=column) for column in columns]),
        )

    def _base_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "source_count": len(report.get("sources", [])),
            "source_keys": dg.MetadataValue.json(report.get("source_keys", [])),
            "totals": dg.MetadataValue.json(report.get("totals", {})),
            "coverage": dg.MetadataValue.json(report.get("coverage", {})),
        }

    def _structured_source_count_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            **_base_report_metadata(report),
            "failed_source_count": len(report.get("failed_sources", [])),
            "failed_sources": dg.MetadataValue.json(report.get("failed_sources", [])),
            "minimum_bar": dg.MetadataValue.json(report.get("minimum_bar", {})),
            "passes_minimum_bar": bool(report.get("passes_minimum_bar", False)),
            "source_count_table": _metadata_table(
                report.get("sources", []),
                _STRUCTURED_SOURCE_COUNT_TABLE_COLUMNS,
            ),
        }

    def _source_health_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        return {
            **_base_report_metadata(report),
            "failed_source_count": len(report.get("failed_sources", [])),
            "failed_sources": dg.MetadataValue.json(report.get("failed_sources", [])),
            "minimum_bar": dg.MetadataValue.json(report.get("minimum_bar", {})),
            "passes_minimum_bar": bool(report.get("passes_minimum_bar", False)),
            "summary": dg.MetadataValue.json(report.get("summary", {})),
            "triage_source_count": len(report.get("triage_sources", [])),
            "triage_sources": dg.MetadataValue.json(report.get("triage_sources", [])),
            "watch_source_count": len(report.get("watch_sources", [])),
            "watch_sources": dg.MetadataValue.json(report.get("watch_sources", [])),
            "full_text_blocking_sources": dg.MetadataValue.json(report.get("full_text_blocking_sources", [])),
            "full_text_triage": dg.MetadataValue.json(report.get("full_text_triage", [])),
            "source_health_table": _metadata_table(
                report.get("sources", []),
                _SOURCE_HEALTH_TABLE_COLUMNS,
            ),
        }

    def _entity_resolution_source_rows(report: Mapping[str, Any]) -> list[dict[str, Any]]:
        rows = []
        for source_report in report.get("sources", []):
            resolution = source_report.get("resolution", {})
            qa = source_report.get("qa", {})
            rows.append(
                {
                    "source_key": source_report.get("source_key"),
                    "chunks_seen": resolution.get("chunks_seen", 0),
                    "chunks_with_mentions": resolution.get("chunks_with_mentions", 0),
                    "entities_upserted": resolution.get("entities_upserted", 0),
                    "aliases_upserted": resolution.get("aliases_upserted", 0),
                    "mentions_upserted": resolution.get("mentions_upserted", 0),
                    "entity_mentions": qa.get("entity_mentions", 0),
                    "claims": qa.get("claims", 0),
                    "passes_minimum_bar": qa.get("passes_minimum_bar"),
                    "errors": resolution.get("errors", []),
                }
            )
        return rows

    def _entity_resolution_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        errors = report.get("errors", [])
        totals = report.get("totals", {})
        return {
            **_base_report_metadata(report),
            "error_count": len(errors),
            "errors": dg.MetadataValue.json(errors),
            "minimum_entity_mentions": 1,
            "passes_minimum_bar": not errors and totals.get("entity_mentions", 0) >= 1,
            "entity_resolution_table": _metadata_table(
                _entity_resolution_source_rows(report),
                _ENTITY_RESOLUTION_TABLE_COLUMNS,
            ),
        }

    def _embedding_index_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        embedding_coverage = report.get("embedding_coverage", {})
        totals = report.get("totals", {})
        errors = report.get("errors", [])
        return {
            "embedding_model": report.get("embedding_model"),
            "source_key": report.get("source_key"),
            "chunks_seen": totals.get("chunks_seen", 0),
            "embeddings_created": totals.get("embeddings_created", 0),
            "embeddings_updated": totals.get("embeddings_updated", 0),
            "embeddings_skipped": totals.get("embeddings_skipped", 0),
            "error_count": len(errors),
            "errors": dg.MetadataValue.json(errors),
            "total_chunks": embedding_coverage.get("total_chunks", 0),
            "embedded_chunks": embedding_coverage.get("embedded_chunks", 0),
            "missing_chunks": embedding_coverage.get("missing_chunks", 0),
            "coverage_ratio": embedding_coverage.get("coverage_ratio", 0.0),
            "embedding_models": dg.MetadataValue.json(embedding_coverage.get("embedding_models", {})),
            "coverage": dg.MetadataValue.json(report.get("coverage", {})),
            "passes_minimum_bar": bool(report.get("passes_minimum_bar", False)),
        }

    def _embedding_maintenance_report_metadata(report: Mapping[str, Any]) -> dict[str, Any]:
        embedding_coverage = report.get("embedding_coverage", {})
        orphan_embeddings = report.get("orphan_embeddings", {})
        errors = report.get("errors", [])
        return {
            "embedding_model": report.get("embedding_model"),
            "prune_embedding_model": orphan_embeddings.get("embedding_model", "all"),
            "orphan_embeddings_seen": orphan_embeddings.get("seen", 0),
            "orphan_embeddings_deleted": orphan_embeddings.get("deleted", 0),
            "prune_enabled": bool(orphan_embeddings.get("prune_enabled", False)),
            "error_count": len(errors),
            "errors": dg.MetadataValue.json(errors),
            "total_chunks": embedding_coverage.get("total_chunks", 0),
            "embedded_chunks": embedding_coverage.get("embedded_chunks", 0),
            "missing_chunks": embedding_coverage.get("missing_chunks", 0),
            "coverage_ratio": embedding_coverage.get("coverage_ratio", 0.0),
            "embedding_models": dg.MetadataValue.json(embedding_coverage.get("embedding_models", {})),
            "coverage": dg.MetadataValue.json(report.get("coverage", {})),
            "passes_minimum_bar": bool(report.get("passes_minimum_bar", False)),
        }

    def _run_literature_full_text_refresh(
        research_repository: ResearchRepositoryResource,
        *,
        source_keys: Sequence[str],
        source_limits: Mapping[str, int],
        extract_limit: int,
        curate_limit: int,
    ) -> dict:
        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        report = run_structured_sources_pipeline(
            repository,
            source_keys=source_keys,
            source_limits=source_limits,
            extract_limit=extract_limit,
            curate_limit=curate_limit,
        )
        return _annotate_full_text_report(report, mode="refresh")

    def _run_literature_full_text_ingestion(
        research_repository: ResearchRepositoryResource,
        *,
        source_keys: Sequence[str],
        source_limits: Mapping[str, int],
    ) -> dict:
        from .structured_orchestration import run_structured_sources_ingestion_pipeline

        repository = research_repository.build_repository()
        report = run_structured_sources_ingestion_pipeline(
            repository,
            source_keys=source_keys,
            source_limits=source_limits,
        )
        return _annotate_full_text_report(report, mode="ingestion_only")

    def _annotate_full_text_report(report: dict, *, mode: str) -> dict:
        report["mode"] = mode
        report["full_text_runtime_config"] = _full_text_runtime_config()
        report["full_text_triage"] = _full_text_triage_rows(report, mode=mode)
        report["full_text_blocking_sources"] = [
            row["source_key"]
            for row in report["full_text_triage"]
            if row.get("should_block_schedule")
        ]
        return report

    def _full_text_runtime_config() -> dict[str, str | None]:
        env_names = (
            "HSA_FULL_TEXT_REQUEST_TIMEOUT_SECONDS",
            "HSA_FULL_TEXT_REQUEST_ATTEMPTS",
            "HSA_FULL_TEXT_FETCH_TIME_BUDGET_SECONDS",
            "HSA_PMC_OA_MAX_CANDIDATE_RECORDS",
        )
        return {name: os.getenv(name) for name in env_names}

    def _full_text_triage_rows(report: Mapping[str, Any], *, mode: str) -> list[dict[str, Any]]:
        rows = []
        for source_report in report.get("sources", []):
            full_text_qa = source_report.get("full_text_qa") or {}
            triage = full_text_qa.get("triage") or {}
            if not triage:
                continue
            rows.append(
                {
                    "source_key": source_report.get("source_key"),
                    "mode": mode,
                    "passes_full_text_bar": full_text_qa.get("passes_full_text_bar"),
                    "triage_action": triage.get("action"),
                    "triage_severity": triage.get("severity"),
                    "should_retry": triage.get("should_retry"),
                    "should_block_schedule": triage.get("should_block_schedule"),
                    "reasons": triage.get("reasons", []),
                    "recommended_next_actions": triage.get("recommended_next_actions", []),
                }
            )
        return rows

    def _full_text_check_result(
        report: Mapping[str, Any],
        *,
        source_limits: Mapping[str, int],
    ) -> dg.AssetCheckResult:
        source_reports = report.get("sources", [])
        failed_sources = [
            source_report["source_key"]
            for source_report in source_reports
            if (
                not _has_minimum_ingested_source_outputs(source_report)
                or not _has_required_full_text_outputs(source_report)
            )
        ]
        errors = report.get("errors", [])
        return dg.AssetCheckResult(
            passed=not failed_sources and not errors,
            metadata={
                "failed_sources": failed_sources,
                "errors": errors,
                "source_keys": report.get("source_keys", []),
                "source_limits": dict(source_limits),
                "full_text_blocking_sources": report.get("full_text_blocking_sources", []),
                "full_text_triage": _metadata_table(
                    report.get("full_text_triage", []),
                    _FULL_TEXT_TRIAGE_TABLE_COLUMNS,
                ),
                "totals": report.get("totals", {}),
            },
        )

    def _full_text_ingestion_check_result(
        report: Mapping[str, Any],
        *,
        source_limits: Mapping[str, int],
    ) -> dg.AssetCheckResult:
        source_reports = report.get("sources", [])
        failed_sources = [
            source_report["source_key"]
            for source_report in source_reports
            if (
                not _has_minimum_source_ingestion_outputs(source_report)
                or not _has_required_full_text_outputs(source_report)
            )
        ]
        errors = report.get("errors", [])
        return dg.AssetCheckResult(
            passed=not failed_sources and not errors,
            metadata={
                "mode": report.get("mode"),
                "failed_sources": failed_sources,
                "errors": errors,
                "source_keys": report.get("source_keys", []),
                "source_limits": dict(source_limits),
                "full_text_runtime_config": report.get("full_text_runtime_config", {}),
                "full_text_blocking_sources": report.get("full_text_blocking_sources", []),
                "full_text_triage": _metadata_table(
                    report.get("full_text_triage", []),
                    _FULL_TEXT_TRIAGE_TABLE_COLUMNS,
                ),
                "totals": report.get("totals", {}),
            },
        )

    @dg.asset(group_name="ingestion_bridge_v2")
    def source_registry() -> list[dict]:
        """Canonical source registry for ingestion."""

        return [source.model_dump(mode="json") for source in get_initial_sources()]

    @dg.asset(group_name="ingestion_bridge_v2")
    def source_queries(source_registry: list[dict]) -> list[dict]:
        """Starter source-specific queries."""

        enabled_sources = {source["source_key"] for source in source_registry if source["enabled"]}
        queries = [query for query in build_source_queries() if query.source_key in enabled_sources]
        return [query.model_dump(mode="json") for query in queries]

    @dg.asset(group_name="ingestion_bridge_v2")
    def source_scout_plan(source_registry: list[dict]) -> dict:
        """Placeholder for the Source Scout agent's ingestion gap plan."""

        request = SourceScoutRequest(focus="all", max_phase=3, max_recommendations=12)
        return {
            "status": "pending_source_scout_agent",
            "registered_sources": len(source_registry),
            "default_scout_contract": request.model_dump(mode="json"),
        }

    @dg.asset(group_name="ingestion_bridge_v2")
    def raw_source_records(source_queries: list[dict], source_scout_plan: dict) -> list[dict]:
        """Placeholder for raw records harvested by source-specific workers."""

        return [
            {
                "source_key": query["source_key"],
                "query_name": query["query_name"],
                "status": "pending_harvester",
                "scout_status": source_scout_plan["status"],
            }
            for query in source_queries
        ]

    @dg.asset(group_name="ingestion_bridge_v2")
    def canonical_research_objects(raw_source_records: list[dict]) -> list[dict]:
        """Placeholder for resolver-normalized research objects."""

        objects = [
            ResearchObject(
                object_type="publication",
                title=f"Pending resolver output for {record['source_key']}:{record['query_name']}",
                source_key=record["source_key"],
                metadata={"status": record["status"]},
            )
            for record in raw_source_records
        ]
        return [obj.model_dump(mode="json") for obj in objects]

    @dg.asset(group_name="ingestion_bridge_v2")
    def document_chunks(canonical_research_objects: list[dict]) -> list[dict]:
        """Placeholder for legal text and abstract chunks."""

        return [
            {
                "research_object_id": obj["id"],
                "chunk_index": 0,
                "status": "pending_chunker",
                "title": obj["title"],
            }
            for obj in canonical_research_objects
        ]

    @dg.asset(group_name="ingestion_bridge_v2")
    def normalized_entities(document_chunks: list[dict]) -> list[dict]:
        """Placeholder for entity mentions and normalized entities."""

        return [
            {
                "research_object_id": chunk["research_object_id"],
                "status": "pending_entity_mapper",
            }
            for chunk in document_chunks
        ]

    @dg.asset(group_name="ingestion_bridge_v2")
    def claims(normalized_entities: list[dict]) -> dict:
        """Placeholder for claim extraction output."""

        request = ClaimSearchRequest(query="canine hemangiosarcoma", species="canine", limit=20)
        return {
            "status": "pending_claim_extractor",
            "entity_batches": len(normalized_entities),
            "default_search_contract": request.model_dump(mode="json"),
        }

    @dg.asset(group_name="ingestion_bridge_v2")
    def curated_claims(claims: dict) -> dict:
        """Placeholder for the claim curator agent output."""

        request = ClaimCurationRequest(limit=250, model_profile="reviewer")
        return {
            "status": "pending_claim_curator_agent",
            "upstream_status": claims["status"],
            "default_curation_contract": request.model_dump(mode="json"),
        }

    @dg.asset(group_name="ingestion_bridge_v2")
    def coverage_snapshot(source_registry: list[dict], curated_claims: dict) -> dict:
        """Coverage report placeholder."""

        return {
            "source_count": len(source_registry),
            "claim_status": curated_claims["status"],
            "phase": 0,
        }

    @dg.asset(group_name="structured_source_refresh")
    def structured_source_pipeline_report(research_repository: ResearchRepositoryResource) -> dict:
        """Executable structured-source refresh, extraction, curation, and QA report."""

        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_pipeline(repository)

    @dg.asset(group_name="structured_source_refresh")
    def structured_source_smoke_report(research_repository: ResearchRepositoryResource) -> dict:
        """Small hosted-runtime validation run against a single structured source."""

        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=STRUCTURED_SOURCE_SMOKE_KEYS,
            source_limits={"pubchem": 1},
            extract_limit=50,
            curate_limit=50,
        )

    @dg.asset(group_name="structured_source_refresh")
    def structured_source_multisource_smoke_report(research_repository: ResearchRepositoryResource) -> dict:
        """Small hosted-runtime validation run across all structured API harvesters."""

        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=STRUCTURED_SOURCE_MULTISOURCE_SMOKE_KEYS,
            source_limits={source_key: 1 for source_key in STRUCTURED_SOURCE_MULTISOURCE_SMOKE_KEYS},
            extract_limit=250,
            curate_limit=250,
        )

    @dg.asset(group_name="literature_clinical_refresh")
    def literature_clinical_smoke_report(research_repository: ResearchRepositoryResource) -> dict:
        """Small hosted-runtime validation run across literature and clinical APIs."""

        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=LITERATURE_CLINICAL_SMOKE_KEYS,
            source_limits={source_key: 1 for source_key in LITERATURE_CLINICAL_SMOKE_KEYS},
            extract_limit=250,
            curate_limit=250,
        )

    @dg.asset(group_name="hosted_api_refresh")
    def all_api_smoke_report(research_repository: ResearchRepositoryResource) -> dict:
        """Small hosted-runtime validation run across every implemented API harvester."""

        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=ALL_API_SMOKE_KEYS,
            source_limits={source_key: 1 for source_key in ALL_API_SMOKE_KEYS},
            extract_limit=500,
            curate_limit=500,
        )

    @dg.asset(group_name="literature_corpus_harvest")
    def literature_corpus_harvest_report(research_repository: ResearchRepositoryResource) -> dict:
        """Hundreds-scale hosted literature ingestion across metadata and abstract sources."""

        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=LITERATURE_CORPUS_SOURCE_KEYS,
            source_limits=LITERATURE_CORPUS_SOURCE_LIMITS,
            extract_limit=5000,
            curate_limit=5000,
        )

    @dg.asset(group_name="literature_full_text_refresh")
    def literature_full_text_refresh_report(research_repository: ResearchRepositoryResource) -> dict:
        """Bounded hosted full-text ingestion for licensed open-access sources."""

        return _run_literature_full_text_refresh(
            research_repository,
            source_keys=LITERATURE_FULL_TEXT_SOURCE_KEYS,
            source_limits=LITERATURE_FULL_TEXT_SOURCE_LIMITS,
            extract_limit=1000,
            curate_limit=1000,
        )

    @dg.asset(group_name="literature_full_text_refresh")
    def europe_pmc_full_text_refresh_report(research_repository: ResearchRepositoryResource) -> dict:
        """Single-source hosted full-text refresh for Europe PMC."""

        return _run_literature_full_text_refresh(
            research_repository,
            source_keys=("europe_pmc",),
            source_limits={"europe_pmc": LITERATURE_FULL_TEXT_SOURCE_LIMITS["europe_pmc"]},
            extract_limit=500,
            curate_limit=500,
        )

    @dg.asset(group_name="literature_full_text_refresh")
    def pmc_oa_full_text_refresh_report(research_repository: ResearchRepositoryResource) -> dict:
        """Single-source hosted full-text refresh for PMC OA."""

        return _run_literature_full_text_refresh(
            research_repository,
            source_keys=("pmc_oa",),
            source_limits={"pmc_oa": LITERATURE_FULL_TEXT_SOURCE_LIMITS["pmc_oa"]},
            extract_limit=500,
            curate_limit=500,
        )

    @dg.asset(group_name="literature_full_text_ingestion")
    def literature_full_text_ingest_smoke_report(research_repository: ResearchRepositoryResource) -> dict:
        """Fast hosted full-text pull validation without extraction or curation."""

        return _run_literature_full_text_ingestion(
            research_repository,
            source_keys=LITERATURE_FULL_TEXT_SOURCE_KEYS,
            source_limits=LITERATURE_FULL_TEXT_SMOKE_LIMITS,
        )

    @dg.asset(group_name="literature_full_text_ingestion")
    def europe_pmc_full_text_ingest_report(research_repository: ResearchRepositoryResource) -> dict:
        """Europe PMC full-text pull and persistence path without extraction."""

        return _run_literature_full_text_ingestion(
            research_repository,
            source_keys=("europe_pmc",),
            source_limits={"europe_pmc": LITERATURE_FULL_TEXT_SOURCE_LIMITS["europe_pmc"]},
        )

    @dg.asset(group_name="literature_full_text_ingestion")
    def pmc_oa_full_text_ingest_report(research_repository: ResearchRepositoryResource) -> dict:
        """PMC OA full-text pull and persistence path without extraction."""

        return _run_literature_full_text_ingestion(
            research_repository,
            source_keys=("pmc_oa",),
            source_limits={"pmc_oa": LITERATURE_FULL_TEXT_SOURCE_LIMITS["pmc_oa"]},
        )

    @dg.asset(group_name="literature_full_text_refresh")
    def literature_full_text_smoke_report(research_repository: ResearchRepositoryResource) -> dict:
        """Fast hosted full-text validation with one record per full-text source."""

        return _run_literature_full_text_refresh(
            research_repository,
            source_keys=LITERATURE_FULL_TEXT_SOURCE_KEYS,
            source_limits=LITERATURE_FULL_TEXT_SMOKE_LIMITS,
            extract_limit=25,
            curate_limit=25,
        )

    @dg.asset(group_name="structured_source_refresh")
    def structured_source_count_report(research_repository: ResearchRepositoryResource) -> dg.MaterializeResult:
        """Persisted count report for hosted API source coverage."""

        from .structured_orchestration import build_structured_source_count_report

        repository = research_repository.build_repository()
        report = build_structured_source_count_report(
            repository,
            source_keys=HOSTED_API_REPORT_KEYS,
            sample_limit=3,
            require_claims=True,
        )
        return dg.MaterializeResult(value=report, metadata=_structured_source_count_report_metadata(report))

    @dg.asset(group_name="hosted_api_refresh")
    def source_health_report(research_repository: ResearchRepositoryResource) -> dg.MaterializeResult:
        """Persisted source health report for hosted API source coverage."""

        from .source_health import build_source_health_report

        repository = research_repository.build_repository()
        report = build_source_health_report(
            repository,
            source_keys=HOSTED_API_REPORT_KEYS,
            sample_limit=3,
            require_claims=True,
        )
        return dg.MaterializeResult(value=report, metadata=_source_health_report_metadata(report))

    @dg.asset(group_name="entity_resolution")
    def entity_resolution_report(research_repository: ResearchRepositoryResource) -> dg.MaterializeResult:
        """Deterministic entity resolution over persisted hosted API chunks."""

        from .entity_resolution import resolve_entities_for_repository
        from .structured_orchestration import structured_source_qa

        repository = research_repository.build_repository()
        reports = []
        for source_key in HOSTED_API_REPORT_KEYS:
            resolution = resolve_entities_for_repository(
                repository,
                source_key=source_key,
                limit=1000,
            ).model_dump(mode="json")
            reports.append(
                {
                    "source_key": source_key,
                    "resolution": resolution,
                    "qa": structured_source_qa(repository, source_key, sample_limit=2),
                }
            )
        report = {
            "source_keys": list(HOSTED_API_REPORT_KEYS),
            "sources": reports,
            "totals": {
                "chunks_seen": sum(report["resolution"].get("chunks_seen", 0) for report in reports),
                "entities_upserted": sum(report["resolution"].get("entities_upserted", 0) for report in reports),
                "aliases_upserted": sum(report["resolution"].get("aliases_upserted", 0) for report in reports),
                "mentions_upserted": sum(report["resolution"].get("mentions_upserted", 0) for report in reports),
                "entity_mentions": sum(report["qa"].get("entity_mentions", 0) for report in reports),
            },
            "errors": [
                f"{report['source_key']}: {error}"
                for report in reports
                for error in report["resolution"].get("errors", [])
            ],
            "coverage": repository.coverage_summary(),
        }
        return dg.MaterializeResult(value=report, metadata=_entity_resolution_report_metadata(report))

    @dg.asset(group_name="embedding_index")
    def embedding_index_report(research_repository: ResearchRepositoryResource) -> dg.MaterializeResult:
        """Deterministic local embedding index over persisted document chunks."""

        from .embeddings import index_embeddings_for_repository

        repository = research_repository.build_repository()
        result = index_embeddings_for_repository(repository)
        embedding_coverage = repository.embedding_coverage(embedding_model=result.embedding_model)
        coverage = embedding_coverage.model_dump(mode="json")
        totals = {
            "chunks_seen": result.chunks_seen,
            "embeddings_created": result.embeddings_created,
            "embeddings_updated": result.embeddings_updated,
            "embeddings_skipped": result.embeddings_skipped,
            "total_chunks": coverage["total_chunks"],
            "embedded_chunks": coverage["embedded_chunks"],
            "missing_chunks": coverage["missing_chunks"],
        }
        errors = list(result.errors)
        passes_minimum_bar = not errors and (
            (coverage["total_chunks"] == 0 and result.chunks_seen == 0 and coverage["embedded_chunks"] == 0)
            or (coverage["total_chunks"] > 0 and result.chunks_seen >= 1 and coverage["embedded_chunks"] >= 1)
        )
        report = {
            "embedding_model": result.embedding_model,
            "source_key": result.source_key,
            "totals": totals,
            "errors": errors,
            "embedding_coverage": coverage,
            "coverage": repository.coverage_summary(),
            "passes_minimum_bar": passes_minimum_bar,
        }
        return dg.MaterializeResult(value=report, metadata=_embedding_index_report_metadata(report))

    @dg.asset(group_name="embedding_index")
    def embedding_maintenance_report(research_repository: ResearchRepositoryResource) -> dg.MaterializeResult:
        """Prune orphan embedding rows and verify active-model coverage."""

        from .embeddings import maintain_embedding_index

        repository = research_repository.build_repository()
        report = maintain_embedding_index(repository).to_report()
        return dg.MaterializeResult(value=report, metadata=_embedding_maintenance_report_metadata(report))

    @dg.asset_check(asset=source_registry)
    def source_registry_has_phase_one_sources(source_registry: list[dict]) -> dg.AssetCheckResult:
        """Ensure the first bridge has the minimum source backbone."""

        source_keys = {source["source_key"] for source in source_registry}
        required = {"pubmed", "europe_pmc", "openalex", "crossref", "pmc_oa"}
        missing = sorted(required - source_keys)
        return dg.AssetCheckResult(
            passed=not missing,
            metadata={"missing": missing, "required": sorted(required)},
        )

    @dg.asset_check(asset=structured_source_pipeline_report)
    def structured_source_pipeline_has_minimum_outputs(
        structured_source_pipeline_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure each structured source produced source objects and claims."""

        source_reports = structured_source_pipeline_report.get("sources", [])
        failed_sources = [
            report["source_key"]
            for report in source_reports
            if not report.get("qa", {}).get("passes_minimum_bar", False)
        ]
        errors = structured_source_pipeline_report.get("errors", [])
        return dg.AssetCheckResult(
            passed=not failed_sources and not errors,
            metadata={
                "failed_sources": failed_sources,
                "errors": errors,
                "totals": structured_source_pipeline_report.get("totals", {}),
            },
        )

    @dg.asset_check(asset=structured_source_smoke_report)
    def structured_source_smoke_has_minimum_outputs(
        structured_source_smoke_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the hosted-runtime smoke run writes at least one PubChem result."""

        totals = structured_source_smoke_report.get("totals", {})
        errors = structured_source_smoke_report.get("errors", [])
        passed = not errors and all(totals.get(field, 0) >= 1 for field in ("raw_records", "research_objects", "claims"))
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "errors": errors,
                "source_keys": structured_source_smoke_report.get("source_keys", []),
                "totals": totals,
            },
        )

    @dg.asset_check(asset=structured_source_multisource_smoke_report)
    def structured_source_multisource_smoke_has_minimum_outputs(
        structured_source_multisource_smoke_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure each structured API source can write records, chunks, and claims."""

        source_reports = structured_source_multisource_smoke_report.get("sources", [])
        failed_sources = [
            report["source_key"]
            for report in source_reports
            if not report.get("qa", {}).get("passes_minimum_bar", False)
        ]
        errors = structured_source_multisource_smoke_report.get("errors", [])
        passed = not failed_sources and not errors
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "failed_sources": failed_sources,
                "errors": errors,
                "source_keys": structured_source_multisource_smoke_report.get("source_keys", []),
                "totals": structured_source_multisource_smoke_report.get("totals", {}),
            },
        )

    @dg.asset_check(asset=literature_clinical_smoke_report)
    def literature_clinical_smoke_has_minimum_outputs(
        literature_clinical_smoke_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure literature and clinical API sources can write records and produce claims."""

        source_reports = literature_clinical_smoke_report.get("sources", [])
        failed_sources = [
            report["source_key"]
            for report in source_reports
            if not _has_minimum_ingested_source_outputs(report)
        ]
        errors = literature_clinical_smoke_report.get("errors", [])
        totals = literature_clinical_smoke_report.get("totals", {})
        passed = not failed_sources and not errors and totals.get("claims", 0) >= 1
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "failed_sources": failed_sources,
                "errors": errors,
                "minimum_total_claims": 1,
                "source_keys": literature_clinical_smoke_report.get("source_keys", []),
                "totals": totals,
            },
        )

    @dg.asset_check(asset=all_api_smoke_report)
    def all_api_smoke_has_minimum_outputs(
        all_api_smoke_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure every implemented API source can write records, chunks, and claims."""

        source_reports = all_api_smoke_report.get("sources", [])
        failed_sources = [
            report["source_key"]
            for report in source_reports
            if not _has_minimum_ingested_source_outputs(report)
        ]
        errors = all_api_smoke_report.get("errors", [])
        passed = not failed_sources and not errors
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "failed_sources": failed_sources,
                "errors": errors,
                "source_keys": all_api_smoke_report.get("source_keys", []),
                "totals": all_api_smoke_report.get("totals", {}),
            },
        )

    @dg.asset_check(asset=literature_corpus_harvest_report)
    def literature_corpus_harvest_has_hundreds_of_records(
        literature_corpus_harvest_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the corpus run produces a meaningful persisted literature set."""

        errors = literature_corpus_harvest_report.get("errors", [])
        totals = literature_corpus_harvest_report.get("totals", {})
        passed = (
            not errors
            and totals.get("raw_records", 0) >= 200
            and totals.get("research_objects", 0) >= 100
            and totals.get("document_chunks", 0) >= 100
            and totals.get("claims", 0) >= 50
        )
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "errors": errors,
                "minimum_totals": {
                    "raw_records": 200,
                    "research_objects": 100,
                    "document_chunks": 100,
                    "claims": 50,
                },
                "source_keys": literature_corpus_harvest_report.get("source_keys", []),
                "source_limits": LITERATURE_CORPUS_SOURCE_LIMITS,
                "totals": totals,
            },
        )

    @dg.asset_check(asset=literature_full_text_refresh_report)
    def literature_full_text_refresh_has_outputs(
        literature_full_text_refresh_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the bounded full-text lane writes persisted outputs."""

        return _full_text_check_result(
            literature_full_text_refresh_report,
            source_limits=LITERATURE_FULL_TEXT_SOURCE_LIMITS,
        )

    @dg.asset_check(asset=europe_pmc_full_text_refresh_report)
    def europe_pmc_full_text_refresh_has_outputs(
        europe_pmc_full_text_refresh_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the Europe PMC full-text lane writes persisted outputs."""

        return _full_text_check_result(
            europe_pmc_full_text_refresh_report,
            source_limits={"europe_pmc": LITERATURE_FULL_TEXT_SOURCE_LIMITS["europe_pmc"]},
        )

    @dg.asset_check(asset=pmc_oa_full_text_refresh_report)
    def pmc_oa_full_text_refresh_has_outputs(
        pmc_oa_full_text_refresh_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the PMC OA full-text lane writes persisted outputs."""

        return _full_text_check_result(
            pmc_oa_full_text_refresh_report,
            source_limits={"pmc_oa": LITERATURE_FULL_TEXT_SOURCE_LIMITS["pmc_oa"]},
        )

    @dg.asset_check(asset=literature_full_text_ingest_smoke_report)
    def literature_full_text_ingest_smoke_has_outputs(
        literature_full_text_ingest_smoke_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the pull-only full-text smoke lane writes persisted body chunks."""

        return _full_text_ingestion_check_result(
            literature_full_text_ingest_smoke_report,
            source_limits=LITERATURE_FULL_TEXT_SMOKE_LIMITS,
        )

    @dg.asset_check(asset=europe_pmc_full_text_ingest_report)
    def europe_pmc_full_text_ingest_has_outputs(
        europe_pmc_full_text_ingest_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the Europe PMC pull-only lane writes persisted body chunks."""

        return _full_text_ingestion_check_result(
            europe_pmc_full_text_ingest_report,
            source_limits={"europe_pmc": LITERATURE_FULL_TEXT_SOURCE_LIMITS["europe_pmc"]},
        )

    @dg.asset_check(asset=pmc_oa_full_text_ingest_report)
    def pmc_oa_full_text_ingest_has_outputs(
        pmc_oa_full_text_ingest_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the PMC OA pull-only lane writes persisted body chunks."""

        return _full_text_ingestion_check_result(
            pmc_oa_full_text_ingest_report,
            source_limits={"pmc_oa": LITERATURE_FULL_TEXT_SOURCE_LIMITS["pmc_oa"]},
        )

    @dg.asset_check(asset=literature_full_text_smoke_report)
    def literature_full_text_smoke_has_outputs(
        literature_full_text_smoke_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure the fast full-text smoke lane writes persisted outputs."""

        return _full_text_check_result(
            literature_full_text_smoke_report,
            source_limits=LITERATURE_FULL_TEXT_SMOKE_LIMITS,
        )

    @dg.asset_check(asset=structured_source_count_report)
    def structured_source_count_report_has_minimum_outputs(
        structured_source_count_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure persisted structured-source counts are healthy after hosted runs."""

        failed_sources = structured_source_count_report.get("failed_sources", [])
        return dg.AssetCheckResult(
            passed=not failed_sources,
            metadata={
                "failed_sources": failed_sources,
                "source_keys": structured_source_count_report.get("source_keys", []),
                "totals": structured_source_count_report.get("totals", {}),
            },
        )

    @dg.asset_check(asset=source_health_report)
    def source_health_report_has_no_failed_sources(
        source_health_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure persisted source health has no hard source failures."""

        failed_sources = source_health_report.get("failed_sources", [])
        return dg.AssetCheckResult(
            passed=not failed_sources,
            metadata={
                "failed_sources": failed_sources,
                "triage_sources": source_health_report.get("triage_sources", []),
                "watch_sources": source_health_report.get("watch_sources", []),
                "summary": source_health_report.get("summary", {}),
                "totals": source_health_report.get("totals", {}),
            },
        )

    @dg.asset_check(asset=entity_resolution_report)
    def entity_resolution_has_minimum_outputs(
        entity_resolution_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure deterministic entity resolution writes first-class mentions."""

        errors = entity_resolution_report.get("errors", [])
        totals = entity_resolution_report.get("totals", {})
        return dg.AssetCheckResult(
            passed=not errors and totals.get("entity_mentions", 0) >= 1,
            metadata={
                "errors": errors,
                "minimum_entity_mentions": 1,
                "totals": totals,
            },
        )

    @dg.asset_check(asset=embedding_index_report)
    def embedding_index_has_minimum_outputs(
        embedding_index_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure stored chunks can produce at least one deterministic embedding."""

        errors = embedding_index_report.get("errors", [])
        totals = embedding_index_report.get("totals", {})
        embedding_coverage = embedding_index_report.get("embedding_coverage", {})
        total_chunks = embedding_coverage.get("total_chunks", 0)
        embedded_chunks = embedding_coverage.get("embedded_chunks", 0)
        passed = not errors and (
            (total_chunks == 0 and totals.get("chunks_seen", 0) == 0 and embedded_chunks == 0)
            or (total_chunks > 0 and totals.get("chunks_seen", 0) >= 1 and embedded_chunks >= 1)
        )
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "errors": errors,
                "minimum_contract": {
                    "when_chunks_exist": "at_least_one_embedding",
                    "when_no_chunks_exist": "zero_chunks_zero_embeddings",
                },
                "totals": totals,
                "embedding_coverage": embedding_coverage,
            },
        )

    @dg.asset_check(asset=embedding_maintenance_report)
    def embedding_maintenance_has_clean_coverage(
        embedding_maintenance_report: dict,
    ) -> dg.AssetCheckResult:
        """Ensure active embeddings have no orphan rows and full chunk coverage."""

        errors = embedding_maintenance_report.get("errors", [])
        embedding_coverage = embedding_maintenance_report.get("embedding_coverage", {})
        total_chunks = embedding_coverage.get("total_chunks", 0)
        embedded_chunks = embedding_coverage.get("embedded_chunks", 0)
        missing_chunks = embedding_coverage.get("missing_chunks", 0)
        passed = not errors and (
            (total_chunks == 0 and embedded_chunks == 0)
            or (total_chunks > 0 and missing_chunks == 0)
        )
        return dg.AssetCheckResult(
            passed=passed,
            metadata={
                "errors": errors,
                "minimum_contract": {
                    "when_chunks_exist": "active_embedding_model_covers_every_chunk",
                    "when_no_chunks_exist": "zero_chunks_zero_embeddings",
                },
                "orphan_embeddings": embedding_maintenance_report.get("orphan_embeddings", {}),
                "embedding_coverage": embedding_coverage,
            },
        )

    ingestion_bridge_assets = [
        source_registry,
        source_scout_plan,
        source_queries,
        raw_source_records,
        canonical_research_objects,
        document_chunks,
        normalized_entities,
        claims,
        curated_claims,
        coverage_snapshot,
        structured_source_pipeline_report,
        structured_source_smoke_report,
        structured_source_multisource_smoke_report,
        literature_clinical_smoke_report,
        all_api_smoke_report,
        literature_corpus_harvest_report,
        literature_full_text_refresh_report,
        europe_pmc_full_text_refresh_report,
        pmc_oa_full_text_refresh_report,
        literature_full_text_ingest_smoke_report,
        europe_pmc_full_text_ingest_report,
        pmc_oa_full_text_ingest_report,
        literature_full_text_smoke_report,
        structured_source_count_report,
        source_health_report,
        entity_resolution_report,
        embedding_index_report,
        embedding_maintenance_report,
    ]

    structured_source_pipeline_job = dg.define_asset_job(
        "structured_source_pipeline_job",
        selection=dg.AssetSelection.assets(structured_source_pipeline_report),
    )
    structured_source_smoke_job = dg.define_asset_job(
        "structured_source_smoke_job",
        selection=dg.AssetSelection.assets(structured_source_smoke_report),
    )
    structured_source_multisource_smoke_job = dg.define_asset_job(
        "structured_source_multisource_smoke_job",
        selection=dg.AssetSelection.assets(structured_source_multisource_smoke_report),
    )
    literature_clinical_smoke_job = dg.define_asset_job(
        "literature_clinical_smoke_job",
        selection=dg.AssetSelection.assets(literature_clinical_smoke_report),
    )
    all_api_smoke_job = dg.define_asset_job(
        "all_api_smoke_job",
        selection=dg.AssetSelection.assets(all_api_smoke_report),
    )
    literature_corpus_harvest_job = dg.define_asset_job(
        "literature_corpus_harvest_job",
        selection=dg.AssetSelection.assets(literature_corpus_harvest_report),
    )
    literature_full_text_refresh_job = dg.define_asset_job(
        "literature_full_text_refresh_job",
        selection=dg.AssetSelection.assets(literature_full_text_refresh_report),
    )
    europe_pmc_full_text_refresh_job = dg.define_asset_job(
        "europe_pmc_full_text_refresh_job",
        selection=dg.AssetSelection.assets(europe_pmc_full_text_refresh_report),
    )
    pmc_oa_full_text_refresh_job = dg.define_asset_job(
        "pmc_oa_full_text_refresh_job",
        selection=dg.AssetSelection.assets(pmc_oa_full_text_refresh_report),
    )
    literature_full_text_ingest_smoke_job = dg.define_asset_job(
        "literature_full_text_ingest_smoke_job",
        selection=dg.AssetSelection.assets(literature_full_text_ingest_smoke_report),
    )
    europe_pmc_full_text_ingest_job = dg.define_asset_job(
        "europe_pmc_full_text_ingest_job",
        selection=dg.AssetSelection.assets(europe_pmc_full_text_ingest_report),
    )
    pmc_oa_full_text_ingest_job = dg.define_asset_job(
        "pmc_oa_full_text_ingest_job",
        selection=dg.AssetSelection.assets(pmc_oa_full_text_ingest_report),
    )
    literature_full_text_smoke_job = dg.define_asset_job(
        "literature_full_text_smoke_job",
        selection=dg.AssetSelection.assets(literature_full_text_smoke_report),
    )
    structured_source_count_report_job = dg.define_asset_job(
        "structured_source_count_report_job",
        selection=dg.AssetSelection.assets(structured_source_count_report),
    )
    source_health_report_job = dg.define_asset_job(
        "source_health_report_job",
        selection=dg.AssetSelection.assets(source_health_report),
    )
    entity_resolution_job = dg.define_asset_job(
        "entity_resolution_job",
        selection=dg.AssetSelection.assets(entity_resolution_report),
    )
    embedding_index_job = dg.define_asset_job(
        "embedding_index_job",
        selection=dg.AssetSelection.assets(embedding_index_report),
    )
    embedding_maintenance_job = dg.define_asset_job(
        "embedding_maintenance_job",
        selection=dg.AssetSelection.assets(embedding_maintenance_report),
    )
    structured_source_pipeline_weekly_schedule = dg.ScheduleDefinition(
        name="structured_source_pipeline_weekly_schedule",
        job=structured_source_pipeline_job,
        cron_schedule="0 2 * * 1",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    literature_corpus_daily_schedule = dg.ScheduleDefinition(
        name="literature_corpus_daily_schedule",
        job=literature_corpus_harvest_job,
        cron_schedule="0 1 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    literature_full_text_weekly_schedule = dg.ScheduleDefinition(
        name="literature_full_text_weekly_schedule",
        job=literature_full_text_refresh_job,
        cron_schedule="0 2 * * 0",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.STOPPED,
    )
    all_api_smoke_weekly_schedule = dg.ScheduleDefinition(
        name="all_api_smoke_weekly_schedule",
        job=all_api_smoke_job,
        cron_schedule="0 3 * * 2",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    embedding_index_daily_schedule = dg.ScheduleDefinition(
        name="embedding_index_daily_schedule",
        job=embedding_index_job,
        cron_schedule="0 5 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    embedding_maintenance_daily_schedule = dg.ScheduleDefinition(
        name="embedding_maintenance_daily_schedule",
        job=embedding_maintenance_job,
        cron_schedule="45 5 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    source_health_daily_schedule = dg.ScheduleDefinition(
        name="source_health_daily_schedule",
        job=source_health_report_job,
        cron_schedule="15 6 * * *",
        execution_timezone=SCHEDULE_TIMEZONE,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )

    defs = dg.Definitions(
        assets=ingestion_bridge_assets,
        asset_checks=[
            source_registry_has_phase_one_sources,
            structured_source_pipeline_has_minimum_outputs,
            structured_source_smoke_has_minimum_outputs,
            structured_source_multisource_smoke_has_minimum_outputs,
            literature_clinical_smoke_has_minimum_outputs,
            all_api_smoke_has_minimum_outputs,
            literature_corpus_harvest_has_hundreds_of_records,
            literature_full_text_refresh_has_outputs,
            europe_pmc_full_text_refresh_has_outputs,
            pmc_oa_full_text_refresh_has_outputs,
            literature_full_text_ingest_smoke_has_outputs,
            europe_pmc_full_text_ingest_has_outputs,
            pmc_oa_full_text_ingest_has_outputs,
            literature_full_text_smoke_has_outputs,
            structured_source_count_report_has_minimum_outputs,
            source_health_report_has_no_failed_sources,
            entity_resolution_has_minimum_outputs,
            embedding_index_has_minimum_outputs,
            embedding_maintenance_has_clean_coverage,
        ],
        jobs=[
            structured_source_pipeline_job,
            structured_source_smoke_job,
            structured_source_multisource_smoke_job,
            literature_clinical_smoke_job,
            all_api_smoke_job,
            literature_corpus_harvest_job,
            literature_full_text_refresh_job,
            europe_pmc_full_text_refresh_job,
            pmc_oa_full_text_refresh_job,
            literature_full_text_ingest_smoke_job,
            europe_pmc_full_text_ingest_job,
            pmc_oa_full_text_ingest_job,
            literature_full_text_smoke_job,
            structured_source_count_report_job,
            source_health_report_job,
            entity_resolution_job,
            embedding_index_job,
            embedding_maintenance_job,
        ],
        schedules=[
            structured_source_pipeline_weekly_schedule,
            literature_corpus_daily_schedule,
            literature_full_text_weekly_schedule,
            all_api_smoke_weekly_schedule,
            embedding_index_daily_schedule,
            embedding_maintenance_daily_schedule,
            source_health_daily_schedule,
        ],
        resources={
            "research_repository": ResearchRepositoryResource(),
        },
    )

else:
    ingestion_bridge_assets = []
    structured_source_pipeline_job = None
    structured_source_smoke_job = None
    structured_source_multisource_smoke_job = None
    literature_clinical_smoke_job = None
    all_api_smoke_job = None
    literature_corpus_harvest_job = None
    literature_full_text_refresh_job = None
    europe_pmc_full_text_refresh_job = None
    pmc_oa_full_text_refresh_job = None
    literature_full_text_ingest_smoke_job = None
    europe_pmc_full_text_ingest_job = None
    pmc_oa_full_text_ingest_job = None
    literature_full_text_smoke_job = None
    structured_source_count_report_job = None
    source_health_report_job = None
    entity_resolution_job = None
    embedding_index_job = None
    embedding_maintenance_job = None
    structured_source_pipeline_weekly_schedule = None
    literature_corpus_daily_schedule = None
    literature_full_text_weekly_schedule = None
    all_api_smoke_weekly_schedule = None
    embedding_index_daily_schedule = None
    embedding_maintenance_daily_schedule = None
    source_health_daily_schedule = None
    defs = None


def _has_minimum_ingested_source_outputs(report: dict) -> bool:
    qa = report.get("qa", {})
    return all(qa.get(field, 0) >= 1 for field in ("raw_records", "research_objects", "document_chunks", "claims"))


def _has_minimum_source_ingestion_outputs(report: dict) -> bool:
    qa = report.get("qa", {})
    return all(qa.get(field, 0) >= 1 for field in ("raw_records", "research_objects", "document_chunks"))


def _has_required_full_text_outputs(report: dict) -> bool:
    full_text_qa = report.get("full_text_qa")
    return full_text_qa is None or bool(full_text_qa.get("passes_full_text_bar"))
