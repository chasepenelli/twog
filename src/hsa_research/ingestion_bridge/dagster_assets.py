"""Dagster asset scaffold for Ingestion Bridge v2.

These assets define the durable graph before each harvester is implemented.
They are intentionally lightweight and deterministic.
"""

from __future__ import annotations

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

try:
    import dagster as dg
except ImportError:  # pragma: no cover - Dagster is optional until the orchestration env is installed
    dg = None  # type: ignore[assignment]


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
    def structured_source_pipeline_report() -> dict:
        """Executable structured-source refresh, extraction, curation, and QA report."""

        from .storage import build_sql_repository
        from .structured_orchestration import run_structured_sources_pipeline

        repository = build_sql_repository()
        return run_structured_sources_pipeline(repository)

    @dg.asset(group_name="structured_source_refresh")
    def structured_source_smoke_report() -> dict:
        """Small hosted-runtime validation run against a single structured source."""

        from .storage import build_sql_repository
        from .structured_orchestration import run_structured_sources_pipeline

        repository = build_sql_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=["pubchem"],
            source_limits={"pubchem": 1},
            extract_limit=50,
            curate_limit=50,
        )

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
    ]

    structured_source_pipeline_job = dg.define_asset_job(
        "structured_source_pipeline_job",
        selection=dg.AssetSelection.assets(structured_source_pipeline_report),
    )
    structured_source_smoke_job = dg.define_asset_job(
        "structured_source_smoke_job",
        selection=dg.AssetSelection.assets(structured_source_smoke_report),
    )

    defs = dg.Definitions(
        assets=ingestion_bridge_assets,
        asset_checks=[
            source_registry_has_phase_one_sources,
            structured_source_pipeline_has_minimum_outputs,
            structured_source_smoke_has_minimum_outputs,
        ],
        jobs=[structured_source_pipeline_job, structured_source_smoke_job],
    )

else:
    ingestion_bridge_assets = []
    structured_source_pipeline_job = None
    structured_source_smoke_job = None
    defs = None
