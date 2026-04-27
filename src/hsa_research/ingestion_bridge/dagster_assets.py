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


STRUCTURED_SOURCE_SMOKE_KEYS = ("pubchem",)
STRUCTURED_SOURCE_MULTISOURCE_SMOKE_KEYS = (
    "pubchem",
    "chembl",
    "uniprot",
    "rcsb_pdb",
    "openfda_animal_events",
)
CANINE_DATA_OMICS_SMOKE_KEYS = (
    "icdc",
    "geo",
    "sra",
)
LITERATURE_CLINICAL_SMOKE_KEYS = (
    "openalex",
    "pubmed",
    "europe_pmc",
    "crossref",
    "pmc_oa",
    "clinicaltrials_gov",
)
ALL_API_SMOKE_KEYS = (
    *STRUCTURED_SOURCE_MULTISOURCE_SMOKE_KEYS,
    *CANINE_DATA_OMICS_SMOKE_KEYS,
    *LITERATURE_CLINICAL_SMOKE_KEYS,
)
HOSTED_API_REPORT_KEYS = ALL_API_SMOKE_KEYS


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
            source_keys=STRUCTURED_SOURCE_SMOKE_KEYS,
            source_limits={"pubchem": 1},
            extract_limit=50,
            curate_limit=50,
        )

    @dg.asset(group_name="structured_source_refresh")
    def structured_source_multisource_smoke_report() -> dict:
        """Small hosted-runtime validation run across all structured API harvesters."""

        from .storage import build_sql_repository
        from .structured_orchestration import run_structured_sources_pipeline

        repository = build_sql_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=STRUCTURED_SOURCE_MULTISOURCE_SMOKE_KEYS,
            source_limits={source_key: 1 for source_key in STRUCTURED_SOURCE_MULTISOURCE_SMOKE_KEYS},
            extract_limit=250,
            curate_limit=250,
        )

    @dg.asset(group_name="literature_clinical_refresh")
    def literature_clinical_smoke_report() -> dict:
        """Small hosted-runtime validation run across literature and clinical APIs."""

        from .storage import build_sql_repository
        from .structured_orchestration import run_structured_sources_pipeline

        repository = build_sql_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=LITERATURE_CLINICAL_SMOKE_KEYS,
            source_limits={source_key: 1 for source_key in LITERATURE_CLINICAL_SMOKE_KEYS},
            extract_limit=250,
            curate_limit=250,
        )

    @dg.asset(group_name="hosted_api_refresh")
    def all_api_smoke_report() -> dict:
        """Small hosted-runtime validation run across every implemented API harvester."""

        from .storage import build_sql_repository
        from .structured_orchestration import run_structured_sources_pipeline

        repository = build_sql_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=ALL_API_SMOKE_KEYS,
            source_limits={source_key: 1 for source_key in ALL_API_SMOKE_KEYS},
            extract_limit=500,
            curate_limit=500,
        )

    @dg.asset(group_name="structured_source_refresh")
    def structured_source_count_report() -> dict:
        """Persisted count report for hosted API source coverage."""

        from .storage import build_sql_repository
        from .structured_orchestration import build_structured_source_count_report

        repository = build_sql_repository()
        return build_structured_source_count_report(
            repository,
            source_keys=HOSTED_API_REPORT_KEYS,
            sample_limit=3,
            require_claims=True,
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
        structured_source_count_report,
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
    structured_source_count_report_job = dg.define_asset_job(
        "structured_source_count_report_job",
        selection=dg.AssetSelection.assets(structured_source_count_report),
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
            structured_source_count_report_has_minimum_outputs,
        ],
        jobs=[
            structured_source_pipeline_job,
            structured_source_smoke_job,
            structured_source_multisource_smoke_job,
            literature_clinical_smoke_job,
            all_api_smoke_job,
            structured_source_count_report_job,
        ],
    )

else:
    ingestion_bridge_assets = []
    structured_source_pipeline_job = None
    structured_source_smoke_job = None
    structured_source_multisource_smoke_job = None
    literature_clinical_smoke_job = None
    all_api_smoke_job = None
    structured_source_count_report_job = None
    defs = None


def _has_minimum_ingested_source_outputs(report: dict) -> bool:
    qa = report.get("qa", {})
    return all(qa.get(field, 0) >= 1 for field in ("raw_records", "research_objects", "document_chunks", "claims"))
