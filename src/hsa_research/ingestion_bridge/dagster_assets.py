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

        from .structured_orchestration import run_structured_sources_pipeline

        repository = research_repository.build_repository()
        return run_structured_sources_pipeline(
            repository,
            source_keys=LITERATURE_FULL_TEXT_SOURCE_KEYS,
            source_limits=LITERATURE_FULL_TEXT_SOURCE_LIMITS,
            extract_limit=1000,
            curate_limit=1000,
        )

    @dg.asset(group_name="structured_source_refresh")
    def structured_source_count_report(research_repository: ResearchRepositoryResource) -> dict:
        """Persisted count report for hosted API source coverage."""

        from .structured_orchestration import build_structured_source_count_report

        repository = research_repository.build_repository()
        return build_structured_source_count_report(
            repository,
            source_keys=HOSTED_API_REPORT_KEYS,
            sample_limit=3,
            require_claims=True,
        )

    @dg.asset(group_name="hosted_api_refresh")
    def source_health_report(research_repository: ResearchRepositoryResource) -> dict:
        """Persisted source health report for hosted API source coverage."""

        from .source_health import build_source_health_report

        repository = research_repository.build_repository()
        return build_source_health_report(
            repository,
            source_keys=HOSTED_API_REPORT_KEYS,
            sample_limit=3,
            require_claims=True,
        )

    @dg.asset(group_name="entity_resolution")
    def entity_resolution_report(research_repository: ResearchRepositoryResource) -> dict:
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
        return {
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

        source_reports = literature_full_text_refresh_report.get("sources", [])
        failed_sources = [
            report["source_key"]
            for report in source_reports
            if not _has_minimum_ingested_source_outputs(report)
        ]
        errors = literature_full_text_refresh_report.get("errors", [])
        return dg.AssetCheckResult(
            passed=not failed_sources and not errors,
            metadata={
                "failed_sources": failed_sources,
                "errors": errors,
                "source_keys": literature_full_text_refresh_report.get("source_keys", []),
                "source_limits": LITERATURE_FULL_TEXT_SOURCE_LIMITS,
                "totals": literature_full_text_refresh_report.get("totals", {}),
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
        structured_source_count_report,
        source_health_report,
        entity_resolution_report,
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
    literature_corpus_daily_schedule = dg.ScheduleDefinition(
        job=literature_corpus_harvest_job,
        cron_schedule="0 7 * * *",
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    literature_full_text_weekly_schedule = dg.ScheduleDefinition(
        job=literature_full_text_refresh_job,
        cron_schedule="0 8 * * 0",
        default_status=dg.DefaultScheduleStatus.STOPPED,
    )
    source_health_daily_schedule = dg.ScheduleDefinition(
        job=source_health_report_job,
        cron_schedule="0 9 * * *",
        default_status=dg.DefaultScheduleStatus.STOPPED,
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
            structured_source_count_report_has_minimum_outputs,
            source_health_report_has_no_failed_sources,
            entity_resolution_has_minimum_outputs,
        ],
        jobs=[
            structured_source_pipeline_job,
            structured_source_smoke_job,
            structured_source_multisource_smoke_job,
            literature_clinical_smoke_job,
            all_api_smoke_job,
            literature_corpus_harvest_job,
            literature_full_text_refresh_job,
            structured_source_count_report_job,
            source_health_report_job,
            entity_resolution_job,
        ],
        schedules=[
            literature_corpus_daily_schedule,
            literature_full_text_weekly_schedule,
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
    structured_source_count_report_job = None
    source_health_report_job = None
    entity_resolution_job = None
    literature_corpus_daily_schedule = None
    literature_full_text_weekly_schedule = None
    source_health_daily_schedule = None
    defs = None


def _has_minimum_ingested_source_outputs(report: dict) -> bool:
    qa = report.get("qa", {})
    return all(qa.get(field, 0) >= 1 for field in ("raw_records", "research_objects", "document_chunks", "claims"))
