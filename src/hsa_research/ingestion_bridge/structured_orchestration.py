"""Executable structured-source orchestration for the local ingestion bridge."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .claim_curator import curate_claims_for_repository
from .claim_extractor import extract_claims_for_repository
from .contracts import ClaimCurationRequest
from .local_ingest import LocalIngestionPipeline
from .local_store import SQLiteResearchRepository


STRUCTURED_SOURCE_KEYS = (
    "pubchem",
    "chembl",
    "uniprot",
    "rcsb_pdb",
    "openfda_animal_events",
)

DEFAULT_STRUCTURED_SOURCE_LIMITS = {
    "pubchem": 10,
    "chembl": 50,
    "uniprot": 20,
    "rcsb_pdb": 20,
    "openfda_animal_events": 15,
}

DEFAULT_EXTRACT_LIMIT = 500
DEFAULT_CURATE_LIMIT = 1000
DEFAULT_PROMOTE_THRESHOLD = 0.5


def run_structured_sources_pipeline(
    repository: SQLiteResearchRepository,
    *,
    source_keys: Sequence[str] | None = None,
    source_limits: Mapping[str, int] | None = None,
    extract_limit: int | None = DEFAULT_EXTRACT_LIMIT,
    curate_limit: int = DEFAULT_CURATE_LIMIT,
    promote_threshold: float = DEFAULT_PROMOTE_THRESHOLD,
    initialize: bool = True,
) -> dict[str, Any]:
    """Run refresh, extraction, curation, and QA for structured API sources."""

    pipeline = LocalIngestionPipeline(repository)
    if initialize:
        pipeline.initialize()

    selected_sources = list(STRUCTURED_SOURCE_KEYS if source_keys is None else source_keys)
    reports = [
        run_structured_source_pipeline(
            repository,
            source_key,
            source_limits=source_limits,
            extract_limit=extract_limit,
            curate_limit=curate_limit,
            promote_threshold=promote_threshold,
        )
        for source_key in selected_sources
    ]
    errors = [
        f"{report['source_key']}: {error}"
        for report in reports
        for error in [
            *report.get("ingestion_errors", []),
            *report.get("extraction", {}).get("errors", []),
            *report.get("curation", {}).get("errors", []),
        ]
    ]
    return {
        "source_keys": selected_sources,
        "sources": reports,
        "totals": _sum_source_reports(reports),
        "errors": errors,
        "coverage": repository.coverage_summary(),
    }


def run_structured_source_pipeline(
    repository: SQLiteResearchRepository,
    source_key: str,
    *,
    source_limits: Mapping[str, int] | None = None,
    extract_limit: int | None = DEFAULT_EXTRACT_LIMIT,
    curate_limit: int = DEFAULT_CURATE_LIMIT,
    promote_threshold: float = DEFAULT_PROMOTE_THRESHOLD,
) -> dict[str, Any]:
    """Run one structured source through refresh, extraction, curation, and QA."""

    limit = _source_limit(source_key, source_limits)
    pipeline = LocalIngestionPipeline(repository)
    ingestion_results = [result.model_dump(mode="json") for result in pipeline.ingest_source(source_key, limit=limit)]
    extraction = extract_claims_for_repository(
        repository,
        source_key=source_key,
        limit=extract_limit,
    ).model_dump(mode="json")
    curation = curate_claims_for_repository(
        repository,
        ClaimCurationRequest(
            source_key=source_key,
            limit=curate_limit,
            promote_threshold=promote_threshold,
            model_profile="structured_source_reviewer",
        ),
    ).model_dump(mode="json")
    curation.pop("decisions", None)
    qa = structured_source_qa(repository, source_key)

    return {
        "source_key": source_key,
        "limit": limit,
        "ingestion": ingestion_results,
        "ingestion_errors": [
            error
            for result in ingestion_results
            for error in result.get("errors", [])
        ],
        "extraction": extraction,
        "curation": curation,
        "qa": qa,
    }


def structured_source_qa(
    repository: SQLiteResearchRepository,
    source_key: str,
    *,
    sample_limit: int = 5,
) -> dict[str, Any]:
    """Return source-scoped counts and a small promoted-claim sample."""

    if hasattr(repository, "source_runtime_summary"):
        return repository.source_runtime_summary(source_key, sample_limit=sample_limit)
    raise TypeError(f"Repository does not support source runtime QA: {type(repository).__name__}")


def build_structured_source_count_report(
    repository: SQLiteResearchRepository,
    *,
    source_keys: Sequence[str] | None = None,
    sample_limit: int = 5,
    require_claims: bool = True,
) -> dict[str, Any]:
    """Return persisted runtime counts for structured sources without harvesting."""

    selected_sources = list(STRUCTURED_SOURCE_KEYS if source_keys is None else source_keys)
    source_reports = [
        structured_source_qa(repository, source_key, sample_limit=sample_limit)
        for source_key in selected_sources
    ]
    failed_sources = [
        report["source_key"]
        for report in source_reports
        if not _runtime_summary_passes(report, require_claims=require_claims)
    ]
    return {
        "source_keys": selected_sources,
        "sources": source_reports,
        "totals": _sum_runtime_summaries(source_reports),
        "failed_sources": failed_sources,
        "passes_minimum_bar": not failed_sources,
        "minimum_bar": {"require_claims": require_claims},
        "coverage": repository.coverage_summary(),
    }


def _source_limit(source_key: str, source_limits: Mapping[str, int] | None) -> int:
    if source_limits and source_key in source_limits:
        return source_limits[source_key]
    return DEFAULT_STRUCTURED_SOURCE_LIMITS.get(source_key, 10)


def _sum_source_reports(reports: Sequence[dict[str, Any]]) -> dict[str, int]:
    fields = ("raw_records", "research_objects", "document_chunks", "claims")
    return {
        field: sum(report.get("qa", {}).get(field, 0) for report in reports)
        for field in fields
    }


def _sum_runtime_summaries(reports: Sequence[dict[str, Any]]) -> dict[str, int]:
    fields = ("raw_records", "research_objects", "document_chunks", "claims")
    return {
        field: sum(report.get(field, 0) for report in reports)
        for field in fields
    }


def _runtime_summary_passes(report: dict[str, Any], *, require_claims: bool) -> bool:
    required_fields = ("raw_records", "research_objects", "document_chunks", "claims")
    if not require_claims:
        required_fields = required_fields[:-1]
    return all(report.get(field, 0) >= 1 for field in required_fields)
