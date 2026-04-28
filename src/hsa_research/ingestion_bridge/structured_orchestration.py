"""Executable structured-source orchestration for the local ingestion bridge."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import re
from typing import Any

from .claim_curator import curate_claims_for_repository
from .claim_extractor import extract_claims_for_repository
from .contracts import ClaimCurationRequest, FullTextTriageRequest
from .entity_resolution import resolve_entities_for_repository
from .full_text_triage import triage_full_text_issue
from .local_ingest import LocalIngestionPipeline
from .local_store import SQLiteResearchRepository
from .source_sets import ALL_API_SOURCE_KEYS, LITERATURE_FULL_TEXT_SOURCE_KEYS, STRUCTURED_SOURCE_KEYS

DEFAULT_STRUCTURED_SOURCE_LIMITS = {
    "pubchem": 10,
    "chembl": 50,
    "uniprot": 20,
    "rcsb_pdb": 20,
    "openfda_animal_events": 15,
    "pmc_oa": 3,
}

DEFAULT_EXTRACT_LIMIT = 500
DEFAULT_CURATE_LIMIT = 1000
DEFAULT_PROMOTE_THRESHOLD = 0.5
MIN_FULL_TEXT_BODY_CHARS = 1


def run_structured_sources_pipeline(
    repository: SQLiteResearchRepository,
    *,
    source_keys: Sequence[str] | None = None,
    source_limits: Mapping[str, int] | None = None,
    extract_limit: int | None = DEFAULT_EXTRACT_LIMIT,
    curate_limit: int = DEFAULT_CURATE_LIMIT,
    promote_threshold: float = DEFAULT_PROMOTE_THRESHOLD,
    initialize: bool = True,
    partition_date: str | None = None,
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
            partition_date=partition_date,
        )
        for source_key in selected_sources
    ]
    errors = [
        f"{report['source_key']}: {error}"
        for report in reports
        for error in [
            *report.get("ingestion_errors", []),
            *report.get("entity_resolution", {}).get("errors", []),
            *report.get("extraction", {}).get("errors", []),
            *report.get("curation", {}).get("errors", []),
        ]
    ]
    return {
        "source_keys": selected_sources,
        "partition_date": partition_date,
        "sources": reports,
        "totals": _sum_source_reports(reports),
        "errors": errors,
        "coverage": repository.coverage_summary(),
    }


def run_structured_sources_ingestion_pipeline(
    repository: SQLiteResearchRepository,
    *,
    source_keys: Sequence[str] | None = None,
    source_limits: Mapping[str, int] | None = None,
    initialize: bool = True,
    partition_date: str | None = None,
) -> dict[str, Any]:
    """Run only source ingestion and QA, without entity resolution or claim work."""

    pipeline = LocalIngestionPipeline(repository)
    if initialize:
        pipeline.initialize()

    selected_sources = list(STRUCTURED_SOURCE_KEYS if source_keys is None else source_keys)
    reports = [
        run_structured_source_ingestion_pipeline(
            repository,
            source_key,
            source_limits=source_limits,
            partition_date=partition_date,
        )
        for source_key in selected_sources
    ]
    errors = [
        f"{report['source_key']}: {error}"
        for report in reports
        for error in report.get("ingestion_errors", [])
    ]
    return {
        "mode": "ingestion_only",
        "source_keys": selected_sources,
        "partition_date": partition_date,
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
    partition_date: str | None = None,
) -> dict[str, Any]:
    """Run one structured source through refresh, extraction, curation, and QA."""

    limit = _source_limit(source_key, source_limits)
    query_param_overrides = source_date_query_params(source_key, partition_date)
    ingestion_kwargs = _ingestion_partition_kwargs(partition_date, query_param_overrides)
    pipeline = LocalIngestionPipeline(repository)
    ingestion_results = [
        result.model_dump(mode="json")
        for result in pipeline.ingest_source(source_key, limit=limit, **ingestion_kwargs)
    ]
    entity_resolution = resolve_entities_for_repository(
        repository,
        source_key=source_key,
        limit=extract_limit,
    ).model_dump(mode="json")
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
    full_text_qa = (
        full_text_source_qa(
            repository,
            source_key,
            ingestion_results=ingestion_results,
            runtime_summary=qa,
            allow_empty_current_run=partition_date is not None,
        )
        if source_key in LITERATURE_FULL_TEXT_SOURCE_KEYS
        else None
    )

    report = {
        "source_key": source_key,
        "limit": limit,
        "partition_date": partition_date,
        "query_param_overrides": query_param_overrides,
        "ingestion": ingestion_results,
        "ingestion_errors": [
            error
            for result in ingestion_results
            for error in result.get("errors", [])
        ],
        "entity_resolution": entity_resolution,
        "extraction": extraction,
        "curation": curation,
        "qa": qa,
    }
    if full_text_qa is not None:
        report["full_text_qa"] = full_text_qa
        report.update(full_text_triage_summary(full_text_qa))
    return report


def run_structured_source_ingestion_pipeline(
    repository: SQLiteResearchRepository,
    source_key: str,
    *,
    source_limits: Mapping[str, int] | None = None,
    partition_date: str | None = None,
) -> dict[str, Any]:
    """Run one source through fetch, normalization, persistence, and QA only."""

    limit = _source_limit(source_key, source_limits)
    query_param_overrides = source_date_query_params(source_key, partition_date)
    ingestion_kwargs = _ingestion_partition_kwargs(partition_date, query_param_overrides)
    pipeline = LocalIngestionPipeline(repository)
    ingestion_results = [
        result.model_dump(mode="json")
        for result in pipeline.ingest_source(source_key, limit=limit, **ingestion_kwargs)
    ]
    qa = structured_source_qa(repository, source_key)
    full_text_qa = (
        full_text_source_qa(
            repository,
            source_key,
            ingestion_results=ingestion_results,
            runtime_summary=qa,
            allow_empty_current_run=partition_date is not None,
        )
        if source_key in LITERATURE_FULL_TEXT_SOURCE_KEYS
        else None
    )

    report = {
        "mode": "ingestion_only",
        "source_key": source_key,
        "limit": limit,
        "partition_date": partition_date,
        "query_param_overrides": query_param_overrides,
        "ingestion": ingestion_results,
        "ingestion_errors": [
            error
            for result in ingestion_results
            for error in result.get("errors", [])
        ],
        "entity_resolution": {"status": "skipped", "reason": "ingestion_only"},
        "extraction": {"status": "skipped", "reason": "ingestion_only"},
        "curation": {"status": "skipped", "reason": "ingestion_only"},
        "qa": qa,
    }
    if full_text_qa is not None:
        report["full_text_qa"] = full_text_qa
        report.update(full_text_triage_summary(full_text_qa))
    return report


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


def full_text_source_qa(
    repository: SQLiteResearchRepository,
    source_key: str,
    *,
    ingestion_results: Sequence[Mapping[str, Any]] | None = None,
    runtime_summary: Mapping[str, Any] | None = None,
    allow_empty_current_run: bool = False,
) -> dict[str, Any]:
    """Return full-text-specific persisted and current-run QA for OA sources."""

    if runtime_summary is None:
        runtime_summary = structured_source_qa(repository, source_key)
    research_objects = repository.list_research_objects(source_key=source_key) if hasattr(repository, "list_research_objects") else []
    document_chunks = repository.list_document_chunks(source_key=source_key) if hasattr(repository, "list_document_chunks") else []
    full_text_objects = [
        research_object
        for research_object in research_objects
        if research_object.metadata.get("full_text_available") is True
    ]
    full_text_chunks = [
        chunk
        for chunk in document_chunks
        if chunk.section_label == "full_text" and chunk.text_content.strip()
    ]
    title_abstract_chunks = [
        chunk
        for chunk in document_chunks
        if chunk.section_label == "title_abstract"
    ]
    current = _current_ingestion_full_text_counts(ingestion_results)
    full_text_body_chars = sum(len(chunk.text_content) for chunk in full_text_chunks)
    persisted_passes = (
        len(full_text_chunks) >= 1
        and full_text_body_chars >= MIN_FULL_TEXT_BODY_CHARS
    )
    current_run_required = ingestion_results is not None
    current_empty_passes = (
        allow_empty_current_run
        and current_run_required
        and current["current_ingestion_runs"] >= 1
        and current["current_raw_records"] == 0
        and not current["current_failed_runs"]
    )
    current_passes = (
        not current_run_required
        or current_empty_passes
        or (
            current["current_ingestion_runs"] >= 1
            and current["current_raw_records"] >= 1
            and current["current_research_objects"] >= 1
            and current["current_document_chunks"] >= 1
            and current["current_full_text_document_chunks"] >= 1
            and not current["current_failed_runs"]
        )
    )
    result = {
        "source_key": source_key,
        "full_text_research_objects": len(full_text_objects),
        "full_text_document_chunks": len(full_text_chunks),
        "title_abstract_document_chunks": len(title_abstract_chunks),
        "full_text_body_chars": full_text_body_chars,
        "minimum_full_text_body_chars": MIN_FULL_TEXT_BODY_CHARS,
        "passes_persisted_full_text_bar": persisted_passes,
        "passes_current_full_text_bar": current_passes,
        "passes_full_text_bar": current_empty_passes or (persisted_passes and current_passes),
        "allow_empty_current_run": allow_empty_current_run,
        "current_empty_passes": current_empty_passes,
        **current,
    }
    result["triage"] = _triage_full_text_qa(
        source_key=source_key,
        qa=result,
        runtime_summary=runtime_summary,
        ingestion_results=ingestion_results,
    )
    result.update(full_text_triage_summary(result))
    return result


def full_text_triage_summary(full_text_qa: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return flat full-text triage fields for tables and source-health rows."""

    triage = (full_text_qa or {}).get("triage") or {}
    return {
        "full_text_triage_action": triage.get("action"),
        "full_text_triage_severity": triage.get("severity"),
        "full_text_triage_should_retry": triage.get("should_retry"),
        "full_text_triage_should_block_schedule": triage.get("should_block_schedule"),
    }


def build_structured_source_count_report(
    repository: SQLiteResearchRepository,
    *,
    source_keys: Sequence[str] | None = None,
    sample_limit: int = 5,
    require_claims: bool = True,
) -> dict[str, Any]:
    """Return persisted runtime counts for structured sources without harvesting."""

    selected_sources = list(STRUCTURED_SOURCE_KEYS if source_keys is None else source_keys)
    source_reports = []
    for source_key in selected_sources:
        source_report = structured_source_qa(repository, source_key, sample_limit=sample_limit)
        if source_key in LITERATURE_FULL_TEXT_SOURCE_KEYS:
            full_text_qa = full_text_source_qa(
                repository,
                source_key,
                runtime_summary=source_report,
            )
            source_report = {
                **source_report,
                "full_text_qa": full_text_qa,
                **full_text_triage_summary(full_text_qa),
            }
        source_reports.append(source_report)
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
    fields = ("raw_records", "research_objects", "document_chunks", "entity_mentions", "claims")
    return {
        field: sum(report.get("qa", {}).get(field, 0) for report in reports)
        for field in fields
    }


def _sum_runtime_summaries(reports: Sequence[dict[str, Any]]) -> dict[str, int]:
    fields = ("raw_records", "research_objects", "document_chunks", "entity_mentions", "claims")
    return {
        field: sum(report.get(field, 0) for report in reports)
        for field in fields
    }


def _runtime_summary_passes(report: dict[str, Any], *, require_claims: bool) -> bool:
    required_fields = ("raw_records", "research_objects", "document_chunks", "claims")
    if not require_claims:
        required_fields = required_fields[:-1]
    counts_pass = all(report.get(field, 0) >= 1 for field in required_fields)
    full_text_qa = report.get("full_text_qa")
    if full_text_qa is not None:
        return counts_pass and bool(full_text_qa.get("passes_full_text_bar"))
    return counts_pass


def _current_ingestion_full_text_counts(
    ingestion_results: Sequence[Mapping[str, Any]] | None,
) -> dict[str, Any]:
    if ingestion_results is None:
        return {
            "current_ingestion_runs": 0,
            "current_raw_records": 0,
            "current_research_objects": 0,
            "current_document_chunks": 0,
            "current_full_text_research_objects": 0,
            "current_full_text_document_chunks": 0,
            "current_failed_runs": [],
        }

    failed_runs = [
        str(result.get("query_name") or result.get("fetch_run_id"))
        for result in ingestion_results
        if str(result.get("status")) != "completed"
    ]
    return {
        "current_ingestion_runs": len(ingestion_results),
        "current_raw_records": sum(int(result.get("raw_records", 0)) for result in ingestion_results),
        "current_research_objects": sum(int(result.get("research_objects", 0)) for result in ingestion_results),
        "current_document_chunks": sum(int(result.get("document_chunks", 0)) for result in ingestion_results),
        "current_full_text_research_objects": sum(
            int(result.get("full_text_research_objects", 0)) for result in ingestion_results
        ),
        "current_full_text_document_chunks": sum(
            int((result.get("section_chunk_counts") or {}).get("full_text", 0))
            for result in ingestion_results
        ),
        "current_failed_runs": failed_runs,
    }


def _triage_full_text_qa(
    *,
    source_key: str,
    qa: Mapping[str, Any],
    runtime_summary: Mapping[str, Any],
    ingestion_results: Sequence[Mapping[str, Any]] | None,
) -> dict[str, Any]:
    use_current_run_counts = ingestion_results is not None
    errors = [
        str(error)
        for result in (ingestion_results or [])
        for error in result.get("errors", [])
        if error
    ]
    query_names = [
        str(result.get("query_name"))
        for result in (ingestion_results or [])
        if result.get("query_name")
    ]
    full_text_document_chunks = (
        int(qa.get("current_full_text_document_chunks", 0))
        if use_current_run_counts
        else int(qa.get("full_text_document_chunks", 0))
    )
    full_text_body_chars = int(qa.get("full_text_body_chars", 0)) if full_text_document_chunks > 0 else 0
    request = FullTextTriageRequest(
        source_key=source_key,
        stage="qa",
        query_name=query_names[0] if len(query_names) == 1 else None,
        error_message=errors[0] if errors else None,
        errors=errors[1:],
        raw_records=(
            int(qa.get("current_raw_records", 0))
            if use_current_run_counts
            else int(runtime_summary.get("raw_records", 0))
        ),
        research_objects=(
            int(qa.get("current_research_objects", 0))
            if use_current_run_counts
            else int(runtime_summary.get("research_objects", 0))
        ),
        document_chunks=(
            int(qa.get("current_document_chunks", 0))
            if use_current_run_counts
            else int(runtime_summary.get("document_chunks", 0))
        ),
        full_text_document_chunks=full_text_document_chunks,
        full_text_body_chars=full_text_body_chars,
        claims=int(runtime_summary.get("claims", 0)),
        entity_mentions=int(runtime_summary.get("entity_mentions", 0)),
        current_failed_runs=list(qa.get("current_failed_runs", [])),
        metadata={
            "passes_persisted_full_text_bar": qa.get("passes_persisted_full_text_bar"),
            "passes_current_full_text_bar": qa.get("passes_current_full_text_bar"),
            "passes_full_text_bar": qa.get("passes_full_text_bar"),
            "query_names": query_names,
            "current_ingestion_runs": qa.get("current_ingestion_runs", 0),
            "allow_empty_current_run": qa.get("allow_empty_current_run", False),
            "current_empty_passes": qa.get("current_empty_passes", False),
            "current_full_text_research_objects": qa.get("current_full_text_research_objects", 0),
            "full_text_research_objects": qa.get("full_text_research_objects", 0),
        },
    )
    return triage_full_text_issue(request).model_dump(mode="json")


def source_date_query_params(source_key: str, partition_date: str | None) -> dict[str, Any]:
    """Return harvester query-param overrides for one source/date partition."""

    if partition_date is None:
        return {}
    _validate_partition_date(partition_date)
    if source_key in LITERATURE_FULL_TEXT_SOURCE_KEYS:
        return {
            "published_after": partition_date,
            "published_before": partition_date,
        }
    return {}


def _ingestion_partition_kwargs(
    partition_date: str | None,
    query_param_overrides: Mapping[str, Any],
) -> dict[str, Any]:
    if not partition_date:
        return {}
    return {
        "query_param_overrides": dict(query_param_overrides),
        "query_name_suffix": f"partition_{partition_date}",
    }


def _validate_partition_date(partition_date: str) -> None:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", partition_date):
        raise ValueError(f"Expected partition_date as YYYY-MM-DD; got {partition_date!r}")
