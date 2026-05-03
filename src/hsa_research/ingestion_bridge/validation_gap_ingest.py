"""Ingest active validation-gap source queries without touching starter query lanes."""

from __future__ import annotations

from typing import Any

from .contracts import (
    RunStatus,
    SourceQuery,
    ValidationGapSourceIngestRequest,
    ValidationGapSourceIngestResult,
)
from .local_ingest import LocalIngestionPipeline
from .repository import ResearchRepository
from .source_query_params import with_source_safe_query_params


def ingest_validation_gap_source_queries(
    repository: ResearchRepository,
    request: ValidationGapSourceIngestRequest,
) -> ValidationGapSourceIngestResult:
    """Run only persisted SourceQuery rows whose track is validation_gap."""

    queries = _select_validation_gap_queries(repository, request)
    result = ValidationGapSourceIngestResult(
        dry_run=request.dry_run,
        source_keys=sorted({query.source_key for query in queries}),
        query_count=len(queries),
        source_queries=queries,
    )
    if request.dry_run:
        return result

    pipeline = LocalIngestionPipeline(repository)  # type: ignore[arg-type]
    for query in queries:
        result.attempted_query_count += 1
        try:
            safe_query = with_source_safe_query_params(query)
            ingestion = pipeline.ingest_query(
                safe_query,
                limit=request.limit_per_query,
                persist_query=False,
            )
            result.results.append(ingestion)
            result.raw_records += ingestion.raw_records
            result.research_objects += ingestion.research_objects
            result.document_chunks += ingestion.document_chunks
            result.full_text_research_objects += ingestion.full_text_research_objects
            if ingestion.status == RunStatus.COMPLETED:
                result.completed_query_count += 1
            else:
                result.failed_query_count += 1
                result.errors.extend(
                    f"{ingestion.source_key}:{ingestion.query_name}: {error}" for error in ingestion.errors
                )
        except Exception as exc:
            result.failed_query_count += 1
            result.errors.append(f"{query.source_key}:{query.query_name}: {exc}")
    return result


def _select_validation_gap_queries(
    repository: ResearchRepository,
    request: ValidationGapSourceIngestRequest,
) -> list[SourceQuery]:
    source_filter = set(request.source_keys)
    query_filter = set(request.query_names)
    queries = []
    for source_key in source_filter or [None]:
        source_queries = repository.list_source_queries(source_key=source_key, active_only=True)  # type: ignore[arg-type]
        queries.extend(
            query
            for query in source_queries
            if query.track == "validation_gap"
            and (not source_filter or query.source_key in source_filter)
            and (not query_filter or query.query_name in query_filter)
        )
    deduped: dict[tuple[str, str], SourceQuery] = {}
    for query in queries:
        deduped[(query.source_key, query.query_name)] = query
    return sorted(deduped.values(), key=_query_sort_key)[: request.max_queries]


def summarize_validation_gap_source_ingest(result: ValidationGapSourceIngestResult) -> dict[str, Any]:
    return {
        "dry_run": result.dry_run,
        "source_keys": result.source_keys,
        "query_count": result.query_count,
        "attempted_query_count": result.attempted_query_count,
        "completed_query_count": result.completed_query_count,
        "failed_query_count": result.failed_query_count,
        "raw_records": result.raw_records,
        "research_objects": result.research_objects,
        "document_chunks": result.document_chunks,
        "errors": len(result.errors),
    }


def _query_sort_key(query: SourceQuery) -> tuple[int, str, str]:
    priority = int((query.query_params or {}).get("priority", 100))
    lane = str((query.query_params or {}).get("lane", ""))
    lane_priority = {
        "safety_signal": 0,
        "pkpd": 1,
        "clinical_response": 2,
        "assay_protocol": 3,
        "general_evidence": 4,
    }.get(lane, 10)
    return (lane_priority + priority, query.source_key, query.query_name)
