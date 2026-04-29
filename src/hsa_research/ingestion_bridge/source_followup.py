"""Queue and ingest primary-source follow-ups extracted from scraper reviews."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from .contracts import (
    ResearchObjectType,
    SourceFollowupIngestItemResult,
    SourceFollowupIngestRequest,
    SourceFollowupIngestResult,
    SourceFollowupQueueItem,
    SourceFollowupQueueRequest,
    SourceFollowupQueueResult,
    SourceQuery,
)
from .local_ingest import LocalIngestionPipeline
from .repository import ResearchRepository


SUPPORTED_FOLLOWUP_SOURCES = {
    "crossref": {"doi"},
    "pubmed": {"pmid"},
    "pmc_oa": {"pmcid"},
    "clinicaltrials_gov": {"nct"},
}


def queue_source_followups_from_scrape_reviews(
    repository: ResearchRepository,
    request: SourceFollowupQueueRequest,
) -> SourceFollowupQueueResult:
    """Promote parsed linked-article primary-source links into a durable queue."""

    reviews = repository.list_scrape_reviews(
        source_key=request.source_key,
        review_status=request.review_status,
        review_ids=request.review_ids or None,
        limit=request.limit,
    )
    result = SourceFollowupQueueResult(source_key=request.source_key, reviewed_records=len(reviews))
    for review in reviews:
        primary_source_links = review.fields.get("primary_source_links", [])
        if not isinstance(primary_source_links, list):
            result.errors.append(f"{review.review_id}: primary_source_links was not a list")
            continue
        for raw_link in primary_source_links:
            if not isinstance(raw_link, Mapping):
                result.skipped_uningestible += 1
                continue
            item = _queue_item_from_link(raw_link, review)
            if item is None:
                result.skipped_uningestible += 1
                continue
            existing = _find_existing_followup(repository, item)
            if existing is not None and not request.include_existing:
                result.skipped_existing += 1
                result.items.append(existing)
                continue
            persisted = repository.upsert_source_followup(item)
            result.items.append(persisted)
            result.queued += 1 if existing is None else 0
    return result


def ingest_source_followups(
    repository: ResearchRepository,
    request: SourceFollowupIngestRequest,
) -> SourceFollowupIngestResult:
    """Run queued primary-source identifiers through existing API harvesters."""

    queue_items = _select_queue_items(repository, request)
    result = SourceFollowupIngestResult(queue_items_seen=len(queue_items))
    if request.dry_run:
        result.items = [
            SourceFollowupIngestItemResult(
                followup_id=item.followup_id,
                source_key=item.source_key,
                identifier_type=item.identifier_type,
                identifier=item.identifier,
                status="skipped",
            )
            for item in queue_items
        ]
        result.skipped = len(result.items)
        return result

    pipeline = LocalIngestionPipeline(repository)  # type: ignore[arg-type]
    ingested_sources: set[str] = set()
    for item in queue_items:
        if item.source_key not in SUPPORTED_FOLLOWUP_SOURCES:
            updated = repository.update_source_followup(
                item.followup_id,
                status="skipped",
                attempts=item.attempts,
                last_error=f"Unsupported follow-up source: {item.source_key}",
            )
            result.skipped += 1
            result.items.append(_item_result(updated or item, error=f"Unsupported source: {item.source_key}"))
            continue

        result.attempted += 1
        attempts = item.attempts + 1
        try:
            query = _query_for_followup(item)
            ingestion = pipeline.ingest_query(query, limit=1, persist_query=False)
            item_report = ingestion.model_dump(mode="json")
            result.source_reports.append(item_report)
            result.raw_records += ingestion.raw_records
            result.research_objects += ingestion.research_objects
            result.document_chunks += ingestion.document_chunks
            if ingestion.errors or ingestion.raw_records < 1:
                message = "; ".join(ingestion.errors) or "No records returned for follow-up identifier"
                updated = repository.update_source_followup(
                    item.followup_id,
                    status="failed",
                    attempts=attempts,
                    last_error=message,
                    metadata={"last_ingestion_report": item_report},
                )
                result.failed += 1
                result.errors.append(f"{item.source_key}:{item.identifier}: {message}")
                result.items.append(_item_result(updated or item, error=message, report=item_report))
                continue
            updated = repository.update_source_followup(
                item.followup_id,
                status="ingested",
                attempts=attempts,
                last_error=None,
                metadata={"last_ingestion_report": item_report, "approved_by": request.approved_by},
            )
            result.ingested += 1
            ingested_sources.add(item.source_key)
            result.items.append(_item_result(updated or item, report=item_report))
        except Exception as exc:
            updated = repository.update_source_followup(
                item.followup_id,
                status="failed",
                attempts=attempts,
                last_error=str(exc),
            )
            result.failed += 1
            result.errors.append(f"{item.source_key}:{item.identifier}: {exc}")
            result.items.append(_item_result(updated or item, error=str(exc)))

    if request.run_claim_extraction and ingested_sources:
        _refresh_entity_claim_layers(repository, sorted(ingested_sources), result)
    return result


def _select_queue_items(
    repository: ResearchRepository,
    request: SourceFollowupIngestRequest,
) -> list[SourceFollowupQueueItem]:
    items: list[SourceFollowupQueueItem] = []
    source_keys = request.source_keys or [None]
    per_source_limit = request.limit if len(source_keys) == 1 else None
    for source_key in source_keys:
        items.extend(
            repository.list_source_followups(
                source_key=source_key,
                statuses=list(request.statuses),
                limit=per_source_limit,
            )
        )
    items.sort(key=lambda item: (item.priority, item.created_at))
    return items[: request.limit]


def _queue_item_from_link(raw_link: Mapping[str, Any], review) -> SourceFollowupQueueItem | None:
    if raw_link.get("should_ingest") is False:
        return None
    source_key = str(raw_link.get("recommended_source_key") or "").strip()
    identifier_type = str(raw_link.get("identifier_type") or "unknown").strip()
    identifier = str(raw_link.get("identifier") or "").strip()
    if source_key not in SUPPORTED_FOLLOWUP_SOURCES:
        return None
    if identifier_type not in SUPPORTED_FOLLOWUP_SOURCES[source_key] or not identifier:
        return None
    return SourceFollowupQueueItem(
        source_key=source_key,
        identifier_type=identifier_type,  # type: ignore[arg-type]
        identifier=identifier,
        url=_optional_str(raw_link.get("url")),
        title=review.title,
        origin_source_key=review.source_key,
        origin_review_id=review.review_id,
        origin_artifact_id=review.artifact_id,
        reason=_optional_str(raw_link.get("reason")),
        metadata={
            "source_record_id": review.source_record_id,
            "canonical_url": review.canonical_url,
            "parser_confidence": review.parser_confidence,
            "link_metadata": raw_link.get("metadata") if isinstance(raw_link.get("metadata"), dict) else {},
        },
    )


def _find_existing_followup(
    repository: ResearchRepository,
    item: SourceFollowupQueueItem,
) -> SourceFollowupQueueItem | None:
    candidates = repository.list_source_followups(
        source_key=item.source_key,
        identifier_type=item.identifier_type,
        limit=None,
    )
    return next((candidate for candidate in candidates if candidate.identity_key == item.identity_key), None)


def _query_for_followup(item: SourceFollowupQueueItem) -> SourceQuery:
    query_params: dict[str, Any] = {"require_policy_match": False}
    if item.source_key in {"crossref", "pubmed", "pmc_oa"}:
        query_params["comparative_policy"] = "disabled"
    if item.source_key == "pmc_oa":
        query_params["license_required"] = True
        query_params["max_candidate_records"] = 5
    return SourceQuery(
        source_key=item.source_key,
        query_name=f"source_followup_{item.identifier_type}_{_safe_query_name(item.identifier)}",
        query_text=item.identifier,
        query_params=query_params,
        track="source_followup",
        object_type=(
            ResearchObjectType.CLINICAL_TRIAL
            if item.source_key == "clinicaltrials_gov"
            else ResearchObjectType.PUBLICATION
        ),
        active=False,
    )


def _item_result(
    item: SourceFollowupQueueItem,
    *,
    error: str | None = None,
    report: Mapping[str, Any] | None = None,
) -> SourceFollowupIngestItemResult:
    return SourceFollowupIngestItemResult(
        followup_id=item.followup_id,
        source_key=item.source_key,
        identifier_type=item.identifier_type,
        identifier=item.identifier,
        status=item.status,
        raw_records=int((report or {}).get("raw_records", 0)),
        research_objects=int((report or {}).get("research_objects", 0)),
        document_chunks=int((report or {}).get("document_chunks", 0)),
        error=error,
        fetch_run_id=(report or {}).get("fetch_run_id"),
    )


def _refresh_entity_claim_layers(
    repository: ResearchRepository,
    source_keys: Iterable[str],
    result: SourceFollowupIngestResult,
) -> None:
    from .claim_curator import curate_claims_for_repository
    from .claim_extractor import extract_claims_for_repository
    from .entity_resolution import resolve_entities_for_repository

    for source_key in source_keys:
        try:
            resolution = resolve_entities_for_repository(repository, source_key=source_key, limit=500)
            extraction = extract_claims_for_repository(repository, source_key=source_key, limit=500)
            curation = curate_claims_for_repository(repository, source_key=source_key, limit=500)
            result.source_reports.append(
                {
                    "source_key": source_key,
                    "post_ingest_enrichment": {
                        "entity_resolution": resolution.model_dump(mode="json"),
                        "claim_extraction": extraction.model_dump(mode="json"),
                        "claim_curation": curation.model_dump(mode="json"),
                    },
                }
            )
        except Exception as exc:
            result.errors.append(f"{source_key}: post-ingest enrichment failed: {exc}")


def _safe_query_name(value: str) -> str:
    return "".join(character if character.isalnum() else "_" for character in value.lower()).strip("_")[:80]


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
