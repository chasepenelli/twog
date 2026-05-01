"""Queue and ingest primary-source and DOI enrichment follow-ups."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import re
from typing import Any
from uuid import UUID

from .contracts import (
    ClaimCurationRequest,
    DoiOpenAccessFollowupQueueRequest,
    ResearchObject,
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
    "unpaywall": {"doi"},
}
X_LINKED_ARTICLE_REVIEW_AGENT_NAME = "x_linked_article_review_agent"
X_TOPIC_REVIEW_AGENT_NAME = "x_topic_review_agent"

_DOI_PATTERN = re.compile(r"^10\.\S+/.+$", re.I)


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
    review_by_id = {review.review_id: review for review in reviews}
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
            _persist_queue_item(repository, request, result, item)

    if request.include_agent_recommendations and request.agent_run_limit:
        _queue_agent_recommendations(repository, request, result, review_by_id)
    return result


def queue_unpaywall_doi_followups(
    repository: ResearchRepository,
    request: DoiOpenAccessFollowupQueueRequest,
) -> SourceFollowupQueueResult:
    """Queue DOI-only open-access enrichment lookups through Unpaywall."""

    result = SourceFollowupQueueResult(source_key="unpaywall")
    source_keys = request.source_keys or [None]
    seen_dois: set[str] = set()
    for source_key in source_keys:
        remaining = request.limit - result.reviewed_records
        if remaining <= 0:
            return result
        research_objects = repository.list_research_objects(source_key=source_key, limit=remaining)
        for research_object in research_objects:
            result.reviewed_records += 1
            doi = _doi_from_research_object(research_object)
            if doi is None:
                result.skipped_uningestible += 1
                continue
            if doi in seen_dois:
                result.skipped_existing += 1
                continue
            seen_dois.add(doi)
            item = SourceFollowupQueueItem(
                source_key="unpaywall",
                identifier_type="doi",
                identifier=doi,
                url=f"https://doi.org/{doi}",
                title=research_object.title,
                origin_source_key=research_object.source_key,
                reason="DOI open-access enrichment via Unpaywall DOI lookup.",
                priority=120,
                metadata={
                    "followup_type": "doi_open_access_enrichment",
                    "lookup_mode": "doi",
                    "research_object_id": str(research_object.id),
                    "raw_record_id": str(research_object.raw_record_id) if research_object.raw_record_id else None,
                    "research_object_source_key": research_object.source_key,
                    "title_search": False,
                },
            )
            existing = _find_existing_followup(repository, item)
            if existing is not None and not request.include_existing:
                result.skipped_existing += 1
                result.items.append(existing)
                continue
            persisted = repository.upsert_source_followup(item)
            result.items.append(persisted)
            result.queued += 1 if existing is None else 0
            if result.reviewed_records >= request.limit:
                return result
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
                fallback_items = _queue_pmc_metadata_fallbacks(repository, item, approved_by=request.approved_by)
                if fallback_items:
                    fallback_message = (
                        f"{message}; queued {len(fallback_items)} PubMed/Crossref metadata fallback(s)"
                    )
                    updated = repository.update_source_followup(
                        item.followup_id,
                        status="skipped",
                        attempts=attempts,
                        last_error=fallback_message,
                        metadata={"last_ingestion_report": item_report, "fallback_followup_ids": [str(row.followup_id) for row in fallback_items]},
                    )
                    result.skipped += 1
                    result.items.append(_item_result(updated or item, report=item_report))
                    continue
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
    if request.followup_ids:
        allowed_statuses = set(request.statuses)
        selected_items: list[SourceFollowupQueueItem] = []
        for followup_id in request.followup_ids:
            item = repository.get_source_followup(followup_id)
            if item is None or item.status not in allowed_statuses:
                continue
            if request.source_keys and item.source_key not in set(request.source_keys):
                continue
            selected_items.append(item)
        selected_items.sort(key=lambda item: (item.priority, item.created_at))
        return selected_items[: request.limit]

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


def _queue_item_from_link(
    raw_link: Mapping[str, Any],
    review,
    *,
    origin_agent_run_id: UUID | None = None,
    agent_action_reason: str | None = None,
) -> SourceFollowupQueueItem | None:
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
        origin_agent_run_id=origin_agent_run_id,
        reason=_optional_str(raw_link.get("reason")),
        metadata={
            "source_record_id": review.source_record_id,
            "canonical_url": review.canonical_url,
            "parser_confidence": review.parser_confidence,
            "recommendation_source": (
                "linked_article_review_agent" if origin_agent_run_id else "scrape_parser"
            ),
            "agent_action_reason": agent_action_reason,
            "link_metadata": raw_link.get("metadata") if isinstance(raw_link.get("metadata"), dict) else {},
        },
    )


def _queue_agent_recommendations(
    repository: ResearchRepository,
    request: SourceFollowupQueueRequest,
    result: SourceFollowupQueueResult,
    review_by_id: dict[UUID, Any],
) -> None:
    if request.source_key == "x_linked_article":
        _queue_linked_article_agent_recommendations(repository, request, result, review_by_id)
    elif request.source_key == "x_topic_monitor":
        _queue_x_topic_agent_recommendations(repository, request, result)


def _queue_linked_article_agent_recommendations(
    repository: ResearchRepository,
    request: SourceFollowupQueueRequest,
    result: SourceFollowupQueueResult,
    review_by_id: dict[UUID, Any],
) -> None:
    agent_runs = repository.list_agent_runs(
        agent_name=X_LINKED_ARTICLE_REVIEW_AGENT_NAME,
        status="completed",
        source_key=request.source_key,
        limit=request.agent_run_limit,
    )
    result.agent_runs_seen += len(agent_runs)
    requested_review_ids = set(request.review_ids)
    for run in agent_runs:
        for action in run.output_payload.get("actions", []):
            if not isinstance(action, Mapping):
                continue
            if action.get("action") != "queue_primary_source_followup":
                continue
            review_id = _parse_uuid(action.get("review_id"))
            if review_id is None:
                result.errors.append(f"{run.agent_run_id}: queue action missing review_id")
                continue
            if requested_review_ids and review_id not in requested_review_ids:
                continue
            review = review_by_id.get(review_id)
            if review is None:
                fetched_reviews = repository.list_scrape_reviews(
                    source_key=request.source_key,
                    review_ids=[review_id],
                    limit=1,
                )
                if fetched_reviews:
                    review = fetched_reviews[0]
                    review_by_id[review.review_id] = review
            if review is None:
                result.errors.append(f"{run.agent_run_id}: no scrape review found for {review_id}")
                continue
            followup_links = action.get("followup_links", [])
            if not isinstance(followup_links, list):
                result.skipped_uningestible += 1
                continue
            for raw_link in followup_links:
                result.agent_recommendations_seen += 1
                if not isinstance(raw_link, Mapping):
                    result.skipped_uningestible += 1
                    continue
                item = _queue_item_from_link(
                    raw_link,
                    review,
                    origin_agent_run_id=run.agent_run_id,
                    agent_action_reason=_optional_str(action.get("reason")),
                )
                _persist_queue_item(repository, request, result, item)


def _queue_x_topic_agent_recommendations(
    repository: ResearchRepository,
    request: SourceFollowupQueueRequest,
    result: SourceFollowupQueueResult,
) -> None:
    agent_runs = repository.list_agent_runs(
        agent_name=X_TOPIC_REVIEW_AGENT_NAME,
        status="completed",
        source_key="x_topic_monitor",
        limit=request.agent_run_limit,
    )
    result.agent_runs_seen += len(agent_runs)
    for run in agent_runs:
        for action in run.output_payload.get("actions", []):
            if not isinstance(action, Mapping):
                continue
            if action.get("action") != "flag_for_ingestion":
                continue
            links = action.get("ingestible_links", [])
            if not isinstance(links, list):
                result.skipped_uningestible += 1
                continue
            for raw_link in links:
                result.agent_recommendations_seen += 1
                if not isinstance(raw_link, Mapping):
                    result.skipped_uningestible += 1
                    continue
                item = _queue_item_from_x_topic_link(raw_link, action, run.agent_run_id)
                _persist_queue_item(repository, request, result, item)


def _queue_item_from_x_topic_link(
    raw_link: Mapping[str, Any],
    action: Mapping[str, Any],
    agent_run_id: UUID,
) -> SourceFollowupQueueItem | None:
    if raw_link.get("should_ingest") is False:
        return None
    source_key = str(raw_link.get("recommended_source_key") or "").strip()
    identifier_type = str(raw_link.get("identifier_type") or "unknown").strip()
    identifier = str(raw_link.get("identifier") or "").strip()
    if source_key not in SUPPORTED_FOLLOWUP_SOURCES:
        return None
    if identifier_type not in SUPPORTED_FOLLOWUP_SOURCES[source_key] or not identifier:
        return None
    post_id = _optional_str(action.get("source_record_id"))
    return SourceFollowupQueueItem(
        source_key=source_key,
        identifier_type=identifier_type,  # type: ignore[arg-type]
        identifier=identifier,
        url=_optional_str(raw_link.get("url")),
        title=f"X topic signal {post_id}" if post_id else "X topic signal",
        origin_source_key="x_topic_monitor",
        origin_agent_run_id=agent_run_id,
        reason=_optional_str(raw_link.get("reason") or action.get("reason")),
        metadata={
            "recommendation_source": "x_topic_review_agent",
            "source_record_id": post_id,
            "query_name": _optional_str(action.get("query_name")),
            "username": _optional_str(action.get("username")),
            "agent_action": _optional_str(action.get("action")),
            "agent_action_reason": _optional_str(action.get("reason")),
            "link_metadata": raw_link.get("metadata") if isinstance(raw_link.get("metadata"), dict) else {},
        },
    )


def _persist_queue_item(
    repository: ResearchRepository,
    request: SourceFollowupQueueRequest,
    result: SourceFollowupQueueResult,
    item: SourceFollowupQueueItem | None,
) -> None:
    if item is None:
        result.skipped_uningestible += 1
        return
    existing = _find_existing_followup(repository, item)
    if existing is not None:
        if not request.include_existing:
            result.skipped_existing += 1
        result.items.append(existing)
        return
    persisted = repository.upsert_source_followup(item)
    result.items.append(persisted)
    result.queued += 1 if existing is None else 0


def _queue_pmc_metadata_fallbacks(
    repository: ResearchRepository,
    item: SourceFollowupQueueItem,
    *,
    approved_by: str | None = None,
) -> list[SourceFollowupQueueItem]:
    if item.source_key != "pmc_oa" or item.identifier_type != "pmcid":
        return []
    metadata = _pmc_idconv_metadata(item.identifier)
    if not metadata:
        return []
    fallback_items: list[SourceFollowupQueueItem] = []
    raw_pmid = metadata.get("pmid")
    raw_doi = metadata.get("doi")
    if raw_pmid:
        fallback_items.append(
            SourceFollowupQueueItem(
                source_key="pubmed",
                identifier_type="pmid",
                identifier=str(raw_pmid),
                url=f"https://pubmed.ncbi.nlm.nih.gov/{raw_pmid}/",
                title=item.title,
                origin_source_key=item.source_key,
                origin_review_id=item.origin_review_id,
                origin_artifact_id=item.origin_artifact_id,
                origin_agent_run_id=item.origin_agent_run_id,
                reason="PMC OA full text unavailable; PubMed metadata fallback from NCBI ID Converter.",
                priority=item.priority + 10,
                metadata={
                    **item.metadata,
                    "fallback_from_source_key": item.source_key,
                    "fallback_from_identifier": item.identifier,
                    "fallback_type": "pmc_idconv_pubmed",
                    "approved_by": approved_by,
                },
            )
        )
    if raw_doi:
        fallback_items.append(
            SourceFollowupQueueItem(
                source_key="crossref",
                identifier_type="doi",
                identifier=str(raw_doi),
                url=f"https://doi.org/{raw_doi}",
                title=item.title,
                origin_source_key=item.source_key,
                origin_review_id=item.origin_review_id,
                origin_artifact_id=item.origin_artifact_id,
                origin_agent_run_id=item.origin_agent_run_id,
                reason="PMC OA full text unavailable; Crossref DOI metadata fallback from NCBI ID Converter.",
                priority=item.priority + 10,
                metadata={
                    **item.metadata,
                    "fallback_from_source_key": item.source_key,
                    "fallback_from_identifier": item.identifier,
                    "fallback_type": "pmc_idconv_crossref",
                    "approved_by": approved_by,
                },
            )
        )
    persisted_items: list[SourceFollowupQueueItem] = []
    for fallback in fallback_items:
        existing = _find_existing_followup(repository, fallback)
        persisted_items.append(existing or repository.upsert_source_followup(fallback))
    return persisted_items


def _pmc_idconv_metadata(pmcid: str) -> dict[str, Any] | None:
    from .harvesters_v2 import _get_json

    data = _get_json(
        "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/",
        {
            "ids": pmcid,
            "format": "json",
            "tool": "hsa-dagster",
            "email": "poppa@bradyandgraffiti.com",
        },
    )
    records = data.get("records") if isinstance(data, dict) else None
    if not isinstance(records, list) or not records:
        return None
    record = records[0]
    return record if isinstance(record, dict) else None


def _parse_uuid(value: Any) -> UUID | None:
    text = _optional_str(value)
    if text is None:
        return None
    try:
        return UUID(text)
    except ValueError:
        return None


def _doi_from_research_object(research_object: ResearchObject) -> str | None:
    raw_doi = research_object.identifiers.get("doi") or research_object.identifiers.get("DOI")
    doi = _normalize_doi(raw_doi)
    if doi and _DOI_PATTERN.match(doi):
        return doi
    return None


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
            curation = curate_claims_for_repository(
                repository,
                ClaimCurationRequest(source_key=source_key, limit=500),
            )
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


def _normalize_doi(value: str | None) -> str | None:
    if value is None:
        return None
    doi = str(value).strip()
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi, flags=re.I)
    doi = doi.split("?", 1)[0].split("#", 1)[0].rstrip(").,;:&")
    for suffix in ("/full", "/abstract", "/pdf", "/epdf", "/html"):
        if doi.lower().endswith(suffix):
            doi = doi[: -len(suffix)]
            break
    doi = doi.strip().strip(".")
    return doi.lower() or None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
