"""Controlled follow-up lane for articles discovered through X monitoring."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .contracts import (
    AgentRunRecord,
    ScrapeFetchRequest,
    ScrapeProfileReviewRequest,
    XLinkedArticleFollowupRequest,
    XLinkedArticleFollowupResult,
)
from .repository import ResearchRepository
from .scraper_bridge import ScrapeBridge
from .x_topic_review import X_TOPIC_REVIEW_AGENT_NAME


X_LINKED_ARTICLE_SOURCE_KEY = "x_linked_article"


def run_x_linked_article_followup(
    repository: ResearchRepository,
    request: XLinkedArticleFollowupRequest,
) -> XLinkedArticleFollowupResult:
    """Fetch and parse credible article links queued by the X review agent.

    This lane never treats the social post as evidence. It only snapshots the
    linked article under scraper controls and extracts any primary-source links
    that can be sent back through PubMed, Crossref, PMC, or ClinicalTrials.gov.
    """

    recent_runs = repository.list_agent_runs(
        agent_name=X_TOPIC_REVIEW_AGENT_NAME,
        status="completed",
        source_key="x_topic_monitor",
        limit=request.recent_run_limit,
    )
    candidate_urls = _dedupe_urls(
        [
            *request.urls,
            *[
                url
                for run in recent_runs
                for url in _queued_article_urls(run)
            ],
        ]
    )[: request.max_urls]
    result = XLinkedArticleFollowupResult(
        candidate_urls=candidate_urls,
        candidate_results=_candidate_results_for_status(candidate_urls, "pending"),
        agent_run_ids=[run.agent_run_id for run in recent_runs],
        metadata={
            "direct_url_count": len(request.urls),
            "recent_run_limit": request.recent_run_limit,
            "fetch_requested": request.fetch,
            "parse_requested": request.parse,
        },
    )
    if not candidate_urls:
        return result
    if request.fetch and not request.approved_by:
        return result.model_copy(
            update={
                "requires_fetch_approval": True,
                "candidate_results": _candidate_results_for_status(
                    candidate_urls,
                    "requires_fetch_approval",
                    reason="Explicit approval is required before fetching X-linked article URLs.",
                ),
            }
        )
    if not request.fetch:
        return result.model_copy(
            update={
                "candidate_results": _candidate_results_for_status(
                    candidate_urls,
                    "fetch_skipped",
                    reason="Fetch was disabled for this follow-up run.",
                )
            }
        )

    bridge = ScrapeBridge(repository)
    bridge.review_profile(
        ScrapeProfileReviewRequest(
            source_key=X_LINKED_ARTICLE_SOURCE_KEY,
            robots_policy=request.robots_policy,
            approved_for_fetch=True,
            reviewed_by=request.approved_by or "system",
            review_note=request.approval_note,
        )
    )
    fetch = bridge.fetch(
        ScrapeFetchRequest(
            source_key=X_LINKED_ARTICLE_SOURCE_KEY,
            urls=candidate_urls,
            max_pages=len(candidate_urls),
            approved_by=request.approved_by,
            approval_note=request.approval_note,
        )
    )
    parsed_records = []
    review_ids = []
    primary_source_links: list[dict[str, Any]] = []
    parse_errors: list[str] = []
    if request.parse and fetch.artifact_ids:
        parse = bridge.parse(X_LINKED_ARTICLE_SOURCE_KEY, limit=len(fetch.artifact_ids))
        parsed_records = parse.records
        review_ids = parse.review_ids
        parse_errors = parse.errors
        primary_source_links = _dedupe_primary_source_links(
            link
            for record in parsed_records
            for link in record.fields.get("primary_source_links", [])
            if isinstance(link, dict)
        )

    candidate_results = _candidate_results_from_fetch(
        repository,
        urls=candidate_urls,
        artifact_ids=fetch.artifact_ids,
        fetch_errors=fetch.errors,
        parse_requested=request.parse,
        parsed_records=parsed_records,
        review_ids=review_ids,
        parse_errors=parse_errors,
    )
    return result.model_copy(
        update={
            "fetched_pages": fetch.fetched_pages,
            "skipped_pages": fetch.skipped_pages,
            "artifact_ids": fetch.artifact_ids,
            "parsed_records": len(parsed_records),
            "review_ids": review_ids,
            "primary_source_links": primary_source_links,
            "candidate_results": candidate_results,
            "errors": [*fetch.errors, *parse_errors],
        }
    )


def _queued_article_urls(run: AgentRunRecord) -> list[str]:
    urls: list[str] = []
    for action in run.output_payload.get("actions", []):
        if not isinstance(action, dict):
            continue
        if action.get("action") != "queue_source_followup":
            continue
        for link in action.get("ingestible_links", []):
            if not isinstance(link, dict):
                continue
            if link.get("recommended_source_key") != X_LINKED_ARTICLE_SOURCE_KEY:
                continue
            url = link.get("url")
            if isinstance(url, str) and url.strip():
                urls.append(url.strip())
    return urls


def _dedupe_urls(urls: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for url in urls:
        normalized = url.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _dedupe_primary_source_links(links: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for link in links:
        key = (
            str(link.get("identifier_type") or "unknown"),
            str(link.get("identifier") or link.get("url") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(link)
    return deduped


def _candidate_results_for_status(
    urls: Iterable[str],
    status: str,
    *,
    reason: str | None = None,
) -> list[dict[str, Any]]:
    return [
        {
            "url": url,
            "status": status,
            **({"reason": reason} if reason else {}),
        }
        for url in urls
    ]


def _candidate_results_from_fetch(
    repository: ResearchRepository,
    *,
    urls: list[str],
    artifact_ids: list[Any],
    fetch_errors: list[str],
    parse_requested: bool,
    parsed_records: list[Any],
    review_ids: list[Any],
    parse_errors: list[str],
) -> list[dict[str, Any]]:
    artifacts_by_url: dict[str, Any] = {}
    artifacts_by_id: dict[str, Any] = {}
    for artifact_id in artifact_ids:
        artifact = repository.get_artifact(artifact_id)
        if artifact is None:
            continue
        artifacts_by_id[str(artifact.artifact_id)] = artifact
        for url_key in (artifact.metadata.get("requested_url"), artifact.metadata.get("source_url")):
            if isinstance(url_key, str) and url_key.strip():
                artifacts_by_url[url_key.strip()] = artifact

    parsed_by_artifact: dict[str, Any] = {}
    review_by_artifact: dict[str, Any] = {}
    for record, review_id in zip(parsed_records, review_ids, strict=False):
        if record.artifact_id is None:
            continue
        artifact_key = str(record.artifact_id)
        parsed_by_artifact[artifact_key] = record
        review_by_artifact[artifact_key] = review_id

    results: list[dict[str, Any]] = []
    for url in urls:
        artifact = artifacts_by_url.get(url)
        fetch_error = _error_for_url(fetch_errors, url)
        if artifact is None:
            results.append(
                {
                    "url": url,
                    "status": "fetch_failed" if fetch_error else "skipped",
                    **({"error": fetch_error} if fetch_error else {}),
                }
            )
            continue

        artifact_key = str(artifact.artifact_id)
        parsed_record = parsed_by_artifact.get(artifact_key)
        parse_error = _error_for_artifact(parse_errors, artifact_key)
        if parsed_record is not None:
            review_id = review_by_artifact.get(artifact_key)
            primary_links = [
                link
                for link in parsed_record.fields.get("primary_source_links", [])
                if isinstance(link, dict)
            ]
            results.append(
                {
                    "url": url,
                    "status": "parsed",
                    "artifact_id": artifact_key,
                    "review_id": str(review_id) if review_id else None,
                    "source_url": artifact.metadata.get("source_url"),
                    "primary_source_link_count": len(primary_links),
                    "primary_source_links": primary_links,
                }
            )
            continue

        status = "parse_failed" if parse_error else ("fetched_unparsed" if parse_requested else "fetched")
        results.append(
            {
                "url": url,
                "status": status,
                "artifact_id": artifact_key,
                "source_url": artifact.metadata.get("source_url"),
                **({"error": parse_error} if parse_error else {}),
            }
        )
    return results


def _error_for_url(errors: Iterable[str], url: str) -> str | None:
    for error in errors:
        if url in error:
            return error
    return None


def _error_for_artifact(errors: Iterable[str], artifact_id: str) -> str | None:
    for error in errors:
        if artifact_id in error:
            return error
    return None
