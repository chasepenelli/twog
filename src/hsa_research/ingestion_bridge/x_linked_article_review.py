"""Recommend-only agent for linked articles discovered from social monitoring."""

from __future__ import annotations

from collections.abc import Mapping
import json
import os
import re
from typing import Any
from uuid import UUID
import urllib.error
import urllib.request

from .contracts import (
    XLinkedArticleReviewAction,
    XLinkedArticleReviewRequest,
    XLinkedArticleReviewResult,
    XTopicLinkedSource,
)
from .repository import ResearchRepository
from .x_linked_article_followup import X_LINKED_ARTICLE_SOURCE_KEY


X_LINKED_ARTICLE_REVIEW_AGENT_NAME = "x_linked_article_review_agent"
X_LINKED_ARTICLE_REVIEW_AGENT_VERSION = "v1"
DEFAULT_X_LINKED_ARTICLE_REVIEW_MODEL = "~anthropic/claude-sonnet-latest"
DEFAULT_X_LINKED_ARTICLE_COMPARE_MODELS = (DEFAULT_X_LINKED_ARTICLE_REVIEW_MODEL,)
_ALLOWED_ACTIONS = {
    "queue_primary_source_followup",
    "needs_human_review",
    "reject_low_signal",
}
_ALLOWED_SEVERITIES = {"info", "watch", "blocking"}


class XLinkedArticleReviewAgent:
    """Review parsed article pages and recommend primary-source follow-ups."""

    agent_name = X_LINKED_ARTICLE_REVIEW_AGENT_NAME
    agent_version = X_LINKED_ARTICLE_REVIEW_AGENT_VERSION

    def __init__(self, repository: ResearchRepository) -> None:
        self.repository = repository

    def run(self, request: XLinkedArticleReviewRequest) -> XLinkedArticleReviewResult:
        reviews = self.repository.list_scrape_reviews(
            source_key=X_LINKED_ARTICLE_SOURCE_KEY,
            review_status=request.review_status,
            review_ids=request.review_ids or None,
            limit=request.limit,
        )
        deterministic_result = _finalize_result(
            XLinkedArticleReviewResult(
                model_profile=request.model_profile,
                actions=[_review_article_record(review) for review in reviews],
                evidence={
                    "review_mode": request.review_mode,
                    "deterministic_floor": True,
                    "review_records": len(reviews),
                },
            )
        )
        if request.review_mode == "deterministic_only":
            return deterministic_result

        review_payload = _build_review_payload(request, reviews, deterministic_result)
        if request.review_mode in {"openrouter_required", "openrouter_compare"}:
            return _run_openrouter_reviews(request, deterministic_result, review_payload)

        return _external_review_required_result(request, deterministic_result, review_payload)


def _review_article_record(review) -> XLinkedArticleReviewAction:
    primary_links = [
        _linked_source_from_payload(link)
        for link in review.fields.get("primary_source_links", [])
        if isinstance(link, Mapping)
    ]
    primary_links = [link for link in primary_links if link is not None and link.should_ingest]
    evidence_refs = [f"scrape_review:{review.review_id}"]
    if review.canonical_url:
        evidence_refs.append(review.canonical_url)

    if primary_links:
        return XLinkedArticleReviewAction(
            review_id=review.review_id,
            source_record_id=review.source_record_id,
            action="queue_primary_source_followup",
            severity="watch",
            reason="Parsed linked article exposes durable primary-source identifiers for API ingestion.",
            followup_links=primary_links,
            evidence_refs=evidence_refs + [link.url for link in primary_links],
            metadata={
                "parser_confidence": review.parser_confidence,
                "title": review.title,
                "canonical_url": review.canonical_url,
            },
        )

    if review.parser_confidence < 0.25:
        return XLinkedArticleReviewAction(
            review_id=review.review_id,
            source_record_id=review.source_record_id,
            action="reject_low_signal",
            severity="info",
            reason="Parser confidence is low and no primary-source identifiers were found.",
            followup_links=[],
            evidence_refs=evidence_refs,
            metadata={"parser_confidence": review.parser_confidence, "title": review.title},
        )

    return XLinkedArticleReviewAction(
        review_id=review.review_id,
        source_record_id=review.source_record_id,
        action="needs_human_review",
        severity="watch",
        reason="Article parsed cleanly enough to inspect, but no durable primary-source identifier was found.",
        followup_links=[],
        evidence_refs=evidence_refs,
        metadata={"parser_confidence": review.parser_confidence, "title": review.title},
    )


def _linked_source_from_payload(raw_link: Mapping[str, Any]) -> XTopicLinkedSource | None:
    url = _optional_str(raw_link.get("url"))
    if url is None:
        return None
    return XTopicLinkedSource(
        url=url,
        recommended_source_key=_optional_str(raw_link.get("recommended_source_key")),
        identifier_type=str(raw_link.get("identifier_type") or "unknown"),  # type: ignore[arg-type]
        identifier=_optional_str(raw_link.get("identifier")),
        should_ingest=bool(raw_link.get("should_ingest")),
        reason=_optional_str(raw_link.get("reason")) or "Primary source link from parsed article.",
        metadata=raw_link.get("metadata") if isinstance(raw_link.get("metadata"), dict) else {},
    )


def _finalize_result(result: XLinkedArticleReviewResult) -> XLinkedArticleReviewResult:
    return result.model_copy(
        update={
            "queue_candidate_count": sum(
                1 for action in result.actions if action.action == "queue_primary_source_followup"
            ),
            "needs_human_review_count": sum(
                1 for action in result.actions if action.action == "needs_human_review"
            ),
            "rejected_count": sum(1 for action in result.actions if action.action == "reject_low_signal"),
        }
    )


def _build_review_payload(
    request: XLinkedArticleReviewRequest,
    reviews: list[Any],
    deterministic_result: XLinkedArticleReviewResult,
) -> dict[str, Any]:
    return {
        "task": "Review parsed linked articles and decide whether primary-source identifiers should be queued.",
        "rules": [
            "The linked article is discovery context, not evidence.",
            "Queue only durable primary-source links that map to implemented API harvesters.",
            "Use needs_human_review when parser output may matter but lacks a durable identifier.",
            "Use reject_low_signal when the parser output is weak and there are no useful primary-source links.",
            "Return JSON only.",
        ],
        "allowed_actions": sorted(_ALLOWED_ACTIONS),
        "allowed_identifier_types": ["doi", "pmid", "pmcid", "nct", "unknown"],
        "output_shape": {
            "actions": [
                {
                    "review_id": "scrape review UUID",
                    "source_record_id": "parsed article source record id",
                    "action": "queue_primary_source_followup",
                    "severity": "info|watch|blocking",
                    "reason": "short reason",
                    "followup_links": [
                        {
                            "url": "durable URL",
                            "recommended_source_key": "pubmed|pmc_oa|crossref|clinicaltrials_gov",
                            "identifier_type": "doi|pmid|pmcid|nct",
                            "identifier": "identifier",
                            "should_ingest": True,
                            "reason": "why this should be harvested",
                            "metadata": {},
                        }
                    ],
                    "evidence_refs": ["scrape_review:<id>", "url"],
                    "metadata": {},
                }
            ],
            "evidence": {"review_summary": "short summary"},
            "errors": [],
        },
        "articles": [_compact_review(review) for review in reviews],
        "deterministic_result": deterministic_result.model_dump(mode="json"),
        "metadata": request.metadata,
    }


def _compact_review(review) -> dict[str, Any]:
    fields = review.fields or {}
    return {
        "review_id": str(review.review_id),
        "source_record_id": review.source_record_id,
        "title": review.title,
        "canonical_url": review.canonical_url,
        "parser_confidence": review.parser_confidence,
        "review_status": review.review_status,
        "primary_source_links": fields.get("primary_source_links", []),
        "article_metadata": fields.get("article_metadata", {}),
    }


def _external_review_required_result(
    request: XLinkedArticleReviewRequest,
    deterministic_result: XLinkedArticleReviewResult,
    review_payload: dict[str, Any],
) -> XLinkedArticleReviewResult:
    actions = list(deterministic_result.actions)
    actions.append(
        XLinkedArticleReviewAction(
            review_id=UUID("00000000-0000-0000-0000-000000000000"),
            source_record_id="external_review",
            action="needs_human_review",
            severity="watch",
            reason="External model review is required before linked article follow-ups are queued.",
            evidence_refs=["review_payload"],
            metadata={"review_payload": review_payload},
        )
    )
    return _finalize_result(
        deterministic_result.model_copy(
            update={
                "actions": actions,
                "evidence": {
                    **deterministic_result.evidence,
                    "review_mode": request.review_mode,
                    "external_review_required": True,
                    "review_payload": review_payload,
                },
            }
        )
    )


def _run_openrouter_reviews(
    request: XLinkedArticleReviewRequest,
    deterministic_result: XLinkedArticleReviewResult,
    review_payload: dict[str, Any],
) -> XLinkedArticleReviewResult:
    reviews = []
    errors = []
    selected: XLinkedArticleReviewResult | None = None

    for model_name in _review_models(request):
        try:
            review = _openrouter_review_model(model_name, review_payload)
            payload = _parse_json_object(review["text"])
            result = _result_from_payload(request, payload)
            result = _apply_deterministic_guardrails(result, deterministic_result)
            selected = result
            reviews.append(
                {
                    "model_name": model_name,
                    "status": "completed",
                    "resolved_model": review["metadata"].get("model_name"),
                    "usage": review["metadata"].get("usage", {}),
                    "action_count": len(result.actions),
                    "queue_candidate_count": result.queue_candidate_count,
                }
            )
            if request.review_mode == "openrouter_required":
                break
        except Exception as exc:
            errors.append(f"{model_name}: {exc}")
            reviews.append({"model_name": model_name, "status": "failed", "error": str(exc)})

    if selected is None:
        if request.review_mode == "openrouter_required":
            raise RuntimeError(f"OpenRouter linked-article review failed for all models: {errors}")
        selected = deterministic_result

    return _finalize_result(
        selected.model_copy(
            update={
                "evidence": {
                    **selected.evidence,
                    "review_mode": request.review_mode,
                    "model_reviews": reviews,
                    "openrouter_errors": errors,
                },
                "errors": [*selected.errors, *errors],
            }
        )
    )


def _review_models(request: XLinkedArticleReviewRequest) -> list[str]:
    if request.review_models:
        return request.review_models
    configured = os.getenv("HSA_X_LINKED_ARTICLE_REVIEW_MODELS")
    if configured:
        return [model.strip() for model in configured.split(",") if model.strip()]
    if request.review_mode == "openrouter_compare":
        return list(DEFAULT_X_LINKED_ARTICLE_COMPARE_MODELS)
    return [os.getenv("HSA_X_LINKED_ARTICLE_REVIEW_MODEL", DEFAULT_X_LINKED_ARTICLE_REVIEW_MODEL)]


def _openrouter_review_model(model_name: str, review_payload: dict[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required for OpenRouter linked-article review.")
    payload = {
        "model": model_name,
        "temperature": float(os.getenv("HSA_X_LINKED_ARTICLE_REVIEW_TEMPERATURE", "0")),
        "max_tokens": int(os.getenv("HSA_X_LINKED_ARTICLE_REVIEW_MAX_TOKENS", "8000")),
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _X_LINKED_ARTICLE_REVIEW_SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(review_payload, sort_keys=True, default=str)},
        ],
    }
    request = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "HTTP-Referer": os.getenv("OPENROUTER_HTTP_REFERER", "https://github.com/chasepenelli/hsa-dagster"),
            "X-Title": os.getenv("OPENROUTER_APP_TITLE", "hsa-dagster"),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=float(os.getenv("HSA_X_LINKED_ARTICLE_REVIEW_TIMEOUT_SECONDS", "120")),
        ) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenRouter HTTP {error.code}: {body}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"OpenRouter request failed: {error}") from error

    choices = response_payload.get("choices") or []
    if not choices:
        raise RuntimeError(f"OpenRouter response had no choices: {response_payload}")
    text = (choices[0].get("message") or {}).get("content") or ""
    if not text:
        raise RuntimeError(f"OpenRouter response had no text content: {response_payload}")
    return {
        "text": text,
        "metadata": {
            "provider": "openrouter",
            "model_name": response_payload.get("model", model_name),
            "requested_model": model_name,
            "request_id": response_payload.get("id"),
            "usage": response_payload.get("usage", {}),
        },
    }


def _parse_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", stripped, flags=re.DOTALL)
    if fenced:
        stripped = fenced.group(1).strip()
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start < 0 or end <= start:
            raise
        parsed = json.loads(stripped[start : end + 1])
    if not isinstance(parsed, dict):
        raise RuntimeError("OpenRouter model response must be a JSON object.")
    return parsed


def _result_from_payload(
    request: XLinkedArticleReviewRequest,
    payload: Mapping[str, Any],
) -> XLinkedArticleReviewResult:
    raw_actions = payload.get("actions") or payload.get("recommendations") or []
    if not isinstance(raw_actions, list):
        raise RuntimeError("Linked-article review response must include an actions list.")
    actions = [
        action
        for raw_action in raw_actions
        if isinstance(raw_action, Mapping)
        if (action := _sanitize_model_action(raw_action)) is not None
    ]
    return _finalize_result(
        XLinkedArticleReviewResult(
            model_profile=request.model_profile,
            actions=actions,
            evidence=payload.get("evidence", {}) if isinstance(payload.get("evidence"), dict) else {},
            errors=list(payload.get("errors", [])) if isinstance(payload.get("errors"), list) else [],
        )
    )


def _sanitize_model_action(raw_action: Mapping[str, Any]) -> XLinkedArticleReviewAction | None:
    review_id = _parse_uuid(raw_action.get("review_id"))
    source_record_id = _optional_str(raw_action.get("source_record_id"))
    if review_id is None or source_record_id is None:
        return None
    raw_links = raw_action.get("followup_links") or raw_action.get("links") or []
    if not isinstance(raw_links, list):
        raw_links = []
    links = [
        link
        for raw_link in raw_links
        if isinstance(raw_link, Mapping)
        if (link := _linked_source_from_payload(raw_link)) is not None
    ]
    return XLinkedArticleReviewAction(
        review_id=review_id,
        source_record_id=source_record_id,
        action=_allowed_value(raw_action.get("action"), _ALLOWED_ACTIONS, "needs_human_review"),  # type: ignore[arg-type]
        severity=_allowed_value(raw_action.get("severity"), _ALLOWED_SEVERITIES, "watch"),  # type: ignore[arg-type]
        reason=_optional_str(raw_action.get("reason") or raw_action.get("rationale"))
        or "Model review did not provide a reason.",
        followup_links=links,
        evidence_refs=[
            str(ref)
            for ref in raw_action.get("evidence_refs", [])
            if isinstance(ref, str | int | float)
        ],
        metadata=raw_action.get("metadata") if isinstance(raw_action.get("metadata"), dict) else {},
    )


def _apply_deterministic_guardrails(
    model_result: XLinkedArticleReviewResult,
    deterministic_result: XLinkedArticleReviewResult,
) -> XLinkedArticleReviewResult:
    actions = list(model_result.actions)
    existing = {
        (action.review_id, action.action, tuple(sorted(link.url for link in action.followup_links)))
        for action in actions
    }
    for action in deterministic_result.actions:
        if action.action != "queue_primary_source_followup":
            continue
        key = (action.review_id, action.action, tuple(sorted(link.url for link in action.followup_links)))
        if key not in existing:
            actions.append(action)
            existing.add(key)
    return _finalize_result(model_result.model_copy(update={"actions": actions}))


def _parse_uuid(value: Any) -> UUID | None:
    text = _optional_str(value)
    if text is None:
        return None
    try:
        return UUID(text)
    except ValueError:
        return None


def _allowed_value(value: Any, allowed: set[str], default: str) -> str:
    text = str(value or "").strip()
    return text if text in allowed else default


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


_X_LINKED_ARTICLE_REVIEW_SYSTEM_PROMPT = """You are a scientific ingestion-review agent for linked article follow-up.

Your job is to review parsed article-page metadata and decide whether extracted
primary-source identifiers should enter the API ingestion queue.

Rules:
- A linked article is only discovery context, never scientific evidence.
- Queue durable primary sources only when they map to implemented sources:
  pubmed, pmc_oa, crossref, or clinicaltrials_gov.
- Preserve deterministic primary-source links unless there is a clear parser
  or license problem.
- Use needs_human_review for ambiguous parser output.
- Use reject_low_signal for weak pages with no primary-source identifiers.
- Return only one valid JSON object matching the requested output shape.
"""
