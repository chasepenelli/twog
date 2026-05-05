"""Escalate evaluator findings into durable follow-up work."""

from __future__ import annotations

from collections.abc import Mapping
import re
from typing import Any
from uuid import UUID

from .contracts import (
    AgentFindingEscalationRequest,
    AgentFindingEscalationResult,
    AgentRunRecord,
    AgentRunReviewRecord,
    ResearchLeadRecord,
    ResearchObjectType,
    SourceQuery,
)
from .repository import ResearchRepository


AGENT_FINDING_ESCALATION_AGENT_NAME = "agent_finding_escalation_agent"
AGENT_FINDING_ESCALATION_AGENT_VERSION = "v1"
AGENT_FINDING_LANE = "agent_evaluator_followup"
AGENT_FINDING_TRACK = "validation_gap"
_DEFAULT_LITERATURE_SOURCES = ("pubmed", "europe_pmc", "openalex")
_CANINE_SOURCES = ("icdc",)
_SAFETY_SOURCES = ("openfda_animal_events",)
_CLINICAL_TRIAL_SOURCES = ("clinicaltrials_gov",)
_SKIPPED_AUTOMATIC_SOURCES = {"avma_vctr"}


def escalate_agent_findings(
    repository: ResearchRepository,
    request: AgentFindingEscalationRequest,
) -> AgentFindingEscalationResult:
    """Convert bad/needs-followup evaluator reviews into leads and source queries."""

    result = AgentFindingEscalationResult(
        dry_run=request.dry_run,
        agent_name=AGENT_FINDING_ESCALATION_AGENT_NAME,
        agent_version=AGENT_FINDING_ESCALATION_AGENT_VERSION,
    )
    existing_lead_keys = {
        lead.identity_key
        for lead in repository.list_research_leads(limit=None)
        if lead.identity_key
    }
    existing_query_keys = {
        (query.source_key, query.query_name)
        for query in repository.list_source_queries(active_only=False)
    }
    reviews = _select_reviews(repository, request)
    result.scanned_count = len(reviews)
    for review in reviews:
        if review.verdict not in set(request.verdicts):
            result.skipped.append(
                {
                    "review_id": str(review.review_id),
                    "agent_run_id": str(review.agent_run_id),
                    "reason": "verdict_not_selected",
                    "verdict": review.verdict,
                }
            )
            continue
        run = repository.get_agent_run(review.agent_run_id)
        if run is None:
            result.skipped.append(
                {
                    "review_id": str(review.review_id),
                    "agent_run_id": str(review.agent_run_id),
                    "reason": "agent_run_missing",
                }
            )
            continue

        context = _finding_context(run, review)
        source_keys = _source_keys(request, run, review, context)
        result.escalated_count += 1

        if request.create_research_leads:
            lead = _build_research_lead(run, review, context, source_keys, request)
            if not request.dry_run:
                is_new = lead.identity_key not in existing_lead_keys
                lead = repository.upsert_research_lead(lead)
                if is_new:
                    result.research_leads_created += 1
                    if lead.identity_key:
                        existing_lead_keys.add(lead.identity_key)
            result.research_leads.append(lead)

        if request.create_source_queries:
            queries = _build_source_queries(run, review, context, source_keys, request)
            if not request.dry_run:
                persisted_queries: list[SourceQuery] = []
                for query in queries:
                    is_new = (query.source_key, query.query_name) not in existing_query_keys
                    persisted = repository.upsert_source_query(query)
                    persisted_queries.append(persisted)
                    if is_new:
                        result.source_queries_created += 1
                        existing_query_keys.add((query.source_key, query.query_name))
                queries = persisted_queries
            result.source_queries.extend(queries)

    return result


def summarize_agent_finding_escalation(result: AgentFindingEscalationResult) -> dict[str, Any]:
    return {
        "scanned_count": result.scanned_count,
        "escalated_count": result.escalated_count,
        "research_leads_created": result.research_leads_created,
        "source_queries_created": result.source_queries_created,
        "dry_run": result.dry_run,
    }


def _select_reviews(
    repository: ResearchRepository,
    request: AgentFindingEscalationRequest,
) -> list[AgentRunReviewRecord]:
    if request.review_ids:
        reviews = [
            review
            for review_id in request.review_ids
            if (review := repository.get_agent_run_review(review_id)) is not None
        ]
        return reviews[: request.limit]

    selected_run_ids = set(request.agent_run_ids)
    reviews = repository.list_agent_run_reviews(limit=max(request.limit * 20, 100))
    latest_by_run: dict[UUID, AgentRunReviewRecord] = {}
    for review in reviews:
        if review.reviewer_type != "llm_evaluator":
            continue
        if selected_run_ids and review.agent_run_id not in selected_run_ids:
            continue
        current = latest_by_run.get(review.agent_run_id)
        if current is None or review.created_at > current.created_at:
            latest_by_run[review.agent_run_id] = review
    return [
        review
        for review in latest_by_run.values()
        if review.verdict in set(request.verdicts)
    ][: request.limit]


def _build_research_lead(
    run: AgentRunRecord,
    review: AgentRunReviewRecord,
    context: dict[str, Any],
    source_keys: list[str],
    request: AgentFindingEscalationRequest,
) -> ResearchLeadRecord:
    title = context.get("title") or _short_text(run.summary.get("topic")) or run.agent_name
    readable_title = f"Evaluator follow-up: {title}"
    priority = 5 if review.verdict == "bad" else 25
    evidence_refs = [
        f"agent_run:{run.agent_run_id}",
        f"agent_review:{review.review_id}",
        *[str(ref) for ref in context.get("evidence_refs", [])[:10]],
    ]
    return ResearchLeadRecord(
        identity_key=f"research_lead:agent_evaluator:{run.agent_run_id}:{review.review_id}",
        title=readable_title[:240],
        lead_type="unknown",
        status="followup",
        priority=priority,
        source_key=(source_keys[0] if source_keys else run.source_key),
        origin_source_key=run.source_key,
        origin_record_id=str(context["lead_id"]) if context.get("lead_id") else None,
        origin_review_id=review.review_id,
        origin_agent_run_id=run.agent_run_id,
        reason=review.feedback or f"Evaluator marked run {review.verdict}.",
        summary=_lead_summary(run, review, context),
        evidence_refs=evidence_refs,
        topic_tags=_topic_tags(run, review, context),
        suggested_sources=source_keys,
        metadata={
            "created_by": AGENT_FINDING_ESCALATION_AGENT_NAME,
            "operator": request.operator,
            "reviewer": review.reviewer,
            "reviewer_type": review.reviewer_type,
            "verdict": review.verdict,
            "confidence": review.metadata.get("confidence"),
            "followup_actions": review.followup_actions,
            "query_text": _query_text(run, review, context),
            "source_keys": source_keys,
            "dry_run": request.dry_run,
            **request.metadata,
        },
    )


def _build_source_queries(
    run: AgentRunRecord,
    review: AgentRunReviewRecord,
    context: dict[str, Any],
    source_keys: list[str],
    request: AgentFindingEscalationRequest,
) -> list[SourceQuery]:
    query_text = _query_text(run, review, context)
    name_slug = _query_name_slug(query_text)
    queries: list[SourceQuery] = []
    for source_key in source_keys:
        if source_key in _SKIPPED_AUTOMATIC_SOURCES:
            continue
        queries.append(
            SourceQuery(
                source_key=source_key,
                query_name=f"agent_eval_{str(review.review_id)[:8]}_{name_slug}",
                query_text=query_text,
                query_params=_query_params(source_key, run, review, request),
                track=AGENT_FINDING_TRACK,
                object_type=_object_type_for_source(source_key),
                active=True,
            )
        )
    return queries


def _finding_context(run: AgentRunRecord, review: AgentRunReviewRecord) -> dict[str, Any]:
    payloads = [run.output_payload, run.input_payload, run.summary, run.metadata]
    lead_results = _find_list(run.output_payload, "lead_results")
    first_lead = lead_results[0] if lead_results and isinstance(lead_results[0], Mapping) else {}
    lead_id = first_lead.get("lead_id") or _first_value(payloads, ("lead_id", "lead_ids"))
    evidence_refs = _first_list(payloads, "evidence_refs")
    durable_sources = _first_list(payloads, "durable_source_keys")
    title = first_lead.get("title") or _first_value(payloads, ("title", "topic", "objective", "query"))
    actions_text = " ".join([*review.followup_actions, review.feedback or ""])
    return {
        "lead_id": lead_id,
        "title": _short_text(title),
        "evidence_refs": evidence_refs,
        "durable_source_keys": durable_sources,
        "actions_text": actions_text,
    }


def _source_keys(
    request: AgentFindingEscalationRequest,
    run: AgentRunRecord,
    review: AgentRunReviewRecord,
    context: dict[str, Any],
) -> list[str]:
    if request.source_keys:
        return _dedupe_sources(request.source_keys)

    text = _haystack(run, review, context)
    sources = [*_DEFAULT_LITERATURE_SOURCES]
    if re.search(r"\b(trial|clinical|phase|nct)\b", text):
        sources.extend(_CLINICAL_TRIAL_SOURCES)
    if re.search(r"\b(canine|dog|veterinary|comparative oncology|hsa|hemangiosarcoma)\b", text):
        sources.extend(_CANINE_SOURCES)
    if re.search(r"\b(safety|toxicity|toxicology|adverse|tolerability|dlt|mtd|dose limiting)\b", text):
        sources.extend(_SAFETY_SOURCES)
    if run.source_key:
        sources.append(run.source_key)
    sources.extend(str(source) for source in context.get("durable_source_keys", []) if source)
    return [source for source in _dedupe_sources(sources) if source not in _SKIPPED_AUTOMATIC_SOURCES]


def _query_text(run: AgentRunRecord, review: AgentRunReviewRecord, context: dict[str, Any]) -> str:
    actions = " ".join(review.followup_actions)
    candidates: list[str] = []
    candidates.extend(_quoted_terms(actions))
    text = _haystack(run, review, context)
    if "sorafenib" in text:
        candidates.extend(
            [
                "sorafenib canine maximum tolerated dose",
                "sorafenib veterinary phase i dog",
                "robat sorafenib dog",
            ]
        )
    if re.search(r"\b(dlt|mtd|dose limiting|maximum tolerated)\b", text):
        candidates.append("canine dose limiting toxicity maximum tolerated dose")
    if re.search(r"\b(hemangiosarcoma|hsa|angiosarcoma)\b", text):
        candidates.append("canine hemangiosarcoma therapy translational oncology")
    if context.get("title"):
        candidates.append(str(context["title"]))
    if review.feedback:
        candidates.append(review.feedback)
    terms = [_clean_query_term(term) for term in candidates]
    terms = [term for term in _dedupe_strings(terms) if term]
    if not terms:
        terms = [_clean_query_term(run.agent_name)]
    return " OR ".join(terms[:4])[:1000]


def _query_params(
    source_key: str,
    run: AgentRunRecord,
    review: AgentRunReviewRecord,
    request: AgentFindingEscalationRequest,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "followup_lane": AGENT_FINDING_LANE,
        "origin_agent_run_id": str(run.agent_run_id),
        "origin_review_id": str(review.review_id),
        "verdict": review.verdict,
        "operator": request.operator,
    }
    if source_key == "clinicaltrials_gov":
        params["search_area"] = "term"
    if source_key == "openfda_animal_events":
        params["species"] = "Dog"
    if source_key == "icdc":
        params["diagnosis"] = ["Hemangiosarcoma"]
    return params


def _object_type_for_source(source_key: str) -> ResearchObjectType:
    if source_key == "clinicaltrials_gov":
        return ResearchObjectType.CLINICAL_TRIAL
    if source_key == "icdc":
        return ResearchObjectType.DATASET
    if source_key == "openfda_animal_events":
        return ResearchObjectType.SAFETY_REPORT
    return ResearchObjectType.PUBLICATION


def _lead_summary(run: AgentRunRecord, review: AgentRunReviewRecord, context: dict[str, Any]) -> str:
    parts = [
        f"LLM evaluator marked {run.agent_name} as {review.verdict}.",
        review.feedback or "No evaluator feedback recorded.",
    ]
    if context.get("actions_text"):
        parts.append(f"Next actions: {_human_text(str(context['actions_text']))[:500]}")
    return " ".join(parts)[:1200]


def _topic_tags(run: AgentRunRecord, review: AgentRunReviewRecord, context: dict[str, Any]) -> list[str]:
    tags = ["agent_evaluator", review.verdict, run.agent_name]
    text = _haystack(run, review, context)
    for token in (
        "sorafenib",
        "canine",
        "hemangiosarcoma",
        "angiosarcoma",
        "safety",
        "mtd",
        "dlt",
        "omics",
        "manual_research",
    ):
        if token in text:
            tags.append(token)
    return tags


def _haystack(run: AgentRunRecord, review: AgentRunReviewRecord, context: dict[str, Any]) -> str:
    return _human_text(
        " ".join(
            [
                run.agent_name,
                str(run.summary),
                str(run.input_payload),
                str(run.output_payload),
                str(run.metadata),
                review.feedback or "",
                " ".join(review.followup_actions),
                str(context.get("title") or ""),
            ]
        )
    ).lower()


def _quoted_terms(value: str) -> list[str]:
    terms: list[str] = []
    for single, double in re.findall(r"'([^']+)'|\"([^\"]+)\"", value):
        terms.append(single or double)
    return terms


def _clean_query_term(value: Any) -> str:
    text = _human_text(str(value))
    text = re.sub(r"\b(rerun search with refined terms|manually ingest known|if not already indexed)\b", "", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip(" ;,._")
    return text[:220]


def _human_text(value: str) -> str:
    return re.sub(r"_+", " ", value).replace("\\n", " ").strip()


def _short_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        value = value[0] if value else None
    if value is None:
        return None
    text = _human_text(str(value))
    return text[:240] if text else None


def _query_name_slug(value: str) -> str:
    slug = "-".join(re.findall(r"[a-z0-9]+", value.lower()))[:48]
    return slug or "followup"


def _dedupe_sources(values: list[str] | tuple[str, ...]) -> list[str]:
    return [
        source
        for source in _dedupe_strings([str(value).strip().lower() for value in values])
        if source
    ]


def _dedupe_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        key = normalized.casefold()
        if not normalized or key in seen:
            continue
        deduped.append(normalized)
        seen.add(key)
    return deduped


def _find_list(payload: Mapping[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    if isinstance(value, list):
        return value
    return []


def _first_list(payloads: list[Mapping[str, Any]], key: str) -> list[Any]:
    for payload in payloads:
        value = _find_value(payload, key)
        if isinstance(value, list):
            return value
    return []


def _first_value(payloads: list[Mapping[str, Any]], keys: tuple[str, ...]) -> Any | None:
    for payload in payloads:
        for key in keys:
            value = _find_value(payload, key)
            if value not in (None, "", []):
                return value
    return None


def _find_value(value: Any, key: str) -> Any | None:
    if isinstance(value, Mapping):
        if key in value:
            return value[key]
        for nested in value.values():
            found = _find_value(nested, key)
            if found is not None:
                return found
    elif isinstance(value, list):
        for item in value[:25]:
            found = _find_value(item, key)
            if found is not None:
                return found
    return None
