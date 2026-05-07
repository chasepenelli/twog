"""Create refined source queries from evaluator follow-up feedback."""

from __future__ import annotations

from collections.abc import Mapping
import re
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from .agent_finding_escalation import AGENT_FINDING_LANE, AGENT_FINDING_TRACK
from .contracts import (
    AgentRunRecord,
    AgentRunReviewRecord,
    ResearchFollowupRefinementRequest,
    ResearchFollowupRefinementResult,
    ResearchLeadRecord,
    ResearchObjectType,
    RunStatus,
    SourceQuery,
)
from .repository import ResearchRepository


RESEARCH_FOLLOWUP_REFINEMENT_AGENT_NAME = "research_followup_refinement_agent"
RESEARCH_FOLLOWUP_REFINEMENT_AGENT_VERSION = "v1"
_DEFAULT_REFINEMENT_SOURCES = ("pubmed", "europe_pmc", "openalex")
_SOURCE_HINTS = {
    "pubmed": ("pubmed", "pmid", "pmids", "medline"),
    "europe_pmc": ("europe pmc", "europepmc", "pmc"),
    "openalex": ("openalex",),
    "clinicaltrials_gov": ("clinical trial", "clinicaltrials", "clinicaltrials.gov", "nct"),
    "openfda_animal_events": ("openfda", "adverse event", "adverse events", "animal event"),
    "icdc": ("icdc", "integrated canine", "canine data commons"),
}
_OPERATIONAL_RECOMMENDATION_PATTERNS = (
    r"\b(?:increase|raise|expand|bump)\s+(?:the\s+)?(?:search\s+)?limit\b",
    r"\b(?:add|enable|wire|implement|include)\s+(?:new\s+)?source(?:\s+keys?)?\b",
    r"\b(?:avma|conference\s+abstracts?|cab\s+abstracts?|vetmed|veterinary\s+databases?)\b",
    r"\b(?:manual|manually|human\s+review|operator\s+review|inspect|curator\s+review)\b",
    r"\b(?:check|determine|verify)\s+(?:whether|if)\b",
    r"\b(?:replace|substitute)\s+['\"]?[^'\"]+['\"]?\s+with\b",
    r"\b(?:reconsider|promote|demote)\s+(?:the\s+)?lead\b",
    r"\b(?:adjust|reduce|increase)\s+batch(?:\s+size)?\b",
    r"\b(?:start|stop|enable|disable|mutate|change)\s+(?:a\s+)?(?:dagster\s+)?(?:schedule|job)\b",
    r"\b(?:fetch|chunking|converted\s+to\s+document\s+chunks?|raw\s+records?)\b",
    r"\b(?:max\s+utilization|allowed\s+search\s+terms)\b",
)
_NON_SEARCH_QUERY_PATTERNS = (
    r"\b(?:check|determine|verify)\s+(?:whether|if)\b",
    r"\b(?:add|enable|wire|implement|include)\b",
    r"\b(?:replace|substitute)\b",
    r"\b(?:reconsider|promote|demote|watching)\b",
    r"\b(?:fetch|chunking|converted\s+to\s+document\s+chunks?|raw\s+records?)\b",
    r"\b(?:max\s+utilization|allowed\s+search\s+terms)\b",
)


def refine_research_followups(
    repository: ResearchRepository,
    request: ResearchFollowupRefinementRequest,
) -> ResearchFollowupRefinementResult:
    result = ResearchFollowupRefinementResult(
        dry_run=request.dry_run,
        agent_name=RESEARCH_FOLLOWUP_REFINEMENT_AGENT_NAME,
        agent_version=RESEARCH_FOLLOWUP_REFINEMENT_AGENT_VERSION,
    )
    candidates = _select_candidates(repository, request)
    result.scanned_count = len(candidates)
    existing_query_keys = {
        (query.source_key, query.query_name)
        for query in repository.list_source_queries(active_only=False)
    }
    seen_leads: set[UUID] = set()
    for review, run, lead in candidates:
        if review.verdict not in set(request.verdicts):
            result.skipped.append(_skip(review, run, lead, "verdict_not_selected"))
            continue
        query_specs, skipped = _query_specs_from_review(review, run, lead, request)
        result.skipped.extend(skipped)
        if not query_specs:
            if not skipped:
                result.skipped.append(_skip(review, run, lead, "no_refined_query_terms"))
            continue
        seen_leads.add(lead.lead_id)
        for attempt_index, (source_key, query_text, reason) in enumerate(query_specs[: request.max_queries_per_review], start=1):
            source_query = SourceQuery(
                source_key=source_key,
                query_name=_query_name(review, attempt_index, query_text),
                query_text=query_text,
                query_params=_query_params(review, run, lead, request, attempt_index, reason, source_key),
                track=AGENT_FINDING_TRACK,
                object_type=_object_type_for_source(source_key),
                active=True,
            )
            if not request.dry_run:
                is_new = (source_query.source_key, source_query.query_name) not in existing_query_keys
                persisted = repository.upsert_source_query(source_query)
                source_query = persisted
                if is_new:
                    result.source_queries_created += 1
                    existing_query_keys.add((source_query.source_key, source_query.query_name))
            result.source_queries.append(source_query)

    result.lead_count = len(seen_leads)
    result.query_count = len(result.source_queries)
    return result


def summarize_research_followup_refinement(result: ResearchFollowupRefinementResult) -> dict[str, Any]:
    return {
        "scanned_count": result.scanned_count,
        "lead_count": result.lead_count,
        "query_count": result.query_count,
        "source_queries_created": result.source_queries_created,
        "dry_run": result.dry_run,
        "errors": len(result.errors),
    }


def _select_candidates(
    repository: ResearchRepository,
    request: ResearchFollowupRefinementRequest,
) -> list[tuple[AgentRunReviewRecord, AgentRunRecord, ResearchLeadRecord]]:
    selected_review_ids = set(request.review_ids)
    selected_lead_ids = set(request.lead_ids)
    reviews = (
        [
            review
            for review_id in request.review_ids
            if (review := repository.get_agent_run_review(review_id)) is not None
        ]
        if selected_review_ids
        else repository.list_agent_run_reviews(limit=max(request.limit * 20, 100))
    )
    latest_by_key: dict[tuple[UUID, UUID], tuple[AgentRunReviewRecord, AgentRunRecord, ResearchLeadRecord]] = {}
    for review in reviews:
        if review.reviewer_type != "llm_evaluator":
            continue
        if review.verdict not in set(request.verdicts):
            continue
        run = repository.get_agent_run(review.agent_run_id)
        if run is None:
            continue
        for lead_id in _lead_ids_from_run(run):
            if selected_lead_ids and lead_id not in selected_lead_ids:
                continue
            lead = repository.get_research_lead(lead_id)
            if lead is None:
                continue
            key = (lead.lead_id, review.agent_run_id)
            current = latest_by_key.get(key)
            if current is None or review.created_at > current[0].created_at:
                latest_by_key[key] = (review, run, lead)
    for review, run, lead in _lead_only_candidates(repository, request, set(latest_by_key)):
        latest_by_key[(lead.lead_id, review.agent_run_id)] = (review, run, lead)
    candidates = list(latest_by_key.values())
    candidates.sort(key=lambda item: (item[2].priority, item[0].created_at), reverse=False)
    return candidates[: request.limit]


def _lead_only_candidates(
    repository: ResearchRepository,
    request: ResearchFollowupRefinementRequest,
    existing_keys: set[tuple[UUID, UUID]],
) -> list[tuple[AgentRunReviewRecord, AgentRunRecord, ResearchLeadRecord]]:
    if request.review_ids:
        return []
    if request.lead_ids:
        leads = [
            lead
            for lead_id in request.lead_ids
            if (lead := repository.get_research_lead(lead_id)) is not None
        ]
    else:
        leads = repository.list_research_leads(statuses=["followup"], limit=request.limit)

    candidates: list[tuple[AgentRunReviewRecord, AgentRunRecord, ResearchLeadRecord]] = []
    for lead in leads:
        if not _lead_can_be_refined_without_review(lead):
            continue
        run = _synthetic_run_for_lead(repository, lead)
        key = (lead.lead_id, run.agent_run_id)
        if key in existing_keys:
            continue
        review = _synthetic_review_for_lead(lead, run)
        if review.verdict not in set(request.verdicts):
            continue
        candidates.append((review, run, lead))
    return candidates


def _lead_can_be_refined_without_review(lead: ResearchLeadRecord) -> bool:
    if lead.status != "followup":
        return False
    metadata = lead.metadata if isinstance(lead.metadata, Mapping) else {}
    followup_meta = metadata.get("research_followup_queue")
    if isinstance(followup_meta, Mapping):
        return not bool(followup_meta.get("requires_manual_research"))
    return lead.origin_source_key == "research_brief_quality"


def _synthetic_run_for_lead(repository: ResearchRepository, lead: ResearchLeadRecord) -> AgentRunRecord:
    if lead.origin_agent_run_id:
        existing = repository.get_agent_run(lead.origin_agent_run_id)
        if existing is not None:
            return existing
    run_id = lead.origin_agent_run_id or uuid5(NAMESPACE_URL, f"research-followup-refinement-run:{lead.lead_id}")
    metadata = lead.metadata if isinstance(lead.metadata, Mapping) else {}
    return AgentRunRecord(
        agent_run_id=run_id,
        agent_name=str(metadata.get("origin_agent_name") or "research_brief_quality_followup"),
        agent_version="v1",
        model_profile="deterministic_refinement",
        status=RunStatus.COMPLETED,
        source_key=lead.source_key,
        output_payload={
            "lead_results": [
                {
                    "lead_id": str(lead.lead_id),
                    "title": lead.title,
                    "durable_source_keys": lead.suggested_sources,
                    "metadata": lead.metadata,
                }
            ]
        },
        summary={"lead_id": str(lead.lead_id), "origin_source_key": lead.origin_source_key},
        metadata=lead.metadata,
    )


def _synthetic_review_for_lead(lead: ResearchLeadRecord, run: AgentRunRecord) -> AgentRunReviewRecord:
    metadata = lead.metadata if isinstance(lead.metadata, Mapping) else {}
    followup_meta = metadata.get("research_followup_queue") if isinstance(metadata, Mapping) else None
    followup_meta = followup_meta if isinstance(followup_meta, Mapping) else {}
    feedback_items = [
        str(item.get("text") or "")
        for item in followup_meta.get("feedback_items", [])
        if isinstance(item, Mapping) and item.get("text")
    ]
    feedback_parts = [
        lead.reason or "",
        lead.summary or "",
        str(followup_meta.get("topic") or ""),
        *feedback_items,
    ]
    review_id = lead.origin_review_id or uuid5(NAMESPACE_URL, f"research-followup-refinement-review:{lead.lead_id}")
    return AgentRunReviewRecord(
        review_id=review_id,
        agent_run_id=run.agent_run_id,
        reviewer="research_brief_quality_followup",
        reviewer_type="llm_evaluator",
        verdict="needs_followup",
        feedback=" ".join(part for part in feedback_parts if part).strip() or lead.title,
        followup_actions=[part for part in feedback_parts if part],
        metadata={
            "synthetic_from_research_lead": True,
            "lead_id": str(lead.lead_id),
            "origin_source_key": lead.origin_source_key,
            "followup_kind": followup_meta.get("followup_kind"),
        },
    )


def _lead_ids_from_run(run: AgentRunRecord) -> list[UUID]:
    lead_ids: list[UUID] = []
    for lead_result in _find_list(run.output_payload, "lead_results"):
        if not isinstance(lead_result, Mapping):
            continue
        lead_id = _parse_uuid(lead_result.get("lead_id"))
        if lead_id is not None:
            lead_ids.append(lead_id)
    for value in _find_values([run.output_payload, run.input_payload, run.summary, run.metadata], "lead_id"):
        lead_id = _parse_uuid(value)
        if lead_id is not None:
            lead_ids.append(lead_id)
    return _dedupe_uuids(lead_ids)


def _query_specs_from_review(
    review: AgentRunReviewRecord,
    run: AgentRunRecord,
    lead: ResearchLeadRecord,
    request: ResearchFollowupRefinementRequest,
) -> tuple[list[tuple[str, str, str]], list[dict[str, Any]]]:
    source_keys = _source_keys_for_review(review, run, lead, request)
    base_terms = _base_terms(lead, review, run)
    actions = review.followup_actions or []
    query_texts: list[tuple[str, str]] = []
    skipped: list[dict[str, Any]] = []
    if _prefer_base_terms_for_lead(lead):
        base_query = _lead_base_query(lead, base_terms)
        if base_query:
            query_texts.append((base_query, "lead_terms"))
    else:
        for action in actions:
            queries, skip_reason = _queries_from_action(action)
            if skip_reason:
                skipped.append(
                    _skip(
                        review,
                        run,
                        lead,
                        skip_reason,
                        {"action": _human_text(action)[:500]},
                    )
                )
            query_texts.extend((query, action) for query in queries)
    if not query_texts and review.feedback and not _prefer_base_terms_for_lead(lead):
        queries, skip_reason = _queries_from_action(review.feedback)
        if skip_reason:
            skipped.append(
                _skip(
                    review,
                    run,
                    lead,
                    skip_reason,
                    {"action": _human_text(review.feedback)[:500], "source": "evaluator_feedback"},
                )
        )
        query_texts.extend((query, "evaluator_feedback") for query in queries)
    if not query_texts:
        base_query = _lead_base_query(lead, base_terms)
        if base_query:
            query_texts.append((base_query, "lead_terms"))

    specs: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()
    for raw_query, reason in query_texts:
        query_text = _finalize_query(raw_query, base_terms)
        if not query_text:
            continue
        if _is_non_search_query(query_text):
            skipped.append(
                _skip(
                    review,
                    run,
                    lead,
                    "non_search_query_text",
                    {"query_text": query_text[:500], "action": _human_text(reason)[:500]},
                )
            )
            continue
        hinted_sources = _hinted_sources(reason)
        if request.source_keys and hinted_sources:
            candidate_sources = [source for source in hinted_sources if source in source_keys]
            if not candidate_sources:
                skipped.append(
                    _skip(
                        review,
                        run,
                        lead,
                        "hinted_source_not_selected",
                        {
                            "query_text": query_text[:500],
                            "hinted_sources": hinted_sources,
                            "selected_sources": source_keys,
                        },
                    )
                )
                continue
        elif request.source_keys:
            candidate_sources = source_keys
        else:
            candidate_sources = hinted_sources or source_keys
        for source_key in candidate_sources:
            key = (source_key, query_text.casefold())
            if key in seen:
                continue
            seen.add(key)
            specs.append((source_key, query_text, reason))
    if not specs and query_texts:
        base_query = _lead_base_query(lead, base_terms)
        if base_query:
            for source_key in source_keys:
                key = (source_key, base_query.casefold())
                if key in seen:
                    continue
                seen.add(key)
                specs.append((source_key, base_query, "lead_terms"))
    return specs, skipped


def _prefer_base_terms_for_lead(lead: ResearchLeadRecord) -> bool:
    metadata = lead.metadata if isinstance(lead.metadata, Mapping) else {}
    followup_meta = metadata.get("research_followup_queue")
    followup_meta = followup_meta if isinstance(followup_meta, Mapping) else {}
    followup_kind = str(followup_meta.get("followup_kind") or "")
    return followup_kind in {"citation_dedupe_repair", "citation_provenance_repair"}


def _lead_base_query(lead: ResearchLeadRecord, base_terms: list[str]) -> str:
    if base_terms:
        return " ".join(base_terms[:10])
    return _clean_query(
        " ".join(
            [
                lead.title or "",
                lead.reason or "",
                lead.summary or "",
                " ".join(lead.topic_tags),
            ]
        )
    )


def _queries_from_action(action: str) -> tuple[list[str], str | None]:
    text = _human_text(action)
    queries: list[str] = []
    for single, double in re.findall(r"'([^']+)'|\"([^\"]+)\"", text):
        query = single or double
        if query:
            queries.append(query)
    if "known sorafenib" in text.lower() or "robat" in text.lower() or "london" in text.lower():
        queries.extend(
            [
                "sorafenib Robat dog toxicity",
                "sorafenib London canine maximum tolerated dose",
                "Nexavar dog veterinary oncology toxicity",
            ]
        )
    if not queries and _is_operational_recommendation(text):
        return [], "operational_recommendation_not_query"
    if not queries and re.search(r"\b(refined terms|retry|re-run|rerun|search)\b", text, flags=re.I):
        cleaned = re.sub(r"\b(retry|re-run|rerun|search|with|refined|terms|query|for|the|and|or)\b", " ", text, flags=re.I)
        cleaned = re.sub(r"[:;]", " ", cleaned)
        cleaned = _clean_query(cleaned)
        if cleaned:
            queries.append(cleaned)
    return [_clean_query(query) for query in queries if _clean_query(query)], None


def _is_operational_recommendation(value: str) -> bool:
    return any(re.search(pattern, value, flags=re.I) for pattern in _OPERATIONAL_RECOMMENDATION_PATTERNS)


def _is_non_search_query(value: str) -> bool:
    text = _human_text(value)
    return any(re.search(pattern, text, flags=re.I) for pattern in _NON_SEARCH_QUERY_PATTERNS)


def _finalize_query(raw_query: str, base_terms: list[str]) -> str:
    query = _clean_query(raw_query)
    if not query:
        return ""
    tokens = query.lower()
    additions = []
    for term in base_terms:
        if term.lower() not in tokens:
            additions.append(term)
    if additions:
        query = f"{query} {' '.join(additions[:3])}"
    return query[:1000]


def _base_terms(
    lead: ResearchLeadRecord,
    review: AgentRunReviewRecord,
    run: AgentRunRecord,
) -> list[str]:
    text = _human_text(
        " ".join(
            [
                lead.title or "",
                lead.reason or "",
                lead.summary or "",
                " ".join(lead.topic_tags),
                str(lead.metadata),
                review.feedback or "",
                " ".join(review.followup_actions),
                str(run.summary),
            ]
        )
    ).lower()
    terms: list[str] = []
    for term, aliases in (
        ("ca-4f12-e6", ("ca-4f12-e6", "ca4f12", "ca 4f12", "4f12-e6", "4f12")),
        ("anti-pd-1", ("anti-pd-1", "anti pd-1", "anti-canine pd-1", "anti canine pd-1")),
        ("pd-1", ("pd-1", "pd1", "pdcd1", "cd279", "programmed death-1", "programmed death 1")),
        ("checkpoint inhibitor", ("checkpoint inhibitor", "immune checkpoint", "pd-1 blockade")),
        ("caninized antibody", ("caninized antibody", "caninised antibody", "canine antibody")),
        ("mTOR", ("mtor", "rapamycin", "sirolimus", "everolimus", "temsirolimus", "rapalog")),
        ("PIK3CA", ("pik3ca", "pi3k")),
        ("TP53", ("tp53", "p53")),
        ("PTEN", ("pten",)),
        ("PLCG1", ("plcg1",)),
        ("co-mutation", ("co-mutation", "comutation", "co mutation")),
        ("VEGF", ("vegf", "vascular endothelial growth factor")),
        ("VEGFR", ("vegfr", "vegfr-2", "vegfr2", "kdr", "flk-1")),
        ("toceranib", ("toceranib", "palladia")),
        ("pazopanib", ("pazopanib",)),
        ("sorafenib", ("sorafenib", "nexavar")),
        ("doxorubicin", ("doxorubicin", "adriamycin")),
        ("PSMA", ("psma", "folh1")),
        ("PD-L1", ("pd-l1", "pdl1", "cd274", "programmed death-ligand 1")),
        ("canine", ("canine", "dog", "dogs", "veterinary")),
        ("maximum tolerated dose", ("maximum tolerated", "mtd")),
        ("dose limiting toxicity", ("dose limiting", "dlt")),
        ("safety", ("safety", "toxicity", "tolerability")),
        ("hemangiosarcoma", ("hemangiosarcoma", "hsa")),
        ("angiosarcoma", ("angiosarcoma",)),
    ):
        if any(alias in text for alias in aliases):
            terms.append(term)
    return _dedupe_strings(terms)


def _source_keys_for_review(
    review: AgentRunReviewRecord,
    run: AgentRunRecord,
    lead: ResearchLeadRecord,
    request: ResearchFollowupRefinementRequest,
) -> list[str]:
    if request.source_keys:
        return request.source_keys
    values = [
        *lead.suggested_sources,
        lead.source_key or "",
        run.source_key or "",
        *_first_list([run.output_payload, run.input_payload, run.summary, run.metadata], "durable_source_keys"),
    ]
    text = _human_text(" ".join([lead.title or "", review.feedback or "", " ".join(review.followup_actions)])).lower()
    sources = [source for source in values if source]
    if not sources:
        sources.extend(_DEFAULT_REFINEMENT_SOURCES)
    if re.search(r"\b(phase|trial|clinical|nct)\b", text):
        sources.append("clinicaltrials_gov")
    if re.search(r"\b(adverse|toxicity|safety|tolerability)\b", text):
        sources.append("openfda_animal_events")
    return [source for source in _dedupe_strings([str(source).lower() for source in sources]) if source]


def _hinted_sources(value: str) -> list[str]:
    text = _human_text(value).lower()
    sources = [
        source
        for source, hints in _SOURCE_HINTS.items()
        if any(hint in text for hint in hints)
    ]
    return _dedupe_strings(sources)


def _query_params(
    review: AgentRunReviewRecord,
    run: AgentRunRecord,
    lead: ResearchLeadRecord,
    request: ResearchFollowupRefinementRequest,
    attempt_index: int,
    reason: str,
    source_key: str,
) -> dict[str, Any]:
    required_terms = _base_terms(lead, review, run)
    params: dict[str, Any] = {
        "followup_lane": AGENT_FINDING_LANE,
        "origin_agent_run_id": str(lead.origin_agent_run_id or run.agent_run_id),
        "origin_review_id": str(lead.origin_review_id or review.review_id),
        "origin_evaluator_review_id": str(review.review_id),
        "origin_evaluator_agent_run_id": str(review.agent_run_id),
        "lead_id": str(lead.lead_id),
        "verdict": review.verdict,
        "operator": request.operator,
        "refinement_attempt": attempt_index,
        "refinement_source": "llm_evaluator_followup_action",
        "comparative_policy": "disabled",
        "required_terms": required_terms,
        "why_this_query_exists": _human_text(reason)[:500],
        **request.metadata,
    }
    if source_key == "europe_pmc":
        params.update(
            {
                "open_access": False,
                "fetch_full_text": True,
                "full_text_timeout_seconds": 8,
                "full_text_attempts": 2,
                "full_text_time_budget_seconds": 20,
            }
        )
    return params


def _query_name(review: AgentRunReviewRecord, attempt_index: int, query_text: str) -> str:
    slug = "-".join(re.findall(r"[a-z0-9]+", query_text.lower()))[:52] or "refined-query"
    return f"agent_refine_{str(review.review_id)[:8]}_{attempt_index}_{slug}"


def _object_type_for_source(source_key: str) -> ResearchObjectType:
    if source_key == "clinicaltrials_gov":
        return ResearchObjectType.CLINICAL_TRIAL
    if source_key == "icdc":
        return ResearchObjectType.DATASET
    if source_key == "openfda_animal_events":
        return ResearchObjectType.SAFETY_REPORT
    return ResearchObjectType.PUBLICATION


def _skip(
    review: AgentRunReviewRecord,
    run: AgentRunRecord | None,
    lead: ResearchLeadRecord | None,
    reason: str,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "review_id": str(review.review_id),
        "agent_run_id": str(review.agent_run_id),
        "run_agent_name": run.agent_name if run else None,
        "lead_id": str(lead.lead_id) if lead else None,
        "reason": reason,
    }
    if metadata:
        payload.update(dict(metadata))
    return payload


def _find_list(payload: Mapping[str, Any], key: str) -> list[Any]:
    value = payload.get(key)
    if isinstance(value, list):
        return value
    for nested in payload.values():
        if isinstance(nested, Mapping):
            found = _find_list(nested, key)
            if found:
                return found
        elif isinstance(nested, list):
            for item in nested:
                if isinstance(item, Mapping):
                    found = _find_list(item, key)
                    if found:
                        return found
    return []


def _find_values(payloads: list[Mapping[str, Any]], key: str) -> list[Any]:
    values: list[Any] = []
    for payload in payloads:
        if key in payload:
            values.append(payload[key])
        for nested in payload.values():
            if isinstance(nested, Mapping):
                values.extend(_find_values([nested], key))
            elif isinstance(nested, list):
                for item in nested:
                    if isinstance(item, Mapping):
                        values.extend(_find_values([item], key))
    return values


def _first_list(payloads: list[Mapping[str, Any]], key: str) -> list[str]:
    for payload in payloads:
        values = _find_values([payload], key)
        for value in values:
            if isinstance(value, list):
                return [str(item) for item in value if item]
    return []


def _parse_uuid(value: Any) -> UUID | None:
    if not value:
        return None
    if isinstance(value, list):
        value = value[0] if value else None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _dedupe_uuids(values: list[UUID]) -> list[UUID]:
    deduped: list[UUID] = []
    seen: set[UUID] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


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


def _clean_query(value: str) -> str:
    text = _human_text(value)
    text = re.sub(r"\b(e\.g\.|eg|if|not|already|indexed|manual|manually|inspect|confirm|whether)\b", " ", text, flags=re.I)
    text = re.sub(r"[^A-Za-z0-9+./ -]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" -_/.,")
    return text[:220]


def _human_text(value: str) -> str:
    return re.sub(r"_+", " ", str(value)).replace("\\n", " ").strip()
