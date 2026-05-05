"""Build targeted source-query packs for validation evidence gaps."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import re
from typing import Any
from uuid import UUID

from .contracts import (
    ResearchLeadRecord,
    ResearchObjectType,
    SourceQuery,
    ValidationAgentResult,
    ValidationGapEvidenceLane,
    ValidationGapSourcePackRequest,
    ValidationGapSourcePackResult,
    ValidationGapSourceQuery,
    ValidationRequestQueueItem,
)
from .repository import ResearchRepository


VALIDATION_GAP_SOURCE_PACK_AGENT_NAME = "validation_gap_source_pack_agent"
VALIDATION_GAP_SOURCE_PACK_AGENT_VERSION = "v1"


@dataclass(frozen=True)
class _GapContext:
    lane: str
    gap_text: str
    title: str | None
    priority: int
    source_keys: tuple[str, ...]
    lead_id: UUID | None = None
    queue_item_id: UUID | None = None
    evidence_refs: tuple[str, ...] = ()
    topic: str | None = None
    candidate_name: str | None = None
    target_name: str | None = None
    task_type: str | None = None
    validation_type: str | None = None
    origin_review_id: UUID | None = None
    origin_agent_run_id: UUID | None = None
    metadata: Mapping[str, Any] | None = None


def build_validation_gap_source_pack(
    repository: ResearchRepository,
    request: ValidationGapSourcePackRequest,
) -> ValidationGapSourcePackResult:
    """Build and optionally persist targeted source queries for validation gaps."""

    contexts, skipped = _select_contexts(repository, request)
    result = ValidationGapSourcePackResult(
        dry_run=request.dry_run,
        persist_queries=request.persist_queries,
        skipped=skipped,
    )
    result.lead_count = len({context.lead_id for context in contexts if context.lead_id is not None})
    result.queue_item_count = len({context.queue_item_id for context in contexts if context.queue_item_id is not None})

    grouped_counts: dict[str, int] = defaultdict(int)
    seen: set[tuple[str, str]] = set()
    for context in contexts:
        lane = _normalize_lane(context.lane)
        if request.lanes and lane not in set(request.lanes):
            result.skipped.append(
                {
                    "lead_id": str(context.lead_id) if context.lead_id else None,
                    "queue_item_id": str(context.queue_item_id) if context.queue_item_id else None,
                    "reason": "lane_not_selected",
                    "lane": lane,
                }
            )
            continue
        for query in _queries_for_context(context, request):
            lane_key = str(query.lane)
            if grouped_counts[lane_key] >= request.max_queries_per_lane:
                result.skipped.append(
                    {
                        "lead_id": str(context.lead_id) if context.lead_id else None,
                        "queue_item_id": str(context.queue_item_id) if context.queue_item_id else None,
                        "reason": "max_queries_per_lane_reached",
                        "lane": lane_key,
                    }
                )
                break
            identity = (query.source_key, query.query_name)
            if identity in seen:
                continue
            seen.add(identity)
            grouped_counts[lane_key] += 1
            result.queries.append(query)

    result.query_count = len(result.queries)
    result.skipped_count = len(result.skipped)

    if request.persist_queries and not request.dry_run:
        _deactivate_stale_source_pack_queries(repository, result.queries)
        for query in result.queries:
            try:
                persisted = repository.upsert_source_query(query.as_source_query())  # type: ignore[attr-defined]
                result.source_queries.append(persisted)
                result.persisted_query_count += 1
            except Exception as exc:
                result.errors.append(f"{query.source_key}:{query.query_name}: {exc}")
    return result


def summarize_validation_gap_source_pack(result: ValidationGapSourcePackResult) -> dict[str, Any]:
    return {
        "source_pack_id": str(result.source_pack_id),
        "lead_count": result.lead_count,
        "queue_item_count": result.queue_item_count,
        "query_count": result.query_count,
        "persisted_query_count": result.persisted_query_count,
        "skipped_count": result.skipped_count,
        "dry_run": result.dry_run,
        "persist_queries": result.persist_queries,
        "errors": len(result.errors),
    }


def _select_contexts(
    repository: ResearchRepository,
    request: ValidationGapSourcePackRequest,
) -> tuple[list[_GapContext], list[dict[str, Any]]]:
    contexts: list[_GapContext] = []
    skipped: list[dict[str, Any]] = []
    queue_items_by_id: dict[UUID, ValidationRequestQueueItem] = {}

    queue_item_ids = list(request.queue_item_ids)
    for lead in _select_leads(repository, request):
        context = _context_from_lead(repository, lead)
        if context is None:
            skipped.append({"lead_id": str(lead.lead_id), "reason": "not_validation_gap_lead"})
            continue
        if context.queue_item_id is not None:
            queue_items_by_id[context.queue_item_id] = repository.get_validation_request_queue_item(context.queue_item_id)  # type: ignore[assignment]
        contexts.append(context)

    for queue_item_id in queue_item_ids:
        item = repository.get_validation_request_queue_item(queue_item_id)
        if item is None:
            skipped.append({"queue_item_id": str(queue_item_id), "reason": "missing_validation_queue_item"})
            continue
        queue_items_by_id[item.queue_item_id] = item

    for item in queue_items_by_id.values():
        if item is None:
            continue
        if any(context.queue_item_id == item.queue_item_id for context in contexts):
            continue
        item_contexts = _contexts_from_queue_item(item)
        if not item_contexts:
            skipped.append({"queue_item_id": str(item.queue_item_id), "reason": "missing_validation_agent_gaps"})
            continue
        contexts.extend(item_contexts)

    return contexts[: request.limit], skipped


def _select_leads(
    repository: ResearchRepository,
    request: ValidationGapSourcePackRequest,
) -> list[ResearchLeadRecord]:
    if request.lead_ids:
        return [
            lead
            for lead_id in request.lead_ids
            if (lead := repository.get_research_lead(lead_id)) is not None
        ][: request.limit]

    leads: list[ResearchLeadRecord] = []
    statuses = request.lead_statuses or ["new", "followup"]
    for status in statuses:
        leads.extend(repository.list_research_leads(status=status, limit=request.limit))
    deduped: dict[UUID, ResearchLeadRecord] = {}
    for lead in leads:
        if lead.lead_id not in deduped:
            deduped[lead.lead_id] = lead
    return sorted(deduped.values(), key=lambda lead: (lead.priority, lead.created_at))[: request.limit]


def _context_from_lead(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
) -> _GapContext | None:
    metadata = lead.metadata.get("evidence_gap_resolver")
    if not isinstance(metadata, Mapping):
        return None
    queue_item_id = _uuid_or_none(metadata.get("queue_item_id"))
    queue_item = repository.get_validation_request_queue_item(queue_item_id) if queue_item_id else None
    request = queue_item.validation_request if queue_item else None
    lane = _normalize_lane(str(metadata.get("lane") or "general_evidence"))
    gap_text = str(metadata.get("gap_text") or lead.reason or lead.title or "validation evidence gap")
    task_type = str(metadata.get("task_type") or (queue_item.task_type if queue_item else ""))
    validation_type = str(metadata.get("validation_type") or (request.validation_type if request else ""))
    return _GapContext(
        lane=lane,
        gap_text=gap_text,
        title=lead.title,
        priority=lead.priority,
        source_keys=tuple(lead.suggested_sources or _default_sources_for_lane(lane)),
        lead_id=lead.lead_id,
        queue_item_id=queue_item_id,
        evidence_refs=tuple(lead.evidence_refs),
        topic=queue_item.topic if queue_item else None,
        candidate_name=request.candidate_name if request else _candidate_from_text(gap_text),
        target_name=request.target_name if request else _target_from_text(gap_text),
        task_type=task_type,
        validation_type=validation_type,
        origin_review_id=lead.origin_review_id,
        origin_agent_run_id=lead.origin_agent_run_id,
        metadata=metadata,
    )


def _contexts_from_queue_item(item: ValidationRequestQueueItem) -> list[_GapContext]:
    agent_result = _validation_agent_result(item)
    if agent_result is None:
        return []
    contexts: list[_GapContext] = []
    request = item.validation_request
    for gap_type, values in (
        ("missing_evidence", agent_result.missing_evidence),
        ("risk", agent_result.risks),
        ("next_action", agent_result.next_actions),
    ):
        for value in values:
            gap_text = _normalize_text(value)
            if not gap_text:
                continue
            lane = _classify_lane(gap_text, item)
            contexts.append(
                _GapContext(
                    lane=lane,
                    gap_text=gap_text,
                    title=item.title,
                    priority=item.priority,
                    source_keys=tuple(_default_sources_for_lane(lane)),
                    queue_item_id=item.queue_item_id,
                    evidence_refs=tuple(agent_result.evidence_used),
                    topic=item.topic,
                    candidate_name=request.candidate_name,
                    target_name=request.target_name,
                    task_type=item.task_type,
                    validation_type=request.validation_type,
                    metadata={
                        "origin": "validation_queue_item",
                        "gap_type": gap_type,
                        "decision": agent_result.decision,
                        "validation_agent_run_id": str(agent_result.agent_run_id) if agent_result.agent_run_id else None,
                    },
                )
            )
    return contexts


def _queries_for_context(
    context: _GapContext,
    request: ValidationGapSourcePackRequest,
) -> list[ValidationGapSourceQuery]:
    lane = _normalize_lane(context.lane)
    source_keys = request.source_keys or list(context.source_keys) or _default_sources_for_lane(lane)
    source_keys = _dedupe_source_keys(source_keys)
    candidates = _candidate_terms(context)
    targets = _target_terms(context)
    disease_terms = ['"canine hemangiosarcoma"', '"dog hemangiosarcoma"', '"human angiosarcoma"', "angiosarcoma"]
    queries: list[ValidationGapSourceQuery] = []

    for source_key in source_keys:
        query_text, required_terms = _query_text_for_source(
            source_key=source_key,
            lane=lane,
            candidates=candidates,
            targets=targets,
            disease_terms=disease_terms,
            context=context,
        )
        if not query_text:
            continue
        query_name = _query_name(context, source_key, lane, query_text)
        query_params = _query_params_for_source(source_key, lane, required_terms, context, request)
        queries.append(
            ValidationGapSourceQuery(
                lane=lane,  # type: ignore[arg-type]
                source_key=source_key,
                query_name=query_name,
                query_text=query_text,
                query_params=query_params,
                track="validation_gap",
                object_type=_object_type_for_source(source_key),
                active=request.active,
                priority=context.priority,
                reason=context.gap_text[:1000],
                required_terms=required_terms,
                excluded_terms=_excluded_terms_for_lane(lane),
                lead_ids=[context.lead_id] if context.lead_id else [],
                queue_item_ids=[context.queue_item_id] if context.queue_item_id else [],
                evidence_refs=list(context.evidence_refs),
                metadata={
                    **dict(context.metadata or {}),
                    **request.metadata,
                    "source_pack": {
                        "lead_id": str(context.lead_id) if context.lead_id else None,
                        "queue_item_id": str(context.queue_item_id) if context.queue_item_id else None,
                        "lane": lane,
                        "task_type": context.task_type,
                        "validation_type": context.validation_type,
                        "candidate_name": context.candidate_name,
                        "target_name": context.target_name,
                        "dagster_run_id": request.dagster_run_id,
                    },
                },
            )
        )
    return queries


def _query_text_for_source(
    *,
    source_key: str,
    lane: str,
    candidates: list[str],
    targets: list[str],
    disease_terms: list[str],
    context: _GapContext,
) -> tuple[str, list[str]]:
    candidate = _or_group(candidates) if candidates else ""
    target = _or_group(targets) if targets else ""
    disease = _or_group(disease_terms)
    dog = "dog OR canine"
    required_terms = [*candidates[:3], *targets[:3]]

    if source_key in {"pubchem", "chembl"}:
        if not candidates and not targets:
            return "", []
        if source_key == "chembl" and targets and candidates:
            return f"({_or_group(candidates)}) AND ({_or_group(targets)})", required_terms
        return _or_group(candidates or targets), required_terms

    if source_key == "openfda_animal_events":
        if not candidates:
            return "", []
        required_terms = [*candidates[:3], "Dog"]
        return f"{_or_group(candidates)} Dog", required_terms

    if source_key in {"uniprot", "rcsb_pdb"}:
        if not targets:
            return "", []
        return _or_group(targets), targets

    if source_key in {"geo", "sra"}:
        base = f"({disease})"
        if targets:
            base = f"{base} AND ({target})"
            required_terms = targets[:3]
        return base, required_terms or ["hemangiosarcoma"]

    if source_key == "clinicaltrials_gov":
        parts = [disease]
        if candidate:
            parts.append(candidate)
        return " AND ".join(f"({part})" for part in parts), candidates[:3] or ["hemangiosarcoma"]

    if lane == "safety_signal":
        if candidate:
            return (
                f"({candidate}) AND ({dog}) AND (safety OR toxicity OR tolerability OR "
                f'"dose limiting toxicity" OR "maximum tolerated dose" OR adverse)',
                [*candidates[:3], "dog", "safety"],
            )
        return f"({disease}) AND ({dog}) AND (safety OR toxicity OR adverse)", ["hemangiosarcoma", "safety"]

    if lane == "pkpd":
        if candidate:
            return (
                f"({candidate}) AND ({dog}) AND (pharmacokinetic OR pharmacodynamic OR PK OR PD OR "
                f"bioavailability OR Cmax OR AUC OR dose)",
                [*candidates[:3], "dog", "pharmacokinetic"],
            )
        return f"({disease}) AND ({dog}) AND (pharmacokinetic OR dose OR exposure)", ["hemangiosarcoma", "dose"]

    if lane == "clinical_response":
        if candidate:
            return (
                f"({candidate}) AND ({disease}) AND (response OR survival OR outcome OR trial OR retrospective)",
                [*candidates[:3], "hemangiosarcoma", "response"],
            )
        return f"({disease}) AND (response OR survival OR outcome OR trial)", ["hemangiosarcoma", "response"]

    if lane == "mutation_function":
        if target:
            return f"({target}) AND ({disease}) AND (mutation OR variant OR function OR activating)", [*targets[:3], "mutation"]
        return f"({disease}) AND (mutation OR variant OR function)", ["hemangiosarcoma", "mutation"]

    if lane == "assay_protocol":
        focus = candidate or target or disease
        return (
            f"({focus}) AND ({disease}) AND (assay OR protocol OR cell line OR organoid OR ex vivo OR readout)",
            required_terms or ["assay", "hemangiosarcoma"],
        )

    if lane in {"omics_context", "species_translation"}:
        focus = target or candidate or disease
        return (
            f"({focus}) AND ({disease}) AND (canine OR dog OR human) AND "
            f"(expression OR transcriptomic OR proteomic OR genomic OR ortholog)",
            required_terms or ["hemangiosarcoma", "canine"],
        )

    if candidate and target:
        return f"({candidate}) AND ({target}) AND ({disease})", [*candidates[:3], *targets[:3]]
    if candidate:
        return f"({candidate}) AND ({disease})", candidates[:3]
    if target:
        return f"({target}) AND ({disease})", targets[:3]
    return f"({disease}) AND ({_keywords_from_text(context.gap_text, max_terms=6)})", ["hemangiosarcoma"]


def _query_params_for_source(
    source_key: str,
    lane: str,
    required_terms: list[str],
    context: _GapContext,
    request: ValidationGapSourcePackRequest,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "validation_gap": True,
        "lane": lane,
        "required_terms": required_terms,
        "lead_id": str(context.lead_id) if context.lead_id else None,
        "queue_item_id": str(context.queue_item_id) if context.queue_item_id else None,
    }
    if context.lead_id:
        params["followup_lane"] = "agent_evaluator_followup"
    if context.origin_review_id:
        params["origin_review_id"] = str(context.origin_review_id)
    if context.origin_agent_run_id:
        params["origin_agent_run_id"] = str(context.origin_agent_run_id)
    if source_key == "europe_pmc":
        params["open_access"] = False
    elif source_key == "pmc_oa":
        params["license_required"] = True
    elif source_key == "pubchem":
        params.update({"records_per_term": 2, "require_exact_match": False})
    elif source_key == "chembl":
        params.update(
            {
                "molecules_per_term": 2,
                "activities_per_molecule": 10,
                "target_organisms": ["Homo sapiens", "Canis lupus familiaris"],
                "standard_types": ["IC50", "Ki", "Kd", "EC50"],
                "assay_types": ["B", "F"],
                "include_cell_line_assays": True,
                "cell_line_terms": ["angiosarcoma", "hemangiosarcoma", "canine", "dog", "endothelial"],
            }
        )
    elif source_key == "openfda_animal_events":
        params["species"] = "Dog"
    elif source_key == "clinicaltrials_gov":
        params["search_area"] = "term"
    elif source_key == "geo":
        params["db"] = "gds"
    elif source_key == "sra":
        params["db"] = "sra"
    params["source_pack_request"] = {
        "dry_run": request.dry_run,
        "persist_queries": request.persist_queries,
        "active": request.active,
    }
    return params


def _deactivate_stale_source_pack_queries(
    repository: ResearchRepository,
    desired_queries: list[ValidationGapSourceQuery],
) -> int:
    desired = {(query.source_key, query.query_name) for query in desired_queries}
    scopes = {
        (str(lead_id), str(query.lane), query.source_key)
        for query in desired_queries
        for lead_id in query.lead_ids
    }
    if not scopes:
        return 0

    deactivated = 0
    for source_key in sorted({source_key for _, _, source_key in scopes}):
        for existing in repository.list_source_queries(source_key=source_key, active_only=True):  # type: ignore[arg-type]
            params = existing.query_params or {}
            if not isinstance(params, Mapping) or not params.get("source_pack_request"):
                continue
            scope = (str(params.get("lead_id") or ""), str(params.get("lane") or ""), existing.source_key)
            if scope not in scopes or (existing.source_key, existing.query_name) in desired:
                continue
            repository.upsert_source_query(_inactive_source_query(existing))
            deactivated += 1
    return deactivated


def _inactive_source_query(query: SourceQuery) -> SourceQuery:
    return SourceQuery(
        source_key=query.source_key,
        query_name=query.query_name,
        query_text=query.query_text,
        query_params=query.query_params,
        track=query.track,
        object_type=query.object_type,
        active=False,
    )


def _validation_agent_result(item: ValidationRequestQueueItem) -> ValidationAgentResult | None:
    payload = item.metadata.get("validation_agent_result")
    if not isinstance(payload, Mapping):
        return None
    try:
        return ValidationAgentResult.model_validate(payload)
    except Exception:
        return None


def _candidate_terms(context: _GapContext) -> list[str]:
    primary: list[str | None] = []
    for text in (context.gap_text, context.title):
        primary.extend(_candidate_terms_from_text(text or ""))
    primary.append(_candidate_from_text(context.gap_text))
    primary.append(_candidate_from_text(context.title or ""))
    primary_terms = _dedupe_terms(primary)
    if primary_terms:
        return primary_terms

    fallback: list[str | None] = []
    fallback.extend(_candidate_terms_from_text(context.candidate_name or ""))
    fallback.append(_compact_fallback_term(context.candidate_name))
    return _dedupe_terms(fallback)


def _target_terms(context: _GapContext) -> list[str]:
    primary: list[str | None] = []
    for text in (context.gap_text, context.title):
        primary.extend(_target_terms_from_text(text or ""))
    primary.append(_target_from_text(context.gap_text))
    primary.append(_target_from_text(context.title or ""))
    primary_terms = _dedupe_terms(primary)
    if primary_terms:
        return primary_terms

    fallback: list[str | None] = []
    fallback.extend(_target_terms_from_text(context.target_name or ""))
    fallback.append(_compact_fallback_term(context.target_name))
    return _dedupe_terms(fallback)


def _candidate_terms_from_text(text: str) -> list[str]:
    lower = text.lower()
    terms: list[str] = []
    for term, pattern in (
        ("ca-4F12-E6", r"\bca[- ]?4f12[- ]?e6\b"),
        ("anti-PD-1", r"\banti[- ]?pd[- ]?1\b|\bpd[- ]?1\s+(?:inhibitor|blockade|antibody)\b"),
        ("toceranib", r"\btoceranib\b|\bpalladia\b"),
        ("pazopanib", r"\bpazopanib\b"),
        ("axitinib", r"\baxitinib\b"),
        ("sorafenib", r"\bsorafenib\b"),
        ("doxorubicin", r"\bdoxorubicin\b"),
        ("propranolol", r"\bpropranolol\b"),
        ("sirolimus", r"\bsirolimus\b|\brapamycin\b"),
        ("vorinostat", r"\bvorinostat\b"),
        ("paclitaxel", r"\bpaclitaxel\b"),
        ("cyclophosphamide", r"\bcyclophosphamide\b"),
        ("enoxacin", r"\benoxacin\b"),
    ):
        if re.search(pattern, lower, flags=re.I):
            terms.append(term)
    return terms


def _target_terms_from_text(text: str) -> list[str]:
    lower = text.lower()
    terms: list[str] = []
    for term, pattern in (
        ("PD-1", r"\bpd[- ]?1\b"),
        ("PD-L1", r"\bpd[- ]?l1\b|\bpdl1\b"),
        ("VEGFR-2", r"\bvegfr[- ]?2\b|\bkdr\b"),
        ("VEGF", r"\bvegf(?:a)?\b"),
        ("KIT", r"\bkit\b|\bc-kit\b"),
        ("FLT4", r"\bflt4\b|\bvegfr[- ]?3\b"),
        ("MTOR", r"\bmtor\b"),
        ("CD47", r"\bcd47\b"),
        ("SIRPA", r"\bsirpa\b|\bsirp[ -]?a\b"),
        ("TP53", r"\btp53\b"),
        ("ATF4", r"\batf4\b"),
    ):
        if re.search(pattern, lower, flags=re.I):
            terms.append(term)
    return terms


def _compact_fallback_term(value: str | None) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    if len(normalized) > 80:
        return None
    if re.search(r"\s(?:and|or)\s|/|;|,|\be\.g\.\b", normalized, flags=re.I):
        return None
    return normalized


def _candidate_from_text(text: str) -> str | None:
    lower = text.lower()
    for term in (
        "sorafenib",
        "toceranib",
        "palladia",
        "doxorubicin",
        "propranolol",
        "sirolimus",
        "rapamycin",
        "vorinostat",
        "paclitaxel",
        "cyclophosphamide",
        "enoxacin",
    ):
        if term in lower:
            return term
    return None


def _target_from_text(text: str) -> str | None:
    lower = text.lower()
    for term in (
        "vegfr2",
        "vegfr",
        "kdr",
        "kit",
        "flt4",
        "vegfa",
        "mtor",
        "cd47",
        "sirpa",
        "tp53",
        "atf4",
    ):
        if term in lower:
            return term.upper() if len(term) <= 5 else term
    return None


def _default_sources_for_lane(lane: str) -> list[str]:
    return {
        "mutation_function": ["pubmed", "europe_pmc", "openalex", "uniprot", "rcsb_pdb"],
        "clinical_response": ["pubmed", "clinicaltrials_gov", "europe_pmc", "openalex"],
        "pkpd": ["pubmed", "europe_pmc", "chembl", "pubchem", "openfda_animal_events"],
        "safety_signal": ["pubmed", "europe_pmc", "openfda_animal_events", "chembl"],
        "assay_protocol": ["pubmed", "europe_pmc", "pmc_oa", "openalex"],
        "trial_design": ["clinicaltrials_gov", "pubmed", "europe_pmc"],
        "omics_context": ["pubmed", "europe_pmc", "openalex", "geo", "sra"],
        "species_translation": ["pubmed", "europe_pmc", "openalex", "uniprot"],
        "chemistry": ["chembl", "pubchem"],
        "general_evidence": ["pubmed", "europe_pmc", "openalex"],
    }.get(lane, ["pubmed", "europe_pmc", "openalex"])


def _object_type_for_source(source_key: str) -> ResearchObjectType:
    return {
        "clinicaltrials_gov": ResearchObjectType.CLINICAL_TRIAL,
        "pubchem": ResearchObjectType.COMPOUND_RECORD,
        "chembl": ResearchObjectType.BIOACTIVITY_ASSAY,
        "uniprot": ResearchObjectType.STRUCTURE,
        "rcsb_pdb": ResearchObjectType.STRUCTURE,
        "geo": ResearchObjectType.DATASET,
        "sra": ResearchObjectType.DATASET,
        "openfda_animal_events": ResearchObjectType.SAFETY_REPORT,
    }.get(source_key, ResearchObjectType.PUBLICATION)


def _classify_lane(text: str, item: ValidationRequestQueueItem | None = None) -> str:
    lower = text.lower()
    if _has_any(lower, ("toxicity", "coagulation", "hemorrhage", "safety", "contraindication", "adverse", "tolerability")):
        return "safety_signal"
    if _has_any(lower, ("pk", "pd", "pk/pd", "bioavailability", "cmax", "exposure", "auc", "dose", "ic50")):
        return "pkpd"
    if _has_any(lower, ("response", "pfs", "orr", "dcr", "clinical", "retrospective", "prospective", "trial")):
        return "clinical_response"
    if _has_any(lower, ("mutation", "activating", "loss-of-function", "passenger", "kinase-domain", "variant")):
        return "mutation_function"
    if _has_any(lower, ("assay", "ihc", "cell line", "organoid", "readout", "control", "protocol")):
        return "assay_protocol"
    if _has_any(lower, ("sample size", "stratified", "endpoint", "power", "enrollment", "statistical")):
        return "trial_design"
    if _has_any(lower, ("omics", "transcript", "proteom", "genom", "co-mutation", "expression", "spatial")):
        return "omics_context"
    if _has_any(lower, ("ortholog", "species", "canine vs human", "homology", "cross-species")):
        return "species_translation"
    if _has_any(lower, ("chembl", "pubchem", "compound", "binding", "potency")):
        return "chemistry"
    if item and item.task_type == "safety":
        return "safety_signal"
    if item and item.task_type == "omics":
        return "omics_context"
    return "general_evidence"


def _normalize_lane(lane: str) -> str:
    normalized = lane.strip().lower()
    aliases = {
        "omics": "omics_context",
        "translation": "species_translation",
        "species": "species_translation",
        "chemical": "chemistry",
    }
    return aliases.get(normalized, normalized if normalized in set(_default_lane_values()) else "general_evidence")


def _default_lane_values() -> tuple[str, ...]:
    return (
        "mutation_function",
        "clinical_response",
        "pkpd",
        "safety_signal",
        "assay_protocol",
        "trial_design",
        "omics_context",
        "species_translation",
        "chemistry",
        "general_evidence",
    )


def _excluded_terms_for_lane(lane: str) -> list[str]:
    if lane in {"pkpd", "safety_signal"}:
        return ["mouse-only", "in vitro only without dosing"]
    if lane == "clinical_response":
        return ["review-only"]
    return []


def _query_name(context: _GapContext, source_key: str, lane: str, query_text: str) -> str:
    origin = str(context.lead_id or context.queue_item_id or hashlib.sha1(query_text.encode("utf-8")).hexdigest()[:12])
    digest = hashlib.sha1(f"{source_key}:{lane}:{query_text}:{origin}".lower().encode("utf-8")).hexdigest()[:10]
    return f"validation_gap_{lane}_{source_key}_{origin[:8]}_{digest}"


def _or_group(values: list[str]) -> str:
    return " OR ".join(_quote_query_term(value) for value in values if value)


def _quote_query_term(value: str) -> str:
    value = value.strip()
    if not value:
        return value
    if value.startswith('"') and value.endswith('"'):
        return value
    if re.search(r"\s", value):
        return f'"{value}"'
    return value


def _keywords_from_text(text: str, *, max_terms: int) -> str:
    stopwords = {
        "the",
        "and",
        "for",
        "with",
        "that",
        "from",
        "this",
        "need",
        "needs",
        "missing",
        "evidence",
        "specific",
        "direct",
        "canine",
        "human",
    }
    words = [
        word
        for word in re.findall(r"[A-Za-z0-9][A-Za-z0-9/-]{2,}", text.lower())
        if word not in stopwords
    ]
    return " OR ".join(_dedupe_terms(words)[:max_terms]) or "hemangiosarcoma"


def _dedupe_terms(values: list[str | None]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _normalize_text(value)
        if not normalized:
            continue
        key = normalized.casefold()
        if key in seen:
            continue
        deduped.append(normalized)
        seen.add(key)
    return deduped


def _dedupe_source_keys(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = str(value).strip().lower()
        if not key or key in seen:
            continue
        deduped.append(key)
        seen.add(key)
    return deduped


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except Exception:
        return None


def _has_any(text: str, needles: tuple[str, ...]) -> bool:
    return any(needle in text for needle in needles)


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()
