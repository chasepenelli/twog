"""Convert validation-agent evidence gaps into follow-up research work."""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import re
from typing import Any

from .contracts import (
    EvidenceGapResolverLane,
    EvidenceGapResolverRequest,
    EvidenceGapResolverResult,
    EvidenceGapType,
    ResearchBriefQueueRequest,
    ResearchLeadRecord,
    ValidationAgentResult,
    ValidationRequestQueueItem,
)
from .repository import ResearchRepository


EVIDENCE_GAP_RESOLVER_AGENT_NAME = "evidence_gap_resolver_agent"
EVIDENCE_GAP_RESOLVER_AGENT_VERSION = "v1"


def resolve_evidence_gaps(
    repository: ResearchRepository,
    request: EvidenceGapResolverRequest,
    *,
    queue_research_brief,
) -> EvidenceGapResolverResult:
    """Create durable research leads from completed validation-agent gaps."""

    items = _select_validation_items(repository, request)
    result = EvidenceGapResolverResult(
        model_profile="deterministic_resolver",
        queue_items_seen=len(items),
        dry_run=request.dry_run,
    )
    existing_identity_keys = {
        lead.identity_key
        for lead in repository.list_research_leads(limit=None)
        if lead.identity_key
    }

    for item in items:
        agent_result = _validation_agent_result(item)
        if agent_result is None:
            result.skipped.append(
                {
                    "queue_item_id": str(item.queue_item_id),
                    "reason": "missing_validation_agent_result",
                    "status": item.status,
                }
            )
            continue
        if request.decisions and agent_result.decision not in set(request.decisions):
            result.skipped.append(
                {
                    "queue_item_id": str(item.queue_item_id),
                    "reason": "decision_not_selected",
                    "decision": agent_result.decision,
                }
            )
            continue
        gaps = _gaps_from_agent_result(item, agent_result, request)[: request.max_gaps_per_item]
        if not gaps:
            result.skipped.append(
                {
                    "queue_item_id": str(item.queue_item_id),
                    "reason": "no_selected_gaps",
                    "decision": agent_result.decision,
                }
            )
            continue
        for gap in gaps:
            result.gap_count += 1
            lead = _lead_from_gap(item, agent_result, gap, request)
            if lead.identity_key in existing_identity_keys:
                result.existing_leads += 1
            else:
                result.leads_created += 0 if request.dry_run else 1
                existing_identity_keys.add(lead.identity_key)
            persisted = lead if request.dry_run else repository.upsert_research_lead(lead)
            result.research_leads.append(persisted)
            if request.queue_research_briefs and not request.dry_run:
                queue_item = queue_research_brief(
                    ResearchBriefQueueRequest(
                        topic=_brief_topic_from_gap(persisted),
                        source_key=persisted.suggested_sources[0] if persisted.suggested_sources else None,
                        priority=min(request.priority, persisted.priority),
                        metadata={
                            **request.metadata,
                            "evidence_gap_resolver": {
                                "origin": "validation_agent_gap",
                                "lead_id": str(persisted.lead_id),
                                "queue_item_id": str(item.queue_item_id),
                                "plan_id": str(item.plan_id),
                                "task_type": item.task_type,
                                "validation_type": item.validation_request.validation_type,
                                "gap_type": gap["gap_type"],
                                "lane": gap["lane"],
                                "dry_run": request.dry_run,
                            },
                        },
                    )
                )
                result.brief_queue_items.append(queue_item)
                result.brief_queue_count += 1

    result.skipped_count = len(result.skipped)
    return result


def summarize_evidence_gap_resolver(result: EvidenceGapResolverResult) -> dict[str, Any]:
    return {
        "queue_items_seen": result.queue_items_seen,
        "gap_count": result.gap_count,
        "leads_created": result.leads_created,
        "existing_leads": result.existing_leads,
        "brief_queue_count": result.brief_queue_count,
        "skipped_count": result.skipped_count,
        "dry_run": result.dry_run,
        "errors": len(result.errors),
    }


def _select_validation_items(
    repository: ResearchRepository,
    request: EvidenceGapResolverRequest,
) -> list[ValidationRequestQueueItem]:
    if request.queue_item_ids:
        items = [
            item
            for queue_item_id in request.queue_item_ids
            if (item := repository.get_validation_request_queue_item(queue_item_id)) is not None
        ]
    else:
        status = request.statuses[0] if len(request.statuses) == 1 else None
        statuses = None if status else list(request.statuses)
        items = repository.list_validation_request_queue_items(
            plan_id=request.plan_id,
            status=status,
            statuses=statuses,
            limit=request.limit,
        )
    if request.task_types:
        allowed_task_types = set(request.task_types)
        items = [item for item in items if item.task_type in allowed_task_types]
    return sorted(items, key=lambda item: (item.priority, item.updated_at))[: request.limit]


def _validation_agent_result(item: ValidationRequestQueueItem) -> ValidationAgentResult | None:
    payload = item.metadata.get("validation_agent_result")
    if not isinstance(payload, Mapping):
        return None
    try:
        return ValidationAgentResult.model_validate(payload)
    except Exception:
        return None


def _gaps_from_agent_result(
    item: ValidationRequestQueueItem,
    agent_result: ValidationAgentResult,
    request: EvidenceGapResolverRequest,
) -> list[dict[str, Any]]:
    selected_gap_types = set(request.gap_types)
    gaps: list[dict[str, Any]] = []
    for gap_type, values in (
        ("missing_evidence", agent_result.missing_evidence),
        ("risk", agent_result.risks),
        ("next_action", agent_result.next_actions),
    ):
        if gap_type not in selected_gap_types:
            continue
        for value in values:
            normalized = _normalize_text(value)
            if not normalized:
                continue
            lane = _classify_gap_lane(normalized, item)
            gaps.append(
                {
                    "gap_type": gap_type,
                    "gap_text": normalized,
                    "lane": lane,
                    "suggested_sources": _suggested_sources_for_lane(lane),
                }
            )
    return _dedupe_gaps(gaps)


def _lead_from_gap(
    item: ValidationRequestQueueItem,
    agent_result: ValidationAgentResult,
    gap: Mapping[str, Any],
    request: EvidenceGapResolverRequest,
) -> ResearchLeadRecord:
    lane = str(gap["lane"])
    gap_type = str(gap["gap_type"])
    gap_text = str(gap["gap_text"])
    digest = hashlib.sha1(
        f"{item.queue_item_id}:{agent_result.agent_run_id}:{gap_type}:{lane}:{gap_text}".lower().encode("utf-8")
    ).hexdigest()[:16]
    title = f"{_lane_title(lane)}: {_short_gap_title(gap_text)}"
    suggested_sources = list(gap.get("suggested_sources") or [])
    return ResearchLeadRecord(
        identity_key=f"research_lead:validation_gap:{item.queue_item_id}:{digest}",
        title=title[:240],
        lead_type="unknown",
        status="new",
        priority=_priority_for_gap(lane, gap_type, request.priority),
        source_key=suggested_sources[0] if suggested_sources else None,
        origin_source_key="validation_agent",
        origin_record_id=str(item.queue_item_id),
        origin_review_id=item.queue_item_id,
        origin_agent_run_id=agent_result.agent_run_id,
        reason=gap_text[:1000],
        summary=(
            f"{agent_result.agent_name} returned decision '{agent_result.decision}' "
            f"for {item.task_type}. Resolve this {gap_type.replace('_', ' ')} before promotion."
        )[:1000],
        evidence_refs=[*agent_result.evidence_used[:12], f"validation_queue:{item.queue_item_id}"],
        topic_tags=[
            "validation_gap",
            lane,
            gap_type,
            str(item.task_type),
            str(item.validation_request.validation_type),
            agent_result.decision,
        ],
        suggested_sources=suggested_sources,
        metadata={
            **request.metadata,
            "evidence_gap_resolver": {
                "origin": "validation_agent_result",
                "gap_type": gap_type,
                "lane": lane,
                "gap_text": gap_text,
                "queue_item_id": str(item.queue_item_id),
                "plan_id": str(item.plan_id),
                "task_id": str(item.task_id),
                "task_type": item.task_type,
                "validation_type": item.validation_request.validation_type,
                "validation_agent_run_id": str(agent_result.agent_run_id) if agent_result.agent_run_id else None,
                "decision": agent_result.decision,
                "confidence": agent_result.confidence,
                "idea_id": item.metadata.get("idea_id"),
                "idea_title": item.metadata.get("idea_title"),
                "committee_run_id": item.metadata.get("committee_run_id"),
                "suggested_sources": suggested_sources,
                "dry_run": request.dry_run,
                "dagster_run_id": request.dagster_run_id,
            }
        },
    )


def _brief_topic_from_gap(lead: ResearchLeadRecord) -> str:
    lane = (lead.metadata.get("evidence_gap_resolver") or {}).get("lane", "general_evidence")
    reason = lead.reason or lead.title or "validation evidence gap"
    return (
        f"Resolve {str(lane).replace('_', ' ')} evidence gap for canine hemangiosarcoma and human "
        f"angiosarcoma translation: {reason}"
    )[:1000]


def _classify_gap_lane(text: str, item: ValidationRequestQueueItem) -> EvidenceGapResolverLane:
    lower = text.lower()
    if _has_any(lower, ("toxicity", "coagulation", "hemorrhage", "safety", "contraindication", "adverse")):
        return "safety_signal"
    if _has_any(lower, ("response", "pfs", "orr", "dcr", "clinical", "retrospective", "prospective", "trial")):
        return "clinical_response"
    if _has_any(lower, ("mutation", "activating", "loss-of-function", "passenger", "kinase-domain", "variant")):
        return "mutation_function"
    if _has_any(lower, ("pk", "pd", "pk/pd", "bioavailability", "cmax", "exposure", "dose", "ic50")):
        return "pkpd"
    if _has_any(lower, ("assay", "ihc", "cell line", "organoid", "readout", "control", "protocol")):
        return "assay_protocol"
    if _has_any(lower, ("sample size", "stratified", "endpoint", "power", "enrollment", "statistical")):
        return "trial_design"
    if _has_any(lower, ("omics", "transcript", "proteom", "genom", "co-mutation", "expression", "spatial")):
        return "omics_context"
    if item.task_type == "safety":
        return "safety_signal"
    if item.task_type == "wet_lab":
        return "assay_protocol"
    return "general_evidence"


def _suggested_sources_for_lane(lane: str) -> list[str]:
    return {
        "mutation_function": ["pubmed", "europe_pmc", "pmc_oa", "uniprot", "rcsb_pdb"],
        "clinical_response": ["pubmed", "clinicaltrials_gov", "europe_pmc", "openalex"],
        "pkpd": ["pubmed", "pubchem", "chembl", "openfda_animal_events"],
        "safety_signal": ["pubmed", "openfda_animal_events", "chembl", "europe_pmc"],
        "assay_protocol": ["pubmed", "europe_pmc", "pmc_oa", "openalex"],
        "trial_design": ["clinicaltrials_gov", "pubmed", "europe_pmc"],
        "omics_context": ["pubmed", "europe_pmc", "openalex"],
        "general_evidence": ["pubmed", "europe_pmc", "openalex"],
    }.get(lane, ["pubmed", "europe_pmc", "openalex"])


def _priority_for_gap(lane: str, gap_type: str, base_priority: int) -> int:
    lane_priority = {
        "safety_signal": 10,
        "mutation_function": 15,
        "clinical_response": 20,
        "pkpd": 25,
        "assay_protocol": 30,
        "trial_design": 35,
        "omics_context": 40,
        "general_evidence": 50,
    }.get(lane, 50)
    gap_adjustment = {"missing_evidence": 0, "risk": 5, "next_action": 10}.get(gap_type, 10)
    return min(max(min(base_priority, lane_priority + gap_adjustment), 0), 1000)


def _lane_title(lane: str) -> str:
    return {
        "mutation_function": "Mutation function",
        "clinical_response": "Clinical response correlation",
        "pkpd": "PK/PD evidence",
        "safety_signal": "Safety signal",
        "assay_protocol": "Assay protocol",
        "trial_design": "Trial design",
        "omics_context": "Omics context",
        "general_evidence": "General evidence",
    }.get(lane, "Evidence gap")


def _short_gap_title(text: str) -> str:
    cleaned = _normalize_text(text)
    return cleaned[:120].rstrip(" ,.;:") or "Untitled gap"


def _dedupe_gaps(gaps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for gap in gaps:
        key = f"{gap['gap_type']}:{gap['lane']}:{gap['gap_text']}".casefold()
        if key in seen:
            continue
        deduped.append(gap)
        seen.add(key)
    return deduped


def _has_any(text: str, needles: tuple[str, ...]) -> bool:
    return any(needle in text for needle in needles)


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()
