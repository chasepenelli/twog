"""Resolve evidence-light research leads into durable source evidence."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
import re
from typing import Any
from uuid import UUID

from .contracts import (
    ResearchChunkSearchRequest,
    ResearchFollowupLeadResult,
    ResearchFollowupResolverRequest,
    ResearchFollowupResolverResult,
    ResearchLeadRecord,
    ResearchObjectType,
    SourceFollowupIngestRequest,
    SourceFollowupQueueItem,
    SourceQuery,
)
from .evidence_fit import assess_research_followup_ref_evidence_fit
from .local_ingest import LocalIngestionPipeline
from .repository import ResearchRepository
from .source_followup import ingest_source_followups


RESEARCH_FOLLOWUP_RESOLVER_AGENT_NAME = "research_followup_resolver_agent"
RESEARCH_FOLLOWUP_RESOLVER_AGENT_VERSION = "v1"

DEFAULT_RESEARCH_FOLLOWUP_SEARCH_SOURCES = (
    "pubmed",
    "crossref",
    "openalex",
    "europe_pmc",
    "pmc_oa",
    "clinicaltrials_gov",
)
_FOLLOWUP_SOURCE_BY_IDENTIFIER_TYPE = {
    "doi": "crossref",
    "pmid": "pubmed",
    "pmcid": "pmc_oa",
    "nct": "clinicaltrials_gov",
}
_OBJECT_TYPE_BY_SOURCE = {
    "clinicaltrials_gov": ResearchObjectType.CLINICAL_TRIAL,
}
_NON_EVIDENCE_SOURCES = {"x_linked_article", "x_topic", "x_topic_monitor"}
_TOKEN_PATTERN = re.compile(r"[a-z0-9][a-z0-9-]*[a-z0-9]", re.I)
_DOI_PATTERN = re.compile(r"\b10\.\d{4,9}/[^\s,;)\]]+", re.I)
_NCT_PATTERN = re.compile(r"\bNCT\d{8}\b", re.I)
_PMCID_PATTERN = re.compile(r"\bPMC\d+\b", re.I)
_PMID_PATTERN = re.compile(r"\bPMID[:\s]*(\d+)\b", re.I)
_QUERY_STOPWORDS = {
    "about",
    "acquire",
    "acquisition",
    "before",
    "brief",
    "citation",
    "citations",
    "claim",
    "claims",
    "create",
    "dedupe",
    "deduplicate",
    "deduplication",
    "duplicate",
    "duplicates",
    "any",
    "and",
    "data",
    "evidence",
    "evaluation",
    "feedback",
    "find",
    "followup",
    "follow",
    "focused",
    "for",
    "needs",
    "plan",
    "profile",
    "promotion",
    "proposed",
    "provenance",
    "record",
    "repair",
    "require",
    "requires",
    "retrieval",
    "retrieve",
    "run",
    "signal",
    "source",
    "strengthen",
    "the",
    "this",
    "verification",
    "with",
}
_KNOWN_QUERY_TERM_PATTERNS: tuple[tuple[str, str], ...] = (
    ("ca-4f12-e6", r"\bca[- ]?4f12[- ]?e6\b"),
    ("anti-pd-1", r"\banti[- ]?pd[- ]?1\b|\bpd[- ]?1\s+(?:inhibitor|blockade|antibody)\b"),
    ("pd-l1", r"\bpd[- ]?l1\b|\bpdl1\b"),
    ("pd-1", r"\bpd[- ]?1\b"),
    ("vegfr-2", r"\bvegfr[- ]?2\b|\bkdr\b"),
    ("vegf", r"\bvegf(?:a)?\b"),
    ("toceranib", r"\btoceranib\b|\bpalladia\b"),
    ("sorafenib", r"\bsorafenib\b"),
    ("pazopanib", r"\bpazopanib\b"),
    ("axitinib", r"\baxitinib\b"),
    ("doxorubicin", r"\bdoxorubicin\b"),
    ("propranolol", r"\bpropranolol\b"),
    ("sirolimus", r"\bsirolimus\b|\brapamycin\b"),
    ("vorinostat", r"\bvorinostat\b"),
    ("paclitaxel", r"\bpaclitaxel\b"),
    ("cyclophosphamide", r"\bcyclophosphamide\b"),
    ("enoxacin", r"\benoxacin\b"),
)


def resolve_research_followup_leads(
    repository: ResearchRepository,
    request: ResearchFollowupResolverRequest,
) -> ResearchFollowupResolverResult:
    """Resolve follow-up leads without allowing weak evidence into synthesis."""

    result = ResearchFollowupResolverResult(
        model_profile="deterministic_resolver",
        dry_run=request.dry_run,
        force_live_search=request.force_live_search,
    )
    leads, skipped = _select_leads(repository, request)
    result.leads_seen = len(leads)
    result.skipped_leads = len(skipped)
    result.skip_reasons = skipped
    result.unresolved_lead_ids = [
        UUID(item["lead_id"])
        for item in skipped
        if item.get("reason") == "lead_not_found" and item.get("lead_id")
    ]
    if request.lead_ids and skipped:
        result.errors.extend(_skip_error(item) for item in skipped)
        result.failed_leads += len(result.unresolved_lead_ids)
    if request.lead_ids and not leads:
        result.blocked = True
        if not result.errors:
            result.errors.append("No requested research follow-up leads were eligible for resolution.")

    for lead in leads:
        lead_result = _resolve_one_lead(repository, request, lead)
        result.lead_results.append(lead_result)
        result.source_followups_queued += int("queued_source_followup" in lead_result.actions)
        result.source_followups_ingested += int("ingested_source_followup" in lead_result.actions)
        result.durable_source_searches += int("searched_durable_sources" in lead_result.actions)
        result.evidence_inspections += int("evidence_inspection" in lead_result.metadata)
        result.promoted_leads += int(lead_result.promoted)
        result.manual_research_required += int(lead_result.manual_research_required)
        result.kept_in_followup += int("kept_in_followup" in lead_result.actions)
        result.failed_leads += int("failed" in lead_result.actions)
        result.errors.extend(f"lead {lead.lead_id}: {error}" for error in lead_result.errors)

    return result


def summarize_research_followup_resolver(result: ResearchFollowupResolverResult) -> dict[str, Any]:
    return {
        "leads_seen": result.leads_seen,
        "dry_run": result.dry_run,
        "force_live_search": result.force_live_search,
        "blocked": result.blocked,
        "skipped_leads": result.skipped_leads,
        "unresolved_lead_ids": len(result.unresolved_lead_ids),
        "source_followups_queued": result.source_followups_queued,
        "source_followups_ingested": result.source_followups_ingested,
        "durable_source_searches": result.durable_source_searches,
        "evidence_inspections": result.evidence_inspections,
        "promoted_leads": result.promoted_leads,
        "manual_research_required": result.manual_research_required,
        "kept_in_followup": result.kept_in_followup,
        "failed_leads": result.failed_leads,
        "errors": len(result.errors),
    }


def _select_leads(
    repository: ResearchRepository,
    request: ResearchFollowupResolverRequest,
) -> tuple[list[ResearchLeadRecord], list[dict[str, Any]]]:
    skipped: list[dict[str, Any]] = []
    if request.lead_ids:
        leads = []
        for lead_id in request.lead_ids:
            lead = repository.get_research_lead(lead_id)
            if lead is None:
                skipped.append({"lead_id": str(lead_id), "reason": "lead_not_found"})
                continue
            leads.append(lead)
    else:
        leads = repository.list_research_leads(statuses=list(request.statuses), limit=request.limit)
    if request.source_keys:
        allowed_sources = set(request.source_keys)
        eligible = []
        for lead in leads:
            if lead.source_key in allowed_sources or lead.origin_source_key in allowed_sources:
                eligible.append(lead)
                continue
            if request.lead_ids:
                skipped.append(_lead_skip_reason(lead, "source_not_allowed"))
        leads = eligible
    allowed_statuses = set(request.statuses)
    eligible = []
    for lead in leads:
        if lead.status in allowed_statuses:
            eligible.append(lead)
            continue
        if request.lead_ids:
            skipped.append(_lead_skip_reason(lead, "status_not_allowed"))
    leads = eligible
    leads.sort(key=lambda item: (item.priority, item.created_at))
    if len(leads) > request.limit:
        if request.lead_ids:
            skipped.extend(_lead_skip_reason(lead, "limit_exceeded") for lead in leads[request.limit :])
        leads = leads[: request.limit]
    return leads, skipped


def _lead_skip_reason(lead: ResearchLeadRecord, reason: str) -> dict[str, Any]:
    return {
        "lead_id": str(lead.lead_id),
        "title": lead.title,
        "status": lead.status,
        "source_key": lead.source_key,
        "origin_source_key": lead.origin_source_key,
        "reason": reason,
    }


def _skip_error(item: dict[str, Any]) -> str:
    lead_id = item.get("lead_id", "unknown")
    reason = item.get("reason", "skipped")
    status = item.get("status")
    source_key = item.get("source_key")
    details = []
    if status:
        details.append(f"status={status}")
    if source_key:
        details.append(f"source_key={source_key}")
    suffix = f" ({', '.join(details)})" if details else ""
    return f"lead {lead_id}: {reason}{suffix}"


def _resolve_one_lead(
    repository: ResearchRepository,
    request: ResearchFollowupResolverRequest,
    lead: ResearchLeadRecord,
) -> ResearchFollowupLeadResult:
    lead_result = ResearchFollowupLeadResult(
        lead_id=lead.lead_id,
        title=lead.title,
        status_before=lead.status,
        status_after=lead.status,
    )
    try:
        source_followups = _ensure_identifier_followups(repository, lead, request, lead_result)
        source_followups.extend(_metadata_source_followups(repository, lead))
        source_followups = _dedupe_followups(source_followups)
        lead_result.source_followup_ids = _dedupe_uuids(
            [*lead_result.source_followup_ids, *(item.followup_id for item in source_followups)]
        )

        ingestable = [
            item
            for item in source_followups
            if item.status in {"queued", "approved"}
        ]
        if request.ingest_source_followups and source_followups and request.dry_run and ingestable:
            lead_result.metadata["planned_source_followup_ingest"] = {
                "action": "would_ingest_source_followups",
                "followup_ids": [str(item.followup_id) for item in ingestable],
                "source_keys": sorted({item.source_key for item in ingestable}),
            }

        if request.ingest_source_followups and source_followups and not request.dry_run:
            if ingestable:
                ingest_result = ingest_source_followups(
                    repository,
                    SourceFollowupIngestRequest(
                        followup_ids=[item.followup_id for item in ingestable],
                        source_keys=sorted({item.source_key for item in ingestable}),
                        statuses=["queued", "approved"],
                        limit=min(len(ingestable), request.limit),
                        approved_by=request.approved_by,
                        run_claim_extraction=request.run_claim_extraction,
                        dry_run=False,
                        metadata={
                            **request.metadata,
                            "research_followup_resolver": True,
                            "research_lead_id": str(lead.lead_id),
                        },
                    )
                )
                if ingest_result.ingested:
                    lead_result.actions.append("ingested_source_followup")
                lead_result.metadata["source_followup_ingest"] = ingest_result.model_dump(mode="json")
                source_followups = _metadata_source_followups(repository, lead)

        if (
            request.ingest_source_followups
            and not request.dry_run
            and "source_followup_ingest" not in lead_result.metadata
        ):
            lead_result.metadata["source_followup_ingest"] = _source_followup_ingest_skip_report(
                source_followups,
                ingestable,
            )

        evidence_refs, durable_sources = _source_followup_evidence(source_followups, request.min_evidence_chunks)
        if len(evidence_refs) < request.min_evidence_chunks:
            chunk_refs, chunk_sources = _stored_chunk_evidence(repository, lead, request)
            evidence_refs.extend(chunk_refs)
            durable_sources.extend(chunk_sources)

        should_search = request.force_live_search or (
            len(evidence_refs) < request.min_evidence_chunks and request.search_missing_identifiers
        )
        if should_search:
            search_forced = request.force_live_search and len(evidence_refs) >= request.min_evidence_chunks
            if request.dry_run:
                _append_action_once(lead_result, "searched_durable_sources")
                lead_result.metadata["planned_search"] = {
                    "action": "would_force_live_search" if search_forced else "would_search_durable_sources",
                    "force_live_search": request.force_live_search,
                    "evidence_refs_before_search": len(_dedupe_strings(evidence_refs)),
                    "query": _resolver_search_query(lead, request),
                    "source_keys": _search_source_keys(request),
                    "dry_run": True,
                }
            else:
                search_report = _search_durable_sources(repository, lead, request)
                search_report["force_live_search"] = request.force_live_search
                search_report["evidence_refs_before_search"] = len(_dedupe_strings(evidence_refs))
                _append_action_once(lead_result, "searched_durable_sources")
                lead_result.metadata["durable_source_search"] = search_report
                chunk_refs, chunk_sources = _stored_chunk_evidence(repository, lead, request)
                evidence_refs.extend(chunk_refs)
                durable_sources.extend(chunk_sources)

        evidence_refs = _dedupe_strings(evidence_refs)
        durable_sources = _dedupe_strings(durable_sources)
        lead_result.evidence_refs = evidence_refs
        lead_result.durable_source_keys = durable_sources
        if evidence_refs and request.inspect_evidence_refs:
            lead_result.metadata["evidence_inspection"] = _inspect_evidence_refs(repository, evidence_refs, request, lead)

        if len(evidence_refs) >= request.min_evidence_chunks and durable_sources:
            evidence_fit = assess_research_followup_ref_evidence_fit(
                repository,
                lead,
                evidence_refs,
                durable_sources,
            )
            lead_result.metadata["evidence_fit"] = evidence_fit.model_dump(mode="json")
            if evidence_fit.fit != "strong":
                if request.dry_run:
                    lead_result.metadata["planned_action"] = "would_keep_in_followup_evidence_fit"
                else:
                    updated = repository.update_research_lead(
                        lead.lead_id,
                        status="followup",
                        metadata={
                            "research_followup_resolver": {
                                "last_checked_at": datetime.now(UTC).isoformat(),
                                "requires_manual_research": False,
                                "reason": "Evidence did not pass the direct-fit promotion gate.",
                                "evidence_refs": evidence_refs,
                                "durable_source_keys": durable_sources,
                                "evidence_fit": evidence_fit.model_dump(mode="json"),
                                "dagster_run_id": request.dagster_run_id,
                                "min_evidence_chunks": request.min_evidence_chunks,
                            },
                            "research_followup_resolver_last_action": "kept_in_followup_evidence_fit",
                        },
                    )
                    lead_result.status_after = updated.status if updated else lead.status
                _append_action_once(lead_result, "kept_in_followup")
            elif request.promote_ready_leads and not request.dry_run:
                promoted = _promote_lead(repository, lead, evidence_refs, durable_sources, request, lead_result)
                lead_result.status_after = promoted.status
                lead_result.promoted = promoted.status in {"new", "watching"}
                _append_action_once(lead_result, "promoted_to_watching")
            else:
                if request.dry_run and request.promote_ready_leads:
                    lead_result.metadata["planned_action"] = "would_promote_to_watching"
                elif request.dry_run:
                    lead_result.metadata["planned_action"] = "would_keep_in_followup"
                _append_action_once(lead_result, "kept_in_followup")
        else:
            lead_result.manual_research_required = True
            _append_action_once(lead_result, "manual_research_required")
            _append_action_once(lead_result, "kept_in_followup")
            if request.dry_run:
                lead_result.metadata["planned_action"] = "would_mark_manual_research_required"
            if not request.dry_run:
                updated = repository.update_research_lead(
                    lead.lead_id,
                    status="followup",
                    metadata={
                        "research_followup_resolver": {
                            "last_checked_at": datetime.now(UTC).isoformat(),
                            "requires_manual_research": True,
                            "reason": "No durable source evidence reached the promotion threshold.",
                            "evidence_refs": evidence_refs,
                            "durable_source_keys": durable_sources,
                        }
                    },
                )
                lead_result.status_after = updated.status if updated else lead.status
    except Exception as exc:  # pragma: no cover - defensive resolver boundary
        lead_result.errors.append(str(exc))
        lead_result.actions.append("failed")
        lead_result.status_after = lead.status
    return lead_result


def _append_action_once(lead_result: ResearchFollowupLeadResult, action: str) -> None:
    if action not in lead_result.actions:
        lead_result.actions.append(action)  # type: ignore[arg-type]


def _source_followup_ingest_skip_report(
    source_followups: list[SourceFollowupQueueItem],
    ingestable: list[SourceFollowupQueueItem],
) -> dict[str, Any]:
    if not source_followups:
        reason = "no_source_followups_queued_or_linked"
    elif not ingestable:
        reason = "no_queued_or_approved_source_followups"
    else:
        reason = "ingest_not_attempted"
    return {
        "status": "skipped",
        "reason": reason,
        "source_followup_count": len(source_followups),
        "ingestable_count": len(ingestable),
        "statuses": sorted({item.status for item in source_followups}),
    }


def _inspect_evidence_refs(
    repository: ResearchRepository,
    evidence_refs: list[str],
    request: ResearchFollowupResolverRequest,
    lead: ResearchLeadRecord,
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    for ref in evidence_refs:
        if len(records) >= request.evidence_inspection_limit:
            break
        ref_type, _, ref_id = ref.partition(":")
        if ref_type not in {"chunk", "research_object"} or not ref_id:
            continue
        try:
            parsed_id = UUID(ref_id)
        except ValueError:
            continue
        key = f"{ref_type}:{parsed_id}"
        if key in seen:
            continue
        seen.add(key)
        if ref_type == "chunk":
            chunk = repository.get_document_chunk(parsed_id)
            if chunk is None:
                records.append({"ref": ref, "ref_type": ref_type, "status": "missing"})
                continue
            obj = repository.get_research_object(chunk.research_object_id)
            records.append(
                {
                    "ref": ref,
                    "ref_type": "chunk",
                    "chunk_id": str(chunk.id),
                    "research_object_id": str(chunk.research_object_id),
                    "source_key": obj.source_key if obj else None,
                    "title": obj.title if obj else None,
                    "canonical_url": obj.canonical_url if obj else None,
                    "identifiers": obj.identifiers if obj else {},
                    "section_label": chunk.section_label,
                    "chunk_index": chunk.chunk_index,
                    "text_preview": _preview_text(chunk.text_content),
                }
            )
            continue
        obj = repository.get_research_object(parsed_id)
        if obj is None:
            records.append({"ref": ref, "ref_type": ref_type, "status": "missing"})
            continue
        records.append(
            {
                "ref": ref,
                "ref_type": "research_object",
                "research_object_id": str(obj.id),
                "source_key": obj.source_key,
                "title": obj.title,
                "canonical_url": obj.canonical_url,
                "identifiers": obj.identifiers,
                "publication_year": obj.publication_year,
            }
        )
    return {
        "query": _lead_search_query(lead, max_terms=request.max_search_terms),
        "inspected_count": len(records),
        "limit": request.evidence_inspection_limit,
        "records": records,
    }


def _preview_text(value: str, limit: int = 600) -> str:
    normalized = re.sub(r"\s+", " ", value).strip()
    return normalized if len(normalized) <= limit else f"{normalized[: limit - 3]}..."


def _ensure_identifier_followups(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
    request: ResearchFollowupResolverRequest,
    lead_result: ResearchFollowupLeadResult,
) -> list[SourceFollowupQueueItem]:
    items: list[SourceFollowupQueueItem] = []
    for identifier_type, source_key in _FOLLOWUP_SOURCE_BY_IDENTIFIER_TYPE.items():
        identifier = lead.identifiers.get(identifier_type)
        if not identifier:
            continue
        item = SourceFollowupQueueItem(
            source_key=source_key,
            identifier_type=identifier_type,  # type: ignore[arg-type]
            identifier=identifier,
            url=_source_followup_url(identifier_type, identifier, fallback=lead.url),
            title=lead.title,
            origin_source_key=lead.origin_source_key or lead.source_key,
            origin_review_id=lead.origin_review_id,
            origin_artifact_id=lead.origin_artifact_id,
            origin_agent_run_id=lead.origin_agent_run_id,
            reason="Research follow-up resolver queued durable evidence ingestion.",
            priority=lead.priority,
            metadata={
                "followup_type": "research_lead_evidence_enrichment",
                "research_lead_id": str(lead.lead_id),
                "resolver_dagster_run_id": request.dagster_run_id,
            },
        )
        existing = _find_existing_followup(repository, item)
        if existing is not None:
            items.append(existing)
            lead_result.source_followup_ids.append(existing.followup_id)
            continue
        if request.dry_run:
            lead_result.metadata.setdefault("planned_source_followups", []).append(
                {
                    "action": "would_queue_source_followup",
                    "source_key": item.source_key,
                    "identifier_type": item.identifier_type,
                    "identifier": item.identifier,
                    "url": item.url,
                }
            )
            items.append(item)
            continue
        persisted = repository.upsert_source_followup(item)
        lead_result.actions.append("queued_source_followup")
        lead_result.source_followup_ids.append(persisted.followup_id)
        items.append(persisted)
    return items


def _metadata_source_followups(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
) -> list[SourceFollowupQueueItem]:
    items: list[SourceFollowupQueueItem] = []
    metadata_queue = lead.metadata.get("research_followup_queue") or {}
    followup_id = _parse_uuid(metadata_queue.get("source_followup_id"))
    if followup_id is not None and (item := repository.get_source_followup(followup_id)) is not None:
        items.append(item)
    for item in repository.list_source_followups(limit=None):
        if item.metadata.get("research_lead_id") == str(lead.lead_id):
            items.append(item)
    return items


def _source_followup_evidence(
    source_followups: Iterable[SourceFollowupQueueItem],
    min_evidence_chunks: int,
) -> tuple[list[str], list[str]]:
    refs: list[str] = []
    sources: list[str] = []
    for item in source_followups:
        if item.status != "ingested":
            continue
        report = item.metadata.get("last_ingestion_report") if isinstance(item.metadata, dict) else None
        document_chunks = int((report or {}).get("document_chunks") or 0)
        research_objects = int((report or {}).get("research_objects") or 0)
        if document_chunks < min_evidence_chunks and research_objects < 1:
            continue
        refs.append(f"source_followup:{item.followup_id}")
        refs.append(f"{item.source_key}:{item.identifier_type}:{item.identifier}")
        if report and report.get("fetch_run_id"):
            refs.append(f"fetch_run:{report['fetch_run_id']}")
        sources.append(item.source_key)
    return refs, sources


def _stored_chunk_evidence(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
    request: ResearchFollowupResolverRequest,
) -> tuple[list[str], list[str]]:
    query = _resolver_search_query(lead, request)
    refs: list[str] = []
    sources: list[str] = []
    for source_key in _search_source_keys(request):
        try:
            hits = repository.search_research_chunks(
                ResearchChunkSearchRequest(
                    query=query,
                    source_key=source_key,
                    limit=request.min_evidence_chunks,
                    max_chunk_chars=1200,
                    include_keyword_fallback=True,
                )
            )
        except Exception:
            continue
        for hit in hits:
            if hit.research_object and hit.research_object.source_key in _NON_EVIDENCE_SOURCES:
                continue
            refs.append(f"chunk:{hit.chunk.id}")
            refs.append(f"research_object:{hit.chunk.research_object_id}")
            sources.append(source_key)
            if len(refs) >= request.min_evidence_chunks:
                break
    return _dedupe_strings(refs), _dedupe_strings(sources)


def _search_durable_sources(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
    request: ResearchFollowupResolverRequest,
) -> dict[str, Any]:
    pipeline = LocalIngestionPipeline(repository)  # type: ignore[arg-type]
    query_text = _resolver_search_query(lead, request)
    reports = []
    errors = []
    for source_key in _search_source_keys(request):
        query = SourceQuery(
            source_key=source_key,
            query_name=f"research_followup_{str(lead.lead_id)[:8]}_{_safe_query_name(source_key)}",
            query_text=query_text,
            query_params=_query_params_for_search_source(source_key),
            track="research_followup",
            object_type=_OBJECT_TYPE_BY_SOURCE.get(source_key, ResearchObjectType.PUBLICATION),
            active=False,
        )
        try:
            ingestion = pipeline.ingest_query(
                query,
                limit=request.search_limit_per_source,
                persist_query=False,
            )
            reports.append(ingestion.model_dump(mode="json"))
        except Exception as exc:
            errors.append(f"{source_key}: {exc}")
    return {
        "query_text": query_text,
        "source_keys": _search_source_keys(request),
        "limit_per_source": request.search_limit_per_source,
        "reports": reports,
        "errors": errors,
    }


def _promote_lead(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
    evidence_refs: list[str],
    durable_source_keys: list[str],
    request: ResearchFollowupResolverRequest,
    lead_result: ResearchFollowupLeadResult,
) -> ResearchLeadRecord:
    metadata = {
        **lead.metadata,
        "research_followup_resolver": {
            "last_checked_at": datetime.now(UTC).isoformat(),
            "requires_manual_research": False,
            "evidence_refs": evidence_refs,
            "durable_source_keys": durable_source_keys,
            "evidence_fit": lead_result.metadata.get("evidence_fit"),
            "dagster_run_id": request.dagster_run_id,
            "min_evidence_chunks": request.min_evidence_chunks,
        },
    }
    source_key = durable_source_keys[0]
    enriched = lead.model_copy(
        update={
            "source_key": source_key,
            "suggested_sources": durable_source_keys,
            "evidence_refs": _dedupe_strings([*lead.evidence_refs, *evidence_refs]),
            "metadata": metadata,
        }
    )
    repository.upsert_research_lead(enriched)
    updated = repository.update_research_lead(
        lead.lead_id,
        status="watching",
        metadata={"research_followup_resolver_last_action": "promoted_to_watching"},
    )
    if updated is None:
        lead_result.errors.append("promotion update returned no lead")
        return enriched
    return updated


def _search_source_keys(request: ResearchFollowupResolverRequest) -> list[str]:
    return request.search_source_keys or list(DEFAULT_RESEARCH_FOLLOWUP_SEARCH_SOURCES)


def _query_params_for_search_source(source_key: str) -> dict[str, Any]:
    params: dict[str, Any] = {"comparative_policy": "enabled", "require_policy_match": False}
    if source_key == "pmc_oa":
        params.update({"license_required": True, "max_candidate_records": 5})
    if source_key in {"crossref", "pubmed", "openalex", "europe_pmc"}:
        params["include_human_angiosarcoma"] = True
    return params


def _lead_search_query(lead: ResearchLeadRecord, *, max_terms: int) -> str:
    context_text = " ".join(
        [
            lead.title or "",
            lead.summary or "",
            lead.reason or "",
            " ".join(lead.topic_tags),
        ]
    ).lower()
    disease_terms = ["canine", "hemangiosarcoma", "human", "angiosarcoma"]
    raw_parts = [
        lead.summary or "",
        lead.reason or "",
        " ".join(lead.topic_tags),
        " ".join(lead.identifiers.values()),
        lead.title or "",
    ]
    tokens: list[str] = []
    seen: set[str] = set()
    _extend_query_tokens(tokens, seen, _known_query_terms(context_text), max_terms=max_terms, skip_stopwords=False)
    _extend_query_tokens(tokens, seen, [_lead_query_expansion_text(context_text)], max_terms=max_terms, skip_stopwords=True)
    _extend_query_tokens(tokens, seen, [" ".join(disease_terms)], max_terms=max_terms, skip_stopwords=True)
    for part in raw_parts:
        _extend_query_tokens(tokens, seen, [part], max_terms=max_terms, skip_stopwords=True)
        if len(tokens) >= max_terms:
            return " ".join(tokens)
    return " ".join(tokens) or "canine hemangiosarcoma human angiosarcoma"


def _resolver_search_query(lead: ResearchLeadRecord, request: ResearchFollowupResolverRequest) -> str:
    if request.search_query_text:
        normalized = _normalize_search_query_text(request.search_query_text, max_terms=request.max_search_terms)
        if normalized:
            return normalized
    return _lead_search_query(lead, max_terms=request.max_search_terms)


def _normalize_search_query_text(query_text: str, *, max_terms: int) -> str:
    references: list[str] = []
    references.extend(match.group(0).rstrip(".").casefold() for match in _DOI_PATTERN.finditer(query_text))
    references.extend(match.group(0).upper() for match in _NCT_PATTERN.finditer(query_text))
    references.extend(match.group(0).upper() for match in _PMCID_PATTERN.finditer(query_text))
    references.extend(match.group(1) for match in _PMID_PATTERN.finditer(query_text))

    scrubbed = _DOI_PATTERN.sub(" ", query_text)
    scrubbed = _NCT_PATTERN.sub(" ", scrubbed)
    scrubbed = _PMCID_PATTERN.sub(" ", scrubbed)
    scrubbed = _PMID_PATTERN.sub(" ", scrubbed)
    tokens: list[str] = []
    seen = set(references)
    _extend_query_tokens(tokens, seen, [scrubbed], max_terms=max_terms, skip_stopwords=True)
    return " ".join([*references, *tokens[:max_terms]]).strip()


def _lead_query_expansion_text(context_text: str) -> str:
    terms: list[str] = []
    if any(term in context_text for term in ("dlt", "dose limiting", "dose-limiting", "mtd", "maximum tolerated")):
        terms.extend(
            [
                "canine",
                "dog",
                "veterinary",
                "oncology",
                "dlt",
                "mtd",
                "dose",
                "limiting",
                "toxicity",
                "maximum",
                "tolerated",
            ]
        )
    if any(term in context_text for term in ("safety", "tolerability", "toxicity", "adverse")):
        terms.extend(["canine", "dog", "veterinary", "toxicity", "tolerability", "adverse", "events"])
    if any(term in context_text for term in ("pharmacokinetic", "pk", "cmax", "auc", "tmax", "exposure")):
        terms.extend(["canine", "pharmacokinetics", "cmax", "auc", "tmax", "plasma", "exposure"])
    return " ".join(terms)


def _known_query_terms(context_text: str) -> list[str]:
    return [
        term
        for term, pattern in _KNOWN_QUERY_TERM_PATTERNS
        if re.search(pattern, context_text, flags=re.I)
    ]


def _extend_query_tokens(
    tokens: list[str],
    seen: set[str],
    parts: Iterable[str],
    *,
    max_terms: int,
    skip_stopwords: bool,
) -> None:
    for part in parts:
        for token in _TOKEN_PATTERN.findall(part.lower()):
            if len(token) < 3 or token in seen:
                continue
            if skip_stopwords and token in _QUERY_STOPWORDS:
                continue
            tokens.append(token)
            seen.add(token)
            if len(tokens) >= max_terms:
                return


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


def _dedupe_followups(items: Iterable[SourceFollowupQueueItem]) -> list[SourceFollowupQueueItem]:
    deduped: list[SourceFollowupQueueItem] = []
    seen: set[UUID] = set()
    for item in items:
        if item.followup_id in seen:
            continue
        deduped.append(item)
        seen.add(item.followup_id)
    return deduped


def _dedupe_strings(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        deduped.append(normalized)
        seen.add(normalized)
    return deduped


def _dedupe_uuids(values: Iterable[UUID]) -> list[UUID]:
    deduped: list[UUID] = []
    seen: set[UUID] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _parse_uuid(value: Any) -> UUID | None:
    if not value:
        return None
    try:
        return UUID(str(value))
    except ValueError:
        return None


def _source_followup_url(identifier_type: str, identifier: str, *, fallback: str | None) -> str | None:
    if identifier_type == "doi":
        return f"https://doi.org/{identifier}"
    if identifier_type == "pmid":
        return f"https://pubmed.ncbi.nlm.nih.gov/{identifier}/"
    if identifier_type == "pmcid":
        return f"https://www.ncbi.nlm.nih.gov/pmc/articles/{identifier}/"
    if identifier_type == "nct":
        return f"https://clinicaltrials.gov/study/{identifier}"
    return fallback


def _safe_query_name(value: str) -> str:
    return "_".join(_TOKEN_PATTERN.findall(value.lower()))[:80] or "unknown"
