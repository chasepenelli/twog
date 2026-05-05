"""Evidence-fit checks for research follow-up promotion gates."""

from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from .contracts import (
    DocumentChunk,
    EvidenceFitAssessment,
    ResearchChunkSearchRequest,
    ResearchLeadRecord,
    ResearchObject,
    SourceQuery,
    ValidationGapSourceIngestResult,
)
from .repository import ResearchRepository


def assess_research_followup_ingest_evidence_fit(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
    ingest_result: ValidationGapSourceIngestResult,
) -> EvidenceFitAssessment:
    """Assess only evidence produced by a follow-up ingest run when possible."""

    source_keys = sorted({source_key for source_key in ingest_result.source_keys if source_key})
    if not source_keys:
        source_keys = sorted({query.source_key for query in ingest_result.source_queries if query.source_key})
    requirements = research_followup_requirement_groups(lead, ingest_result.source_queries)

    if not requirements:
        return EvidenceFitAssessment(
            fit="weak",
            source_keys=source_keys,
            chunk_count=ingest_result.document_chunks,
            reason="No concrete required terms could be derived for this follow-up lead.",
        )
    if ingest_result.document_chunks <= 0:
        return EvidenceFitAssessment(
            fit="weak",
            required_terms=[group["label"] for group in requirements],
            missing_terms=[group["label"] for group in requirements],
            total_required_count=len(requirements),
            source_keys=source_keys,
            chunk_count=0,
            reason="Follow-up ingest produced no document chunks to assess.",
        )

    fetch_run_ids = [
        ingestion.fetch_run_id
        for ingestion in ingest_result.results
        if ingestion.fetch_run_id and ingestion.document_chunks > 0
    ]
    chunks = _chunks_for_fetch_runs(repository, fetch_run_ids, limit=100)
    if chunks:
        return _assess_chunks(repository, lead, requirements, chunks, source_keys, fallback_chunk_count=ingest_result.document_chunks)

    chunks = _search_requirement_chunks(repository, lead, requirements, source_keys)
    if not chunks:
        chunks = _source_chunks(repository, source_keys)
    return _assess_chunks(repository, lead, requirements, chunks, source_keys, fallback_chunk_count=ingest_result.document_chunks)


def assess_research_followup_ref_evidence_fit(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
    evidence_refs: list[str],
    source_keys: list[str],
) -> EvidenceFitAssessment:
    """Assess evidence refs before resolver promotion."""

    requirements = research_followup_requirement_groups(lead, [])
    source_keys = sorted({source_key for source_key in source_keys if source_key})
    if not requirements:
        return EvidenceFitAssessment(
            fit="weak",
            source_keys=source_keys,
            chunk_count=0,
            reason="No concrete required terms could be derived for this follow-up lead.",
        )

    chunks, object_texts = _chunks_and_object_texts_for_refs(repository, evidence_refs)
    return _assess_chunks(
        repository,
        lead,
        requirements,
        chunks,
        source_keys,
        fallback_chunk_count=len(chunks),
        extra_texts=object_texts,
    )


def research_followup_requirement_groups(
    lead: ResearchLeadRecord,
    source_queries: list[SourceQuery],
) -> list[dict[str, Any]]:
    explicit_terms: list[str] = []
    query_texts: list[str] = []
    for query in source_queries:
        query_texts.append(str(getattr(query, "query_text", "") or ""))
        params = getattr(query, "query_params", {}) or {}
        raw_required = params.get("required_terms") if isinstance(params, dict) else None
        if isinstance(raw_required, str):
            explicit_terms.extend([part.strip() for part in raw_required.split(",")])
        elif isinstance(raw_required, list):
            explicit_terms.extend(str(term).strip() for term in raw_required)

    lead_text = " ".join(
        part
        for part in [
            lead.title or "",
            lead.reason or "",
            lead.summary or "",
            " ".join(lead.topic_tags),
            " ".join(lead.evidence_refs),
            " ".join(query_texts),
            " ".join(explicit_terms),
        ]
        if part
    )
    normalized = _normalize_evidence_text(lead_text)
    groups: list[dict[str, Any]] = []

    concept_groups = [
        ("sorafenib", ["sorafenib", "nexavar"], True),
        ("toceranib", ["toceranib", "palladia"], True),
        ("pazopanib", ["pazopanib"], True),
        ("propranolol", ["propranolol"], True),
        ("doxorubicin", ["doxorubicin"], True),
        ("paclitaxel", ["paclitaxel"], True),
        ("sirolimus/rapamycin", ["sirolimus", "rapamycin"], True),
        ("imatinib", ["imatinib"], True),
        ("sunitinib", ["sunitinib"], True),
        ("trametinib", ["trametinib"], True),
        ("canine/dog/veterinary", ["canine", "dog", "dogs", "veterinary"], True),
        ("hemangiosarcoma/angiosarcoma", ["hemangiosarcoma", "angiosarcoma", "hsa"], False),
        (
            "maximum tolerated dose/dlt/safety",
            [
                "maximum tolerated dose",
                "mtd",
                "dose limiting",
                "dose-limiting",
                "dlt",
                "toxicity",
                "tolerability",
                "safety",
                "adverse event",
                "adverse events",
            ],
            True,
        ),
        ("clinical response/survival", ["clinical response", "response rate", "survival", "outcome", "outcomes"], False),
        ("mutation/genomic function", ["mutation", "mutations", "genomic", "genomics", "transcriptomic", "omics"], False),
    ]
    for label, aliases, critical in concept_groups:
        if any(_evidence_text_contains(normalized, alias) for alias in aliases):
            groups.append({"label": label, "aliases": aliases, "critical": critical})

    covered_aliases = {alias for group in groups for alias in group["aliases"]}
    for term in explicit_terms:
        normalized_term = _normalize_requirement_term(term)
        if not normalized_term or normalized_term in covered_aliases:
            continue
        groups.append({"label": normalized_term, "aliases": [normalized_term], "critical": True})
        covered_aliases.add(normalized_term)

    if not groups:
        for term in _fallback_requirement_terms(normalized):
            groups.append({"label": term, "aliases": [term], "critical": True})

    deduped: list[dict[str, Any]] = []
    seen_labels: set[str] = set()
    for group in groups:
        label = str(group["label"]).strip()
        if not label or label in seen_labels:
            continue
        deduped.append(group)
        seen_labels.add(label)
    return deduped[:12]


def _assess_chunks(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
    requirements: list[dict[str, Any]],
    chunks: list[DocumentChunk],
    source_keys: list[str],
    *,
    fallback_chunk_count: int,
    extra_texts: list[str] | None = None,
) -> EvidenceFitAssessment:
    evidence_texts = [
        _research_followup_evidence_text(chunk, repository.get_research_object(chunk.research_object_id))
        for chunk in chunks
    ]
    if extra_texts:
        evidence_texts.extend(extra_texts)

    if not evidence_texts:
        return EvidenceFitAssessment(
            fit="weak",
            required_terms=[group["label"] for group in requirements],
            missing_terms=[group["label"] for group in requirements],
            total_required_count=len(requirements),
            source_keys=source_keys,
            chunk_count=fallback_chunk_count,
            reason="No inspectable evidence text was available for this follow-up lead.",
        )

    haystack = _normalize_evidence_text("\n".join(evidence_texts))
    matched: list[str] = []
    missing: list[str] = []
    critical_missing = False
    for group in requirements:
        is_matched = any(_evidence_text_contains(haystack, alias) for alias in group["aliases"])
        if is_matched:
            matched.append(group["label"])
        else:
            missing.append(group["label"])
            if group["critical"]:
                critical_missing = True

    total_required = len(requirements)
    matched_required = len(matched)
    ratio = matched_required / total_required if total_required else 0.0
    if total_required and ratio >= 0.75 and not critical_missing:
        fit = "strong"
        reason = "Retrieved evidence matches the critical follow-up concepts."
    elif matched_required and ratio >= 0.4 and not critical_missing:
        fit = "partial"
        reason = "Retrieved evidence overlaps the follow-up lead but still misses required concepts."
    else:
        fit = "weak"
        reason = "Retrieved evidence is too broad or off-target for this follow-up lead."

    return EvidenceFitAssessment(
        fit=fit,
        matched_terms=matched,
        missing_terms=missing,
        required_terms=[group["label"] for group in requirements],
        matched_required_count=matched_required,
        total_required_count=total_required,
        source_keys=source_keys,
        chunk_count=len(chunks) or fallback_chunk_count,
        reason=reason,
    )


def _chunks_for_fetch_runs(
    repository: ResearchRepository,
    fetch_run_ids: list[UUID],
    *,
    limit: int,
) -> list[DocumentChunk]:
    if not fetch_run_ids:
        return []
    try:
        return repository.list_document_chunks_for_fetch_runs(fetch_run_ids, limit=limit)
    except Exception:
        return []


def _search_requirement_chunks(
    repository: ResearchRepository,
    lead: ResearchLeadRecord,
    requirements: list[dict[str, Any]],
    source_keys: list[str],
) -> list[DocumentChunk]:
    chunks: list[DocumentChunk] = []
    seen: set[UUID] = set()
    search_query = " ".join(alias for group in requirements for alias in group["aliases"])
    for source_key in source_keys:
        try:
            hits = repository.search_research_chunks(
                ResearchChunkSearchRequest(
                    query=search_query[:1000] or lead.title or "research follow-up evidence",
                    source_key=source_key,
                    limit=25,
                    max_chunk_chars=4000,
                )
            )
        except Exception:
            hits = []
        for hit in hits:
            if hit.chunk.id in seen:
                continue
            seen.add(hit.chunk.id)
            chunks.append(hit.chunk)
    return chunks


def _source_chunks(repository: ResearchRepository, source_keys: list[str]) -> list[DocumentChunk]:
    chunks: list[DocumentChunk] = []
    seen: set[UUID] = set()
    for source_key in source_keys:
        for chunk in repository.list_document_chunks(source_key=source_key, limit=25):
            if chunk.id in seen:
                continue
            seen.add(chunk.id)
            chunks.append(chunk)
    return chunks


def _chunks_and_object_texts_for_refs(
    repository: ResearchRepository,
    evidence_refs: list[str],
) -> tuple[list[DocumentChunk], list[str]]:
    chunks: list[DocumentChunk] = []
    object_texts: list[str] = []
    seen_chunks: set[UUID] = set()
    seen_objects: set[UUID] = set()
    fetch_run_ids: list[UUID] = []

    for ref in evidence_refs:
        ref_type, _, ref_id = ref.partition(":")
        if ref_type not in {"chunk", "research_object", "fetch_run"} or not ref_id:
            continue
        try:
            parsed_id = UUID(ref_id)
        except ValueError:
            continue
        if ref_type == "fetch_run":
            fetch_run_ids.append(parsed_id)
            continue
        if ref_type == "chunk":
            chunk = repository.get_document_chunk(parsed_id)
            if chunk is None or chunk.id in seen_chunks:
                continue
            seen_chunks.add(chunk.id)
            chunks.append(chunk)
            continue
        obj = repository.get_research_object(parsed_id)
        if obj is None or obj.id in seen_objects:
            continue
        seen_objects.add(obj.id)
        object_chunks = repository.list_document_chunks(object_id=obj.id, limit=25)
        if object_chunks:
            for chunk in object_chunks:
                if chunk.id in seen_chunks:
                    continue
                seen_chunks.add(chunk.id)
                chunks.append(chunk)
        else:
            object_texts.append(_research_object_text(obj))

    for chunk in _chunks_for_fetch_runs(repository, fetch_run_ids, limit=100):
        if chunk.id in seen_chunks:
            continue
        seen_chunks.add(chunk.id)
        chunks.append(chunk)
    return chunks, object_texts


def _fallback_requirement_terms(text: str) -> list[str]:
    stopwords = {
        "about",
        "agent",
        "clinical",
        "evidence",
        "evaluator",
        "follow",
        "followup",
        "needs",
        "research",
        "review",
        "source",
        "study",
        "therapy",
        "validation",
    }
    terms: list[str] = []
    seen: set[str] = set()
    for term in re.findall(r"[a-z0-9][a-z0-9+\-]{3,}", text):
        if term in stopwords or term in seen:
            continue
        terms.append(term)
        seen.add(term)
        if len(terms) >= 8:
            break
    return terms


def _research_followup_evidence_text(
    chunk: DocumentChunk,
    research_object: ResearchObject | None,
) -> str:
    if research_object is None:
        return chunk.text_content
    return "\n".join(
        part
        for part in [
            research_object.title,
            research_object.abstract,
            chunk.section_label,
            chunk.text_content,
        ]
        if part
    )


def _research_object_text(research_object: ResearchObject) -> str:
    return "\n".join(
        part
        for part in [
            research_object.title,
            research_object.abstract,
            research_object.canonical_url,
            " ".join(research_object.identifiers.values()),
        ]
        if part
    )


def _normalize_requirement_term(value: str) -> str:
    return re.sub(r"\s+", " ", str(value).strip().lower())


def _normalize_evidence_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value).lower()).strip()


def _evidence_text_contains(haystack: str, term: str) -> bool:
    normalized = _normalize_requirement_term(term)
    if not normalized:
        return False
    if " " in normalized:
        return normalized in haystack
    if re.search(r"[^a-z0-9]", normalized):
        return normalized in haystack
    return bool(re.search(rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])", haystack))
