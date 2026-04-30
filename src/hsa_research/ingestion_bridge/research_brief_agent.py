"""Citation-first research brief agents for the AI research lane."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import json
import os
import re
from typing import Any
import urllib.error
import urllib.request
from uuid import UUID

from .contracts import (
    ClaimSearchRequest,
    ClaimSearchResult,
    ResearchBriefCitation,
    ResearchBriefEvidenceStrength,
    ResearchBriefFinding,
    ResearchBriefPlaygroundPack,
    ResearchBriefPlaygroundPrompt,
    ResearchBriefPerspectiveName,
    ResearchBriefPerspectiveReport,
    ResearchBriefRequest,
    ResearchBriefResult,
    ResearchBriefStance,
    ResearchLeadRecord,
    ResearchChunkSearchRequest,
    ResearchChunkSearchResult,
    TextEmbeddingSearchRequest,
)
from .embeddings import LOCAL_HASH_EMBEDDING_MODEL, LocalDeterministicEmbeddingProvider
from .research_leads import active_research_leads_for_brief
from .repository import ResearchRepository


EVIDENCE_SCOUT_AGENT_NAME = "evidence_scout_agent"
TRANSLATIONAL_HYPOTHESIS_AGENT_NAME = "translational_hypothesis_agent"
SKEPTIC_VALIDATION_AGENT_NAME = "skeptic_validation_agent"
RESEARCH_SYNTHESIS_EDITOR_AGENT_NAME = "research_synthesis_editor_agent"
RESEARCH_BRIEF_AGENT_VERSION = "v1"
DEFAULT_RESEARCH_BRIEF_MODEL = "~anthropic/claude-sonnet-latest"
DEFAULT_RESEARCH_BRIEF_COMPARE_MODELS = (DEFAULT_RESEARCH_BRIEF_MODEL,)
PERSPECTIVE_ORDER: tuple[ResearchBriefPerspectiveName, ...] = (
    "evidence_scout",
    "translational_hypothesis",
    "skeptic_validation",
)

_PERSPECTIVE_AGENT_NAMES = {
    "evidence_scout": EVIDENCE_SCOUT_AGENT_NAME,
    "translational_hypothesis": TRANSLATIONAL_HYPOTHESIS_AGENT_NAME,
    "skeptic_validation": SKEPTIC_VALIDATION_AGENT_NAME,
}
_ALLOWED_STANCES = {"supporting", "contradicting", "uncertain", "opportunity", "risk"}
_ALLOWED_STRENGTHS = {"high", "medium", "low", "unknown"}
_RESEARCH_BRIEF_SYSTEM_PROMPT = """You are a scientific research brief agent.
Use only the provided citation IDs. Do not invent papers, identifiers, or claims.
Every finding must cite at least one supplied citation ID.
Research leads are watchlist context only; they can inform open_questions but cannot support findings.
Return strict JSON with: summary, findings, errors.
Each finding requires: claim, stance, citations, evidence_strength, reasoning, open_questions.
Allowed stances: supporting, contradicting, uncertain, opportunity, risk.
Allowed evidence_strength values: high, medium, low, unknown."""


@dataclass(frozen=True)
class ResearchBriefEvidenceBundle:
    citations: list[ResearchBriefCitation]
    claims: list[ClaimSearchResult]
    research_leads: list[ResearchLeadRecord]
    search_queries: dict[str, list[str]]
    errors: list[str]


@dataclass(frozen=True)
class _PerspectiveQuery:
    lane: str
    query: str


@dataclass
class _CandidateChunk:
    result: ResearchChunkSearchResult
    relevances: list[str]


class ResearchBriefAgent:
    """Run perspective-specific research agents over stored chunks and claims."""

    def __init__(self, repository: ResearchRepository) -> None:
        self.repository = repository

    def build_evidence(self, request: ResearchBriefRequest) -> ResearchBriefEvidenceBundle:
        citations: list[ResearchBriefCitation] = []
        citations_by_chunk: dict[str, ResearchBriefCitation] = {}
        errors: list[str] = []
        query_specs_by_perspective = _perspective_queries(request)
        search_queries = {
            perspective: [query_spec.query for query_spec in query_specs]
            for perspective, query_specs in query_specs_by_perspective.items()
        }
        focus_terms = _focus_terms(request)

        for perspective, query_specs in query_specs_by_perspective.items():
            candidates: dict[str, _CandidateChunk] = {}
            for query_spec in query_specs:
                try:
                    results = _search_chunks_for_brief(
                        self.repository,
                        ResearchChunkSearchRequest(
                            query=query_spec.query,
                            source_key=request.source_key,
                            limit=min(max(request.max_chunks_per_perspective * 4, 12), 50),
                            max_chunk_chars=request.max_chunk_chars,
                            include_keyword_fallback=True,
                        ),
                    )
                except Exception as exc:
                    errors.append(f"{perspective}: chunk search failed: {exc}")
                    continue

                for result in results:
                    if focus_terms and not _result_matches_focus(result, focus_terms):
                        continue
                    chunk_key = str(result.chunk.id)
                    relevance = f"{perspective}:{query_spec.lane}:{query_spec.query}"
                    candidate = candidates.get(chunk_key)
                    if candidate is None:
                        candidates[chunk_key] = _CandidateChunk(result=result, relevances=[relevance])
                    elif relevance not in candidate.relevances:
                        candidate.relevances.append(relevance)

            for candidate in _rank_perspective_candidates(
                candidates.values(),
                perspective=perspective,
                focus_terms=focus_terms,
                limit=request.max_chunks_per_perspective,
            ):
                result = candidate.result
                chunk_key = str(result.chunk.id)
                existing = citations_by_chunk.get(chunk_key)
                if existing is not None:
                    _append_citation_relevance(existing, candidate.relevances)
                    continue
                citation = _citation_from_search_result(
                    citation_id=f"C{len(citations) + 1}",
                    result=result,
                    relevance="|".join(candidate.relevances),
                    max_chars=min(request.max_chunk_chars, 1200),
                )
                citations.append(citation)
                citations_by_chunk[chunk_key] = citation

        claims: list[ClaimSearchResult] = []
        if request.max_claims:
            try:
                claims = self.repository.search_claims(
                    ClaimSearchRequest(query=request.topic, limit=request.max_claims)
                )
            except Exception as exc:
                errors.append(f"claim search failed: {exc}")

        research_leads: list[ResearchLeadRecord] = []
        try:
            research_leads = active_research_leads_for_brief(
                self.repository,
                topic=request.topic,
                disease_scope=request.disease_scope,
                source_key=request.source_key,
            )
        except Exception as exc:
            errors.append(f"research lead lookup failed: {exc}")

        return ResearchBriefEvidenceBundle(
            citations=citations,
            claims=claims,
            research_leads=research_leads,
            search_queries=search_queries,
            errors=errors,
        )

    def build_playground_pack(self, request: ResearchBriefRequest) -> ResearchBriefPlaygroundPack:
        evidence = self.build_evidence(request)
        citations = _merge_citations(evidence.citations)
        prompts = [
            _playground_prompt(request, perspective, evidence)
            for perspective in PERSPECTIVE_ORDER
        ]
        return ResearchBriefPlaygroundPack(
            topic=request.topic,
            disease_scope=request.disease_scope,
            brief_style=request.brief_style,
            model_profile=request.model_profile,
            prompts=prompts,
            citations=citations,
            evidence={
                "search_queries": evidence.search_queries,
                "claim_count": len(evidence.claims),
                "research_lead_count": len(evidence.research_leads),
                "citation_count": len(citations),
                "retrieval_strategy": "embedding_keyword_blended_perspective_rerank",
                "search_query_count": sum(len(queries) for queries in evidence.search_queries.values()),
                "mode": "manual_playground_prompt_pack",
            },
            errors=evidence.errors,
        )

    def run_perspective(
        self,
        request: ResearchBriefRequest,
        *,
        perspective: ResearchBriefPerspectiveName,
        evidence: ResearchBriefEvidenceBundle,
    ) -> ResearchBriefPerspectiveReport:
        if request.review_mode == "deterministic_only":
            return _deterministic_perspective_report(request, perspective, evidence)
        if request.review_mode == "external_required":
            return _external_required_report(request, perspective, evidence)
        if request.review_mode in {"openrouter_required", "openrouter_compare"}:
            return _run_openrouter_perspective(request, perspective, evidence)
        return _deterministic_perspective_report(request, perspective, evidence)

    def synthesize(
        self,
        request: ResearchBriefRequest,
        perspective_reports: Sequence[ResearchBriefPerspectiveReport],
        *,
        evidence: ResearchBriefEvidenceBundle,
    ) -> ResearchBriefResult:
        citations = _merge_citations(evidence.citations)
        hypotheses = [
            finding
            for report in perspective_reports
            if report.perspective == "translational_hypothesis"
            for finding in report.findings
        ][:5]
        unresolved = []
        for report in perspective_reports:
            for finding in report.findings:
                unresolved.extend(finding.open_questions)
        final_brief = _render_final_brief(request, perspective_reports, citations)
        errors = [*evidence.errors]
        for report in perspective_reports:
            errors.extend(report.errors)
        return ResearchBriefResult(
            agent_run_ids=[
                report.agent_run_id
                for report in perspective_reports
                if report.agent_run_id is not None
            ],
            topic=request.topic,
            disease_scope=request.disease_scope,
            brief_style=request.brief_style,
            model_profile=request.model_profile,
            perspective_reports=list(perspective_reports),
            final_brief=final_brief,
            ranked_hypotheses=hypotheses,
            unresolved_questions=_dedupe_strings(unresolved)[:12],
            citations=citations,
            evidence={
                "search_queries": evidence.search_queries,
                "claim_count": len(evidence.claims),
                "citation_count": len(citations),
                "research_lead_count": len(evidence.research_leads),
                "review_mode": request.review_mode,
                "retrieval_strategy": "embedding_keyword_blended_perspective_rerank",
                "search_query_count": sum(len(queries) for queries in evidence.search_queries.values()),
            },
            errors=errors,
        )


def summarize_perspective_report(report: ResearchBriefPerspectiveReport) -> dict[str, Any]:
    return {
        "perspective": report.perspective,
        "finding_count": len(report.findings),
        "citation_count": len(report.citations),
        "error_count": len(report.errors),
    }


def summarize_research_brief(report: ResearchBriefResult) -> dict[str, Any]:
    return {
        "topic": report.topic,
        "perspective_count": len(report.perspective_reports),
        "finding_count": sum(len(item.findings) for item in report.perspective_reports),
        "citation_count": len(report.citations),
        "hypothesis_count": len(report.ranked_hypotheses),
        "error_count": len(report.errors),
    }


def _perspective_queries(request: ResearchBriefRequest) -> dict[ResearchBriefPerspectiveName, list[_PerspectiveQuery]]:
    base = f"{request.topic} {request.disease_scope}".strip()
    topic = request.topic.strip()
    disease_scope = request.disease_scope.strip()
    return {
        "evidence_scout": [
            _PerspectiveQuery("direct_evidence", f"{base} evidence trial review therapy biomarker"),
            _PerspectiveQuery("mechanism", f"{topic} VEGF VEGFR angiogenesis mechanism expression response"),
        ],
        "translational_hypothesis": [
            _PerspectiveQuery("cross_species", f"{base} translational human canine pathway target drug biomarker"),
            _PerspectiveQuery("comparative_model", f"{disease_scope} comparative genomics molecular subtype therapy"),
        ],
        "skeptic_validation": [
            _PerspectiveQuery(
                "clinical_outcomes",
                f"{base} clinical trial outcome response survival efficacy toxicity adverse event",
            ),
            _PerspectiveQuery(
                "negative_evidence",
                f"{topic} negative failed failure limitation resistance no benefit progression recurrence",
            ),
            _PerspectiveQuery(
                "drug_specific",
                f"{disease_scope} VEGF VEGFR inhibitor toceranib sorafenib pazopanib bevacizumab clinical",
            ),
            _PerspectiveQuery(
                "translation_risks",
                f"{base} species mismatch reproducibility pharmacokinetic toxicity limitation validation",
            ),
        ],
    }


def _search_chunks_for_brief(
    repository: ResearchRepository,
    request: ResearchChunkSearchRequest,
) -> list[ResearchChunkSearchResult]:
    embedding_results = _search_chunks_with_embeddings(repository, request)
    keyword_results = repository.search_research_chunks(request) if request.include_keyword_fallback else []
    return _merge_search_results(embedding_results, keyword_results, limit=request.limit)


def _merge_search_results(
    *result_sets: Sequence[ResearchChunkSearchResult],
    limit: int,
) -> list[ResearchChunkSearchResult]:
    merged: dict[UUID, ResearchChunkSearchResult] = {}
    for results in result_sets:
        for result in results:
            existing = merged.get(result.chunk.id)
            if existing is None:
                merged[result.chunk.id] = result
                continue
            existing_score = existing.score or 0.0
            result_score = result.score or 0.0
            if result_score > existing_score or (
                result_score == existing_score and result.match_type == "keyword"
            ):
                merged[result.chunk.id] = result

    ranked = sorted(
        merged.values(),
        key=lambda result: (
            -(result.score or 0.0),
            0 if result.match_type == "keyword" else 1,
            str(result.chunk.research_object_id),
            result.chunk.chunk_index,
        ),
    )
    return [
        result.model_copy(update={"rank": index})
        for index, result in enumerate(ranked[:limit], start=1)
    ]


def _search_chunks_with_embeddings(
    repository: ResearchRepository,
    request: ResearchChunkSearchRequest,
) -> list[ResearchChunkSearchResult]:
    coverage = repository.embedding_coverage(
        source_key=request.source_key,
        object_type=str(request.object_type) if request.object_type else None,
        embedding_model=request.embedding_model,
    )
    if coverage.embedded_chunks == 0:
        return []
    embedding_model = request.embedding_model
    if embedding_model is None:
        embedding_model = (
            max(coverage.embedding_models.items(), key=lambda item: item[1])[0]
            if coverage.embedding_models
            else LOCAL_HASH_EMBEDDING_MODEL
        )
    existing = repository.list_text_embeddings(
        embedding_model=embedding_model,
        source_key=request.source_key,
        research_object_id=request.research_object_id,
        object_type=str(request.object_type) if request.object_type else None,
        limit=1,
    )
    provider = LocalDeterministicEmbeddingProvider(
        embedding_model=embedding_model,
        dimensions=existing[0].embedding_dimensions if existing else LocalDeterministicEmbeddingProvider().dimensions,
    )
    query_vector = provider.embed_text(request.query)
    if not any(query_vector):
        return []
    hits = repository.search_text_embeddings(
        TextEmbeddingSearchRequest(
            query_embedding=query_vector,
            embedding_model=embedding_model,
            source_key=request.source_key,
            research_object_id=request.research_object_id,
            object_type=request.object_type,
            min_score=request.min_score,
            limit=min(max(request.limit * 20, request.limit), 100),
        )
    )
    results: list[ResearchChunkSearchResult] = []
    seen_chunks: set[str] = set()
    for hit in hits:
        chunk = repository.get_document_chunk(hit.embedding.chunk_id)
        if chunk is None or str(chunk.id) in seen_chunks:
            continue
        seen_chunks.add(str(chunk.id))
        research_object = repository.get_research_object(chunk.research_object_id)
        results.append(
            ResearchChunkSearchResult(
                rank=len(results) + 1,
                chunk=chunk,
                research_object=research_object,
                score=hit.score,
                match_type="embedding",
                text_truncated=len(chunk.text_content) > request.max_chunk_chars,
            )
        )
        if len(results) >= request.limit:
            break
    return results


def _focus_terms(request: ResearchBriefRequest) -> set[str]:
    stopwords = {
        "and",
        "human",
        "canine",
        "therapy",
        "therapies",
        "evidence",
        "trial",
        "trials",
        "review",
        "reviews",
        "biomarker",
        "biomarkers",
        "target",
        "targets",
        "translational",
        "clinical",
    }
    terms = {
        term
        for term in re.findall(r"[a-z0-9]{4,}", f"{request.topic} {request.disease_scope}".lower())
        if term not in stopwords
    }
    disease_terms = {term for term in terms if "sarcoma" in term or term == "hsa"}
    return disease_terms or terms


def _result_matches_focus(result, focus_terms: set[str]) -> bool:
    haystack = _result_haystack(result)
    return any(term in haystack for term in focus_terms)


def _result_haystack(result: ResearchChunkSearchResult) -> str:
    obj = result.research_object
    return " ".join(
        value
        for value in (
            result.chunk.text_content,
            result.chunk.section_label or "",
            obj.title if obj else "",
            obj.abstract if obj else "",
        )
        if value
    ).lower()


def _rank_perspective_candidates(
    candidates: Sequence[_CandidateChunk],
    *,
    perspective: ResearchBriefPerspectiveName,
    focus_terms: set[str],
    limit: int,
) -> list[_CandidateChunk]:
    ranked = sorted(
        candidates,
        key=lambda candidate: _candidate_score(candidate, perspective, focus_terms),
        reverse=True,
    )
    return ranked[:limit]


def _candidate_score(
    candidate: _CandidateChunk,
    perspective: ResearchBriefPerspectiveName,
    focus_terms: set[str],
) -> tuple[float, float, int]:
    result = candidate.result
    haystack = _result_haystack(result)
    score = float(result.score or 0.0)
    score += min(len(candidate.relevances), 4) * 0.12
    score += _term_bonus(haystack, focus_terms, weight=0.15, cap=0.45)
    score += _term_bonus(haystack, _PERSPECTIVE_SIGNAL_TERMS[perspective], weight=0.18, cap=0.9)

    if perspective == "skeptic_validation":
        score += _term_bonus(haystack, _SKEPTIC_OUTCOME_TERMS, weight=0.28, cap=1.4)
        score += _term_bonus(haystack, _SKEPTIC_DRUG_TERMS, weight=0.22, cap=0.88)
        score += _term_bonus(haystack, _SKEPTIC_RISK_TERMS, weight=0.18, cap=0.9)
        if not _contains_any(haystack, _SKEPTIC_OUTCOME_TERMS):
            score -= 0.35
        if _contains_any(haystack, _LOW_SIGNAL_SKEPTIC_TERMS) and not _contains_any(
            haystack,
            _SKEPTIC_OUTCOME_TERMS | _SKEPTIC_DRUG_TERMS,
        ):
            score -= 0.6

    return (score, float(result.score or 0.0), -result.rank)


_PERSPECTIVE_SIGNAL_TERMS: dict[ResearchBriefPerspectiveName, set[str]] = {
    "evidence_scout": {
        "clinical",
        "trial",
        "evidence",
        "response",
        "survival",
        "efficacy",
        "therapy",
        "vegf",
        "vegfr",
        "angiogenesis",
        "biomarker",
    },
    "translational_hypothesis": {
        "translational",
        "comparative",
        "canine",
        "human",
        "angiosarcoma",
        "hemangiosarcoma",
        "genomic",
        "molecular",
        "pathway",
        "model",
        "target",
    },
    "skeptic_validation": {
        "limitation",
        "negative",
        "toxicity",
        "adverse",
        "resistance",
        "clinical",
        "trial",
        "outcome",
        "survival",
        "response",
        "efficacy",
        "failed",
        "failure",
    },
}
_SKEPTIC_OUTCOME_TERMS = {
    "clinical",
    "trial",
    "outcome",
    "response",
    "survival",
    "progression",
    "recurrence",
    "efficacy",
    "benefit",
    "phase",
    "median survival",
    "overall survival",
    "progression-free",
}
_SKEPTIC_DRUG_TERMS = {
    "vegf",
    "vegfr",
    "inhibitor",
    "toceranib",
    "sorafenib",
    "pazopanib",
    "bevacizumab",
    "sunitinib",
    "masitinib",
    "anti-angiogenic",
    "antiangiogenic",
}
_SKEPTIC_RISK_TERMS = {
    "negative",
    "failed",
    "failure",
    "toxicity",
    "adverse",
    "resistance",
    "limitation",
    "no benefit",
    "not significant",
    "species mismatch",
}
_LOW_SIGNAL_SKEPTIC_TERMS = {
    "mirna",
    "microrna",
    "expression profile",
    "bioinformatic",
    "predicted target",
}


def _term_bonus(haystack: str, terms: set[str], *, weight: float, cap: float) -> float:
    if not terms:
        return 0.0
    hits = sum(1 for term in terms if term in haystack)
    return min(hits * weight, cap)


def _contains_any(haystack: str, terms: set[str]) -> bool:
    return any(term in haystack for term in terms)


def _append_citation_relevance(citation: ResearchBriefCitation, relevances: Sequence[str]) -> None:
    existing = [value for value in str(citation.relevance or "").split("|") if value]
    changed = False
    for relevance in relevances:
        if relevance not in existing:
            existing.append(relevance)
            changed = True
    if changed:
        citation.relevance = "|".join(existing)
        citation.metadata["perspectives"] = sorted(
            {
                relevance.split(":", 1)[0]
                for relevance in existing
                if ":" in relevance
            }
        )


def _citation_from_search_result(
    *,
    citation_id: str,
    result,
    relevance: str,
    max_chars: int,
) -> ResearchBriefCitation:
    chunk = result.chunk
    obj = result.research_object
    quote = _compact_text(chunk.text_content, max_chars=max_chars)
    return ResearchBriefCitation(
        citation_id=citation_id,
        chunk_id=chunk.id,
        research_object_id=chunk.research_object_id,
        source_key=obj.source_key if obj else chunk.source_key if hasattr(chunk, "source_key") else None,
        title=obj.title if obj else None,
        source_url=obj.canonical_url if obj else None,
        section_label=chunk.section_label,
        quote=quote or "Stored chunk had no displayable text.",
        relevance=relevance,
        metadata={
            "rank": result.rank,
            "score": result.score,
            "match_type": result.match_type,
        },
    )


def _deterministic_perspective_report(
    request: ResearchBriefRequest,
    perspective: ResearchBriefPerspectiveName,
    evidence: ResearchBriefEvidenceBundle,
) -> ResearchBriefPerspectiveReport:
    citations = _citations_for_perspective(evidence.citations, perspective)
    if not citations:
        return ResearchBriefPerspectiveReport(
            perspective=perspective,
            agent_name=_PERSPECTIVE_AGENT_NAMES[perspective],
            model_profile=request.model_profile,
            summary=f"No stored evidence was found for {request.topic}.",
            citations=[],
            findings=[],
            evidence={"review_mode": request.review_mode, "deterministic_floor": True},
            errors=list(evidence.errors),
        )

    citation_ids = [citation.citation_id for citation in citations[:3]]
    if perspective == "evidence_scout":
        claim = f"Stored literature evidence relevant to {request.topic} is present and should be reviewed directly."
        stance: ResearchBriefStance = "supporting"
        strength: ResearchBriefEvidenceStrength = "medium"
        reasoning = "The evidence scout found stored chunks that directly match the topic and disease scope."
    elif perspective == "translational_hypothesis":
        claim = (
            f"{request.topic} has a plausible translational-research lane when canine and human vascular sarcoma "
            "evidence are evaluated together."
        )
        stance = "opportunity"
        strength = "low"
        reasoning = "The translational agent treats stored cross-species evidence as hypothesis-generating until validated."
    else:
        claim = f"The current evidence base for {request.topic} still needs validation before operational decisions."
        stance = "risk"
        strength = "medium"
        reasoning = "The skeptic agent flags species mismatch, source quality, and missing negative evidence as open risks."

    finding = ResearchBriefFinding(
        claim=claim,
        stance=stance,
        citations=citation_ids,
        evidence_strength=strength,
        reasoning=reasoning,
        open_questions=_default_open_questions(perspective),
    )
    return ResearchBriefPerspectiveReport(
        perspective=perspective,
        agent_name=_PERSPECTIVE_AGENT_NAMES[perspective],
        model_profile=request.model_profile,
        summary=_perspective_summary(perspective, request, citation_ids),
        findings=[finding],
        citations=citations,
        evidence={
            "review_mode": request.review_mode,
            "deterministic_floor": True,
            "claim_count": len(evidence.claims),
            "research_lead_count": len(evidence.research_leads),
        },
        errors=list(evidence.errors),
    )


def _external_required_report(
    request: ResearchBriefRequest,
    perspective: ResearchBriefPerspectiveName,
    evidence: ResearchBriefEvidenceBundle,
) -> ResearchBriefPerspectiveReport:
    citations = _citations_for_perspective(evidence.citations, perspective)
    return ResearchBriefPerspectiveReport(
        perspective=perspective,
        agent_name=_PERSPECTIVE_AGENT_NAMES[perspective],
        model_profile=request.model_profile,
        summary="External model review required. Use the evidence payload with Claude/OpenAI Pro or OpenRouter.",
        findings=[],
        citations=citations,
        evidence={
            "review_mode": request.review_mode,
            "external_review_required": True,
            "prompt_payload": _perspective_payload(request, perspective, evidence),
        },
        errors=list(evidence.errors),
    )


def _run_openrouter_perspective(
    request: ResearchBriefRequest,
    perspective: ResearchBriefPerspectiveName,
    evidence: ResearchBriefEvidenceBundle,
) -> ResearchBriefPerspectiveReport:
    models = _select_models(request)
    errors: list[str] = []
    for model_name in models:
        try:
            review = _openrouter_review_model(model_name, _perspective_payload(request, perspective, evidence))
            return _perspective_report_from_model(
                request,
                perspective,
                evidence,
                review,
            )
        except Exception as exc:
            errors.append(f"{model_name}: {exc}")
            if request.review_mode == "openrouter_required":
                break
    if request.review_mode == "openrouter_required":
        raise RuntimeError("; ".join(errors) or "OpenRouter review failed")
    fallback = _deterministic_perspective_report(request, perspective, evidence)
    return fallback.model_copy(update={"errors": [*fallback.errors, *errors]})


def _perspective_report_from_model(
    request: ResearchBriefRequest,
    perspective: ResearchBriefPerspectiveName,
    evidence: ResearchBriefEvidenceBundle,
    review: Mapping[str, Any],
) -> ResearchBriefPerspectiveReport:
    citations = _citations_for_perspective(evidence.citations, perspective)
    valid_ids = {citation.citation_id for citation in citations}
    payload = _load_json_object(str(review.get("text") or ""))
    findings = [
        finding
        for raw in payload.get("findings", [])
        if isinstance(raw, Mapping)
        if (finding := _finding_from_payload(raw, valid_ids)) is not None
    ]
    errors = list(evidence.errors)
    errors.extend(str(error) for error in payload.get("errors", []) if error)
    return ResearchBriefPerspectiveReport(
        perspective=perspective,
        agent_name=_PERSPECTIVE_AGENT_NAMES[perspective],
        model_profile=request.model_profile,
        summary=str(payload.get("summary") or f"{perspective} completed model review."),
        findings=findings,
        citations=citations,
        evidence={
            "review_mode": request.review_mode,
            "model_review": review.get("metadata", {}),
        },
        errors=errors,
    )


def _finding_from_payload(raw: Mapping[str, Any], valid_ids: set[str]) -> ResearchBriefFinding | None:
    citations = [
        str(citation_id)
        for citation_id in raw.get("citations", [])
        if str(citation_id) in valid_ids
    ]
    if not citations:
        return None
    stance = str(raw.get("stance") or "uncertain")
    strength = str(raw.get("evidence_strength") or "unknown")
    return ResearchBriefFinding(
        claim=str(raw.get("claim") or "").strip() or "Model supplied an empty claim.",
        stance=stance if stance in _ALLOWED_STANCES else "uncertain",  # type: ignore[arg-type]
        citations=citations,
        evidence_strength=strength if strength in _ALLOWED_STRENGTHS else "unknown",  # type: ignore[arg-type]
        reasoning=str(raw.get("reasoning") or "").strip() or "No reasoning supplied.",
        open_questions=[
            str(question)
            for question in raw.get("open_questions", [])
            if str(question).strip()
        ][:10],
        metadata={
            key: value
            for key, value in raw.items()
            if key not in {"claim", "stance", "citations", "evidence_strength", "reasoning", "open_questions"}
        },
    )


def _playground_prompt(
    request: ResearchBriefRequest,
    perspective: ResearchBriefPerspectiveName,
    evidence: ResearchBriefEvidenceBundle,
) -> ResearchBriefPlaygroundPrompt:
    payload = _perspective_payload(request, perspective, evidence)
    response_contract = _research_brief_response_contract()
    user_prompt = "\n".join(
        [
            "Review the supplied evidence payload using the assigned perspective.",
            "Return JSON only. Do not include markdown, prose outside JSON, or citations that are not present in the payload.",
            "Use uncertainty explicitly when the supplied evidence is weak or indirect.",
            "",
            "RESPONSE_CONTRACT_JSON:",
            json.dumps(response_contract, indent=2, sort_keys=True, default=str),
            "",
            "EVIDENCE_PAYLOAD_JSON:",
            json.dumps(payload, indent=2, sort_keys=True, default=str),
        ]
    )
    return ResearchBriefPlaygroundPrompt(
        perspective=perspective,
        agent_name=_PERSPECTIVE_AGENT_NAMES[perspective],
        recommended_model=DEFAULT_RESEARCH_BRIEF_MODEL,
        system_prompt=_RESEARCH_BRIEF_SYSTEM_PROMPT,
        user_prompt=user_prompt,
        prompt_payload=payload,
        response_contract=response_contract,
        evaluation_rubric=_research_brief_evaluation_rubric(perspective),
        playground_steps=[
            "Open the model playground and choose the recommended model or the model under test.",
            "Paste system_prompt as the system/developer instruction.",
            "Paste user_prompt as the user message.",
            "Require JSON output if the playground has a response-format option.",
            "Save the raw model JSON and compare it against response_contract before trusting the finding.",
        ],
    )


def _research_brief_response_contract() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["summary", "findings", "errors"],
        "properties": {
            "summary": "One concise paragraph describing what this perspective concluded.",
            "findings": [
                {
                    "claim": "Single evidence-backed claim.",
                    "stance": "supporting | contradicting | uncertain | opportunity | risk",
                    "citations": ["C1"],
                    "evidence_strength": "high | medium | low | unknown",
                    "reasoning": "Why the supplied citations support this claim.",
                    "open_questions": ["Concrete next question or validation gap."],
                }
            ],
            "errors": [
                "Use for evidence gaps, invalid citation pressure, weak supplied context, or reasons the model could not answer."
            ],
        },
        "hard_rules": [
            "Every finding must cite at least one supplied citation ID.",
            "Do not cite papers, identifiers, URLs, or claims that are not supplied in EVIDENCE_PAYLOAD_JSON.",
            "Research leads are watchlist context only; they cannot satisfy citation requirements.",
            "Do not browse or rely on outside knowledge unless a human explicitly asks for a separate web-research pass.",
            "Prefer saying the evidence is weak over filling gaps with plausible biology.",
        ],
    }


def _research_brief_evaluation_rubric(perspective: ResearchBriefPerspectiveName) -> list[str]:
    common = [
        "Uses only supplied citation IDs.",
        "Each finding is specific enough to validate against the cited quote.",
        "Uncertainty is explicit where evidence is indirect.",
        "No invented paper titles, trial names, endpoints, or statistics.",
    ]
    if perspective == "evidence_scout":
        return [
            "Prioritizes direct experimental, clinical, review, and biomarker evidence.",
            "Separates established evidence from hypothesis-generating statements.",
            *common,
        ]
    if perspective == "translational_hypothesis":
        return [
            "Connects canine-human evidence only where the citations justify the bridge.",
            "Frames opportunities as testable hypotheses, not conclusions.",
            *common,
        ]
    return [
        "Actively looks for weak links: clinical outcomes, toxicity, resistance, species mismatch, and negative evidence.",
        "Flags missing primary evidence instead of over-weighting adjacent molecular data.",
        *common,
    ]


def _perspective_payload(
    request: ResearchBriefRequest,
    perspective: ResearchBriefPerspectiveName,
    evidence: ResearchBriefEvidenceBundle,
) -> dict[str, Any]:
    citations = _citations_for_perspective(evidence.citations, perspective)
    return {
        "task": "research_brief_perspective",
        "perspective": perspective,
        "topic": request.topic,
        "disease_scope": request.disease_scope,
        "brief_style": request.brief_style,
        "instructions": _perspective_instruction(perspective),
        "citations": [citation.model_dump(mode="json") for citation in citations],
        "claims": [claim.model_dump(mode="json") for claim in evidence.claims],
        "research_leads": [
            _research_lead_payload(lead)
            for lead in evidence.research_leads[:20]
        ],
        "requirements": {
            "use_only_supplied_citation_ids": True,
            "every_finding_requires_citation": True,
            "research_leads_are_watchlist_context_not_citable_evidence": True,
            "preserve_uncertainty": True,
        },
    }


def _research_lead_payload(lead: ResearchLeadRecord) -> dict[str, Any]:
    return {
        "lead_id": str(lead.lead_id),
        "title": lead.title,
        "url": lead.url,
        "lead_type": lead.lead_type,
        "status": lead.status,
        "priority": lead.priority,
        "source_key": lead.source_key,
        "reason": lead.reason,
        "summary": lead.summary,
        "topic_tags": lead.topic_tags,
        "suggested_sources": lead.suggested_sources,
        "origin_agent_run_id": str(lead.origin_agent_run_id) if lead.origin_agent_run_id else None,
    }


def _perspective_instruction(perspective: ResearchBriefPerspectiveName) -> str:
    if perspective == "evidence_scout":
        return "Find the strongest directly relevant evidence and describe what is actually known."
    if perspective == "translational_hypothesis":
        return "Connect canine and human evidence into hypothesis-generating translational opportunities."
    return "Challenge the evidence, identify weak links, and state what would change confidence."


def _perspective_summary(
    perspective: ResearchBriefPerspectiveName,
    request: ResearchBriefRequest,
    citation_ids: Sequence[str],
) -> str:
    cited = ", ".join(f"[{citation_id}]" for citation_id in citation_ids)
    if perspective == "evidence_scout":
        return f"Evidence scout found stored sources for {request.topic}: {cited}."
    if perspective == "translational_hypothesis":
        return f"Translational hypothesis agent found a hypothesis-generating path: {cited}."
    return f"Skeptic validation agent found review risks that should remain visible: {cited}."


def _default_open_questions(perspective: ResearchBriefPerspectiveName) -> list[str]:
    if perspective == "evidence_scout":
        return ["Which cited studies have the strongest experimental or clinical design?"]
    if perspective == "translational_hypothesis":
        return ["Which canine-human pathway parallels are supported by direct comparative data?"]
    return ["What negative or null evidence is missing from the current retrieved set?"]


def _citations_for_perspective(
    citations: Sequence[ResearchBriefCitation],
    perspective: ResearchBriefPerspectiveName,
) -> list[ResearchBriefCitation]:
    prefix = f"{perspective}:"
    scoped = [citation for citation in citations if prefix in str(citation.relevance or "")]
    return scoped or list(citations[:5])


def _merge_citations(citations: Sequence[ResearchBriefCitation]) -> list[ResearchBriefCitation]:
    seen: set[str] = set()
    merged: list[ResearchBriefCitation] = []
    for citation in citations:
        key = str(citation.chunk_id)
        if key in seen:
            continue
        seen.add(key)
        merged.append(citation)
    return merged


def _render_final_brief(
    request: ResearchBriefRequest,
    reports: Sequence[ResearchBriefPerspectiveReport],
    citations: Sequence[ResearchBriefCitation],
) -> str:
    if not citations:
        return ""
    sections = [
        f"# Research Brief: {request.topic}",
        "",
        f"Scope: {request.disease_scope}.",
        "",
    ]
    for report in reports:
        sections.append(f"## {report.perspective.replace('_', ' ').title()}")
        sections.append(report.summary)
        for finding in report.findings:
            cited = " ".join(f"[{citation_id}]" for citation_id in finding.citations)
            sections.append(f"- {finding.claim} {cited}")
        sections.append("")
    sections.append("## Citation Map")
    for citation in citations[:20]:
        title = citation.title or str(citation.research_object_id)
        sections.append(f"- [{citation.citation_id}] {title}")
    return "\n".join(sections).strip()


def _compact_text(value: str, *, max_chars: int) -> str:
    text = re.sub(r"\s+", " ", value).strip()
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3].rstrip()}..."


def _dedupe_strings(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        cleaned = re.sub(r"\s+", " ", value).strip()
        if not cleaned or cleaned.lower() in seen:
            continue
        seen.add(cleaned.lower())
        deduped.append(cleaned)
    return deduped


def _load_json_object(text: str) -> dict[str, Any]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.S)
        if not match:
            raise
        payload = json.loads(match.group(0))
    if not isinstance(payload, dict):
        raise ValueError("model response JSON must be an object")
    return payload


def _select_models(request: ResearchBriefRequest) -> list[str]:
    if request.review_models:
        return list(request.review_models)
    if request.review_mode == "openrouter_compare":
        return list(DEFAULT_RESEARCH_BRIEF_COMPARE_MODELS)
    return [os.getenv("HSA_RESEARCH_BRIEF_MODEL", DEFAULT_RESEARCH_BRIEF_MODEL)]


def _openrouter_review_model(model_name: str, review_payload: dict[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required for OpenRouter research brief mode.")
    payload = {
        "model": model_name,
        "temperature": float(os.getenv("HSA_RESEARCH_BRIEF_TEMPERATURE", "0.2")),
        "max_tokens": int(os.getenv("HSA_RESEARCH_BRIEF_MAX_TOKENS", "2500")),
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _RESEARCH_BRIEF_SYSTEM_PROMPT},
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
            timeout=float(os.getenv("HSA_RESEARCH_BRIEF_TIMEOUT_SECONDS", "120")),
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
    message = choices[0].get("message") or {}
    text = message.get("content") or ""
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
