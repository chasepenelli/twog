"""Therapy ideation committee agents.

The committee is recommend-only. It turns stored evidence into cited therapy
ideas, critiques them from multiple perspectives, and returns structured output
that can later feed validation planning.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from collections.abc import Mapping, Sequence
import json
import os
import re
from typing import Any
import urllib.error
import urllib.request

from .contracts import (
    ResearchBriefCitation,
    ResearchBriefRequest,
    ResearchBriefEvidenceStrength,
    TherapyCommitteePerspectiveName,
    TherapyCommitteeReport,
    TherapyCommitteeRequest,
    TherapyCommitteeResult,
    TherapyIdea,
)
from .research_brief_agent import ResearchBriefAgent
from .repository import ResearchRepository


THERAPY_COMMITTEE_AGENT_NAME = "therapy_committee_chair_agent"
THERAPY_COMMITTEE_AGENT_VERSION = "v1"
DEFAULT_THERAPY_COMMITTEE_MODEL = "~anthropic/claude-sonnet-latest"
THERAPY_COMMITTEE_PERSPECTIVES: tuple[TherapyCommitteePerspectiveName, ...] = (
    "target_biology",
    "drug_repurposing",
    "translational_clinical",
    "skeptic_risk",
)

_PERSPECTIVE_AGENT_NAMES = {
    "target_biology": "target_biology_committee_agent",
    "drug_repurposing": "drug_repurposing_committee_agent",
    "translational_clinical": "translational_clinical_committee_agent",
    "skeptic_risk": "skeptic_risk_committee_agent",
}
_TARGET_TERMS = (
    "ANGPT2",
    "EGFR",
    "FLT4",
    "KDR",
    "KIT",
    "MET",
    "MTOR",
    "PDGFRB",
    "PIK3CA",
    "PTEN",
    "TP53",
    "VEGFA",
    "VEGFR",
)
_THERAPY_TERMS = (
    "dasatinib",
    "doxorubicin",
    "mirdametinib",
    "paclitaxel",
    "pazopanib",
    "propranolol",
    "rapamycin",
    "sirolimus",
    "sorafenib",
    "toceranib",
    "trametinib",
    "vorinostat",
)
_BIOMARKER_TERMS = (
    "CD31",
    "CD34",
    "KIT",
    "KDR",
    "MKI67",
    "MYC",
    "PDGFRB",
    "PIK3CA",
    "PTEN",
    "VEGFA",
)
_SYSTEM_PROMPT = """You are a therapy ideation committee agent for translational oncology.
Use only the supplied citation IDs and evidence. Do not invent papers, trial results, targets, or drug effects.
Every idea must include at least one supplied citation ID in evidence_refs.
Return strict JSON only with: summary, ideas, evidence_limitations, errors.
Each idea requires: title, hypothesis, rationale, candidate_therapies, targets, biomarkers, mechanism, evidence_refs, evidence_strength, translational_path, risks, next_experiments, priority_score.
Keep ideas recommend-only. Do not claim a cure is proven. Distinguish direct evidence from translational analog evidence."""


def run_therapy_committee(
    repository: ResearchRepository,
    request: TherapyCommitteeRequest,
) -> TherapyCommitteeResult:
    """Run the multi-perspective therapy committee over stored evidence."""

    evidence = _build_evidence(repository, request)
    reports = _run_perspectives(request, evidence)
    ranked_ideas = _rank_ideas([idea for report in reports for idea in report.ideas])[:12]
    errors = _dedupe_strings([*evidence["errors"], *[error for report in reports for error in report.errors]])
    limitations = _dedupe_strings(
        limitation
        for report in reports
        for limitation in report.evidence_limitations
    )
    return TherapyCommitteeResult(
        topic=request.topic,
        disease_scope=request.disease_scope,
        model_profile=request.model_profile,
        review_mode=request.review_mode,
        reports=reports,
        ranked_ideas=ranked_ideas,
        decision_summary=_decision_summary(ranked_ideas, limitations),
        evidence={
            "citation_count": len(evidence["citations"]),
            "claim_count": len(evidence["claims"]),
            "research_lead_count": len(evidence["research_leads"]),
            "search_queries": evidence["search_queries"],
            "evidence_limitations": limitations,
            "recommend_only": True,
        },
        errors=errors,
    )


def summarize_therapy_committee(result: TherapyCommitteeResult) -> dict[str, Any]:
    return {
        "committee_run_id": str(result.committee_run_id),
        "topic": result.topic,
        "perspective_count": len(result.reports),
        "idea_count": len(result.ranked_ideas),
        "error_count": len(result.errors),
        "top_idea": result.ranked_ideas[0].title if result.ranked_ideas else None,
    }


def _run_perspectives(
    request: TherapyCommitteeRequest,
    evidence: Mapping[str, Any],
) -> list[TherapyCommitteeReport]:
    max_workers = max(1, min(len(THERAPY_COMMITTEE_PERSPECTIVES), int(os.getenv("HSA_THERAPY_COMMITTEE_WORKERS", "4"))))
    if max_workers == 1:
        return [_run_perspective(request, perspective, evidence) for perspective in THERAPY_COMMITTEE_PERSPECTIVES]

    reports_by_perspective: dict[TherapyCommitteePerspectiveName, TherapyCommitteeReport] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_by_perspective = {
            executor.submit(_run_perspective, request, perspective, evidence): perspective
            for perspective in THERAPY_COMMITTEE_PERSPECTIVES
        }
        for future in as_completed(future_by_perspective):
            perspective = future_by_perspective[future]
            reports_by_perspective[perspective] = future.result()
    return [reports_by_perspective[perspective] for perspective in THERAPY_COMMITTEE_PERSPECTIVES]


def _build_evidence(repository: ResearchRepository, request: TherapyCommitteeRequest) -> dict[str, Any]:
    brief_request = ResearchBriefRequest(
        topic=request.topic,
        disease_scope=request.disease_scope,
        source_key=request.source_key,
        max_chunks_per_perspective=request.max_chunks_per_perspective,
        max_claims=request.max_claims,
        max_chunk_chars=request.max_chunk_chars,
        model_profile=request.model_profile,
        review_mode="deterministic_only",
    )
    evidence = ResearchBriefAgent(repository).build_evidence(brief_request)
    return {
        "citations": evidence.citations,
        "claims": [claim.model_dump(mode="json") for claim in evidence.claims],
        "research_leads": [lead.model_dump(mode="json") for lead in evidence.research_leads],
        "search_queries": evidence.search_queries,
        "errors": evidence.errors,
    }


def _run_perspective(
    request: TherapyCommitteeRequest,
    perspective: TherapyCommitteePerspectiveName,
    evidence: Mapping[str, Any],
) -> TherapyCommitteeReport:
    citations = list(evidence.get("citations") or [])
    if request.review_mode == "external_required":
        return TherapyCommitteeReport(
            perspective=perspective,
            agent_name=_PERSPECTIVE_AGENT_NAMES[perspective],
            model_profile=request.model_profile,
            summary="External model review required. Use the prompt payload in evidence.",
            evidence_limitations=[],
            errors=list(evidence.get("errors") or []),
            ideas=[],
        ).model_copy(
            update={
                "evidence": {
                    "prompt_payload": _perspective_payload(request, perspective, evidence),
                }
            }
        )
    if request.review_mode in {"openrouter_required", "openrouter_compare"}:
        return _run_openrouter_perspective(request, perspective, evidence)
    return _deterministic_perspective(request, perspective, citations, evidence)


def _run_openrouter_perspective(
    request: TherapyCommitteeRequest,
    perspective: TherapyCommitteePerspectiveName,
    evidence: Mapping[str, Any],
) -> TherapyCommitteeReport:
    errors: list[str] = []
    for model_name in _select_models(request):
        try:
            review = _openrouter_review_model(model_name, _perspective_payload(request, perspective, evidence))
            return _report_from_model(request, perspective, evidence, review)
        except Exception as exc:
            errors.append(f"{model_name}: {exc}")
            if request.review_mode == "openrouter_required":
                break
    if request.review_mode == "openrouter_required":
        raise RuntimeError("; ".join(errors) or "OpenRouter therapy committee review failed")
    fallback = _deterministic_perspective(
        request,
        perspective,
        list(evidence.get("citations") or []),
        evidence,
    )
    return fallback.model_copy(update={"errors": [*fallback.errors, *errors]})


def _deterministic_perspective(
    request: TherapyCommitteeRequest,
    perspective: TherapyCommitteePerspectiveName,
    citations: Sequence[ResearchBriefCitation],
    evidence: Mapping[str, Any],
) -> TherapyCommitteeReport:
    if not citations:
        return TherapyCommitteeReport(
            perspective=perspective,
            agent_name=_PERSPECTIVE_AGENT_NAMES[perspective],
            model_profile=request.model_profile,
            summary="No citations were retrieved, so the committee cannot generate cited therapy ideas.",
            ideas=[],
            evidence_limitations=["No stored citations matched the committee request."],
            errors=list(evidence.get("errors") or []),
        )

    text = " ".join([citation.quote for citation in citations])
    targets = _prioritize_terms(_extract_terms(text, _TARGET_TERMS), request.topic)
    therapies = _prioritize_terms(_extract_terms(text, _THERAPY_TERMS), request.topic)
    biomarkers = _prioritize_terms(_extract_terms(text, _BIOMARKER_TERMS), request.topic)
    refs = [citation.citation_id for citation in citations[:4]]
    ideas: list[TherapyIdea] = []

    if perspective == "target_biology":
        target = targets[0] if targets else "VEGF/PI3K-mTOR axis"
        ideas.append(
            TherapyIdea(
                title=f"Target biology review for {target}",
                hypothesis=f"{target} may define a tractable vulnerability in comparative HSA/angiosarcoma evidence.",
                rationale="Prioritize targets repeatedly present in the retrieved corpus before selecting therapeutic combinations.",
                targets=targets[:5],
                biomarkers=biomarkers[:5],
                mechanism=f"Map {target} signaling to angiogenesis, survival, and vascular tumor phenotype.",
                evidence_refs=refs,
                evidence_strength=_strength_from_refs(refs),
                translational_path="Confirm target expression and pathway activity in canine HSA samples before therapy selection.",
                risks=["Target presence may not imply dependency.", "Human analog evidence may not transfer directly."],
                next_experiments=[
                    "Confirm target and biomarker expression in canine HSA tissue or cell models.",
                    "Compare target activity against human angiosarcoma datasets.",
                ],
                priority_score=0.72 if targets else 0.58,
            )
        )
    elif perspective == "drug_repurposing":
        therapy = therapies[0] if therapies else "repurposed anti-angiogenic or kinase inhibitor"
        ideas.append(
            TherapyIdea(
                title=f"Repurposing screen around {therapy}",
                hypothesis=f"{therapy} or a rational combination may be worth prioritizing if matched to target biology and safety constraints.",
                rationale="Repurposed agents reduce translational friction when supported by target, safety, or analog disease evidence.",
                candidate_therapies=therapies[:6] or [therapy],
                targets=targets[:5],
                biomarkers=biomarkers[:5],
                mechanism="Use mechanism-matched agents rather than broad empiric treatment selection.",
                evidence_refs=refs,
                evidence_strength=_strength_from_refs(refs),
                translational_path="Rank candidates by target match, canine safety feasibility, and evidence density.",
                risks=["Combination toxicity may dominate efficacy signal.", "Bioactivity does not equal tumor response."],
                next_experiments=[
                    "Create a candidate-by-target matrix from ChEMBL, PubChem, UniProt, and literature claims.",
                    "Run conservative in vitro viability and pathway readout assays before animal-facing work.",
                ],
                priority_score=0.7 if therapies else 0.55,
            )
        )
    elif perspective == "translational_clinical":
        ideas.append(
            TherapyIdea(
                title="Canine-human translational bridge",
                hypothesis="Comparative evidence can nominate therapies only when canine context, human angiosarcoma analogs, and assay readouts align.",
                rationale="The committee should separate direct canine HSA evidence from human analog evidence and use both explicitly.",
                candidate_therapies=therapies[:5],
                targets=targets[:5],
                biomarkers=biomarkers[:5],
                mechanism="Bridge therapy ideas through shared vascular tumor biology and measurable response biomarkers.",
                evidence_refs=refs,
                evidence_strength=_strength_from_refs(refs),
                translational_path="Define species, disease context, model system, endpoint, and comparator before validation dispatch.",
                risks=["Cross-species translation can overstate confidence.", "Endpoints may not map cleanly across models."],
                next_experiments=[
                    "Build a direct-versus-analog evidence table for each therapy idea.",
                    "Require assay context before promoting any idea into validation queue dispatch.",
                ],
                priority_score=0.66,
            )
        )
    else:
        ideas.append(
            TherapyIdea(
                title="Skeptic gate before therapy promotion",
                hypothesis="The best near-term cure-oriented ideas should survive contradiction, safety, feasibility, and evidence-density checks.",
                rationale="Negative evidence and missing assay context should demote attractive but weak therapy concepts.",
                candidate_therapies=therapies[:5],
                targets=targets[:5],
                biomarkers=biomarkers[:5],
                mechanism="Stress-test proposed mechanisms against safety, species translation, and contradictory claims.",
                evidence_refs=refs,
                evidence_strength="low" if len(refs) < 3 else "medium",
                translational_path="Promote only ideas with enough cited evidence to specify an assay and failure criteria.",
                risks=["Evidence may be sparse or duplicated.", "The idea may be mechanistically plausible but clinically impractical."],
                next_experiments=[
                    "Search for negative and null evidence before validation planning.",
                    "Document stop criteria for each promoted idea.",
                ],
                priority_score=0.6,
            )
        )

    return TherapyCommitteeReport(
        perspective=perspective,
        agent_name=_PERSPECTIVE_AGENT_NAMES[perspective],
        model_profile=request.model_profile,
        summary=f"{perspective.replace('_', ' ').title()} generated {len(ideas)} cited recommend-only idea(s).",
        ideas=ideas[: request.max_ideas_per_perspective],
        evidence_limitations=_evidence_limitations(citations, therapies, targets),
        errors=list(evidence.get("errors") or []),
    )


def _report_from_model(
    request: TherapyCommitteeRequest,
    perspective: TherapyCommitteePerspectiveName,
    evidence: Mapping[str, Any],
    review: Mapping[str, Any],
) -> TherapyCommitteeReport:
    payload = _load_json_object(str(review.get("text") or ""))
    valid_refs = {citation.citation_id for citation in evidence.get("citations", [])}
    ideas = [
        idea
        for raw in payload.get("ideas", [])
        if isinstance(raw, Mapping)
        if (idea := _idea_from_payload(raw, valid_refs)) is not None
    ][: request.max_ideas_per_perspective]
    return TherapyCommitteeReport(
        perspective=perspective,
        agent_name=_PERSPECTIVE_AGENT_NAMES[perspective],
        model_profile=request.model_profile,
        summary=str(payload.get("summary") or f"{perspective} completed therapy committee review."),
        ideas=ideas,
        evidence_limitations=[
            str(item).strip()
            for item in payload.get("evidence_limitations", [])
            if str(item).strip()
        ],
        errors=[
            str(item).strip()
            for item in payload.get("errors", [])
            if str(item).strip()
        ],
    )


def _idea_from_payload(raw: Mapping[str, Any], valid_refs: set[str]) -> TherapyIdea | None:
    refs = [
        str(ref)
        for ref in raw.get("evidence_refs", [])
        if str(ref) in valid_refs
    ]
    if not refs:
        return None
    return TherapyIdea(
        title=str(raw.get("title") or "Untitled therapy idea"),
        hypothesis=str(raw.get("hypothesis") or raw.get("title") or "No hypothesis supplied."),
        rationale=str(raw.get("rationale") or "No rationale supplied."),
        candidate_therapies=_string_list(raw.get("candidate_therapies")),
        targets=_string_list(raw.get("targets")),
        biomarkers=_string_list(raw.get("biomarkers")),
        mechanism=str(raw.get("mechanism") or "") or None,
        evidence_refs=refs,
        evidence_strength=_strength_value(raw.get("evidence_strength")),
        translational_path=str(raw.get("translational_path") or "") or None,
        risks=_string_list(raw.get("risks")),
        next_experiments=_string_list(raw.get("next_experiments")),
        priority_score=_float_between(raw.get("priority_score"), default=0.5),
        metadata={"model_generated": True},
    )


def _perspective_payload(
    request: TherapyCommitteeRequest,
    perspective: TherapyCommitteePerspectiveName,
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "topic": request.topic,
        "disease_scope": request.disease_scope,
        "perspective": perspective,
        "perspective_mandate": _perspective_mandate(perspective),
        "citations": [
            citation.model_dump(mode="json")
            for citation in evidence.get("citations", [])
        ],
        "claims": evidence.get("claims", []),
        "research_leads": evidence.get("research_leads", []),
        "search_queries": evidence.get("search_queries", {}),
        "max_ideas": request.max_ideas_per_perspective,
    }


def _perspective_mandate(perspective: TherapyCommitteePerspectiveName) -> str:
    return {
        "target_biology": "Find target/pathway vulnerabilities and biomarker-gated mechanisms.",
        "drug_repurposing": "Nominate repurposed or practical therapy candidates from evidence and bioactivity context.",
        "translational_clinical": "Separate canine evidence from human analog evidence and define clinical translation paths.",
        "skeptic_risk": "Challenge weak ideas, identify negative evidence needs, and define failure criteria.",
    }[perspective]


def _openrouter_review_model(model_name: str, review_payload: dict[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required for OpenRouter therapy committee mode.")
    payload = {
        "model": model_name,
        "temperature": float(os.getenv("HSA_THERAPY_COMMITTEE_TEMPERATURE", "0.25")),
        "max_tokens": int(os.getenv("HSA_THERAPY_COMMITTEE_MAX_TOKENS", "3500")),
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "instructions": [
                            "Return JSON only.",
                            "Use only supplied citation IDs.",
                            "Make therapy ideas specific enough to become validation plans later.",
                        ],
                        "response_contract": _response_contract(),
                        "evidence_payload": review_payload,
                    },
                    sort_keys=True,
                    default=str,
                ),
            },
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
            timeout=float(os.getenv("HSA_THERAPY_COMMITTEE_TIMEOUT_SECONDS", "60")),
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
    text = ((choices[0].get("message") or {}).get("content")) or ""
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


def _response_contract() -> dict[str, Any]:
    return {
        "summary": "string",
        "ideas": [
            {
                "title": "string",
                "hypothesis": "string",
                "rationale": "string",
                "candidate_therapies": ["string"],
                "targets": ["string"],
                "biomarkers": ["string"],
                "mechanism": "string",
                "evidence_refs": ["C1"],
                "evidence_strength": "high|medium|low|unknown",
                "translational_path": "string",
                "risks": ["string"],
                "next_experiments": ["string"],
                "priority_score": 0.0,
            }
        ],
        "evidence_limitations": ["string"],
        "errors": ["string"],
    }


def _select_models(request: TherapyCommitteeRequest) -> list[str]:
    if request.review_models:
        return list(request.review_models)
    return [os.getenv("HSA_THERAPY_COMMITTEE_MODEL", DEFAULT_THERAPY_COMMITTEE_MODEL)]


def _rank_ideas(ideas: Sequence[TherapyIdea]) -> list[TherapyIdea]:
    deduped: dict[str, TherapyIdea] = {}
    for idea in ideas:
        key = re.sub(r"[^a-z0-9]+", "-", idea.title.lower()).strip("-")
        existing = deduped.get(key)
        if existing is None or idea.priority_score > existing.priority_score:
            deduped[key] = idea
    return sorted(
        deduped.values(),
        key=lambda idea: (-idea.priority_score, idea.title.lower()),
    )


def _decision_summary(ideas: Sequence[TherapyIdea], limitations: Sequence[str]) -> str:
    if not ideas:
        return "No therapy ideas met the cited evidence bar."
    top = ideas[0]
    limitation_note = f" Main limitation: {limitations[0]}" if limitations else ""
    return f"Top recommend-only idea: {top.title}. {top.hypothesis}{limitation_note}"


def _extract_terms(text: str, terms: Sequence[str]) -> list[str]:
    lower = text.lower()
    found = [term for term in terms if term.lower() in lower]
    return _dedupe_strings(found)


def _prioritize_terms(terms: Sequence[str], topic: str) -> list[str]:
    topic_lower = topic.lower()
    return sorted(
        terms,
        key=lambda term: (
            0 if term.lower() in topic_lower else 1,
            topic_lower.find(term.lower()) if term.lower() in topic_lower else 10_000,
            term.lower(),
        ),
    )


def _evidence_limitations(
    citations: Sequence[ResearchBriefCitation],
    therapies: Sequence[str],
    targets: Sequence[str],
) -> list[str]:
    limitations: list[str] = []
    if len(citations) < 3:
        limitations.append("Evidence bundle has fewer than three citations.")
    if not therapies:
        limitations.append("No explicit therapy terms were detected in the retrieved citations.")
    if not targets:
        limitations.append("No explicit target terms were detected in the retrieved citations.")
    return limitations


def _strength_from_refs(refs: Sequence[str]) -> ResearchBriefEvidenceStrength:
    if len(refs) >= 4:
        return "medium"
    if len(refs) >= 2:
        return "low"
    return "unknown"


def _strength_value(value: Any) -> ResearchBriefEvidenceStrength:
    text = str(value or "unknown").lower()
    if text in {"high", "medium", "low", "unknown"}:
        return text  # type: ignore[return-value]
    return "unknown"


def _float_between(value: Any, *, default: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return min(max(number, 0.0), 1.0)


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _dedupe_strings(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        cleaned = re.sub(r"\s+", " ", str(value)).strip()
        key = cleaned.casefold()
        if cleaned and key not in seen:
            deduped.append(cleaned)
            seen.add(key)
    return deduped


def _load_json_object(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.I)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        cleaned = cleaned[start : end + 1]
    payload = json.loads(cleaned)
    if not isinstance(payload, dict):
        raise ValueError("model response JSON must be an object")
    return payload
