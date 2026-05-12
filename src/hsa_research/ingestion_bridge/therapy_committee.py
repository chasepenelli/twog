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
from .model_policy import default_openrouter_model
from .research_brief_agent import ResearchBriefAgent
from .repository import ResearchRepository


THERAPY_COMMITTEE_AGENT_NAME = "therapy_committee_chair_agent"
THERAPY_COMMITTEE_AGENT_VERSION = "v1"
DEFAULT_THERAPY_COMMITTEE_MODEL = default_openrouter_model()
THERAPY_COMMITTEE_PERSPECTIVES: tuple[TherapyCommitteePerspectiveName, ...] = (
    "target_biology",
    "drug_repurposing",
    "translational_clinical",
    "peptide_specialist",
    "skeptic_risk",
)

_PERSPECTIVE_AGENT_NAMES = {
    "target_biology": "target_biology_committee_agent",
    "drug_repurposing": "drug_repurposing_committee_agent",
    "translational_clinical": "translational_clinical_committee_agent",
    "peptide_specialist": "peptide_specialist_committee_agent",
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
Keep the JSON compact: summary under 120 words, each idea field under 80 words, and no markdown.
Keep ideas recommend-only. Do not claim a cure is proven. Distinguish direct evidence from translational analog evidence."""


def run_therapy_committee(
    repository: ResearchRepository,
    request: TherapyCommitteeRequest,
) -> TherapyCommitteeResult:
    """Run the multi-perspective therapy committee over stored evidence."""

    evidence = _build_evidence(repository, request)
    reports = _run_perspectives(request, evidence)
    idea_limit = request.max_ranked_ideas or (3 if request.program_id else 12)
    ranked_ideas = _rank_ideas([idea for report in reports for idea in report.ideas])[:idea_limit]
    errors = _dedupe_strings([*evidence["errors"], *[error for report in reports for error in report.errors]])
    limitations = _dedupe_strings(
        limitation
        for report in reports
        for limitation in report.evidence_limitations
    )
    return TherapyCommitteeResult(
        topic=str(evidence.get("topic") or request.topic),
        disease_scope=str(evidence.get("disease_scope") or request.disease_scope),
        source_program_id=evidence.get("source_program_id"),
        source_brief_id=evidence.get("source_brief_id"),
        source_evaluation_id=evidence.get("source_evaluation_id"),
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
            "source_program_id": str(evidence["source_program_id"]) if evidence.get("source_program_id") else None,
            "research_program": evidence.get("research_program"),
            "source_brief_id": str(evidence["source_brief_id"]) if evidence.get("source_brief_id") else None,
            "source_evaluation_id": (
                str(evidence["source_evaluation_id"]) if evidence.get("source_evaluation_id") else None
            ),
            "brief_evaluation": evidence.get("brief_evaluation"),
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
    if request.program_id:
        return _build_program_evidence(repository, request)
    if request.brief_id or request.evaluation_id:
        return _build_evaluated_brief_evidence(repository, request)

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
        "topic": request.topic,
        "disease_scope": request.disease_scope,
        "source_program_id": None,
        "research_program": None,
        "source_brief_id": None,
        "source_evaluation_id": None,
        "brief_evaluation": None,
    }


def _build_program_evidence(repository: ResearchRepository, request: TherapyCommitteeRequest) -> dict[str, Any]:
    errors: list[str] = []
    program = repository.get_research_program(request.program_id) if request.program_id else None
    if program is None:
        return _empty_evidence(
            request,
            errors=[f"Research program not found: {request.program_id}"],
            blocked=True,
            source_program_id=request.program_id,
        )

    if program.gate_decision != "ready_for_therapy_ideas":
        errors.append(
            "Research program is not ready for therapy ideas: "
            f"gate_decision={program.gate_decision}, status={program.status}"
        )
        return _empty_evidence(
            request,
            errors=errors,
            blocked=True,
            source_program_id=program.program_id,
            research_program=_research_program_payload(program),
        )

    topic = _program_committee_topic(program, request)
    brief_request = ResearchBriefRequest(
        topic=topic,
        disease_scope=program.disease_scope or request.disease_scope,
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
        "search_queries": {
            **dict(evidence.search_queries or {}),
            "source_program_id": str(program.program_id),
        },
        "errors": [*evidence.errors, *errors],
        "blocked": False,
        "topic": topic,
        "disease_scope": program.disease_scope or request.disease_scope,
        "source_program_id": program.program_id,
        "research_program": _research_program_payload(program),
        "source_brief_id": None,
        "source_evaluation_id": None,
        "brief_evaluation": None,
    }


def _empty_evidence(
    request: TherapyCommitteeRequest,
    *,
    errors: Sequence[str],
    blocked: bool,
    source_program_id: object | None = None,
    research_program: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "citations": [],
        "claims": [],
        "research_leads": [],
        "search_queries": {"source_program_id": str(source_program_id)} if source_program_id else {},
        "errors": list(errors),
        "blocked": blocked,
        "topic": request.topic,
        "disease_scope": request.disease_scope,
        "source_program_id": source_program_id,
        "research_program": dict(research_program or {}),
        "source_brief_id": None,
        "source_evaluation_id": None,
        "brief_evaluation": None,
    }


def _program_committee_topic(program: Any, request: TherapyCommitteeRequest) -> str:
    parts = [
        request.topic,
        f"Research program: {program.title}",
        f"Thesis: {program.thesis}",
        "Convert this finite program into the three strongest high-level therapy ideas.",
    ]
    return _compact_text(" | ".join(part for part in parts if part), max_chars=1000)


def _research_program_payload(program: Any) -> dict[str, Any]:
    return {
        "program_id": str(program.program_id),
        "title": program.title,
        "thesis": program.thesis,
        "disease_model": program.disease_model,
        "disease_scope": program.disease_scope,
        "thesis_area": program.thesis_area,
        "status": program.status,
        "gate_decision": program.gate_decision,
        "evidence_loop_count": program.evidence_loop_count,
        "max_evidence_loops": program.max_evidence_loops,
        "therapy_families": program.therapy_families,
        "modality_families": program.modality_families,
        "downstream_therapy_opportunities": program.downstream_therapy_opportunities,
        "decisive_questions": [question.model_dump(mode="json") for question in program.decisive_questions],
        "evidence_tasks": [task.model_dump(mode="json") for task in program.evidence_tasks],
        "metric_plan": program.metric_plan,
        "recommended_tools": program.recommended_tools,
        "stop_criteria": program.stop_criteria,
        "confidence_increase_criteria": program.confidence_increase_criteria,
        "confidence_decrease_criteria": program.confidence_decrease_criteria,
        "scores": {
            "biological_plausibility": program.biological_plausibility_score,
            "cross_species_support": program.cross_species_support_score,
            "evidence_density": program.evidence_density_score,
            "novelty": program.novelty_score,
            "testability": program.testability_score,
            "therapeutic_leverage": program.therapeutic_leverage_score,
            "failure_risk": program.failure_risk_score,
            "confidence": program.confidence_score,
        },
        "evidence_refs": program.evidence_refs,
        "review_summary": program.review_summary,
        "known_blockers": program.errors,
    }


def _build_evaluated_brief_evidence(
    repository: ResearchRepository,
    request: TherapyCommitteeRequest,
) -> dict[str, Any]:
    errors: list[str] = []
    evaluation = None
    if request.evaluation_id:
        evaluation = repository.get_research_brief_evaluation(request.evaluation_id)
        if evaluation is None:
            errors.append(f"Research brief evaluation not found: {request.evaluation_id}")
    brief_id = request.brief_id or (evaluation.brief_id if evaluation else None)
    brief = repository.get_research_brief(brief_id) if brief_id else None
    if brief is None:
        errors.append(f"Research brief not found: {brief_id}")
        fallback = _build_evidence(
            repository,
            request.model_copy(update={"brief_id": None, "evaluation_id": None}),
        )
        fallback["errors"] = [*fallback.get("errors", []), *errors]
        return fallback

    payload = brief.result_payload or {}
    citations = _citations_from_payload(payload)
    if not citations:
        errors.append("Selected research brief had no valid citations for committee review.")
    brief_evaluation = None
    if evaluation:
        brief_evaluation = {
            "evaluation_id": str(evaluation.evaluation_id),
            "overall_score": evaluation.overall_score,
            "passes_quality_bar": evaluation.passes_quality_bar,
            "readiness": evaluation.readiness,
            "summary": evaluation.summary,
            "weaknesses": (evaluation.result_payload or {}).get("weaknesses", []),
            "recommendations": (evaluation.result_payload or {}).get("recommendations", []),
        }
    claims = []
    if request.max_claims > 0:
        brief_request = ResearchBriefRequest(
            topic=brief.topic,
            disease_scope=brief.disease_scope,
            source_key=request.source_key or brief.source_key,
            max_chunks_per_perspective=1,
            max_claims=request.max_claims,
            max_chunk_chars=request.max_chunk_chars,
            model_profile=request.model_profile,
            review_mode="deterministic_only",
        )
        try:
            evidence = ResearchBriefAgent(repository).build_evidence(brief_request)
            claims = [claim.model_dump(mode="json") for claim in evidence.claims]
        except Exception as exc:
            errors.append(f"Could not refresh claim context for committee brief: {exc}")
    return {
        "citations": citations[: request.max_chunks_per_perspective * 4],
        "claims": claims,
        "research_leads": [],
        "search_queries": {"source_brief_id": str(brief.brief_id)},
        "errors": errors,
        "topic": brief.topic,
        "disease_scope": brief.disease_scope,
        "source_program_id": None,
        "research_program": None,
        "source_brief_id": brief.brief_id,
        "source_evaluation_id": evaluation.evaluation_id if evaluation else request.evaluation_id,
        "brief_evaluation": brief_evaluation,
        "brief_limitations": payload.get("evidence_limitations", []),
    }


def _citations_from_payload(payload: Mapping[str, Any]) -> list[ResearchBriefCitation]:
    citations: list[ResearchBriefCitation] = []
    for raw in payload.get("citations", []):
        if not isinstance(raw, Mapping):
            continue
        try:
            citations.append(ResearchBriefCitation.model_validate(dict(raw)))
        except Exception:
            continue
    return citations


def _run_perspective(
    request: TherapyCommitteeRequest,
    perspective: TherapyCommitteePerspectiveName,
    evidence: Mapping[str, Any],
) -> TherapyCommitteeReport:
    citations = list(evidence.get("citations") or [])
    if evidence.get("blocked"):
        return TherapyCommitteeReport(
            perspective=perspective,
            agent_name=_PERSPECTIVE_AGENT_NAMES[perspective],
            model_profile=request.model_profile,
            summary="Committee run blocked before model review.",
            evidence_limitations=["Research program bridge is not ready for therapy committee ideation."],
            errors=list(evidence.get("errors") or []),
            ideas=[],
        )
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
            try:
                return _report_from_model(request, perspective, evidence, review)
            except ValueError as parse_error:
                repaired = _openrouter_repair_json_model(
                    model_name,
                    str(review.get("text") or ""),
                    parse_error=str(parse_error),
                    original_metadata=review.get("metadata"),
                )
                return _report_from_model(request, perspective, evidence, repaired)
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
    program_payload = evidence.get("research_program")

    if request.program_id and isinstance(program_payload, Mapping):
        opportunities = _string_list(program_payload.get("downstream_therapy_opportunities"))
        families = _string_list(program_payload.get("therapy_families"))
        base_metadata = {
            "source_program_id": str(request.program_id),
            "program_gate_decision": program_payload.get("gate_decision"),
            "program_evidence_loop_count": program_payload.get("evidence_loop_count"),
        }
        if perspective == "target_biology":
            ideas.append(
                TherapyIdea(
                    title="Biomarker-stratified vascular ecology therapy program",
                    hypothesis="Vascular injury, coagulation, and angiogenesis signals may define biomarker-gated therapy families in canine HSA and human angiosarcoma.",
                    rationale="The program has reached the finite idea gate; prioritize mechanisms that can be stratified by target and pathway readouts.",
                    candidate_therapies=opportunities[:4] or families[:4],
                    targets=targets[:5] or ["KDR", "VEGF", "PI3K-mTOR"],
                    biomarkers=biomarkers[:5] or ["KDR", "PIK3CA", "coagulation markers"],
                    mechanism="Select therapy families around vascular ecology dependency rather than isolated monotherapy response.",
                    evidence_refs=refs,
                    evidence_strength=_strength_from_refs(refs),
                    translational_path="Compare direct canine HSA evidence with human angiosarcoma analogs before narrowing child therapy ideas.",
                    risks=["Biomarker association may not equal therapeutic dependency.", "Sparse direct canine evidence may overfit the hypothesis."],
                    next_experiments=[
                        "Build a target-biomarker-readout matrix for the program.",
                        "Select child therapy ideas only when a measurable vascular ecology readout exists.",
                    ],
                    priority_score=0.84,
                    metadata=base_metadata,
                )
            )
        elif perspective == "drug_repurposing":
            ideas.append(
                TherapyIdea(
                    title="Mechanism-matched vascular therapy combination lane",
                    hypothesis="Anti-angiogenic or kinase strategies should be explored as mechanism-matched combinations, not unstratified monotherapy.",
                    rationale="The program review explicitly preserved VEGF-axis, coagulation, and vascular injury gaps while moving to therapy narrowing.",
                    candidate_therapies=therapies[:6] or opportunities[:4] or ["anti-angiogenic combinations", "kinase inhibitor combinations"],
                    targets=targets[:5] or ["KDR", "VEGFR", "PI3K-mTOR"],
                    biomarkers=biomarkers[:5] or ["VEGFR2", "PIK3CA", "coagulation state"],
                    mechanism="Pair vascular signaling inhibition with a second axis only when evidence supports pathway coupling.",
                    evidence_refs=refs,
                    evidence_strength=_strength_from_refs(refs),
                    translational_path="Promote only combinations with direct canine feasibility plus analog mechanistic support.",
                    risks=["Combination toxicity may limit translation.", "Prior negative VEGFR-class signals may constrain unselected use."],
                    next_experiments=[
                        "Rank combinations by target match, canine safety feasibility, and assayable pathway readouts.",
                        "Separate biomarker-enriched and unselected hypotheses before validation packets.",
                    ],
                    priority_score=0.8,
                    metadata=base_metadata,
                )
            )
        elif perspective == "peptide_specialist":
            ideas.append(
                TherapyIdea(
                    title="Endothelial antigen immunomodulation and peptide/vaccine lane",
                    hypothesis="Endothelial or extracellular vascular antigens may support a vaccine, peptide, or immunomodulatory child strategy if target engagement is measurable.",
                    rationale="The peptide specialist lane should convert vascular tumor antigen biology into a modality-aware therapy family, not a formulation protocol.",
                    candidate_therapies=opportunities[:4] or ["anti-extracellular vimentin vaccine", "endothelial antigen immunotherapy"],
                    targets=targets[:5] or ["extracellular vimentin", "endothelial antigens"],
                    biomarkers=biomarkers[:5] or ["target expression", "immune infiltration", "vascular injury markers"],
                    mechanism="Use vascular antigen exposure or endothelial stress biology to guide immunomodulatory therapy design.",
                    evidence_refs=refs,
                    evidence_strength=_strength_from_refs(refs),
                    translational_path="Require target expression, delivery feasibility, and immune-response readouts before validation queueing.",
                    risks=["Antigen accessibility and immunogenicity may not transfer across models.", "Peptide/vaccine feasibility requires specialist review."],
                    next_experiments=[
                        "Run peptide/vaccine specialist review on antigen identity and manufacturability.",
                        "Define target-expression and immune-correlate gates for a child therapy idea.",
                    ],
                    priority_score=0.78,
                    metadata=base_metadata,
                )
            )
        else:
            ideas.append(
                TherapyIdea(
                    title="Comparative vascular tumor translation strategy",
                    hypothesis="The program should produce child therapy ideas only where canine HSA and human angiosarcoma evidence share mechanism and measurable readouts.",
                    rationale="This guards the bridge from becoming a list of unsupported drug names.",
                    candidate_therapies=opportunities[:4] or families[:4],
                    targets=targets[:5],
                    biomarkers=biomarkers[:5],
                    mechanism="Use comparative vascular tumor biology as the selection frame.",
                    evidence_refs=refs,
                    evidence_strength=_strength_from_refs(refs),
                    translational_path="Carry direct-versus-analog provenance into every downstream validation packet.",
                    risks=["Cross-species transfer risk remains high without direct canine evidence.", "Weak citations should trigger evidence repair before dispatch."],
                    next_experiments=[
                        "Create three child therapy candidates with direct-versus-analog evidence tables.",
                        "Attach validation readouts and stop criteria before any dispatch.",
                    ],
                    priority_score=0.72,
                    metadata=base_metadata,
                )
            )
        return TherapyCommitteeReport(
            perspective=perspective,
            agent_name=_PERSPECTIVE_AGENT_NAMES[perspective],
            model_profile=request.model_profile,
            summary="Deterministic program bridge generated high-level therapy strategy candidates.",
            ideas=ideas[: request.max_ideas_per_perspective],
            evidence_limitations=list(evidence.get("errors") or []),
            errors=list(evidence.get("errors") or []),
        )

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
    elif perspective == "peptide_specialist":
        target = targets[0] if targets else "vascular tumor pathway target"
        ideas.append(
            TherapyIdea(
                title=f"Peptide modality review for {target}",
                hypothesis=(
                    "Peptide, cyclic peptide, peptidomimetic, or vaccine-style modalities may be worth exploring "
                    "only when target engagement, delivery, stability, and immunogenicity constraints are explicit."
                ),
                rationale=(
                    "A peptide specialist should separate plausible peptide modality opportunities from ideas that "
                    "are better served by small molecules, antibodies, or assay-only evidence gathering."
                ),
                candidate_therapies=["peptide modality concept"],
                targets=targets[:5],
                biomarkers=biomarkers[:5],
                mechanism=(
                    f"Review whether a peptide modality can engage {target}, survive protease exposure, reach the "
                    "right compartment, and avoid unacceptable immune or formulation risk."
                ),
                evidence_refs=refs,
                evidence_strength=_strength_from_refs(refs),
                translational_path=(
                    "Require sequence/modality identity, delivery route, stability assumptions, and immunogenicity "
                    "risk review before peptide assay planning."
                ),
                risks=[
                    "Peptide delivery and stability may be the dominant failure modes.",
                    "Target engagement may require intracellular access or a geometry unsuitable for peptides.",
                    "Immunogenicity and manufacturability can erase otherwise plausible biology.",
                ],
                next_experiments=[
                    "Route peptide-like ideas to peptide specialist validation review before assay design.",
                    "Collect sequence/modality, delivery route, and stability evidence before promotion.",
                ],
                priority_score=0.64 if targets else 0.5,
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
        evidence=_model_review_evidence(review),
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
        "topic": evidence.get("topic") or request.topic,
        "disease_scope": evidence.get("disease_scope") or request.disease_scope,
        "perspective": perspective,
        "perspective_mandate": _perspective_mandate(perspective),
        "citations": [
            citation.model_dump(mode="json")
            for citation in evidence.get("citations", [])
        ],
        "claims": evidence.get("claims", []),
        "research_leads": evidence.get("research_leads", []),
        "search_queries": evidence.get("search_queries", {}),
        "source_program_id": str(evidence["source_program_id"]) if evidence.get("source_program_id") else None,
        "research_program": evidence.get("research_program"),
        "source_brief_id": str(evidence["source_brief_id"]) if evidence.get("source_brief_id") else None,
        "source_evaluation_id": (
            str(evidence["source_evaluation_id"]) if evidence.get("source_evaluation_id") else None
        ),
        "brief_evaluation": evidence.get("brief_evaluation"),
        "brief_limitations": evidence.get("brief_limitations", []),
        "max_ideas": request.max_ideas_per_perspective,
    }


def _perspective_mandate(perspective: TherapyCommitteePerspectiveName) -> str:
    return {
        "target_biology": "Find target/pathway vulnerabilities and biomarker-gated mechanisms.",
        "drug_repurposing": "Nominate repurposed or practical therapy candidates from evidence and bioactivity context.",
        "translational_clinical": "Separate canine evidence from human analog evidence and define clinical translation paths.",
        "peptide_specialist": (
            "Review peptide, cyclic peptide, peptidomimetic, and vaccine-style modality feasibility, delivery, "
            "stability, target engagement, immunogenicity, and manufacturability."
        ),
        "skeptic_risk": "Challenge weak ideas, identify negative evidence needs, and define failure criteria.",
    }[perspective]


def _openrouter_review_model(model_name: str, review_payload: dict[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required for OpenRouter therapy committee mode.")
    payload = {
        "model": model_name,
        "temperature": float(os.getenv("HSA_THERAPY_COMMITTEE_TEMPERATURE", "0.25")),
        "max_tokens": int(os.getenv("HSA_THERAPY_COMMITTEE_MAX_TOKENS", "7000")),
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
                        "When research_program is present, produce high-level therapeutic strategies, not dosing or protocol tweaks.",
                        "When research_program is present, avoid single weak drug tweaks unless they are part of a larger mechanism strategy.",
                        "When research_program is present, include mechanism, therapy family, target/biomarker logic, risk annotations, and validation readouts for each idea.",
                        "Keep the response compact enough to avoid truncation.",
                            f"Return no more than {review_payload.get('max_ideas', 1)} idea(s).",
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
            "finish_reason": choices[0].get("finish_reason"),
        },
    }


def _openrouter_repair_json_model(
    model_name: str,
    malformed_text: str,
    *,
    parse_error: str,
    original_metadata: Any = None,
) -> dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required for OpenRouter JSON repair.")
    payload = {
        "model": os.getenv("HSA_THERAPY_COMMITTEE_REPAIR_MODEL", model_name),
        "temperature": 0,
        "max_tokens": int(os.getenv("HSA_THERAPY_COMMITTEE_REPAIR_MAX_TOKENS", "7000")),
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "You repair malformed JSON for a therapy committee result. "
                    "Return valid JSON only. Do not add new scientific claims, citations, ideas, or evidence."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "instructions": [
                            "Repair syntax only.",
                            "Preserve supplied fields and values when possible.",
                            "The repaired object must match the response contract.",
                            "If an idea is incomplete, keep it but do not invent citation IDs.",
                            "Condense overly long strings if needed so the repaired JSON is complete.",
                        ],
                        "parse_error": parse_error,
                        "response_contract": _response_contract(),
                        "malformed_json": _compact_text(malformed_text, max_chars=40000),
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
            timeout=float(os.getenv("HSA_THERAPY_COMMITTEE_REPAIR_TIMEOUT_SECONDS", "45")),
        ) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenRouter JSON repair HTTP {error.code}: {body}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"OpenRouter JSON repair request failed: {error}") from error
    choices = response_payload.get("choices") or []
    if not choices:
        raise RuntimeError(f"OpenRouter JSON repair response had no choices: {response_payload}")
    text = ((choices[0].get("message") or {}).get("content")) or ""
    if not text:
        raise RuntimeError(f"OpenRouter JSON repair response had no text content: {response_payload}")

    metadata: dict[str, Any] = {
        "provider": "openrouter",
        "model_name": response_payload.get("model", payload["model"]),
        "requested_model": payload["model"],
        "request_id": response_payload.get("id"),
        "usage": response_payload.get("usage", {}),
        "finish_reason": choices[0].get("finish_reason"),
        "json_repair_attempted": True,
        "json_parse_error": _compact_text(parse_error, max_chars=500),
    }
    if isinstance(original_metadata, Mapping):
        metadata["original_review"] = dict(original_metadata)
    return {"text": text, "metadata": metadata}


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
    return [os.getenv("HSA_THERAPY_COMMITTEE_MODEL", default_openrouter_model())]


def _rank_ideas(ideas: Sequence[TherapyIdea]) -> list[TherapyIdea]:
    deduped: dict[str, TherapyIdea] = {}
    for idea in ideas:
        key = _idea_family_key(idea)
        existing = deduped.get(key)
        if existing is None or idea.priority_score > existing.priority_score:
            deduped[key] = idea
    return sorted(
        deduped.values(),
        key=lambda idea: (-idea.priority_score, idea.title.lower()),
    )


def _idea_family_key(idea: TherapyIdea) -> str:
    """Collapse committee variants that describe the same therapy family."""

    text = " ".join(
        [
            idea.title,
            idea.hypothesis,
            " ".join(idea.candidate_therapies),
            " ".join(idea.targets),
            " ".join(idea.biomarkers),
        ]
    )
    normalized = _normalize_idea_text(text)
    therapy_family = _therapy_family(normalized)
    target_family = _target_family(normalized)
    if therapy_family:
        return f"{therapy_family}:{target_family or 'unspecified'}"
    return re.sub(r"[^a-z0-9]+", "-", idea.title.lower()).strip("-")


def _normalize_idea_text(text: str) -> str:
    return (
        text.lower()
        .replace("β", "beta")
        .replace("α", "alpha")
        .replace("γ", "gamma")
        .replace("-", "")
        .replace("/", "")
        .replace("_", "")
    )


def _therapy_family(text: str) -> str | None:
    if "sorafenib" in text:
        return "sorafenib"
    if any(term in text for term in ("toceranib", "masitinib")):
        return "vegfr_tki"
    if any(term in text for term in ("alpelisib", "copanlisib", "buparlisib", "bkm120")):
        return "pi3k_inhibitor"
    if any(term in text for term in ("anti pd1", "antipd1", "anti pdl1", "antipdl1", "checkpoint")):
        return "checkpoint_modulation"
    return None


def _target_family(text: str) -> str | None:
    if any(term in text for term in ("vegfr", "kdr", "pdgfr", "raf")):
        return "vegfr_pdgfr_raf_axis"
    if any(term in text for term in ("pik3ca", "pi3k", "pten", "akt", "mtor")):
        return "pi3k_akt_mtor_axis"
    if any(term in text for term in ("pd1", "pdl1", "cd274", "cd279")):
        return "pd1_pdl1_axis"
    if "vimentin" in text or "vim" in text:
        return "vimentin_axis"
    return None


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
    cleaned = _strip_json_fences(text).strip()
    candidates = [cleaned]
    extracted = _extract_balanced_json_object(cleaned)
    if extracted and extracted not in candidates:
        candidates.append(extracted)

    errors: list[str] = []
    for candidate in candidates:
        for repaired in _json_repair_candidates(candidate):
            try:
                payload = json.loads(repaired)
                if not isinstance(payload, dict):
                    raise ValueError("model response JSON must be an object")
                return payload
            except (json.JSONDecodeError, ValueError) as exc:
                errors.append(str(exc))
                continue
    preview = _compact_text(cleaned, max_chars=500)
    raise ValueError(f"model response JSON parse failed: {errors[-1] if errors else 'unknown error'}; preview={preview}")


def _model_review_evidence(review: Mapping[str, Any]) -> dict[str, Any]:
    metadata = review.get("metadata")
    if not isinstance(metadata, Mapping):
        return {}
    return {"model_review": dict(metadata)}


def _compact_text(value: str, *, max_chars: int) -> str:
    text = re.sub(r"\s+", " ", value).strip()
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3].rstrip()}..."


def _strip_json_fences(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```(?:json)?\s*", "", stripped, flags=re.I)
        stripped = re.sub(r"\s*```$", "", stripped)
    return stripped


def _extract_balanced_json_object(text: str) -> str | None:
    start = text.find("{")
    if start < 0:
        return None

    depth = 0
    in_string = False
    escaped = False
    for index, char in enumerate(text[start:], start=start):
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return None


def _json_repair_candidates(text: str) -> list[str]:
    candidates = [text]
    without_trailing_commas = re.sub(r",\s*([}\]])", r"\1", text)
    if without_trailing_commas not in candidates:
        candidates.append(without_trailing_commas)

    comma_repaired = without_trailing_commas
    comma_repaired = re.sub(
        r'([}\]"])\s*\n(\s*"[^"\n]+"\s*:)',
        r"\1,\n\2",
        comma_repaired,
    )
    comma_repaired = re.sub(
        r"([}\]])\s*\n(\s*\{)",
        r"\1,\n\2",
        comma_repaired,
    )
    if comma_repaired not in candidates:
        candidates.append(comma_repaired)
    return candidates
