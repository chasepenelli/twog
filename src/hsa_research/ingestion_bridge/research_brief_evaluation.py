"""Synthesis quality evaluator for persisted research briefs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import re
from typing import Any

from .contracts import (
    ResearchBriefEvaluationReadiness,
    ResearchBriefEvaluationRequest,
    ResearchBriefEvaluationResult,
    ResearchBriefRecord,
    ResearchBriefResult,
)
from .research_brief_errors import split_research_brief_errors


RESEARCH_BRIEF_EVALUATION_AGENT_NAME = "research_brief_synthesis_evaluator_agent"
RESEARCH_BRIEF_EVALUATION_AGENT_VERSION = "v1"

_EXPECTED_PERSPECTIVES = {
    "evidence_scout",
    "translational_hypothesis",
    "skeptic_validation",
}
_CONTRADICTION_STANCES = {"contradicting", "risk", "uncertain"}
_LIMITATION_TERMS = {
    "caveat",
    "constraint",
    "limitation",
    "limited",
    "missing",
    "risk",
    "uncertain",
    "unresolved",
    "weak",
}
_ACTION_TERMS = {"next step", "next steps", "prioritize", "validation", "test", "monitor"}
def evaluate_research_brief_synthesis(
    brief: ResearchBriefRecord,
    request: ResearchBriefEvaluationRequest,
) -> ResearchBriefEvaluationResult:
    """Evaluate whether a persisted brief is ready for hypothesis review."""

    payload, parse_errors = _brief_payload(brief)
    final_brief = str(payload.get("final_brief") or brief.final_brief or "")
    citations = _as_list(payload.get("citations"))
    citation_ids = {
        str(citation.get("citation_id"))
        for citation in citations
        if isinstance(citation, Mapping) and citation.get("citation_id")
    }
    perspective_reports = _as_list(payload.get("perspective_reports"))
    ranked_hypotheses = _as_list(payload.get("ranked_hypotheses"))
    unresolved_questions = _as_list(payload.get("unresolved_questions"))
    result_errors = [str(error) for error in _as_list(payload.get("errors"))]
    hard_result_errors, synthesis_limitations = split_research_brief_errors(result_errors)
    all_findings = [
        finding
        for report in perspective_reports
        if isinstance(report, Mapping)
        for finding in _as_list(report.get("findings"))
        if isinstance(finding, Mapping)
    ]
    all_findings.extend(finding for finding in ranked_hypotheses if isinstance(finding, Mapping))

    final_has_citation_tokens = bool(re.search(r"\[C\d+\]", final_brief))
    finding_refs = [
        str(citation_id)
        for finding in all_findings
        for citation_id in _as_list(finding.get("citations") if isinstance(finding, Mapping) else None)
    ]
    invalid_refs = sorted({citation_id for citation_id in finding_refs if citation_id not in citation_ids})
    stance_counts = _stance_counts(all_findings)
    present_perspectives = {
        str(report.get("perspective"))
        for report in perspective_reports
        if isinstance(report, Mapping) and report.get("perspective")
    }

    citation_coverage_score = _citation_coverage_score(
        final_has_citation_tokens=final_has_citation_tokens,
        citation_count=len(citations),
        findings=all_findings,
        ranked_hypotheses=ranked_hypotheses,
        invalid_refs=invalid_refs,
    )
    perspective_balance_score = _perspective_balance_score(perspective_reports, present_perspectives)
    contradiction_handling_score = _contradiction_handling_score(
        perspective_reports=perspective_reports,
        stance_counts=stance_counts,
        unresolved_questions=unresolved_questions,
        final_brief=final_brief,
    )
    novelty_score = _novelty_score(
        ranked_hypotheses=ranked_hypotheses,
        findings=all_findings,
        unresolved_questions=unresolved_questions,
        evidence=_as_mapping(payload.get("evidence")),
    )
    actionability_score = _actionability_score(
        final_brief=final_brief,
        ranked_hypotheses=ranked_hypotheses,
        result_errors=hard_result_errors,
    )
    weakness_transparency_score = _weakness_transparency_score(
        final_brief=final_brief,
        unresolved_questions=unresolved_questions,
        stance_counts=stance_counts,
        result_errors=hard_result_errors,
    )
    overall_score = _rounded(
        citation_coverage_score * 0.30
        + perspective_balance_score * 0.20
        + contradiction_handling_score * 0.15
        + novelty_score * 0.15
        + actionability_score * 0.10
        + weakness_transparency_score * 0.10
    )
    errors = parse_errors + invalid_refs_to_errors(invalid_refs) + hard_result_errors
    readiness = _readiness(
        overall_score=overall_score,
        minimum_overall_score=request.minimum_overall_score,
        citation_coverage_score=citation_coverage_score,
        perspective_balance_score=perspective_balance_score,
        contradiction_handling_score=contradiction_handling_score,
        actionability_score=actionability_score,
        has_citations=bool(citations),
        final_has_citation_tokens=final_has_citation_tokens,
        errors=errors,
    )
    passes_quality_bar = (
        readiness == "ready_for_hypothesis_review"
        and overall_score >= request.minimum_overall_score
        and citation_coverage_score >= 0.70
        and perspective_balance_score >= 0.65
    )
    strengths, weaknesses, recommendations = _narrative(
        citation_coverage_score=citation_coverage_score,
        perspective_balance_score=perspective_balance_score,
        contradiction_handling_score=contradiction_handling_score,
        novelty_score=novelty_score,
        actionability_score=actionability_score,
        weakness_transparency_score=weakness_transparency_score,
        readiness=readiness,
        errors=errors,
        synthesis_limitations=synthesis_limitations,
    )
    evidence = {
        "citation_count": len(citations),
        "finding_count": len(all_findings),
        "ranked_hypothesis_count": len(ranked_hypotheses),
        "unresolved_question_count": len(unresolved_questions),
        "perspective_count": len(present_perspectives),
        "present_perspectives": sorted(present_perspectives),
        "missing_perspectives": sorted(_EXPECTED_PERSPECTIVES - present_perspectives),
        "final_has_citation_tokens": final_has_citation_tokens,
        "citation_reference_count": len(finding_refs),
        "invalid_citation_refs": invalid_refs,
        "stance_counts": stance_counts,
        "minimum_overall_score": request.minimum_overall_score,
        "hard_error_count": len(errors),
        "synthesis_limitation_count": len(synthesis_limitations),
        "synthesis_limitations": synthesis_limitations[:20],
    }
    return ResearchBriefEvaluationResult(
        brief_id=brief.brief_id,
        agent_name=RESEARCH_BRIEF_EVALUATION_AGENT_NAME,
        model_profile=request.model_profile,
        topic=brief.topic,
        source_key=brief.source_key,
        overall_score=overall_score,
        citation_coverage_score=citation_coverage_score,
        perspective_balance_score=perspective_balance_score,
        contradiction_handling_score=contradiction_handling_score,
        novelty_score=novelty_score,
        actionability_score=actionability_score,
        weakness_transparency_score=weakness_transparency_score,
        passes_quality_bar=passes_quality_bar,
        readiness=readiness,
        strengths=strengths,
        weaknesses=weaknesses,
        recommendations=recommendations,
        evidence=evidence,
        errors=errors,
    )


def summarize_research_brief_evaluation(result: ResearchBriefEvaluationResult) -> dict[str, Any]:
    return {
        "brief_id": str(result.brief_id),
        "overall_score": result.overall_score,
        "passes_quality_bar": result.passes_quality_bar,
        "readiness": result.readiness,
        "recommendation_count": len(result.recommendations),
        "error_count": len(result.errors),
    }


def invalid_refs_to_errors(invalid_refs: Sequence[str]) -> list[str]:
    if not invalid_refs:
        return []
    return [f"Brief findings reference unknown citation IDs: {', '.join(invalid_refs)}"]


def _brief_payload(brief: ResearchBriefRecord) -> tuple[dict[str, Any], list[str]]:
    payload = dict(brief.result_payload or {})
    payload.setdefault("brief_id", str(brief.brief_id))
    payload.setdefault("topic", brief.topic)
    payload.setdefault("disease_scope", brief.disease_scope)
    payload.setdefault("brief_style", brief.brief_style)
    payload.setdefault("model_profile", brief.model_profile)
    payload.setdefault("final_brief", brief.final_brief)
    payload.setdefault("perspective_reports", [])
    payload.setdefault("ranked_hypotheses", [])
    payload.setdefault("unresolved_questions", [])
    payload.setdefault("citations", [])
    payload.setdefault("evidence", {})
    payload.setdefault("errors", [])
    try:
        typed = ResearchBriefResult.model_validate(payload)
    except Exception as exc:
        return payload, [f"Could not validate stored research brief payload: {exc}"]
    return typed.model_dump(mode="json"), []


def _citation_coverage_score(
    *,
    final_has_citation_tokens: bool,
    citation_count: int,
    findings: Sequence[Mapping[str, Any]],
    ranked_hypotheses: Sequence[Any],
    invalid_refs: Sequence[str],
) -> float:
    if citation_count == 0 or invalid_refs:
        return 0.0
    cited_findings = [
        finding for finding in findings if _as_list(finding.get("citations"))
    ]
    finding_ratio = len(cited_findings) / max(len(findings), 1)
    cited_ranked = [
        finding
        for finding in ranked_hypotheses
        if isinstance(finding, Mapping) and _as_list(finding.get("citations"))
    ]
    ranked_ratio = len(cited_ranked) / max(len(ranked_hypotheses), 1)
    diversity = min(citation_count / max(len(findings), 3), 1.0)
    return _rounded(
        (0.35 if final_has_citation_tokens else 0.0)
        + 0.35 * finding_ratio
        + 0.15 * ranked_ratio
        + 0.15 * diversity
    )


def _perspective_balance_score(
    perspective_reports: Sequence[Any],
    present_perspectives: set[str],
) -> float:
    if not perspective_reports:
        return 0.0
    expected_ratio = len(present_perspectives & _EXPECTED_PERSPECTIVES) / len(_EXPECTED_PERSPECTIVES)
    reports_with_findings = [
        report
        for report in perspective_reports
        if isinstance(report, Mapping) and _as_list(report.get("findings"))
    ]
    reports_with_citations = [
        report
        for report in perspective_reports
        if isinstance(report, Mapping) and _as_list(report.get("citations"))
    ]
    finding_ratio = len(reports_with_findings) / max(len(_EXPECTED_PERSPECTIVES), 1)
    citation_ratio = len(reports_with_citations) / max(len(_EXPECTED_PERSPECTIVES), 1)
    return _rounded(0.45 * expected_ratio + 0.35 * min(finding_ratio, 1.0) + 0.20 * min(citation_ratio, 1.0))


def _contradiction_handling_score(
    *,
    perspective_reports: Sequence[Any],
    stance_counts: Mapping[str, int],
    unresolved_questions: Sequence[Any],
    final_brief: str,
) -> float:
    skeptic_reports = [
        report
        for report in perspective_reports
        if isinstance(report, Mapping) and report.get("perspective") == "skeptic_validation"
    ]
    skeptic_has_findings = any(_as_list(report.get("findings")) for report in skeptic_reports)
    contradiction_count = sum(stance_counts.get(stance, 0) for stance in _CONTRADICTION_STANCES)
    final_mentions_limits = _contains_any(final_brief, _LIMITATION_TERMS)
    return _rounded(
        (0.45 if skeptic_has_findings else 0.0)
        + (0.25 if contradiction_count else 0.0)
        + (0.20 if unresolved_questions else 0.0)
        + (0.10 if final_mentions_limits else 0.0)
    )


def _novelty_score(
    *,
    ranked_hypotheses: Sequence[Any],
    findings: Sequence[Mapping[str, Any]],
    unresolved_questions: Sequence[Any],
    evidence: Mapping[str, Any],
) -> float:
    opportunity_findings = [
        finding
        for finding in findings
        if str(finding.get("stance") or "") in {"opportunity", "supporting"}
        and _contains_any(str(finding.get("claim") or ""), {"translational", "biomarker", "target", "therapy"})
    ]
    research_leads = int(evidence.get("research_lead_count") or 0)
    return _rounded(
        (0.45 if ranked_hypotheses else 0.0)
        + (0.25 if opportunity_findings else 0.0)
        + (0.15 if research_leads else 0.0)
        + (0.15 if unresolved_questions else 0.0)
    )


def _actionability_score(
    *,
    final_brief: str,
    ranked_hypotheses: Sequence[Any],
    result_errors: Sequence[str],
) -> float:
    ranked_with_citations = [
        finding
        for finding in ranked_hypotheses
        if isinstance(finding, Mapping) and _as_list(finding.get("citations"))
    ]
    return _rounded(
        (0.35 if _contains_any(final_brief, _ACTION_TERMS) else 0.0)
        + (0.35 if ranked_hypotheses else 0.0)
        + (0.15 if ranked_with_citations else 0.0)
        + (0.15 if not result_errors else 0.0)
    )


def _weakness_transparency_score(
    *,
    final_brief: str,
    unresolved_questions: Sequence[Any],
    stance_counts: Mapping[str, int],
    result_errors: Sequence[str],
) -> float:
    contradiction_count = sum(stance_counts.get(stance, 0) for stance in _CONTRADICTION_STANCES)
    return _rounded(
        (0.35 if unresolved_questions else 0.0)
        + (0.25 if contradiction_count else 0.0)
        + (0.25 if _contains_any(final_brief, _LIMITATION_TERMS) else 0.0)
        + (0.15 if not result_errors else 0.0)
    )


def _readiness(
    *,
    overall_score: float,
    minimum_overall_score: float,
    citation_coverage_score: float,
    perspective_balance_score: float,
    contradiction_handling_score: float,
    actionability_score: float,
    has_citations: bool,
    final_has_citation_tokens: bool,
    errors: Sequence[str],
) -> ResearchBriefEvaluationReadiness:
    if not has_citations or not final_has_citation_tokens or any("unknown citation" in error for error in errors):
        return "blocked"
    if errors:
        return "needs_human_review"
    if overall_score < minimum_overall_score or citation_coverage_score < 0.70 or perspective_balance_score < 0.65:
        return "needs_more_evidence"
    if contradiction_handling_score < 0.45 or actionability_score < 0.50:
        return "needs_human_review"
    return "ready_for_hypothesis_review"


def _narrative(
    *,
    citation_coverage_score: float,
    perspective_balance_score: float,
    contradiction_handling_score: float,
    novelty_score: float,
    actionability_score: float,
    weakness_transparency_score: float,
    readiness: str,
    errors: Sequence[str],
    synthesis_limitations: Sequence[str],
) -> tuple[list[str], list[str], list[str]]:
    strengths: list[str] = []
    weaknesses: list[str] = []
    recommendations: list[str] = []
    score_labels = [
        ("citation coverage", citation_coverage_score),
        ("perspective balance", perspective_balance_score),
        ("contradiction handling", contradiction_handling_score),
        ("novelty", novelty_score),
        ("actionability", actionability_score),
        ("weakness transparency", weakness_transparency_score),
    ]
    for label, score in score_labels:
        if score >= 0.75:
            strengths.append(f"Strong {label} signal.")
        elif score < 0.50:
            weaknesses.append(f"Weak {label} signal.")
            recommendations.append(f"Improve {label} before relying on this synthesis for hypothesis review.")
    if errors:
        weaknesses.append("Stored brief payload has validation or synthesis errors.")
        recommendations.append("Resolve evaluator errors and regenerate or repair the brief record.")
    if synthesis_limitations:
        weaknesses.append("Brief reports evidence limitations that should be tracked.")
        recommendations.append("Route evidence limitations into the follow-up research queue before downstream validation.")
    if readiness == "ready_for_hypothesis_review":
        recommendations.append("Promote this brief into hypothesis review and validation planning.")
    elif readiness == "blocked":
        recommendations.append("Regenerate the brief with citation-first synthesis before downstream use.")
    elif readiness == "needs_more_evidence":
        recommendations.append("Run another retrieval or queue pass with a wider source scope.")
    elif readiness == "needs_human_review":
        recommendations.append("Have a human reviewer inspect the weak or error-bearing sections.")
    return strengths[:20], weaknesses[:20], _dedupe(recommendations)[:20]


def _stance_counts(findings: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for finding in findings:
        stance = str(finding.get("stance") or "unknown")
        counts[stance] = counts.get(stance, 0) + 1
    return counts


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _contains_any(value: str, terms: set[str]) -> bool:
    normalized = value.lower()
    return any(term in normalized for term in terms)


def _rounded(value: float) -> float:
    return round(max(0.0, min(1.0, value)), 3)


def _dedupe(values: Sequence[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped
