"""Synthesis quality evaluator for persisted research briefs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
import os
import re
from typing import Any
import urllib.error
import urllib.request

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
DEFAULT_RESEARCH_BRIEF_EVALUATION_MODEL = "anthropic/claude-sonnet-4.6"

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
_INSUFFICIENT_EVIDENCE_TERMS = {
    "cannot answer",
    "does not answer",
    "evidence is insufficient",
    "insufficient evidence",
    "no direct evidence",
    "no primary clinical trial data",
    "no quantitative",
    "not enough evidence",
    "supplied corpus cannot",
    "supplied evidence was insufficient",
    "unanswered",
    "unsupported by supplied evidence",
}
def evaluate_research_brief_synthesis(
    brief: ResearchBriefRecord,
    request: ResearchBriefEvaluationRequest,
) -> ResearchBriefEvaluationResult:
    """Evaluate whether a persisted brief is ready for hypothesis review."""

    deterministic = _evaluate_research_brief_deterministic(brief, request)
    if request.review_mode == "deterministic_only":
        return deterministic
    try:
        return _evaluate_research_brief_openrouter(brief, request, deterministic)
    except Exception as exc:
        if request.review_mode == "openrouter_required":
            raise
        evidence = dict(deterministic.evidence)
        evidence["openrouter_evaluation_error"] = str(exc)
        return deterministic.model_copy(
            update={
                "evidence": evidence,
                "errors": [*deterministic.errors, f"OpenRouter evaluation failed: {exc}"],
            }
        )


def _evaluate_research_brief_deterministic(
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
    insufficient_evidence_flags = _insufficient_evidence_flags(
        payload=payload,
        final_brief=final_brief,
        perspective_reports=perspective_reports,
        findings=all_findings,
        synthesis_limitations=synthesis_limitations,
    )

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
        insufficient_evidence_flags=insufficient_evidence_flags,
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
        insufficient_evidence_flags=insufficient_evidence_flags,
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
        "insufficient_evidence_flag_count": len(insufficient_evidence_flags),
        "insufficient_evidence_flags": insufficient_evidence_flags[:20],
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


def _evaluate_research_brief_openrouter(
    brief: ResearchBriefRecord,
    request: ResearchBriefEvaluationRequest,
    deterministic: ResearchBriefEvaluationResult,
) -> ResearchBriefEvaluationResult:
    errors: list[str] = []
    for model_name in _select_models(request):
        try:
            review = _openrouter_review_model(
                model_name,
                _openrouter_evaluation_payload(brief, request, deterministic),
            )
            return _evaluation_from_model(brief, request, deterministic, review)
        except Exception as exc:
            errors.append(f"{model_name}: {exc}")
            if request.review_mode == "openrouter_required":
                break
    raise RuntimeError("; ".join(errors) or "OpenRouter evaluation failed")


def _openrouter_evaluation_payload(
    brief: ResearchBriefRecord,
    request: ResearchBriefEvaluationRequest,
    deterministic: ResearchBriefEvaluationResult,
) -> dict[str, Any]:
    payload, parse_errors = _brief_payload(brief)
    return {
        "request": request.model_dump(mode="json"),
        "brief": {
            "brief_id": str(brief.brief_id),
            "topic": brief.topic,
            "disease_scope": brief.disease_scope,
            "source_key": brief.source_key,
            "status": brief.status,
            "citation_count": brief.citation_count,
            "finding_count": brief.finding_count,
            "hypothesis_count": brief.hypothesis_count,
            "hard_error_count": brief.hard_error_count,
            "evidence_limitation_count": brief.evidence_limitation_count,
            "final_brief": _compact_text(str(payload.get("final_brief") or brief.final_brief or ""), 5000),
            "citations": _trim_citations(_as_list(payload.get("citations"))),
            "perspective_reports": _trim_perspective_reports(_as_list(payload.get("perspective_reports"))),
            "ranked_hypotheses": _trim_mappings(_as_list(payload.get("ranked_hypotheses")), 6, 900),
            "unresolved_questions": [str(item)[:600] for item in _as_list(payload.get("unresolved_questions"))[:12]],
            "payload_parse_errors": parse_errors,
        },
        "deterministic_floor": deterministic.model_dump(mode="json"),
    }


def _openrouter_review_model(model_name: str, review_payload: dict[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required for OpenRouter research brief evaluation.")
    user_payload = {
        "instructions": [
            "Evaluate whether this research brief is ready to move into validation planning.",
            "Return JSON only. Do not include markdown or prose outside JSON.",
            "Use the deterministic floor as an audit baseline, but make your own scientific judgment.",
            "Penalize unsupported claims, missing citation IDs, weak translational relevance, and vague next steps.",
            "Do not invent citations or facts beyond the supplied brief payload.",
        ],
        "response_contract": {
            "overall_score": "number 0.0-1.0",
            "citation_coverage_score": "number 0.0-1.0",
            "perspective_balance_score": "number 0.0-1.0",
            "contradiction_handling_score": "number 0.0-1.0",
            "novelty_score": "number 0.0-1.0",
            "actionability_score": "number 0.0-1.0",
            "weakness_transparency_score": "number 0.0-1.0",
            "passes_quality_bar": "boolean",
            "readiness": "ready_for_hypothesis_review | needs_more_evidence | needs_human_review | blocked",
            "strengths": ["short strength strings"],
            "weaknesses": ["short weakness strings"],
            "recommendations": ["short operator recommendations"],
            "evidence": {"agent_review_summary": "short summary", "notable_risks": ["strings"]},
            "errors": ["hard errors only; evidence limitations belong in weaknesses"],
        },
        "evaluation_payload": review_payload,
    }
    payload = {
        "model": model_name,
        "temperature": float(os.getenv("HSA_RESEARCH_BRIEF_EVALUATION_TEMPERATURE", "0.1")),
        "max_tokens": int(os.getenv("HSA_RESEARCH_BRIEF_EVALUATION_MAX_TOKENS", "3000")),
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a rigorous translational oncology synthesis evaluator. "
                    "Your job is to decide whether a generated research brief is reliable enough "
                    "to feed downstream hypothesis and validation planning."
                ),
            },
            {"role": "user", "content": json.dumps(user_payload, sort_keys=True, default=str)},
        ],
    }
    http_request = urllib.request.Request(
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
            http_request,
            timeout=float(os.getenv("HSA_RESEARCH_BRIEF_EVALUATION_TIMEOUT_SECONDS", "90")),
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


def _evaluation_from_model(
    brief: ResearchBriefRecord,
    request: ResearchBriefEvaluationRequest,
    deterministic: ResearchBriefEvaluationResult,
    review: Mapping[str, Any],
) -> ResearchBriefEvaluationResult:
    payload = _load_json_object(str(review.get("text") or ""))
    readiness = str(payload.get("readiness") or deterministic.readiness)
    if readiness not in {"ready_for_hypothesis_review", "needs_more_evidence", "needs_human_review", "blocked"}:
        readiness = "needs_human_review"
    evidence = dict(payload.get("evidence") if isinstance(payload.get("evidence"), Mapping) else {})
    evidence.update(
        {
            "review_mode": request.review_mode,
            "model_review": review.get("metadata", {}),
            "deterministic_floor": deterministic.model_dump(mode="json"),
        }
    )
    overall_score = _payload_score(payload, "overall_score", deterministic.overall_score)
    model_passes_quality_bar = _payload_bool(payload.get("passes_quality_bar"))
    passes_quality_bar = (
        model_passes_quality_bar
        and readiness == "ready_for_hypothesis_review"
        and overall_score >= request.minimum_overall_score
    )
    if model_passes_quality_bar and not passes_quality_bar:
        evidence["model_quality_bar_overridden"] = (
            f"Model returned passes_quality_bar=true with readiness={readiness} "
            f"and overall_score={overall_score}; promotion requires ready_for_hypothesis_review "
            f"and score >= {request.minimum_overall_score}."
        )
    return ResearchBriefEvaluationResult(
        brief_id=brief.brief_id,
        agent_name=RESEARCH_BRIEF_EVALUATION_AGENT_NAME,
        model_profile=request.model_profile,
        topic=brief.topic,
        source_key=brief.source_key,
        overall_score=overall_score,
        citation_coverage_score=_payload_score(
            payload, "citation_coverage_score", deterministic.citation_coverage_score
        ),
        perspective_balance_score=_payload_score(
            payload, "perspective_balance_score", deterministic.perspective_balance_score
        ),
        contradiction_handling_score=_payload_score(
            payload, "contradiction_handling_score", deterministic.contradiction_handling_score
        ),
        novelty_score=_payload_score(payload, "novelty_score", deterministic.novelty_score),
        actionability_score=_payload_score(payload, "actionability_score", deterministic.actionability_score),
        weakness_transparency_score=_payload_score(
            payload, "weakness_transparency_score", deterministic.weakness_transparency_score
        ),
        passes_quality_bar=passes_quality_bar,
        readiness=readiness,  # type: ignore[arg-type]
        strengths=_dedupe([str(item) for item in _as_list(payload.get("strengths")) if str(item).strip()])[:20],
        weaknesses=_dedupe([str(item) for item in _as_list(payload.get("weaknesses")) if str(item).strip()])[:20],
        recommendations=_dedupe(
            [str(item) for item in _as_list(payload.get("recommendations")) if str(item).strip()]
        )[:20],
        evidence=evidence,
        errors=[str(item) for item in _as_list(payload.get("errors")) if str(item).strip()],
    )


def _select_models(request: ResearchBriefEvaluationRequest) -> list[str]:
    if request.review_models:
        return list(request.review_models)
    return [
        os.getenv(
            "HSA_RESEARCH_BRIEF_EVALUATION_MODEL",
            os.getenv("HSA_RESEARCH_BRIEF_MODEL", DEFAULT_RESEARCH_BRIEF_EVALUATION_MODEL),
        )
    ]


def _payload_score(payload: Mapping[str, Any], key: str, default: float) -> float:
    try:
        return _rounded(float(payload.get(key, default)))
    except (TypeError, ValueError):
        return _rounded(default)


def _payload_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return bool(value)


def _trim_citations(citations: Sequence[Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in citations[:30]:
        if not isinstance(raw, Mapping):
            continue
        rows.append(
            {
                "citation_id": raw.get("citation_id"),
                "title": _compact_text(str(raw.get("title") or ""), 400),
                "source_key": raw.get("source_key"),
                "published_year": raw.get("published_year"),
                "doi": raw.get("doi"),
                "pmid": raw.get("pmid"),
            }
        )
    return rows


def _trim_perspective_reports(reports: Sequence[Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in reports[:6]:
        if not isinstance(raw, Mapping):
            continue
        rows.append(
            {
                "perspective": raw.get("perspective"),
                "summary": _compact_text(str(raw.get("summary") or ""), 900),
                "findings": _trim_mappings(_as_list(raw.get("findings")), 5, 700),
                "evidence_limitations": [
                    _compact_text(str(item), 500) for item in _as_list(raw.get("evidence_limitations"))[:8]
                ],
                "errors": [_compact_text(str(item), 500) for item in _as_list(raw.get("errors"))[:8]],
            }
        )
    return rows


def _trim_mappings(values: Sequence[Any], limit: int, max_chars: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in values[:limit]:
        if not isinstance(raw, Mapping):
            continue
        rows.append(
            {
                str(key): _compact_text(str(value), max_chars) if isinstance(value, str) else value
                for key, value in raw.items()
            }
        )
    return rows


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
    insufficient_evidence_flags: Sequence[str],
) -> ResearchBriefEvaluationReadiness:
    if not has_citations or not final_has_citation_tokens or any("unknown citation" in error for error in errors):
        return "blocked"
    if errors:
        return "needs_human_review"
    if insufficient_evidence_flags:
        return "needs_more_evidence"
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
    insufficient_evidence_flags: Sequence[str],
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
    if insufficient_evidence_flags:
        weaknesses.append("Brief explicitly says the supplied evidence is insufficient for the focal question.")
        recommendations.append("Queue focused evidence acquisition before promoting this brief.")
    if readiness == "ready_for_hypothesis_review":
        recommendations.append("Promote this brief into hypothesis review and validation planning.")
    elif readiness == "blocked":
        recommendations.append("Regenerate the brief with citation-first synthesis before downstream use.")
    elif readiness == "needs_more_evidence":
        recommendations.append("Run another retrieval or queue pass with a wider source scope.")
    elif readiness == "needs_human_review":
        recommendations.append("Have a human reviewer inspect the weak or error-bearing sections.")
    return strengths[:20], weaknesses[:20], _dedupe(recommendations)[:20]


def _insufficient_evidence_flags(
    *,
    payload: Mapping[str, Any],
    final_brief: str,
    perspective_reports: Sequence[Any],
    findings: Sequence[Mapping[str, Any]],
    synthesis_limitations: Sequence[str],
) -> list[str]:
    candidates: list[tuple[str, str]] = [
        ("final_brief", final_brief),
        ("topic", str(payload.get("topic") or "")),
    ]
    candidates.extend(("synthesis_limitation", item) for item in synthesis_limitations)
    for item in _as_list(payload.get("evidence_limitations")):
        candidates.append(("evidence_limitation", str(item)))
    for report in perspective_reports:
        if not isinstance(report, Mapping):
            continue
        candidates.append((f"perspective:{report.get('perspective') or 'unknown'}:summary", str(report.get("summary") or "")))
    for index, finding in enumerate(findings, start=1):
        candidates.append((f"finding:{index}:claim", str(finding.get("claim") or "")))
        candidates.append((f"finding:{index}:reasoning", str(finding.get("reasoning") or "")))
        for question in _as_list(finding.get("open_questions")):
            candidates.append((f"finding:{index}:open_question", str(question)))

    flags: list[str] = []
    for label, text in candidates:
        normalized = text.lower()
        if "completion bar" in normalized and ("not meet" in normalized or "did not meet" in normalized):
            flags.append(f"{label}: {text[:240]}")
            continue
        if any(term in normalized for term in _INSUFFICIENT_EVIDENCE_TERMS):
            flags.append(f"{label}: {text[:240]}")
    return _dedupe(flags)


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
    preview = _compact_text(cleaned, 500)
    raise ValueError(f"model response JSON parse failed: {errors[-1] if errors else 'unknown error'}; preview={preview}")


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
    return candidates


def _compact_text(value: str, max_chars: int) -> str:
    text = re.sub(r"\s+", " ", value).strip()
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3].rstrip()}..."


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
