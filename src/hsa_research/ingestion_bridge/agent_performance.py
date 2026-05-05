"""Hybrid performance reporting and evaluator agents for persisted agent runs."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
import json
import os
import re
from typing import Any
import urllib.error
import urllib.request

from .contracts import (
    AgentPerformanceEvaluationRequest,
    AgentPerformanceEvaluationResult,
    AgentPerformanceReportRequest,
    AgentPerformanceReportResult,
    AgentPerformanceRow,
    AgentRunRecord,
    AgentRunReviewRecord,
    AgentRunReviewVerdict,
)
from .repository import ResearchRepository


AGENT_PERFORMANCE_EVALUATOR_AGENT_NAME = "agent_performance_evaluator_agent"
AGENT_PERFORMANCE_EVALUATOR_AGENT_VERSION = "v1"
DEFAULT_AGENT_PERFORMANCE_EVALUATOR_MODEL = "~anthropic/claude-sonnet-latest"

VERDICT_SCORES: dict[str, float] = {
    "useful": 1.0,
    "needs_followup": 0.55,
    "unclear": 0.35,
    "bad": 0.0,
}
_MODEL_KEYS = ("model", "model_name", "review_model", "openrouter_model", "selected_model", "requested_model")
_PROMPT_KEYS = ("prompt_key", "prompt_version", "rubric_version", "rubric_name")


def build_agent_performance_report(
    repository: ResearchRepository,
    request: AgentPerformanceReportRequest,
) -> AgentPerformanceReportResult:
    """Aggregate latest operator/evaluator reviews into performance rows."""

    runs = repository.list_agent_runs(
        agent_name=request.agent_name,
        status=request.status,
        source_key=request.source_key,
        limit=request.limit,
    )
    review_limit = request.review_limit or max(request.limit * 20, 500)
    reviews = repository.list_agent_run_reviews(limit=review_limit)
    review_state = _latest_reviews_by_run(reviews)

    buckets: dict[tuple[str, str], _PerformanceBucket] = {}
    primary_verdict_counts: Counter[str] = Counter()
    reviewer_type_counts: Counter[str] = Counter()
    reviewed_run_ids: set[str] = set()
    operator_reviewed_ids: set[str] = set()
    evaluator_reviewed_ids: set[str] = set()
    disagreement_ids: set[str] = set()

    for run in runs:
        state = review_state.get(str(run.agent_run_id), {})
        operator_review = state.get("operator")
        evaluator_review = state.get("llm_evaluator")
        primary_review = evaluator_review or operator_review
        disagreement = bool(operator_review and evaluator_review and operator_review.verdict != evaluator_review.verdict)

        for reviewer_type in state:
            reviewer_type_counts[reviewer_type] += 1
        if primary_review:
            reviewed_run_ids.add(str(run.agent_run_id))
            primary_verdict_counts[primary_review.verdict] += 1
        if operator_review:
            operator_reviewed_ids.add(str(run.agent_run_id))
        if evaluator_review:
            evaluator_reviewed_ids.add(str(run.agent_run_id))
        if disagreement:
            disagreement_ids.add(str(run.agent_run_id))

        for group_type, group_value in _group_values(run):
            bucket = buckets.setdefault((group_type, group_value), _PerformanceBucket(group_type, group_value))
            bucket.add_run(
                run,
                primary_review=primary_review,
                operator_review=operator_review,
                evaluator_review=evaluator_review,
                disagreement=disagreement,
            )

    rows = [bucket.to_row(request.min_sample_size) for bucket in buckets.values()]
    rows.sort(key=lambda row: (row.group_type, row.group_value.casefold()))
    scored_rows = [row for row in rows if row.performance_score is not None]
    top_rows = sorted(
        scored_rows,
        key=lambda row: (row.performance_score or 0, row.reviewed_run_count, row.group_value.casefold()),
        reverse=True,
    )[:10]
    bottom_rows = sorted(
        scored_rows,
        key=lambda row: (row.performance_score or 0, -row.reviewed_run_count, row.group_value.casefold()),
    )[:10]

    run_count = len(runs)
    reviewed_count = len(reviewed_run_ids)
    return AgentPerformanceReportResult(
        agent_run_count=run_count,
        reviewed_run_count=reviewed_count,
        unreviewed_run_count=max(0, run_count - reviewed_count),
        operator_reviewed_count=len(operator_reviewed_ids),
        evaluator_reviewed_count=len(evaluator_reviewed_ids),
        disagreement_count=len(disagreement_ids),
        review_coverage=_ratio(reviewed_count, run_count),
        verdict_counts=dict(sorted(primary_verdict_counts.items())),
        reviewer_type_counts=dict(sorted(reviewer_type_counts.items())),
        rows=rows,
        top_rows=top_rows,
        bottom_rows=bottom_rows,
    )


def run_agent_performance_evaluation(
    repository: ResearchRepository,
    request: AgentPerformanceEvaluationRequest,
) -> AgentPerformanceEvaluationResult:
    """Run OpenRouter specialist evaluators over recent reviewed agent runs."""

    if request.agent_run_ids:
        runs = [
            run
            for agent_run_id in request.agent_run_ids
            if (run := repository.get_agent_run(agent_run_id)) is not None
            and (request.agent_name is None or run.agent_name == request.agent_name)
            and (request.status is None or str(run.status) == request.status)
            and (request.source_key is None or run.source_key == request.source_key)
        ][: request.limit]
    else:
        runs = repository.list_agent_runs(
            agent_name=request.agent_name,
            status=request.status,
            source_key=request.source_key,
            limit=request.limit,
        )
    reviews = repository.list_agent_run_reviews(limit=max(request.limit * 20, 500))
    review_state = _latest_reviews_by_run(reviews)
    candidates = [
        run
        for run in runs
        if run.agent_name != AGENT_PERFORMANCE_EVALUATOR_AGENT_NAME
        and (not request.reviewed_only or review_state.get(str(run.agent_run_id), {}).get("operator") is not None)
    ]

    review_ids = []
    evaluations: list[dict[str, Any]] = []
    models = _review_models(request)
    for run in candidates[: request.limit]:
        specialist = _specialist_for_agent(run.agent_name)
        payload = _evaluation_payload(
            run=run,
            review_state=review_state.get(str(run.agent_run_id), {}),
            specialist=specialist,
            request=request,
        )
        model_name = models[0]
        review = _openrouter_review_model(model_name, payload)
        parsed = _parse_json_object(str(review.get("text") or ""))
        verdict = _validated_verdict(parsed)
        review_record = AgentRunReviewRecord(
            agent_run_id=run.agent_run_id,
            reviewer=f"{specialist}_openrouter_evaluator",
            reviewer_type="llm_evaluator",
            verdict=verdict,
            feedback=_optional_text(parsed.get("rationale")),
            tags=[specialist, "openrouter", _safe_tag(model_name)],
            followup_actions=_string_list(parsed.get("recommended_followup_actions")),
            metadata={
                "agent_performance_evaluation": {
                    "specialist": specialist,
                    "model_name": model_name,
                    "model_metadata": review.get("metadata", {}),
                    "confidence": _bounded_float(parsed.get("confidence"), 0.0, 1.0),
                    "strengths": _string_list(parsed.get("strengths")),
                    "failure_modes": _string_list(parsed.get("failure_modes")),
                    "rubric_scores": _mapping_or_empty(parsed.get("rubric_scores")),
                }
            },
        )
        repository.create_agent_run_review(review_record)
        review_ids.append(review_record.review_id)
        evaluations.append(
            {
                "agent_run_id": str(run.agent_run_id),
                "agent_name": run.agent_name,
                "specialist": specialist,
                "model_name": model_name,
                "review_id": str(review_record.review_id),
                "verdict": verdict,
                "confidence": _bounded_float(parsed.get("confidence"), 0.0, 1.0),
                "rationale": _optional_text(parsed.get("rationale")),
                "recommended_followup_actions": review_record.followup_actions,
            }
        )

    return AgentPerformanceEvaluationResult(
        model_profile=request.model_profile,
        reviewed_only=request.reviewed_only,
        scanned_count=len(runs),
        candidate_count=len(candidates),
        evaluated_count=len(evaluations),
        review_created_count=len(review_ids),
        failed_count=0,
        review_ids=review_ids,
        evaluations=evaluations,
    )


def summarize_agent_performance_report(result: AgentPerformanceReportResult) -> dict[str, Any]:
    return {
        "agent_run_count": result.agent_run_count,
        "reviewed_run_count": result.reviewed_run_count,
        "review_coverage": result.review_coverage,
        "disagreement_count": result.disagreement_count,
        "row_count": len(result.rows),
    }


def summarize_agent_performance_evaluation(result: AgentPerformanceEvaluationResult) -> dict[str, Any]:
    return {
        "evaluated_count": result.evaluated_count,
        "review_created_count": result.review_created_count,
        "failed_count": result.failed_count,
        "candidate_count": result.candidate_count,
    }


class _PerformanceBucket:
    def __init__(self, group_type: str, group_value: str) -> None:
        self.group_type = group_type
        self.group_value = group_value
        self.run_count = 0
        self.reviewed_run_count = 0
        self.operator_reviewed_count = 0
        self.evaluator_reviewed_count = 0
        self.disagreement_count = 0
        self.verdict_counts: Counter[str] = Counter()
        self.scores: list[float] = []
        self.latest_run_at: datetime | None = None

    def add_run(
        self,
        run: AgentRunRecord,
        *,
        primary_review: AgentRunReviewRecord | None,
        operator_review: AgentRunReviewRecord | None,
        evaluator_review: AgentRunReviewRecord | None,
        disagreement: bool,
    ) -> None:
        self.run_count += 1
        if self.latest_run_at is None or run.started_at > self.latest_run_at:
            self.latest_run_at = run.started_at
        if primary_review:
            self.reviewed_run_count += 1
            self.verdict_counts[primary_review.verdict] += 1
            self.scores.append(VERDICT_SCORES[primary_review.verdict])
        if operator_review:
            self.operator_reviewed_count += 1
        if evaluator_review:
            self.evaluator_reviewed_count += 1
        if disagreement:
            self.disagreement_count += 1

    def to_row(self, min_sample_size: int) -> AgentPerformanceRow:
        reviewed = self.reviewed_run_count
        score = round(100 * (sum(self.scores) / len(self.scores))) if self.scores else None
        return AgentPerformanceRow(
            group_type=self.group_type,  # type: ignore[arg-type]
            group_value=self.group_value,
            run_count=self.run_count,
            reviewed_run_count=reviewed,
            unreviewed_run_count=max(0, self.run_count - reviewed),
            operator_reviewed_count=self.operator_reviewed_count,
            evaluator_reviewed_count=self.evaluator_reviewed_count,
            useful_count=self.verdict_counts.get("useful", 0),
            needs_followup_count=self.verdict_counts.get("needs_followup", 0),
            bad_count=self.verdict_counts.get("bad", 0),
            unclear_count=self.verdict_counts.get("unclear", 0),
            useful_rate=_ratio(self.verdict_counts.get("useful", 0), reviewed),
            followup_rate=_ratio(self.verdict_counts.get("needs_followup", 0), reviewed),
            bad_rate=_ratio(self.verdict_counts.get("bad", 0), reviewed),
            unclear_rate=_ratio(self.verdict_counts.get("unclear", 0), reviewed),
            review_coverage=_ratio(reviewed, self.run_count),
            performance_score=score,
            disagreement_count=self.disagreement_count,
            low_sample=reviewed < min_sample_size,
            latest_run_at=self.latest_run_at,
        )


def _latest_reviews_by_run(
    reviews: Sequence[AgentRunReviewRecord],
) -> dict[str, dict[str, AgentRunReviewRecord]]:
    latest: dict[str, dict[str, AgentRunReviewRecord]] = defaultdict(dict)
    for review in reviews:
        run_key = str(review.agent_run_id)
        type_key = review.reviewer_type
        current = latest[run_key].get(type_key)
        if current is None or review.created_at > current.created_at:
            latest[run_key][type_key] = review
    return latest


def _group_values(run: AgentRunRecord) -> list[tuple[str, str]]:
    model_key = _derived_key(run, _MODEL_KEYS, run.model_profile)
    prompt_key = _derived_key(run, _PROMPT_KEYS, run.agent_version)
    return [
        ("agent_name", run.agent_name),
        ("model_profile", run.model_profile),
        ("model_key", model_key),
        ("prompt_key", prompt_key),
    ]


def _derived_key(run: AgentRunRecord, keys: Sequence[str], fallback: str) -> str:
    for payload in (run.metadata, run.input_payload, run.output_payload, run.summary):
        value = _find_key(payload, keys)
        if value is not None:
            return str(value)[:240]
    return fallback or "unknown"


def _find_key(value: Any, keys: Sequence[str]) -> Any | None:
    if isinstance(value, Mapping):
        for key in keys:
            if key in value and value[key] not in (None, ""):
                candidate = value[key]
                if isinstance(candidate, str | int | float | bool):
                    return candidate
        for nested in value.values():
            found = _find_key(nested, keys)
            if found is not None:
                return found
    elif isinstance(value, list):
        for item in value[:20]:
            found = _find_key(item, keys)
            if found is not None:
                return found
    return None


def _evaluation_payload(
    *,
    run: AgentRunRecord,
    review_state: Mapping[str, AgentRunReviewRecord],
    specialist: str,
    request: AgentPerformanceEvaluationRequest,
) -> dict[str, Any]:
    operator_review = review_state.get("operator")
    evaluator_review = review_state.get("llm_evaluator")
    evidence_fit = _evidence_fit_payload(run)
    run_payload = {
        "agent_run_id": str(run.agent_run_id),
        "agent_name": run.agent_name,
        "agent_version": run.agent_version,
        "model_profile": run.model_profile,
        "model_key": _derived_key(run, _MODEL_KEYS, run.model_profile),
        "prompt_key": _derived_key(run, _PROMPT_KEYS, run.agent_version),
        "status": str(run.status),
        "source_key": run.source_key,
        "partition_date": run.partition_date,
        "started_at": run.started_at.isoformat(),
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "summary": _compact_jsonable(run.summary, 2500),
        "input_payload": _compact_jsonable(run.input_payload, 4000),
        "output_payload": _compact_jsonable(run.output_payload, 7000),
        "errors": run.errors[:20],
        "metadata": _compact_jsonable(run.metadata, 2500),
    }
    if evidence_fit:
        run_payload["evidence_fit"] = evidence_fit
        run_payload["evidence_fit_interpretation"] = _evidence_fit_interpretation(evidence_fit)
    return {
        "task": "Judge whether one persisted agent run produced useful operational or scientific output.",
        "specialist": specialist,
        "rubric": _rubric_for_specialist(specialist),
        "allowed_verdicts": ["useful", "needs_followup", "bad", "unclear"],
        "response_contract": {
            "verdict": "useful | needs_followup | bad | unclear",
            "confidence": "number 0.0-1.0",
            "rationale": "short evidence-backed reason",
            "strengths": ["short strings"],
            "failure_modes": ["short strings"],
            "recommended_followup_actions": ["short action strings"],
            "rubric_scores": {"criterion": "number 0.0-1.0"},
        },
        "request": request.model_dump(mode="json"),
        "run": run_payload,
        "operator_review": _review_payload(operator_review),
        "latest_evaluator_review": _review_payload(evaluator_review),
    }


def _evidence_fit_payload(run: AgentRunRecord) -> dict[str, Any] | None:
    for payload in (run.output_payload, run.summary, run.metadata, run.input_payload):
        evidence_fit = _find_mapping(payload, "evidence_fit")
        if evidence_fit:
            return evidence_fit
    return None


def _find_mapping(value: Any, key: str) -> dict[str, Any] | None:
    if isinstance(value, Mapping):
        candidate = value.get(key)
        if isinstance(candidate, Mapping):
            return dict(candidate)
        for nested in value.values():
            found = _find_mapping(nested, key)
            if found is not None:
                return found
    elif isinstance(value, list):
        for item in value[:20]:
            found = _find_mapping(item, key)
            if found is not None:
                return found
    return None


def _evidence_fit_interpretation(evidence_fit: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "policy": (
            "Evaluate target_safety_fit, disease_directness_fit, actionability, and transfer_risk separately. "
            "Do not mark a run bad only because disease_directness_fit is partial when target safety and "
            "actionability are strong and the transfer risk is explicit."
        ),
        "useful_when": [
            "The run retrieved concrete records or chunks that improve the next research action.",
            "Target-safety or mechanism evidence is strong and actionability is strong, even if disease directness is partial.",
            "Transfer risk is clearly labeled rather than hidden.",
        ],
        "needs_followup_when": [
            "Target-safety fit or actionability is weak.",
            "Disease directness is weak and no transfer-risk caveat or next search is provided.",
            "The run found records but left parser, license, identifier, or source-coverage blockers unresolved.",
        ],
        "observed_dimensions": {
            "overall_fit": evidence_fit.get("overall_fit") or evidence_fit.get("fit"),
            "target_safety_fit": evidence_fit.get("target_safety_fit"),
            "disease_directness_fit": evidence_fit.get("disease_directness_fit"),
            "actionability": evidence_fit.get("actionability"),
            "transfer_risk": evidence_fit.get("transfer_risk"),
        },
    }


def _review_payload(review: AgentRunReviewRecord | None) -> dict[str, Any] | None:
    if review is None:
        return None
    return {
        "review_id": str(review.review_id),
        "reviewer": review.reviewer,
        "reviewer_type": review.reviewer_type,
        "verdict": review.verdict,
        "feedback": review.feedback,
        "tags": review.tags,
        "followup_actions": review.followup_actions,
        "created_at": review.created_at.isoformat(),
        "metadata": _compact_jsonable(review.metadata, 2000),
    }


def _specialist_for_agent(agent_name: str) -> str:
    normalized = agent_name.casefold()
    if any(
        token in normalized
        for token in ("research_brief", "research_synthesis", "therapy_committee", "hypothesis", "validation_planning")
    ):
        return "synthesis"
    if any(token in normalized for token in ("validation_agent", "validation_request", "omics", "assay", "evidence_review")):
        return "validation"
    if any(
        token in normalized
        for token in (
            "claim_curator",
            "evidence_gap",
            "evidence_scout",
            "followup_resolver",
            "full_text",
            "gap_resolver",
            "source_followup",
            "source_pack",
            "source_scout",
            "twitter",
            "x_linked",
            "x_topic",
        )
    ):
        return "ingestion"
    return "general"


def _rubric_for_specialist(specialist: str) -> dict[str, Any]:
    rubrics = {
        "synthesis": {
            "criteria": [
                "citation-grounded reasoning",
                "clear translational hypothesis or therapy idea",
                "explicit limitations and contradictions",
                "actionable next step",
            ],
            "failure_modes": ["unsupported leap", "vague idea", "missing citations", "ignored contradictory evidence"],
        },
        "validation": {
            "criteria": [
                "testable validation objective",
                "mutation/function/clinical-response linkage",
                "species and assay context",
                "clear blocker or promotion decision",
            ],
            "failure_modes": ["not executable", "missing omics context", "weak decision rationale", "unsafe overclaim"],
        },
        "ingestion": {
            "criteria": [
                "source-specific operational recommendation",
                "evidence paths and concrete blocker",
                "split evidence-fit dimensions are interpreted correctly",
                "preserves deterministic guardrails",
                "identifies links or records that need ingestion",
            ],
            "failure_modes": [
                "invented source state",
                "no evidence path",
                "unsafe schedule recommendation",
                "missed parser/license risk",
                "treats partial disease directness as failure despite strong target safety and explicit transfer risk",
            ],
        },
        "general": {
            "criteria": [
                "clear output",
                "evidence-backed reasoning",
                "operator-useful next action",
                "no invented facts",
            ],
            "failure_modes": ["unclear output", "unsupported claim", "missing action", "unhandled errors"],
        },
    }
    return rubrics.get(specialist, rubrics["general"])


def _review_models(request: AgentPerformanceEvaluationRequest) -> list[str]:
    if request.review_models:
        return request.review_models
    configured = os.getenv("HSA_AGENT_PERFORMANCE_EVALUATOR_MODELS")
    if configured:
        return [model.strip() for model in configured.split(",") if model.strip()]
    return [os.getenv("HSA_AGENT_PERFORMANCE_EVALUATOR_MODEL", DEFAULT_AGENT_PERFORMANCE_EVALUATOR_MODEL)]


def _openrouter_review_model(model_name: str, review_payload: dict[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required for agent performance evaluation.")
    payload = {
        "model": model_name,
        "temperature": float(os.getenv("HSA_AGENT_PERFORMANCE_EVALUATOR_TEMPERATURE", "0.1")),
        "max_tokens": int(os.getenv("HSA_AGENT_PERFORMANCE_EVALUATOR_MAX_TOKENS", "1800")),
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _AGENT_PERFORMANCE_SYSTEM_PROMPT},
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
            timeout=float(os.getenv("HSA_AGENT_PERFORMANCE_EVALUATOR_TIMEOUT_SECONDS", "90")),
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


def _parse_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", stripped, flags=re.DOTALL)
    if fenced:
        stripped = fenced.group(1).strip()
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start < 0 or end <= start:
            raise
        parsed = json.loads(stripped[start : end + 1])
    if not isinstance(parsed, dict):
        raise RuntimeError("OpenRouter model response must be a JSON object.")
    return parsed


def _validated_verdict(payload: Mapping[str, Any]) -> AgentRunReviewVerdict:
    verdict = str(payload.get("verdict") or "").strip().casefold()
    if verdict not in VERDICT_SCORES:
        raise RuntimeError("OpenRouter agent performance evaluator response is missing a valid verdict.")
    return verdict  # type: ignore[return-value]


def _compact_jsonable(value: Any, max_chars: int) -> Any:
    text = json.dumps(value, sort_keys=True, default=str)
    if len(text) <= max_chars:
        return value
    return {"truncated_json": text[:max_chars], "truncated": True, "original_chars": len(text)}


def _mapping_or_empty(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    output = []
    for item in value:
        text = str(item).strip()
        if text and text not in output:
            output.append(text[:500])
    return output[:20]


def _optional_text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text[:2000] or None


def _bounded_float(value: Any, minimum: float, maximum: float) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return min(max(number, minimum), maximum)


def _safe_tag(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", value.casefold()).strip("_")[:80] or "model"


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


_AGENT_PERFORMANCE_SYSTEM_PROMPT = """You are a specialist evaluator for TWOG's agent layer.

Judge one stored agent run using only the supplied run payload, operator review, errors, and rubric.
Return only a JSON object matching the supplied response_contract. Do not include markdown.

Rules:
- Do not invent citations, source state, clinical facts, or hidden context.
- Treat the operator review as important evidence, but make your own judgment.
- Mark outputs useful only when they create a clear scientific or operational next step.
- Mark needs_followup when the output is directionally useful but lacks enough evidence or specificity.
- Mark bad when the output is misleading, unsupported, unusable, or failed its core task.
- Mark unclear when the payload is insufficient to judge.
- When run.evidence_fit is present, judge target_safety_fit, disease_directness_fit, actionability, and
  transfer_risk separately. Partial disease directness is not automatically bad when target safety and
  actionability are strong and transfer risk is explicitly labeled.
"""
