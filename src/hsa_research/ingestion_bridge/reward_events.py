"""Reward ledger helpers for the agent learning loop.

This layer intentionally starts as measurement infrastructure. It records
operator and evaluator judgments as durable reward events, then reports which
agent/model/task lanes are producing useful downstream outputs.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import (
    AgentRunRecord,
    AgentRunReviewRecord,
    RewardOutcomeBucket,
    RewardEventRecord,
    RewardEventSyncRequest,
    RewardEventSyncResult,
    RewardReportRequest,
    RewardReportResult,
    RewardReportRow,
)
from .repository import ResearchRepository


VERDICT_REWARD_SCORES: dict[str, float] = {
    "useful": 1.0,
    "needs_followup": 0.55,
    "unclear": 0.35,
    "bad": 0.0,
}

_CHURN_TERMS = (
    "entirely off-topic",
    "off-topic",
    "irrelevant",
    "no evidence path",
    "operational dead end",
    "no concrete next",
    "no concrete follow",
    "unsupported",
    "unhandled errors",
    "vague idea",
    "unclear output",
    "failed automated searches",
)

_ACTIONABLE_TERMS = (
    "rerun",
    "re-run",
    "retry",
    "queue",
    "ingest",
    "inspect",
    "resolve",
    "verify",
    "search",
    "add",
    "flag",
    "attempt",
    "full-text",
    "pubmed",
    "crossref",
    "europe_pmc",
    "pmc_oa",
    "doi",
    "pmid",
    "query",
)


def sync_reward_events_from_reviews(
    repository: ResearchRepository,
    request: RewardEventSyncRequest,
) -> RewardEventSyncResult:
    """Convert agent run reviews into idempotent reward events."""

    reviews = repository.list_agent_run_reviews(limit=request.limit)
    result = RewardEventSyncResult(scanned_review_count=len(reviews))
    for review in reviews:
        if request.reviewer_type and review.reviewer_type != request.reviewer_type:
            continue
        run = repository.get_agent_run(review.agent_run_id)
        if run is None:
            result.missing_run_count += 1
            continue
        if request.agent_name and run.agent_name != request.agent_name:
            continue
        if request.source_key and run.source_key != request.source_key:
            continue

        result.eligible_review_count += 1
        event = reward_event_from_review(run, review, created_by=request.created_by)
        existing = repository.list_reward_events(source_review_id=review.review_id, limit=1)
        if existing and not request.include_existing:
            result.skipped_existing_count += 1
            continue
        persisted = repository.create_reward_event(event)
        result.created_count += 1
        result.reward_event_ids.append(persisted.reward_event_id)
    return result


def reward_event_from_review(
    run: AgentRunRecord,
    review: AgentRunReviewRecord,
    *,
    created_by: str = "reward_review_sync",
) -> RewardEventRecord:
    """Build a deterministic reward event from one operator/evaluator review."""

    event_source = {
        "operator": "operator_review",
        "llm_evaluator": "llm_evaluator_review",
        "system": "system_review",
    }.get(review.reviewer_type, "manual")
    score = VERDICT_REWARD_SCORES[review.verdict]
    identity_key = f"reward:{event_source}:review:{review.review_id}"
    dimension_scores = _dimension_scores(review, score)
    outcome_bucket = _classify_review_outcome(review, dimension_scores)
    return RewardEventRecord(
        reward_event_id=uuid5(NAMESPACE_URL, identity_key),
        trace_id=run.trace_id,
        identity_key=identity_key,
        event_source=event_source,  # type: ignore[arg-type]
        score=score,
        dimension_scores=dimension_scores,
        verdict=review.verdict,
        agent_run_id=run.agent_run_id,
        source_review_id=review.review_id,
        agent_name=run.agent_name,
        model_profile=run.model_profile,
        prompt_key=_prompt_key(run),
        task_type=_task_type(run),
        source_key=run.source_key,
        outcome_bucket=outcome_bucket,
        routing_recommendation=_routing_recommendation_for_outcome(outcome_bucket),
        churn_risk_score=_churn_risk_score(outcome_bucket, dimension_scores),
        rationale=review.feedback,
        tags=[review.reviewer_type, *review.tags],
        created_by=created_by,
        created_at=review.created_at,
        metadata={
            "reviewer": review.reviewer,
            "reviewer_type": review.reviewer_type,
            "review_followup_actions": review.followup_actions,
            "review_metadata": review.metadata,
            "run_status": run.status,
            "dagster_run_id": run.dagster_run_id,
        },
    )


def build_reward_report(
    repository: ResearchRepository,
    request: RewardReportRequest,
) -> RewardReportResult:
    """Aggregate durable reward events by agent/model/task/source lane."""

    events = repository.list_reward_events(
        agent_name=request.agent_name,
        source_key=request.source_key,
        event_source=request.event_source,
        limit=request.limit,
    )
    event_source_counts: Counter[str] = Counter(str(event.event_source) for event in events)
    verdict_counts: Counter[str] = Counter(str(event.verdict) for event in events if event.verdict)
    outcome_counts: Counter[str] = Counter(_event_outcome_bucket(event) for event in events)

    buckets: dict[str, _RewardBucket] = {}
    for event in events:
        group_value = _reward_group_value(event, request.group_by)
        bucket = buckets.setdefault(group_value, _RewardBucket(group_value))
        bucket.add(event)

    rows = [bucket.to_row(request.group_by, request.min_sample_size) for bucket in buckets.values()]
    rows.sort(key=lambda row: row.group_value.casefold())
    scored_rows = [row for row in rows if row.reward_score is not None]
    top_rows = sorted(
        scored_rows,
        key=lambda row: (row.reward_score or 0, row.event_count, row.group_value.casefold()),
        reverse=True,
    )[:10]
    bottom_rows = sorted(
        scored_rows,
        key=lambda row: (row.reward_score or 0, -row.event_count, row.group_value.casefold()),
    )[:10]

    average = _average([event.score for event in events])
    return RewardReportResult(
        event_count=len(events),
        average_score=average,
        reward_score=None if average is None else round(average * 100),
        event_source_counts=dict(sorted(event_source_counts.items())),
        verdict_counts=dict(sorted(verdict_counts.items())),
        outcome_counts=dict(sorted(outcome_counts.items())),
        positive_signal_count=outcome_counts["positive_signal"],
        actionable_followup_count=outcome_counts["actionable_followup"],
        low_value_churn_count=outcome_counts["low_value_churn"],
        negative_signal_count=outcome_counts["negative_signal"],
        unclear_signal_count=outcome_counts["unclear_signal"],
        rows=rows,
        top_rows=top_rows,
        bottom_rows=bottom_rows,
    )


def summarize_reward_sync(result: RewardEventSyncResult) -> dict[str, Any]:
    return {
        "scanned_review_count": result.scanned_review_count,
        "eligible_review_count": result.eligible_review_count,
        "created_count": result.created_count,
        "skipped_existing_count": result.skipped_existing_count,
        "missing_run_count": result.missing_run_count,
    }


def summarize_reward_report(result: RewardReportResult) -> dict[str, Any]:
    return {
        "event_count": result.event_count,
        "reward_score": result.reward_score,
        "row_count": len(result.rows),
    }


class _RewardBucket:
    def __init__(self, group_value: str) -> None:
        self.group_value = group_value
        self.events: list[RewardEventRecord] = []

    def add(self, event: RewardEventRecord) -> None:
        self.events.append(event)

    def to_row(self, group_type: str, min_sample_size: int) -> RewardReportRow:
        average = _average([event.score for event in self.events])
        event_source_counts: Counter[str] = Counter(str(event.event_source) for event in self.events)
        verdict_counts: Counter[str] = Counter(str(event.verdict) for event in self.events if event.verdict)
        outcome_counts: Counter[str] = Counter(_event_outcome_bucket(event) for event in self.events)
        dimension_values: dict[str, list[float]] = defaultdict(list)
        for event in self.events:
            for dimension, score in event.dimension_scores.items():
                dimension_values[str(dimension)].append(score)
        latest_event_at = max((event.created_at for event in self.events), default=None)
        return RewardReportRow(
            group_type=group_type,  # type: ignore[arg-type]
            group_value=self.group_value,
            event_count=len(self.events),
            average_score=average,
            reward_score=None if average is None else round(average * 100),
            low_sample=len(self.events) < min_sample_size,
            event_source_counts=dict(sorted(event_source_counts.items())),
            verdict_counts=dict(sorted(verdict_counts.items())),
            outcome_counts=dict(sorted(outcome_counts.items())),
            positive_signal_count=outcome_counts["positive_signal"],
            actionable_followup_count=outcome_counts["actionable_followup"],
            low_value_churn_count=outcome_counts["low_value_churn"],
            negative_signal_count=outcome_counts["negative_signal"],
            unclear_signal_count=outcome_counts["unclear_signal"],
            actionable_followup_rate=round(outcome_counts["actionable_followup"] / len(self.events), 3),
            low_value_churn_rate=round(outcome_counts["low_value_churn"] / len(self.events), 3),
            dimension_averages={
                dimension: round(sum(values) / len(values), 3)
                for dimension, values in sorted(dimension_values.items())
                if values
            },
            latest_event_at=latest_event_at,
        )


def _dimension_scores(review: AgentRunReviewRecord, score: float) -> dict[str, float]:
    scores: dict[str, float] = {"overall": score}
    if review.reviewer_type == "operator":
        scores["operator_usefulness"] = score
    elif review.reviewer_type == "llm_evaluator":
        scores["actionability"] = score
        scores["specificity"] = score
        scores.update(_rubric_dimension_scores(review))
    else:
        scores["downstream_progress"] = score
    return scores


def _classify_review_outcome(
    review: AgentRunReviewRecord,
    dimension_scores: dict[str, float],
) -> RewardOutcomeBucket:
    """Separate useful follow-up work from low-value churn.

    This is deliberately deterministic and auditable. It should inform routing
    policy, not silently rewrite prompts or agent behavior.
    """

    if review.verdict == "useful":
        return "positive_signal"
    if review.verdict == "bad":
        return "negative_signal"
    if review.verdict == "unclear":
        return "unclear_signal"

    actionability = dimension_scores.get("actionability", review.metadata.get("actionability"))
    actionability_value = actionability if isinstance(actionability, int | float) else None
    text = _review_text_for_classification(review)
    has_concrete_action = any(term in text for term in _ACTIONABLE_TERMS)
    has_churn_marker = any(term in text for term in _CHURN_TERMS)
    if actionability_value is not None and float(actionability_value) < 0.35:
        return "low_value_churn"
    if has_churn_marker and not has_concrete_action:
        return "low_value_churn"
    if has_concrete_action:
        return "actionable_followup"
    if has_churn_marker:
        return "low_value_churn"
    return "actionable_followup" if actionability_value is None or float(actionability_value) >= 0.45 else "low_value_churn"


def _event_outcome_bucket(event: RewardEventRecord) -> str:
    if event.outcome_bucket:
        return str(event.outcome_bucket)
    if event.verdict == "useful":
        return "positive_signal"
    if event.verdict == "bad":
        return "negative_signal"
    if event.verdict == "unclear":
        return "unclear_signal"
    actionability = event.dimension_scores.get("actionability", event.score)
    metadata = event.metadata or {}
    text_bits: list[str] = []
    for value in (event.rationale, metadata.get("review_followup_actions"), metadata.get("review_metadata")):
        text_bits.append(str(value or ""))
    text = " ".join(text_bits).casefold()
    if actionability < 0.35:
        return "low_value_churn"
    if any(term in text for term in _ACTIONABLE_TERMS):
        return "actionable_followup"
    if any(term in text for term in _CHURN_TERMS):
        return "low_value_churn"
    return "actionable_followup" if event.verdict == "needs_followup" else "unclear_signal"


def _routing_recommendation_for_outcome(outcome_bucket: str) -> str:
    if outcome_bucket == "positive_signal":
        return "prefer_lane"
    if outcome_bucket == "actionable_followup":
        return "queue_targeted_followup"
    if outcome_bucket == "low_value_churn":
        return "demote_or_rewrite"
    if outcome_bucket == "negative_signal":
        return "suppress_or_archive"
    return "inspect_manually"


def _churn_risk_score(outcome_bucket: str, dimension_scores: dict[str, float]) -> float:
    if outcome_bucket == "negative_signal":
        return 1.0
    if outcome_bucket == "low_value_churn":
        actionability = dimension_scores.get("actionability", 0.0)
        return round(max(0.65, 1.0 - actionability), 3)
    if outcome_bucket == "unclear_signal":
        return 0.6
    if outcome_bucket == "actionable_followup":
        return 0.25
    return 0.0


def _review_text_for_classification(review: AgentRunReviewRecord) -> str:
    parts: list[str] = [
        review.feedback or "",
        " ".join(review.followup_actions),
        " ".join(review.tags),
    ]
    metadata = review.metadata.get("agent_performance_evaluation")
    if isinstance(metadata, dict):
        parts.append(" ".join(str(item) for item in metadata.get("failure_modes", []) if item))
        parts.append(" ".join(str(item) for item in metadata.get("strengths", []) if item))
        parts.append(" ".join(str(item) for item in metadata.get("recommended_followup_actions", []) if item))
    return " ".join(parts).casefold()


def _rubric_dimension_scores(review: AgentRunReviewRecord) -> dict[str, float]:
    metadata = review.metadata.get("agent_performance_evaluation")
    rubric_scores = metadata.get("rubric_scores") if isinstance(metadata, dict) else None
    if not isinstance(rubric_scores, dict):
        return {}
    mapped: dict[str, float] = {}
    key_map = {
        "citation_quality": "citation_quality",
        "provenance_quality": "provenance_quality",
        "actionability": "actionability",
        "novelty": "novelty",
        "specificity": "specificity",
        "scientific_risk": "scientific_risk",
    }
    for raw_key, dimension in key_map.items():
        raw_value = rubric_scores.get(raw_key)
        if isinstance(raw_value, int | float):
            mapped[dimension] = _normalize_rubric_score(float(raw_value))
    return mapped


def _normalize_rubric_score(value: float) -> float:
    if value > 1.0:
        value = value / 5.0
    return min(1.0, max(0.0, value))


def _prompt_key(run: AgentRunRecord) -> str | None:
    for payload in (run.metadata, run.input_payload, run.output_payload, run.summary):
        if not isinstance(payload, dict):
            continue
        for key in ("prompt_key", "prompt_version", "rubric_version", "rubric_name"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return run.agent_version


def _task_type(run: AgentRunRecord) -> str:
    metadata = run.metadata if isinstance(run.metadata, dict) else {}
    task_type = metadata.get("task_type")
    if isinstance(task_type, str) and task_type.strip():
        return task_type.strip()
    return run.agent_name


def _reward_group_value(event: RewardEventRecord, group_by: str) -> str:
    if group_by == "agent_name":
        return event.agent_name or "unknown_agent"
    if group_by == "model_profile":
        return event.model_profile or "unknown_model_profile"
    if group_by == "task_type":
        return event.task_type or "unknown_task_type"
    if group_by == "source_key":
        return event.source_key or "unknown_source"
    if group_by == "event_source":
        return str(event.event_source)
    return "unknown"


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 3)
