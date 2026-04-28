"""Recommend-only operational agent for the full-text ingestion lane."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .contracts import FullTextOpsAction, FullTextOpsRequest, FullTextOpsResult
from .repository import ResearchRepository
from .source_health import build_source_health_report
from .source_sets import LITERATURE_FULL_TEXT_SOURCE_KEYS


FULL_TEXT_OPS_AGENT_NAME = "full_text_ops_agent"
FULL_TEXT_OPS_AGENT_VERSION = "v1"

_SOURCE_INGEST_JOBS = {
    "europe_pmc": "europe_pmc_full_text_ingest_job",
    "pmc_oa": "pmc_oa_full_text_ingest_job",
}
_SOURCE_REFRESH_JOBS = {
    "europe_pmc": "europe_pmc_full_text_refresh_job",
    "pmc_oa": "pmc_oa_full_text_refresh_job",
}


class FullTextOpsAgent:
    """Review full-text health and return bounded operational recommendations."""

    agent_name = FULL_TEXT_OPS_AGENT_NAME
    agent_version = FULL_TEXT_OPS_AGENT_VERSION

    def __init__(self, repository: ResearchRepository) -> None:
        self.repository = repository

    def run(self, request: FullTextOpsRequest) -> FullTextOpsResult:
        source_keys = request.source_keys or list(LITERATURE_FULL_TEXT_SOURCE_KEYS)
        errors: list[str] = []
        try:
            health_report = request.source_health_report or build_source_health_report(
                self.repository,
                source_keys=source_keys,
                require_claims=False,
            )
        except Exception as exc:
            health_report = {"source_keys": source_keys, "sources": []}
            errors.append(f"source health unavailable: {exc}")

        full_text_report = request.full_text_report or {}
        recent_runs = self._recent_runs(source_keys, request.recent_run_limit)
        actions: list[FullTextOpsAction] = []
        clean_sources: list[str] = []
        partition_clean_sources: list[str] = []

        health_by_source = {
            source_report.get("source_key"): source_report
            for source_report in health_report.get("sources", [])
        }
        full_text_by_source = {
            source_report.get("source_key"): source_report
            for source_report in full_text_report.get("sources", [])
        }

        for source_key in source_keys:
            health = health_by_source.get(source_key, {"source_key": source_key})
            report_source = full_text_by_source.get(source_key, {})
            source_actions = self._source_actions(
                source_key,
                request=request,
                health=health,
                report_source=report_source,
                has_partition_report=_has_partition_report(full_text_report, request.partition_date),
            )
            actions.extend(source_actions)
            if _health_is_clean(health):
                clean_sources.append(source_key)
            if _partition_source_is_clean(report_source):
                partition_clean_sources.append(source_key)

        schedule_readiness = _schedule_readiness(
            source_keys=source_keys,
            actions=actions,
            clean_sources=clean_sources,
            partition_clean_sources=partition_clean_sources,
        )
        if schedule_readiness == "ready_to_enable":
            actions.append(
                FullTextOpsAction(
                    source_key="all",
                    action="ready_to_enable_schedule",
                    severity="info",
                    reason="All configured full-text sources and supplied source/date partition evidence are clean.",
                    evidence_refs=["source_health_report", "full_text_report"],
                )
            )
        elif actions and not any(action.action == "keep_schedule_stopped" for action in actions):
            actions.append(
                FullTextOpsAction(
                    source_key="all",
                    action="keep_schedule_stopped",
                    severity="watch",
                    reason="At least one full-text source still needs validation before enabling the daily partition schedule.",
                    evidence_refs=["source_health_report", "full_text_report"],
                )
            )

        deterministic_result = FullTextOpsResult(
            model_profile=request.model_profile,
            actions=actions,
            should_block_schedule=any(
                action.severity == "blocking" or action.action == "keep_schedule_stopped"
                for action in actions
            ),
            schedule_readiness=schedule_readiness,
            evidence={
                "source_keys": source_keys,
                "partition_date": request.partition_date,
                "review_mode": request.review_mode,
                "source_health_summary": health_report.get("summary", {}),
                "full_text_report_mode": full_text_report.get("mode"),
                "full_text_report_errors": full_text_report.get("errors", []),
                "recent_agent_runs": [run.model_dump(mode="json") for run in recent_runs],
            },
            errors=errors + list(full_text_report.get("errors", [])),
        )
        if request.review_mode == "deterministic_only":
            return deterministic_result

        review_payload = _build_review_payload(
            request=request,
            source_keys=source_keys,
            deterministic_result=deterministic_result,
            health_report=health_report,
            full_text_report=full_text_report,
            recent_runs=recent_runs,
        )
        return _external_review_required_result(
            request=request,
            deterministic_result=deterministic_result,
            review_payload=review_payload,
        )

    def _source_actions(
        self,
        source_key: str,
        *,
        request: FullTextOpsRequest,
        health: Mapping[str, Any],
        report_source: Mapping[str, Any],
        has_partition_report: bool,
    ) -> list[FullTextOpsAction]:
        full_text_qa = health.get("full_text_qa") or report_source.get("full_text_qa") or {}
        triage = full_text_qa.get("triage") or {}
        triage_action = triage.get("action")
        partition_date = request.partition_date

        if triage_action == "reduce_batch_size":
            return [
                FullTextOpsAction(
                    source_key=source_key,
                    action="reduce_batch_size",
                    severity="watch",
                    reason="Full-text triage indicates runtime pressure or timeout behavior.",
                    dagster_job_name="literature_full_text_source_date_job",
                    partition_date=partition_date,
                    evidence_refs=[f"{source_key}.full_text_qa.triage"],
                )
            ]
        if triage_action == "needs_parser_fix":
            return [
                FullTextOpsAction(
                    source_key=source_key,
                    action="inspect_parser",
                    severity="blocking",
                    reason="Full-text triage indicates records were found but body chunks were not produced.",
                    evidence_refs=[f"{source_key}.full_text_qa.triage"],
                )
            ]
        if triage_action == "needs_license_review":
            return [
                FullTextOpsAction(
                    source_key=source_key,
                    action="inspect_license",
                    severity="blocking",
                    reason="Full-text triage found unclear open-access or licensing metadata.",
                    evidence_refs=[f"{source_key}.full_text_qa.triage"],
                )
            ]
        if triage_action == "retry_later":
            return [
                FullTextOpsAction(
                    source_key=source_key,
                    action="run_ingest_smoke",
                    severity="watch",
                    reason="The source should be retried through the pull-only lane before a full refresh.",
                    dagster_job_name=_SOURCE_INGEST_JOBS.get(source_key, "literature_full_text_ingest_smoke_job"),
                    evidence_refs=[f"{source_key}.full_text_qa.triage"],
                )
            ]

        if not _has_minimum_counts(health):
            return [
                FullTextOpsAction(
                    source_key=source_key,
                    action="run_ingest_smoke",
                    severity="watch",
                    reason="The source is missing raw records, research objects, or document chunks.",
                    dagster_job_name=_SOURCE_INGEST_JOBS.get(source_key, "literature_full_text_ingest_smoke_job"),
                    evidence_refs=[f"{source_key}.source_health"],
                )
            ]

        if not full_text_qa.get("passes_full_text_bar"):
            return [
                FullTextOpsAction(
                    source_key=source_key,
                    action="run_full_text_smoke",
                    severity="watch",
                    reason="The source has persisted records but has not passed the full-text body chunk bar.",
                    dagster_job_name=_SOURCE_REFRESH_JOBS.get(source_key, "literature_full_text_smoke_job"),
                    evidence_refs=[f"{source_key}.full_text_qa"],
                )
            ]

        if request.partition_date and not _partition_source_is_clean(report_source):
            return [
                FullTextOpsAction(
                    source_key=source_key,
                    action="run_source_date_partition",
                    severity="watch",
                    reason="Full-text persisted state is clean, but the requested source/date partition is not yet validated.",
                    dagster_job_name="literature_full_text_source_date_job",
                    partition_date=request.partition_date,
                    evidence_refs=[f"{source_key}.partition:{request.partition_date}"],
                )
            ]

        if not has_partition_report:
            return [
                FullTextOpsAction(
                    source_key=source_key,
                    action="run_source_date_partition",
                    severity="watch",
                    reason="Full-text persisted state is clean, but no source/date partition report was supplied.",
                    dagster_job_name="literature_full_text_source_date_job",
                    partition_date=partition_date,
                    evidence_refs=[f"{source_key}.partition"],
                )
            ]

        return [
            FullTextOpsAction(
                source_key=source_key,
                action="mark_clean",
                severity="info",
                reason="Full-text health and source/date partition evidence are clean.",
                partition_date=partition_date,
                evidence_refs=[f"{source_key}.source_health", f"{source_key}.full_text_report"],
            )
        ]

    def _recent_runs(self, source_keys: Sequence[str], limit: int):
        if limit <= 0:
            return []
        runs = self.repository.list_agent_runs(limit=limit)
        return [
            run
            for run in runs
            if run.agent_name in {"full_text_triage_agent", FULL_TEXT_OPS_AGENT_NAME}
            and str(run.status) != "running"
            and (run.source_key is None or run.source_key in source_keys)
        ][:limit]


def _has_minimum_counts(source_report: Mapping[str, Any]) -> bool:
    return all(int(source_report.get(field, 0)) >= 1 for field in ("raw_records", "research_objects", "document_chunks"))


def _health_is_clean(source_report: Mapping[str, Any]) -> bool:
    full_text_qa = source_report.get("full_text_qa") or {}
    return _has_minimum_counts(source_report) and bool(full_text_qa.get("passes_full_text_bar"))


def _has_partition_report(report: Mapping[str, Any], partition_date: str | None) -> bool:
    if report.get("mode") != "source_date_partition":
        return False
    if partition_date and report.get("partition_date") != partition_date:
        return False
    return True


def _partition_source_is_clean(source_report: Mapping[str, Any]) -> bool:
    if not source_report:
        return False
    full_text_qa = source_report.get("full_text_qa") or {}
    return bool(full_text_qa.get("current_empty_passes") or full_text_qa.get("passes_full_text_bar"))


def _schedule_readiness(
    *,
    source_keys: Sequence[str],
    actions: Sequence[FullTextOpsAction],
    clean_sources: Sequence[str],
    partition_clean_sources: Sequence[str],
) -> str:
    if any(action.severity == "blocking" for action in actions):
        return "blocked"
    if set(clean_sources) != set(source_keys):
        return "keep_stopped"
    if set(partition_clean_sources) != set(source_keys):
        return "needs_partition_validation"
    return "ready_to_enable"


def _build_review_payload(
    *,
    request: FullTextOpsRequest,
    source_keys: Sequence[str],
    deterministic_result: FullTextOpsResult,
    health_report: Mapping[str, Any],
    full_text_report: Mapping[str, Any],
    recent_runs: Sequence[Any],
) -> dict[str, Any]:
    return {
        "task": "Review full-text ingestion operations and return the next operational recommendation.",
        "allowed_actions": [
            "mark_clean",
            "run_ingest_smoke",
            "run_full_text_smoke",
            "run_source_date_partition",
            "reduce_batch_size",
            "inspect_parser",
            "inspect_license",
            "keep_schedule_stopped",
            "ready_to_enable_schedule",
            "needs_human_review",
        ],
        "allowed_severities": ["info", "watch", "blocking"],
        "allowed_schedule_readiness": [
            "ready_to_enable",
            "needs_partition_validation",
            "keep_stopped",
            "blocked",
        ],
        "request": request.model_dump(
            mode="json",
            exclude={"source_health_report", "full_text_report"},
        ),
        "source_keys": list(source_keys),
        "deterministic_guardrail_result": deterministic_result.model_dump(mode="json"),
        "source_health_report": _compact_report(health_report),
        "full_text_report": _compact_report(full_text_report),
        "recent_agent_runs": [run.model_dump(mode="json") for run in recent_runs],
        "output_contract": {
            "agent_name": FULL_TEXT_OPS_AGENT_NAME,
            "model_profile": request.model_profile,
            "actions": [
                {
                    "source_key": "string",
                    "action": "one allowed action",
                    "severity": "info|watch|blocking",
                    "reason": "specific evidence-backed reason",
                    "dagster_job_name": "optional string",
                    "partition_date": "optional YYYY-MM-DD",
                    "evidence_refs": ["specific evidence paths"],
                    "metadata": {},
                }
            ],
            "should_block_schedule": "boolean",
            "schedule_readiness": "one allowed schedule readiness value",
            "evidence": {},
            "errors": [],
        },
    }


def _compact_report(report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "mode": report.get("mode"),
        "partition_date": report.get("partition_date"),
        "source_keys": report.get("source_keys", []),
        "summary": report.get("summary", {}),
        "totals": report.get("totals", {}),
        "errors": report.get("errors", []),
        "sources": [_compact_source_report(source) for source in report.get("sources", [])],
        "full_text_triage": report.get("full_text_triage", []),
        "full_text_blocking_sources": report.get("full_text_blocking_sources", []),
    }


def _compact_source_report(source_report: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "source_key",
        "source_role",
        "health_status",
        "health_score",
        "raw_records",
        "research_objects",
        "document_chunks",
        "entity_mentions",
        "claims",
        "passes_minimum_bar",
        "full_text_qa",
        "full_text_triage_action",
        "full_text_triage_severity",
        "full_text_triage_should_retry",
        "full_text_triage_should_block_schedule",
        "signals",
        "risks",
        "recommended_actions",
    )
    return {key: source_report.get(key) for key in keys if key in source_report}


def _external_review_required_result(
    *,
    request: FullTextOpsRequest,
    deterministic_result: FullTextOpsResult,
    review_payload: dict[str, Any],
) -> FullTextOpsResult:
    actions = [
        *deterministic_result.actions,
        FullTextOpsAction(
            source_key="all",
            action="needs_human_review",
            severity="watch",
            reason=(
                "External ChatGPT Pro review is required before changing schedule readiness. "
                "Review the evidence.review_packet payload and record the conclusion outside the hosted job."
            ),
            partition_date=request.partition_date,
            evidence_refs=["evidence.review_packet", "deterministic_guardrail_result"],
            metadata={"review_profile": request.model_profile},
        ),
    ]
    readiness = "blocked" if deterministic_result.schedule_readiness == "blocked" else "keep_stopped"
    evidence = {
        **deterministic_result.evidence,
        "review_mode": request.review_mode,
        "review_packet": review_payload,
        "external_reviewer": {
            "provider": "openai_chatgpt_pro",
            "model_profile": request.model_profile,
            "execution": "external_subscription_session",
        },
        "deterministic_guardrail_schedule_readiness": deterministic_result.schedule_readiness,
        "deterministic_guardrail_actions": [
            action.model_dump(mode="json") for action in deterministic_result.actions
        ],
    }
    return deterministic_result.model_copy(
        update={
            "actions": actions,
            "schedule_readiness": readiness,
            "should_block_schedule": True,
            "evidence": evidence,
        }
    )
