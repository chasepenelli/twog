"""Deterministic triage for full-text ingestion edge cases."""

from __future__ import annotations

from typing import Literal, TypeAlias

from .contracts import FullTextTriageRequest, FullTextTriageResult

FullTextTriageAction: TypeAlias = Literal[
    "no_action",
    "retry_later",
    "reduce_batch_size",
    "needs_parser_fix",
    "needs_license_review",
    "skip_record",
    "needs_human_review",
    "inspect_source_health",
]
FullTextTriageSeverity: TypeAlias = Literal["info", "watch", "blocking"]


class FullTextTriageAgent:
    """Classify full-text failures into bounded operational actions."""

    triage_name = "full_text_triage_agent"

    def triage(self, request: FullTextTriageRequest) -> FullTextTriageResult:
        errors = [item for item in [request.error_message, *request.errors] if item]
        normalized_error = " ".join(errors).lower()
        reasons: list[str] = []
        actions: list[str] = []

        if not errors and not request.current_failed_runs and _has_full_text_body(request):
            return self._result(
                request,
                action="no_action",
                severity="info",
                reasons=["Persisted full-text body chunks are present and no errors were reported."],
                recommended_next_actions=["Continue normal source-specific refresh cadence."],
            )

        if _is_rate_limited(request, normalized_error):
            return self._result(
                request,
                action="retry_later",
                severity="watch",
                should_retry=True,
                reasons=["The source appears rate limited or temporarily throttled."],
                recommended_next_actions=[
                    "Retry after provider cooldown.",
                    "Lower source concurrency or add a longer per-source schedule interval.",
                ],
            )

        if _is_timeout(request, normalized_error):
            return self._result(
                request,
                action="reduce_batch_size",
                severity="watch",
                should_retry=True,
                should_block_schedule=True,
                reasons=["The run exceeded its expected runtime or timeout boundary."],
                recommended_next_actions=[
                    "Run a smaller source-specific batch before retrying the full refresh.",
                    "Add source/date partitioning or a mid-tier Dagster job for this source.",
                ],
            )

        if _is_license_issue(normalized_error):
            return self._result(
                request,
                action="needs_license_review",
                severity="blocking",
                should_block_schedule=True,
                reasons=["The record has unclear open-access or license metadata."],
                recommended_next_actions=[
                    "Keep the record out of body-text storage until license status is reviewed.",
                    "Store only metadata and source URL if licensing remains unclear.",
                ],
            )

        if _is_parser_issue(normalized_error):
            return self._result(
                request,
                action="needs_parser_fix",
                severity="blocking",
                should_block_schedule=True,
                reasons=["The source payload appears malformed or unsupported by the parser."],
                recommended_next_actions=[
                    "Add a fixture for the payload shape.",
                    "Update the source parser and lock the behavior with a regression test.",
                ],
            )

        if request.raw_records == 0:
            reasons.append("No raw records were persisted for the source run.")
            actions.append("Confirm the query still returns records and retry the fetch lane.")
            return self._result(
                request,
                action="retry_later",
                severity="watch",
                should_retry=True,
                reasons=reasons,
                recommended_next_actions=actions,
            )

        if request.full_text_document_chunks == 0:
            reasons.append("Records were persisted but no full-text body chunks were written.")
            actions.append("Inspect chunk_text_sections and source full-text payload extraction.")
            return self._result(
                request,
                action="needs_parser_fix",
                severity="blocking",
                should_block_schedule=True,
                reasons=reasons,
                recommended_next_actions=actions,
            )

        if request.stage in {"entity_resolution", "claim_extraction", "claim_curation"}:
            reasons.append("Full-text body chunks exist, but downstream evidence processing needs inspection.")
            actions.append("Run the phase-specific or lower-limit downstream job for this source.")
            return self._result(
                request,
                action="inspect_source_health",
                severity="watch",
                should_retry=True,
                reasons=reasons,
                recommended_next_actions=actions,
            )

        return self._result(
            request,
            action="needs_human_review",
            severity="watch",
            should_block_schedule=bool(errors or request.current_failed_runs),
            reasons=errors or ["The issue did not match a known deterministic triage rule."],
            recommended_next_actions=[
                "Attach the source payload or Dagster run URL to the triage record.",
                "Promote this case into a parser or orchestration regression test before changing production behavior.",
            ],
        )

    def _result(
        self,
        request: FullTextTriageRequest,
        *,
        action: FullTextTriageAction,
        severity: FullTextTriageSeverity,
        reasons: list[str],
        recommended_next_actions: list[str],
        should_retry: bool = False,
        should_block_schedule: bool = False,
    ) -> FullTextTriageResult:
        return FullTextTriageResult(
            triage_name=self.triage_name,
            source_key=request.source_key,
            stage=request.stage,
            action=action,
            severity=severity,
            should_retry=should_retry,
            should_block_schedule=should_block_schedule,
            reasons=reasons,
            recommended_next_actions=recommended_next_actions,
            model_profile=request.model_profile,
            metadata={
                "query_name": request.query_name,
                "runtime_seconds": request.runtime_seconds,
                "timeout_seconds": request.timeout_seconds,
                "raw_records": request.raw_records,
                "research_objects": request.research_objects,
                "document_chunks": request.document_chunks,
                "full_text_document_chunks": request.full_text_document_chunks,
                "full_text_body_chars": request.full_text_body_chars,
                "claims": request.claims,
                "entity_mentions": request.entity_mentions,
                "current_failed_runs": request.current_failed_runs,
                "http_status": request.http_status,
                **request.metadata,
            },
        )


def triage_full_text_issue(request: FullTextTriageRequest) -> FullTextTriageResult:
    """Convenience wrapper for CLI, MCP, and tests."""

    return FullTextTriageAgent().triage(request)


def _has_full_text_body(request: FullTextTriageRequest) -> bool:
    return request.full_text_document_chunks > 0 and request.full_text_body_chars > 0


def _is_rate_limited(request: FullTextTriageRequest, normalized_error: str) -> bool:
    return request.http_status == 429 or any(
        token in normalized_error
        for token in ("429", "rate limit", "rate-limit", "too many requests", "throttled")
    )


def _is_timeout(request: FullTextTriageRequest, normalized_error: str) -> bool:
    hit_runtime_cap = (
        request.runtime_seconds is not None
        and request.timeout_seconds is not None
        and request.runtime_seconds >= request.timeout_seconds
    )
    return hit_runtime_cap or any(
        token in normalized_error
        for token in ("timeout", "timed out", "deadline", "canceling", "cancelled", "canceled")
    )


def _is_license_issue(normalized_error: str) -> bool:
    return any(
        token in normalized_error
        for token in ("license", "licence", "copyright", "not open access", "oa_license")
    )


def _is_parser_issue(normalized_error: str) -> bool:
    return any(
        token in normalized_error
        for token in ("xml", "parse", "parser", "malformed", "jats", "etag", "empty body")
    )
