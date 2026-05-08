"""Local web command center for TWOG operations.

This is intentionally lightweight: a small stdlib HTTP server over the existing
service layer, with static assets bundled beside the package. The goal is a
usable management surface before committing to a heavier frontend stack.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from importlib import resources
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import UUID

from .contracts import (
    AgentFindingEscalationRequest,
    AgentPerformanceEvaluationRequest,
    AgentPerformanceReportRequest,
    AgentRunReviewRecord,
    CommandCenterRequest,
    HypothesisPromotionReportRequest,
    ResearchFollowupRefinementRequest,
    ResearchFollowupLoopRequest,
    ResearchBriefQualityReportRequest,
    TherapyIdeaLibraryRequest,
    ValidationAutopilotRequest,
    ValidationPacketRequest,
    ValidationToolCatalogRequest,
    ValidationToolMatchRequest,
)
from .service import HSAResearchService
from .validation_agents import DEFAULT_VALIDATION_AGENT_MODEL


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8787


def build_command_center_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Return the command-center report from query-string style params."""

    params = params or {}
    request = CommandCenterRequest(
        source_keys=_list_param(params, "source"),
        include_source_health=_bool_param(params, "include_source_health", True),
        include_recent_agents=_bool_param(params, "include_recent_agents", True),
        queue_limit=_int_param(params, "queue_limit", 25),
        lead_limit=_int_param(params, "lead_limit", 25),
        agent_run_limit=_int_param(params, "agent_run_limit", 25),
        min_health_score=_float_param(params, "min_health_score", 0.65),
        require_claims=_bool_param(params, "require_claims", True),
    )
    return service.build_command_center_report(request).model_dump(mode="json")


def runtime_payload() -> dict[str, Any]:
    """Return local command-center runtime readiness without exposing secrets."""

    openrouter_ready = bool(os.getenv("OPENROUTER_API_KEY"))
    model = os.getenv("HSA_VALIDATION_AGENT_MODEL", DEFAULT_VALIDATION_AGENT_MODEL)
    return {
        "validation_dispatch": {
            "openrouter_ready": openrouter_ready,
            "dispatch_ready": openrouter_ready,
            "default_model": model,
            "message": (
                "OpenRouter validation dispatch is ready."
                if openrouter_ready
                else "OpenRouter validation dispatch is disabled because OPENROUTER_API_KEY is not set on this server process."
            ),
        }
    }


def list_validation_queue_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Return validation queue rows plus status counts for the command center."""

    params = params or {}
    statuses = _list_param(params, "status")
    status = statuses[0] if len(statuses) == 1 else None
    status_filter = statuses if len(statuses) > 1 else None
    items = service.list_validation_request_queue_items(
        status=status,
        statuses=status_filter,
        source_key=_str_param(params, "source"),
        task_type=_str_param(params, "task_type"),
        topic_query=_str_param(params, "topic_query"),
        limit=_int_param(params, "limit", 50),
    )
    status_counts: dict[str, int] = {}
    for item in service.list_validation_request_queue_items(limit=None):
        status_counts[str(item.status)] = status_counts.get(str(item.status), 0) + 1
    return {
        "total": sum(status_counts.values()),
        "visible": len(items),
        "status_counts": dict(sorted(status_counts.items())),
        "items": [item.model_dump(mode="json") for item in items],
    }


def list_research_leads_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Return research leads for the command center."""

    params = params or {}
    statuses = _list_param(params, "status")
    status = statuses[0] if len(statuses) == 1 else None
    status_filter = statuses if len(statuses) > 1 else None
    leads = service.list_research_leads(
        status=status,
        statuses=status_filter,
        lead_type=_str_param(params, "lead_type"),
        source_key=_str_param(params, "source"),
        limit=_int_param(params, "limit", 50),
    )
    status_counts: dict[str, int] = {}
    for lead in service.list_research_leads(limit=None):
        status_counts[str(lead.status)] = status_counts.get(str(lead.status), 0) + 1
    return {
        "total": sum(status_counts.values()),
        "visible": len(leads),
        "status_counts": dict(sorted(status_counts.items())),
        "items": [lead.model_dump(mode="json") for lead in leads],
    }


def list_ideas_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Return derived idea records for the command-center idea library."""

    params = params or {}
    kind = _str_param(params, "kind")
    source_key = _str_param(params, "source")
    query = (_str_param(params, "query") or "").casefold()
    limit = _int_param(params, "limit", 100)

    records = _collect_idea_records(service)
    durable_ideas = service.list_therapy_ideas(
        TherapyIdeaLibraryRequest(
            source_program_id=_uuid_param(params, "program_id"),
            source_brief_id=_uuid_param(params, "brief_id"),
            source_evaluation_id=_uuid_param(params, "evaluation_id"),
            topic_query=_str_param(params, "query"),
            limit=limit,
        )
    ).ideas
    records_by_key = {f"{record.get('kind')}:{record.get('idea_id')}": record for record in records}
    for record in durable_ideas:
        payload = _command_center_therapy_idea_record(record.model_dump(mode="json"))
        records_by_key[f"therapy_idea:{payload['idea_id']}"] = payload
    records = list(records_by_key.values())
    if kind:
        records = [record for record in records if record["kind"] == kind]
    if source_key:
        records = [record for record in records if record.get("source_key") == source_key]
    if query:
        records = [
            record
            for record in records
            if query
            in " ".join(
                str(value)
                for value in (
                    record.get("title"),
                    record.get("hypothesis"),
                    record.get("rationale"),
                    " ".join(record.get("candidate_therapies") or []),
                    " ".join(record.get("targets") or []),
                    " ".join(record.get("biomarkers") or []),
                )
            ).casefold()
        ]

    records.sort(
        key=lambda record: (
            float(record.get("priority_score") or record.get("confidence") or 0),
            str(record.get("created_at") or ""),
        ),
        reverse=True,
    )
    visible = records[:limit] if limit is not None else records
    kind_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    validation_status_counts: dict[str, int] = {}
    for record in records:
        kind_counts[str(record.get("kind") or "unknown")] = kind_counts.get(str(record.get("kind") or "unknown"), 0) + 1
        status = str(record.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        for status, count in (record.get("validation_status_counts") or {}).items():
            validation_status_counts[str(status)] = validation_status_counts.get(str(status), 0) + int(count)

    return {
        "total": len(records),
        "visible": len(visible),
        "kind_counts": dict(sorted(kind_counts.items())),
        "status_counts": dict(sorted(status_counts.items())),
        "validation_status_counts": dict(sorted(validation_status_counts.items())),
        "items": visible,
    }


def validation_tool_catalog_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    params = params or {}
    request = ValidationToolCatalogRequest(
        category=_str_param(params, "category"),  # type: ignore[arg-type]
        runner_status=_str_param(params, "runner_status"),  # type: ignore[arg-type]
        validation_type=_str_param(params, "validation_type"),
        task_type=_str_param(params, "task_type"),
        query=_str_param(params, "query"),
        limit=_int_param(params, "limit", 50),
    )
    return service.list_validation_tool_catalog(request).model_dump(mode="json")


def match_validation_tool_payload(
    service: HSAResearchService,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload = payload or {}
    request = ValidationToolMatchRequest(
        validation_type=str(payload.get("validation_type") or "").strip() or None,
        task_type=str(payload.get("task_type") or "").strip() or None,
        objective=str(payload.get("objective") or "").strip() or None,
        candidate_name=str(payload.get("candidate_name") or "").strip() or None,
        target_name=str(payload.get("target_name") or "").strip() or None,
        species=_payload_string_list(payload.get("species")),
        required_inputs=_payload_string_list(payload.get("required_inputs")),
        runner_status=str(payload.get("runner_status") or "recommend_only").strip(),  # type: ignore[arg-type]
        limit=int(payload.get("limit") or 5),
    )
    return service.match_validation_tools(request).model_dump(mode="json")


def hypothesis_promotion_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    params = params or {}
    request = HypothesisPromotionReportRequest(
        brief_id=_uuid_param(params, "brief_id"),
        evaluation_id=_uuid_param(params, "evaluation_id"),
        therapy_idea_id=_uuid_param(params, "therapy_idea_id"),
        topic_query=_str_param(params, "query"),
        source_key=_str_param(params, "source"),
        include_blocked=_bool_param(params, "include_blocked", True),
        include_ready_for_committee=_bool_param(params, "include_ready_for_committee", True),
        include_ready_for_validation=_bool_param(params, "include_ready_for_validation", True),
        limit=_int_param(params, "limit", 50),
    )
    return service.build_hypothesis_promotion_report(request).model_dump(mode="json")


def validation_packets_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    params = params or {}
    request = ValidationPacketRequest(
        candidate_id=_str_param(params, "candidate_id"),
        therapy_idea_id=_uuid_param(params, "therapy_idea_id"),
        plan_id=_uuid_param(params, "plan_id"),
        queue_item_id=_uuid_param(params, "queue_item_id"),
        brief_id=_uuid_param(params, "brief_id"),
        evaluation_id=_uuid_param(params, "evaluation_id"),
        topic_query=_str_param(params, "query"),
        source_key=_str_param(params, "source"),
        include_queue_items=_bool_param(params, "include_queue_items", True),
        queue_if_ready=_bool_param(params, "queue_if_ready", False),
        dry_run=_bool_param(params, "dry_run", True),
        max_tasks=_int_param(params, "max_tasks", 8),
        priority=_int_param(params, "priority", 40),
        limit=_int_param(params, "limit", 10),
    )
    return service.build_validation_packets(request).model_dump(mode="json")


def list_research_briefs_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Return persisted research briefs with latest quality state for the command center."""

    params = params or {}
    quality_status = _str_param(params, "quality_status")
    limit = _int_param(params, "limit", 50)
    report = service.build_research_brief_quality_report(
        ResearchBriefQualityReportRequest(
            status=_str_param(params, "status"),
            source_key=_str_param(params, "source"),
            topic_query=_str_param(params, "query"),
            limit=limit,
            include_evaluations=True,
        )
    )
    briefs_by_id = {
        str(brief.brief_id): brief
        for brief in service.list_research_briefs(
            status=_str_param(params, "status"),
            source_key=_str_param(params, "source"),
            topic_query=_str_param(params, "query"),
            limit=limit,
        )
    }
    rows = [row for row in report.rows if not quality_status or row.quality_status == quality_status]
    items = [_command_center_research_brief(row.model_dump(mode="json"), briefs_by_id.get(str(row.brief_id))) for row in rows]
    return {
        "total": report.brief_count,
        "visible": len(items),
        "evaluated_count": report.evaluated_count,
        "ready_count": report.ready_count,
        "failed_count": report.failed_count,
        "followup_count": report.followup_count,
        "needs_evaluation_count": report.needs_evaluation_count,
        "average_overall_score": report.average_overall_score,
        "status_counts": report.status_counts,
        "quality_status_counts": report.quality_status_counts,
        "items": items,
        "errors": report.errors,
    }


def list_agent_runs_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Return persisted agent ledger rows with full JSON payloads for inspection."""

    params = params or {}
    agent_name = _str_param(params, "agent_name")
    status = _str_param(params, "status")
    source_key = _str_param(params, "source")
    query = (_str_param(params, "query") or "").casefold()
    limit = max(1, min(_int_param(params, "limit", 100), 500))

    runs = service.list_agent_runs(
        agent_name=agent_name,
        status=status,
        source_key=source_key,
        limit=max(limit, 500),
    )
    items = [_command_center_agent_run_detail(run) for run in runs]
    for item in items:
        reviews = service.list_agent_run_reviews(agent_run_id=UUID(item["agent_run_id"]), limit=10)
        item["latest_reviews"] = [_command_center_agent_run_review(review) for review in reviews]
        item["review_count"] = len(reviews)
        item["latest_review"] = item["latest_reviews"][0] if item["latest_reviews"] else None
    if query:
        items = [item for item in items if query in _agent_run_search_text(item)]

    visible = items[:limit]
    return {
        "total": len(items),
        "visible": len(visible),
        "scanned": len(runs),
        "status_counts": _status_counts(items),
        "agent_counts": _count_by(items, "agent_name"),
        "source_counts": _count_by(items, "source_key"),
        "items": visible,
    }


def get_agent_run_payload(service: HSAResearchService, agent_run_id: str) -> dict[str, Any]:
    """Return one persisted agent ledger row."""

    try:
        run_id = UUID(agent_run_id)
    except ValueError as exc:
        raise ValueError("Invalid agent_run_id") from exc
    run = service.get_agent_run(run_id)
    if run is None:
        raise LookupError("Agent run not found")
    item = _command_center_agent_run_detail(run)
    reviews = service.list_agent_run_reviews(agent_run_id=run.agent_run_id, limit=50)
    item["latest_reviews"] = [_command_center_agent_run_review(review) for review in reviews]
    item["review_count"] = len(reviews)
    item["latest_review"] = item["latest_reviews"][0] if item["latest_reviews"] else None
    return {"item": item}


def create_agent_run_review_payload(
    service: HSAResearchService,
    agent_run_id: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist an operator review for one agent run from the command center."""

    payload = payload or {}
    try:
        run_id = UUID(agent_run_id)
    except ValueError as exc:
        raise ValueError("Invalid agent_run_id") from exc
    if service.get_agent_run(run_id) is None:
        raise LookupError("Agent run not found")
    verdict = str(payload.get("verdict") or "").strip()
    if verdict not in {"useful", "needs_followup", "bad", "unclear"}:
        raise ValueError("Invalid agent run review verdict")
    review = service.create_agent_run_review(
        AgentRunReviewRecord(
            agent_run_id=run_id,
            reviewer=str(payload.get("reviewer") or "command_center_operator").strip(),
            reviewer_type="operator",
            verdict=verdict,
            feedback=str(payload.get("feedback") or "").strip() or None,
            tags=_payload_string_list(payload.get("tags")),
            followup_actions=_payload_string_list(payload.get("followup_actions")),
            metadata={
                "command_center": {
                    "operator": str(payload.get("operator") or payload.get("reviewer") or "command_center_operator").strip()
                }
            },
        )
    )
    return {"item": _command_center_agent_run_review(review)}


def agent_performance_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Return hybrid operator/evaluator performance aggregation."""

    params = params or {}
    result = service.build_agent_performance_report(
        AgentPerformanceReportRequest(
            agent_name=_str_param(params, "agent_name"),
            status=_str_param(params, "status"),
            source_key=_str_param(params, "source"),
            limit=max(1, min(_int_param(params, "limit", 500), 2000)),
            min_sample_size=max(1, min(_int_param(params, "min_sample_size", 3), 100)),
        )
    )
    return result.model_dump(mode="json")


def run_agent_performance_evaluation_payload(
    service: HSAResearchService,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run OpenRouter specialist evaluators over recent reviewed runs."""

    payload = payload or {}
    result = service.run_agent_performance_evaluation(
        AgentPerformanceEvaluationRequest(
            agent_name=str(payload.get("agent_name") or "").strip() or None,
            status=str(payload.get("status") or "completed").strip() or None,
            source_key=str(payload.get("source_key") or payload.get("source") or "").strip() or None,
            limit=max(1, min(int(payload.get("limit") or 25), 100)),
            reviewed_only=_payload_bool(payload.get("reviewed_only", True)),
            model_profile=str(payload.get("model_profile") or "agent_performance_evaluator").strip(),
            review_models=_payload_string_list(payload.get("review_models")),
            metadata={
                "command_center": {
                    "operator": str(payload.get("operator") or "command_center_operator").strip(),
                }
            },
        )
    )
    return result.model_dump(mode="json")


def escalate_agent_findings_payload(
    service: HSAResearchService,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Create durable follow-up work from bad/needs-followup evaluator findings."""

    payload = payload or {}
    review_ids = _payload_string_list(payload.get("review_ids") or payload.get("review_id"))
    agent_run_ids = _payload_string_list(payload.get("agent_run_ids") or payload.get("agent_run_id"))
    verdicts = _payload_string_list(payload.get("verdicts") or payload.get("verdict"))
    result = service.escalate_agent_findings(
        AgentFindingEscalationRequest(
            review_ids=[UUID(value) for value in review_ids],
            agent_run_ids=[UUID(value) for value in agent_run_ids],
            verdicts=verdicts or ["bad", "needs_followup"],  # type: ignore[arg-type]
            limit=max(1, min(int(payload.get("limit") or 25), 200)),
            source_keys=_payload_string_list(payload.get("source_keys") or payload.get("source_key")),
            create_research_leads=_payload_bool(payload.get("create_research_leads", True)),
            create_source_queries=_payload_bool(payload.get("create_source_queries", True)),
            dry_run=_payload_bool(payload.get("dry_run", False)),
            operator=str(payload.get("operator") or "command_center_operator").strip(),
            metadata={
                "command_center": {
                    "operator": str(payload.get("operator") or "command_center_operator").strip(),
                }
            },
        )
    )
    return result.model_dump(mode="json")


def update_research_lead_status_payload(
    service: HSAResearchService,
    lead_id: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Update a research lead lifecycle status from the command center."""

    payload = payload or {}
    status = str(payload.get("status") or "").strip()
    if status not in {"new", "watching", "followup", "queued", "ingested", "dismissed", "archived"}:
        raise ValueError("Invalid research lead status")
    updated = service.update_research_lead(
        UUID(lead_id),
        status=status,
        metadata={
            "command_center": {
                "last_status_action": status,
                "operator": str(payload.get("operator") or "command_center_operator").strip(),
            }
        },
    )
    return {"item": None if updated is None else updated.model_dump(mode="json")}


def run_research_followup_loop_payload(
    service: HSAResearchService,
    lead_id: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run one repair-loop step for an escalated research lead."""

    payload = payload or {}
    evaluate = _payload_bool(payload.get("evaluate", False))
    model_profile = str(payload.get("model_profile") or "agent_performance_evaluator").strip()
    if evaluate and _requires_openrouter(model_profile) and not runtime_payload()["validation_dispatch"]["openrouter_ready"]:
        raise RuntimeError(runtime_payload()["validation_dispatch"]["message"])
    result = service.run_research_followup_loop(
        ResearchFollowupLoopRequest(
            lead_id=UUID(lead_id),
            followup_lane=str(payload.get("followup_lane") or "agent_evaluator_followup").strip(),
            source_keys=_payload_string_list(payload.get("source_keys") or payload.get("source_key")),
            query_names=_payload_string_list(payload.get("query_names") or payload.get("query_name")),
            limit_per_query=max(1, min(int(payload.get("limit_per_query") or 2), 100)),
            max_queries=max(1, min(int(payload.get("max_queries") or 10), 100)),
            ingest=_payload_bool(payload.get("ingest", True)),
            resolve=_payload_bool(payload.get("resolve", False)),
            evaluate=evaluate,
            queue_identifier_followups=_payload_bool(payload.get("queue_identifier_followups", True)),
            ingest_identifier_followups=_payload_bool(payload.get("ingest_identifier_followups", True)),
            run_claim_extraction=_payload_bool(payload.get("run_claim_extraction", True)),
            max_identifier_followups=max(1, min(int(payload.get("max_identifier_followups") or 8), 50)),
            dry_run=_payload_bool(payload.get("dry_run", False)),
            force_live_search=_payload_bool(payload.get("force_live_search", True)),
            search_limit_per_source=max(1, min(int(payload.get("search_limit_per_source") or 2), 25)),
            min_evidence_chunks=max(1, min(int(payload.get("min_evidence_chunks") or 1), 25)),
            model_profile=model_profile,
            review_models=_payload_string_list(payload.get("review_models")),
            estimated_evaluator_cost_usd=float(payload.get("estimated_evaluator_cost_usd") or 0.03),
            operator=str(payload.get("operator") or "command_center_operator").strip(),
            metadata={
                "command_center": {
                    "operator": str(payload.get("operator") or "command_center_operator").strip(),
                }
            },
        )
    )
    return result.model_dump(mode="json")


def refine_research_followup_payload(
    service: HSAResearchService,
    lead_id: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Create refined source queries from latest evaluator feedback for one lead."""

    payload = payload or {}
    result = service.refine_research_followups(
        ResearchFollowupRefinementRequest(
            lead_ids=[UUID(lead_id)],
            review_ids=[UUID(review_id) for review_id in _payload_string_list(payload.get("review_ids") or payload.get("review_id"))],
            verdicts=_payload_string_list(payload.get("verdicts") or payload.get("verdict")) or ["bad", "needs_followup"],
            source_keys=_payload_string_list(payload.get("source_keys") or payload.get("source_key")),
            limit=max(1, min(int(payload.get("limit") or 25), 200)),
            max_queries_per_review=max(1, min(int(payload.get("max_queries_per_review") or 4), 20)),
            dry_run=_payload_bool(payload.get("dry_run", False)),
            operator=str(payload.get("operator") or "command_center_operator").strip(),
            metadata={
                "command_center": {
                    "operator": str(payload.get("operator") or "command_center_operator").strip(),
                }
            },
        )
    )
    return result.model_dump(mode="json")


def build_action_items_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build an operator-focused action item feed."""

    params = params or {}
    limit = _int_param(params, "limit", 50)
    report = service.build_command_center_report(
        CommandCenterRequest(
            include_source_health=_bool_param(params, "include_source_health", True),
            include_recent_agents=_bool_param(params, "include_recent_agents", True),
            queue_limit=limit,
            lead_limit=limit,
            agent_run_limit=min(limit, 50),
        )
    )
    validation_items = service.list_validation_request_queue_items(
        statuses=["needs_approval", "approved", "blocked", "failed"],
        limit=limit,
    )
    research_leads = service.list_research_leads(
        statuses=["new", "watching", "followup"],
        limit=limit,
    )

    items: list[dict[str, Any]] = []
    items.extend(_agent_evaluator_action_items(service, limit=limit))
    for index, recommendation in enumerate(report.recommendations):
        items.append(
            {
                "item_id": f"recommendation:{index}",
                "area": recommendation.area,
                "kind": "recommendation",
                "severity": recommendation.severity,
                "status": "open",
                "priority": _action_priority(recommendation.severity, 80),
                "title": recommendation.action,
                "description": recommendation.reason,
                "source_key": None,
                "job_name": recommendation.job_name,
                "actions": [],
                "metadata": recommendation.metadata,
            }
        )
    for item in validation_items:
        request = item.validation_request
        actions: list[str] = []
        if item.status == "needs_approval":
            actions.append("approve_validation")
        if item.status == "approved":
            actions.append("dispatch_validation")
        items.append(
            {
                "item_id": str(item.queue_item_id),
                "area": "validation",
                "kind": "validation_request",
                "severity": "blocking" if item.status in {"blocked", "failed"} else "watch",
                "status": item.status,
                "priority": item.priority,
                "title": item.title,
                "description": item.objective,
                "source_key": item.source_key,
                "job_name": request.validation_type,
                "actions": actions,
                "metadata": {
                    "task_type": item.task_type,
                    "validation_type": request.validation_type,
                    "dispatch_blockers": item.dispatch_blockers,
                    "quality_gates": item.quality_gates,
                    "last_error": item.last_error,
                },
            }
        )
    for lead in research_leads:
        actions = ["promote_lead", "mark_followup", "demote_lead"]
        if lead.origin_review_id and (lead.metadata or {}).get("created_by") == "agent_finding_escalation_agent":
            actions = ["run_followup_search", "reevaluate_followup", *actions]
            last_loop = (lead.metadata or {}).get("research_followup_loop") or {}
            if (
                lead.status == "followup"
                and (
                    last_loop.get("verdict") in {"bad", "needs_followup"}
                    or ((last_loop.get("evidence_fit") or {}).get("fit") in {"weak", "partial"})
                )
            ):
                actions = ["create_refined_queries", *actions]
        items.append(
            {
                "item_id": str(lead.lead_id),
                "area": "research_leads",
                "kind": "research_lead",
                "severity": "watch" if lead.status == "followup" else "info",
                "status": lead.status,
                "priority": lead.priority,
                "title": lead.title or "Untitled research lead",
                "description": lead.reason or lead.summary or "No reason recorded.",
                "source_key": lead.source_key or lead.origin_source_key,
                "job_name": None,
                "actions": actions,
                "metadata": {
                    "lead_type": lead.lead_type,
                    "url": lead.url,
                    "topic_tags": lead.topic_tags,
                    "suggested_sources": lead.suggested_sources,
                    "evidence_refs": lead.evidence_refs,
                    "origin_review_id": str(lead.origin_review_id) if lead.origin_review_id else None,
                    "origin_agent_run_id": str(lead.origin_agent_run_id) if lead.origin_agent_run_id else None,
                    "last_followup_loop": (lead.metadata or {}).get("research_followup_loop"),
                },
            }
        )
    items.sort(key=lambda item: (int(item["priority"]), str(item["kind"]), str(item["title"])))
    return {
        "total": len(items),
        "items": items[:limit],
        "status_counts": _status_counts(items),
        "area_counts": _area_counts(items),
    }


def _agent_evaluator_action_items(service: HSAResearchService, *, limit: int) -> list[dict[str, Any]]:
    reviews = service.list_agent_run_reviews(limit=max(100, limit * 10))
    escalated_review_ids = {
        lead.origin_review_id
        for lead in service.list_research_leads(limit=None)
        if lead.origin_review_id is not None
    }
    latest_by_run: dict[UUID, AgentRunReviewRecord] = {}
    for review in reviews:
        if review.reviewer_type != "llm_evaluator":
            continue
        if review.review_id in escalated_review_ids:
            continue
        latest_by_run.setdefault(review.agent_run_id, review)

    items: list[dict[str, Any]] = []
    for review in latest_by_run.values():
        if review.verdict not in {"bad", "needs_followup"}:
            continue
        run = service.get_agent_run(review.agent_run_id)
        if run is None:
            continue
        severity = "blocking" if review.verdict == "bad" else "watch"
        title = f"{run.agent_name}: {_humanize_verdict(review.verdict)}"
        followups = [action for action in review.followup_actions if action]
        description = review.feedback or "Evaluator marked this run for follow-up."
        if followups:
            description = f"{description} Follow-up: {'; '.join(_humanize_verdict(action) for action in followups[:3])}"
        items.append(
            {
                "item_id": f"agent-review:{review.review_id}",
                "area": "agents",
                "kind": "agent_evaluator_finding",
                "severity": severity,
                "status": review.verdict,
                "priority": _action_priority(severity, 45),
                "title": title,
                "description": description,
                "source_key": run.source_key,
                "job_name": run.agent_name,
                "actions": ["escalate_agent_finding"],
                "metadata": {
                    "agent_run_id": str(run.agent_run_id),
                    "review_id": str(review.review_id),
                    "reviewer": review.reviewer,
                    "reviewer_type": review.reviewer_type,
                    "verdict": review.verdict,
                    "confidence": review.metadata.get("confidence"),
                    "tags": review.tags,
                    "followup_actions": followups,
                    "run_status": run.status,
                    "run_summary": run.summary,
                    "run_errors": run.errors,
                },
            }
        )
    return items[:limit]


def _humanize_verdict(verdict: str) -> str:
    return verdict.replace("_", " ").title()


def approve_validation_request_payload(
    service: HSAResearchService,
    queue_item_id: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Approve one validation queue item from the web command center."""

    payload = payload or {}
    approved_by = str(payload.get("approved_by") or "command_center_operator").strip()
    approval_note = payload.get("approval_note")
    item = service.approve_validation_request_queue_item(
        UUID(queue_item_id),
        approved_by=approved_by,
        approval_note=str(approval_note).strip() if approval_note else None,
    )
    return {"item": None if item is None else item.model_dump(mode="json")}


def dispatch_validation_request_payload(
    service: HSAResearchService,
    queue_item_id: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Dispatch one approved validation queue item from the web command center."""

    payload = payload or {}
    model_profile = str(payload.get("model_profile") or "openrouter_required").strip()
    if _requires_openrouter(model_profile) and not runtime_payload()["validation_dispatch"]["openrouter_ready"]:
        raise RuntimeError(runtime_payload()["validation_dispatch"]["message"])
    item = service.dispatch_validation_request_queue_item(UUID(queue_item_id), model_profile=model_profile)
    return {"item": None if item is None else item.model_dump(mode="json")}


def validation_autopilot_preview_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Preview the validation autopilot policy without mutating queue state."""

    request = _validation_autopilot_request_from_params(params or {}, dry_run=True)
    return service.preview_validation_autopilot(request).model_dump(mode="json")


def run_validation_autopilot_payload(
    service: HSAResearchService,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the validation autopilot once from the command center."""

    payload = payload or {}
    request = ValidationAutopilotRequest(
        enabled=bool(payload.get("enabled", True)),
        dry_run=bool(payload.get("dry_run", True)),
        force=bool(payload.get("force", False)),
        manual_grace_period_hours=float(payload.get("manual_grace_period_hours", 6.0)),
        minimum_queue_age_hours=float(payload.get("minimum_queue_age_hours", 1.0)),
        max_per_run=int(payload.get("max_per_run", 2)),
        hourly_budget_usd=float(payload.get("hourly_budget_usd", 0.25)),
        daily_budget_usd=float(payload.get("daily_budget_usd", 1.50)),
        estimated_cost_per_item_usd=float(payload.get("estimated_cost_per_item_usd", 0.03)),
        allowed_task_types=_payload_string_list(payload.get("allowed_task_types"))
        or ["expert_review", "target_validation", "omics"],
        allowed_validation_types=_payload_string_list(payload.get("allowed_validation_types"))
        or ["expert_review", "homology", "omics"],
        source_keys=_payload_string_list(payload.get("source_keys")),
        model_profile=str(payload.get("model_profile") or "openrouter_required").strip(),
        approved_by=str(payload.get("approved_by") or "validation_autopilot").strip(),
        approval_note=str(payload.get("approval_note") or "").strip() or None,
        metadata={"command_center": {"operator": str(payload.get("operator") or "command_center_operator").strip()}},
    )
    return service.run_validation_autopilot(request).model_dump(mode="json")


def _collect_idea_records(service: HSAResearchService) -> list[dict[str, Any]]:
    validation_items = service.list_validation_request_queue_items(limit=None)
    validation_by_idea: dict[str, list[dict[str, Any]]] = {}
    validation_by_plan: dict[str, list[dict[str, Any]]] = {}
    for item in validation_items:
        item_payload = {
            "queue_item_id": str(item.queue_item_id),
            "status": item.status,
            "task_type": item.task_type,
            "validation_type": item.validation_request.validation_type,
            "title": item.title,
            "priority": item.priority,
            "last_run_id": str(item.last_run_id) if item.last_run_id else None,
        }
        validation_by_plan.setdefault(str(item.plan_id), []).append(item_payload)
        idea_id = str((item.metadata or {}).get("idea_id") or "").strip()
        if idea_id:
            validation_by_idea.setdefault(idea_id, []).append(item_payload)

    records: dict[str, dict[str, Any]] = {}
    for run in service.repository.list_agent_runs(
        agent_name="therapy_committee_chair_agent",
        status="completed",
        limit=500,
    ):
        payload = run.output_payload or {}
        for idea in payload.get("ranked_ideas") or []:
            if not isinstance(idea, Mapping):
                continue
            idea_id = str(idea.get("idea_id") or "").strip()
            if not idea_id:
                continue
            validation_items_for_idea = validation_by_idea.get(idea_id, [])
            records[f"therapy:{idea_id}"] = {
                "idea_id": idea_id,
                "kind": "therapy_idea",
                "title": idea.get("title") or "Untitled therapy idea",
                "hypothesis": idea.get("hypothesis") or "",
                "rationale": idea.get("rationale") or "",
                "candidate_therapies": _string_list(idea.get("candidate_therapies")),
                "targets": _string_list(idea.get("targets")),
                "biomarkers": _string_list(idea.get("biomarkers")),
                "mechanism": idea.get("mechanism"),
                "evidence_refs": _string_list(idea.get("evidence_refs")),
                "evidence_strength": idea.get("evidence_strength") or "unknown",
                "translational_path": idea.get("translational_path"),
                "risks": _string_list(idea.get("risks")),
                "next_experiments": _string_list(idea.get("next_experiments")),
                "priority_score": idea.get("priority_score"),
                "status": _idea_status(validation_items_for_idea),
                "source_key": run.source_key,
                "topic": payload.get("topic"),
                "disease_scope": payload.get("disease_scope"),
                "origin_agent_run_id": str(run.agent_run_id),
                "committee_run_id": payload.get("committee_run_id"),
                "model_profile": payload.get("model_profile") or run.model_profile,
                "review_mode": payload.get("review_mode"),
                "created_at": payload.get("created_at") or run.started_at.isoformat(),
                "validation_status_counts": _count_by(validation_items_for_idea, "status"),
                "validation_items": validation_items_for_idea,
            }

    for plan in service.list_validation_plans(limit=500):
        payload = plan.result_payload or {}
        for draft in payload.get("hypothesis_drafts") or []:
            if not isinstance(draft, Mapping):
                continue
            hypothesis_id = str(draft.get("hypothesis_id") or f"{plan.plan_id}:{draft.get('title', '')}").strip()
            if not hypothesis_id:
                continue
            validation_items_for_plan = validation_by_plan.get(str(plan.plan_id), [])
            records[f"hypothesis:{hypothesis_id}"] = {
                "idea_id": hypothesis_id,
                "kind": "validation_hypothesis",
                "title": draft.get("title") or "Untitled validation hypothesis",
                "hypothesis": draft.get("hypothesis") or "",
                "rationale": draft.get("rationale") or "",
                "candidate_therapies": [],
                "targets": [],
                "biomarkers": [],
                "mechanism": None,
                "evidence_refs": _string_list(draft.get("supporting_claim_ids")),
                "evidence_strength": "unknown",
                "translational_path": None,
                "risks": [],
                "next_experiments": [],
                "priority_score": None,
                "confidence": draft.get("confidence"),
                "status": draft.get("status") or plan.status,
                "source_key": plan.source_key,
                "topic": plan.topic,
                "disease_scope": None,
                "origin_agent_run_id": str(plan.agent_run_id) if plan.agent_run_id else None,
                "plan_id": str(plan.plan_id),
                "brief_id": str(plan.brief_id),
                "evaluation_id": str(plan.evaluation_id) if plan.evaluation_id else None,
                "model_profile": plan.model_profile,
                "review_mode": None,
                "created_at": plan.created_at.isoformat(),
                "validation_status_counts": _count_by(validation_items_for_plan, "status"),
                "validation_items": validation_items_for_plan,
            }

    return list(records.values())


def _command_center_research_brief(row: dict[str, Any], brief: Any | None) -> dict[str, Any]:
    result_payload = brief.result_payload if brief else {}
    citations = result_payload.get("citations") or []
    hypotheses = result_payload.get("ranked_hypotheses") or []
    limitations = result_payload.get("evidence_limitations") or []
    return {
        **row,
        "final_brief": brief.final_brief if brief else "",
        "summary": brief.summary if brief else {},
        "disease_scope": brief.disease_scope if brief else None,
        "agent_run_ids": [str(agent_run_id) for agent_run_id in (brief.agent_run_ids if brief else [])],
        "citation_preview": [
            {
                "citation_id": citation.get("citation_id"),
                "title": citation.get("title"),
                "source_key": citation.get("source_key"),
                "source_url": citation.get("source_url"),
            }
            for citation in citations[:8]
            if isinstance(citation, Mapping)
        ],
        "hypothesis_preview": [
            {
                "claim": hypothesis.get("claim"),
                "evidence_strength": hypothesis.get("evidence_strength"),
                "citations": _string_list(hypothesis.get("citations")),
            }
            for hypothesis in hypotheses[:5]
            if isinstance(hypothesis, Mapping)
        ],
        "evidence_limitations": _string_list(limitations)[:10],
    }


def _command_center_therapy_idea_record(record: dict[str, Any]) -> dict[str, Any]:
    idea = record.get("idea") or {}
    validation_items = []
    return {
        "idea_id": str(record.get("therapy_idea_id") or idea.get("idea_id")),
        "kind": "therapy_idea",
        "title": idea.get("title") or "Untitled therapy idea",
        "hypothesis": idea.get("hypothesis") or "",
        "rationale": idea.get("rationale") or "",
        "candidate_therapies": _string_list(record.get("candidate_therapies") or idea.get("candidate_therapies")),
        "targets": _string_list(record.get("targets") or idea.get("targets")),
        "biomarkers": _string_list(record.get("biomarkers") or idea.get("biomarkers")),
        "mechanism": idea.get("mechanism"),
        "evidence_refs": _string_list(record.get("evidence_refs") or idea.get("evidence_refs")),
        "evidence_strength": idea.get("evidence_strength") or "unknown",
        "translational_path": idea.get("translational_path"),
        "risks": _string_list(record.get("risks") or idea.get("risks")),
        "next_experiments": _string_list(record.get("next_experiments") or idea.get("next_experiments")),
        "priority_score": record.get("score") if record.get("score") is not None else idea.get("priority_score"),
        "status": record.get("status") or "proposed",
        "promotion_state": record.get("promotion_state"),
        "source_key": record.get("source_key"),
        "topic": record.get("topic"),
        "disease_scope": record.get("disease_scope"),
        "origin_agent_run_id": record.get("agent_run_id"),
        "committee_run_id": record.get("committee_run_id"),
        "program_id": record.get("source_program_id"),
        "brief_id": record.get("source_brief_id"),
        "evaluation_id": record.get("source_evaluation_id"),
        "model_profile": (record.get("promotion_metadata") or {}).get("model_profile"),
        "review_mode": (record.get("promotion_metadata") or {}).get("review_mode"),
        "created_at": record.get("created_at"),
        "validation_status_counts": _count_by(validation_items, "status"),
        "validation_items": validation_items,
        "metadata": record.get("metadata") or {},
    }


def _command_center_agent_run_detail(run: Any) -> dict[str, Any]:
    duration_seconds: float | None = None
    if run.completed_at is not None:
        duration_seconds = max(0.0, (run.completed_at - run.started_at).total_seconds())
    input_payload = run.input_payload or {}
    output_payload = run.output_payload or {}
    metadata = run.metadata or {}
    return {
        "agent_run_id": str(run.agent_run_id),
        "agent_name": run.agent_name,
        "agent_version": run.agent_version,
        "model_profile": run.model_profile,
        "status": str(run.status),
        "source_key": run.source_key,
        "partition_date": run.partition_date,
        "dagster_run_id": run.dagster_run_id,
        "started_at": run.started_at.isoformat(),
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "duration_seconds": duration_seconds,
        "input_payload": input_payload,
        "output_payload": output_payload,
        "summary": run.summary or {},
        "errors": run.errors or [],
        "metadata": metadata,
        "input_size": len(json.dumps(input_payload, sort_keys=True, default=str)),
        "output_size": len(json.dumps(output_payload, sort_keys=True, default=str)),
    }


def _command_center_agent_run_review(review: Any) -> dict[str, Any]:
    return {
        "review_id": str(review.review_id),
        "agent_run_id": str(review.agent_run_id),
        "reviewer": review.reviewer,
        "reviewer_type": review.reviewer_type,
        "verdict": review.verdict,
        "feedback": review.feedback,
        "tags": review.tags,
        "followup_actions": review.followup_actions,
        "created_at": review.created_at.isoformat(),
        "metadata": review.metadata,
    }


def _agent_run_search_text(item: Mapping[str, Any]) -> str:
    fields = [
        item.get("agent_run_id"),
        item.get("agent_name"),
        item.get("agent_version"),
        item.get("model_profile"),
        item.get("status"),
        item.get("source_key"),
        item.get("partition_date"),
        item.get("dagster_run_id"),
        json.dumps(item.get("summary") or {}, sort_keys=True, default=str),
        json.dumps(item.get("input_payload") or {}, sort_keys=True, default=str),
        json.dumps(item.get("output_payload") or {}, sort_keys=True, default=str),
        json.dumps(item.get("metadata") or {}, sort_keys=True, default=str),
        json.dumps(item.get("latest_reviews") or [], sort_keys=True, default=str),
        " ".join(str(error) for error in item.get("errors") or []),
    ]
    return " ".join(str(field or "") for field in fields).casefold()


def _idea_status(validation_items: list[dict[str, Any]]) -> str:
    counts = _count_by(validation_items, "status")
    for status in ("failed", "blocked", "approved", "needs_approval", "completed"):
        if counts.get(status):
            return status
    return "idea_recorded"


def _count_by(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = str(item.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        value = [value]
    return [str(item).strip() for item in value if str(item).strip()]


def _requires_openrouter(model_profile: str) -> bool:
    return model_profile not in {"deterministic_only", "external_required"}


def _payload_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().casefold() not in {"0", "false", "no", "off"}
    return bool(value)


def run_server(
    *,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    service_factory: Callable[[], HSAResearchService] = HSAResearchService,
) -> None:
    """Run the local command-center server until interrupted."""

    handler = _make_handler(service_factory)
    server = HTTPServer((host, port), handler)
    print(f"TWOG command center running at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nTWOG command center stopped.")
    finally:
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local TWOG command center")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Bind host; default is local-only 127.0.0.1")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Bind port")
    args = parser.parse_args()
    run_server(host=args.host, port=args.port)


def _make_handler(service_factory: Callable[[], HSAResearchService]) -> type[BaseHTTPRequestHandler]:
    class CommandCenterHandler(BaseHTTPRequestHandler):
        server_version = "TWOGCommandCenter/0.1"

        def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
            parsed = urlparse(self.path)
            if parsed.path == "/" or parsed.path == "/index.html":
                self._send_static("index.html")
                return
            if parsed.path.startswith("/assets/"):
                self._send_static(parsed.path.removeprefix("/assets/"))
                return
            if parsed.path == "/api/command-center":
                self._send_json(build_command_center_payload(service_factory(), parse_qs(parsed.query)))
                return
            if parsed.path == "/api/runtime":
                self._send_json(runtime_payload())
                return
            if parsed.path == "/api/action-items":
                self._send_json(build_action_items_payload(service_factory(), parse_qs(parsed.query)))
                return
            if parsed.path == "/api/validation-requests":
                self._send_json(list_validation_queue_payload(service_factory(), parse_qs(parsed.query)))
                return
            if parsed.path == "/api/validation-autopilot":
                self._send_json(validation_autopilot_preview_payload(service_factory(), parse_qs(parsed.query)))
                return
            if parsed.path == "/api/research-leads":
                self._send_json(list_research_leads_payload(service_factory(), parse_qs(parsed.query)))
                return
            if parsed.path == "/api/ideas":
                self._send_json(list_ideas_payload(service_factory(), parse_qs(parsed.query)))
                return
            if parsed.path == "/api/research-briefs":
                self._send_json(list_research_briefs_payload(service_factory(), parse_qs(parsed.query)))
                return
            if parsed.path == "/api/agent-runs":
                self._send_json(list_agent_runs_payload(service_factory(), parse_qs(parsed.query)))
                return
            if parsed.path == "/api/agent-performance":
                self._send_json(agent_performance_payload(service_factory(), parse_qs(parsed.query)))
                return
            if parsed.path == "/api/validation-tool-catalog":
                self._send_json(validation_tool_catalog_payload(service_factory(), parse_qs(parsed.query)))
                return
            if parsed.path == "/api/hypothesis-promotion":
                self._send_json(hypothesis_promotion_payload(service_factory(), parse_qs(parsed.query)))
                return
            if parsed.path == "/api/validation-packets":
                self._send_json(validation_packets_payload(service_factory(), parse_qs(parsed.query)))
                return
            parts = [part for part in PurePosixPath(parsed.path).parts if part != "/"]
            if len(parts) == 3 and parts[:2] == ["api", "agent-runs"]:
                try:
                    self._send_json(get_agent_run_payload(service_factory(), parts[2]))
                except ValueError as exc:
                    self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
                except LookupError as exc:
                    self._send_error(HTTPStatus.NOT_FOUND, str(exc))
                return
            self._send_error(HTTPStatus.NOT_FOUND, "Not found")

        def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
            parsed = urlparse(self.path)
            parts = [part for part in PurePosixPath(parsed.path).parts if part != "/"]
            if len(parts) == 4 and parts[:2] == ["api", "validation-requests"]:
                queue_item_id = parts[2]
                action = parts[3]
                try:
                    payload = self._read_json_body()
                except (json.JSONDecodeError, ValueError) as exc:
                    self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
                    return
                if action == "approve":
                    try:
                        self._send_json(approve_validation_request_payload(service_factory(), queue_item_id, payload))
                    except ValueError as exc:
                        self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
                    return
                if action == "dispatch":
                    try:
                        self._send_json(dispatch_validation_request_payload(service_factory(), queue_item_id, payload))
                    except RuntimeError as exc:
                        self._send_error(HTTPStatus.CONFLICT, str(exc))
                    except ValueError as exc:
                        self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
                    return
            if parts == ["api", "validation-autopilot", "run"]:
                try:
                    payload = self._read_json_body()
                    self._send_json(run_validation_autopilot_payload(service_factory(), payload))
                except (json.JSONDecodeError, ValueError) as exc:
                    self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            if parts == ["api", "agent-performance", "evaluate"]:
                try:
                    payload = self._read_json_body()
                    self._send_json(run_agent_performance_evaluation_payload(service_factory(), payload))
                except (json.JSONDecodeError, ValueError) as exc:
                    self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
                except RuntimeError as exc:
                    self._send_error(HTTPStatus.CONFLICT, str(exc))
                return
            if parts == ["api", "validation-tool-catalog", "match"]:
                try:
                    payload = self._read_json_body()
                    self._send_json(match_validation_tool_payload(service_factory(), payload))
                except (json.JSONDecodeError, ValueError) as exc:
                    self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            if parts == ["api", "agent-findings", "escalate"]:
                try:
                    payload = self._read_json_body()
                    self._send_json(escalate_agent_findings_payload(service_factory(), payload))
                except (json.JSONDecodeError, ValueError) as exc:
                    self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            if len(parts) == 4 and parts[:2] == ["api", "research-leads"] and parts[3] == "status":
                try:
                    payload = self._read_json_body()
                    self._send_json(update_research_lead_status_payload(service_factory(), parts[2], payload))
                except (json.JSONDecodeError, ValueError) as exc:
                    self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            if len(parts) == 4 and parts[:2] == ["api", "research-leads"] and parts[3] == "followup-loop":
                try:
                    payload = self._read_json_body()
                    self._send_json(run_research_followup_loop_payload(service_factory(), parts[2], payload))
                except RuntimeError as exc:
                    self._send_error(HTTPStatus.CONFLICT, str(exc))
                except (json.JSONDecodeError, ValueError) as exc:
                    self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            if len(parts) == 4 and parts[:2] == ["api", "research-leads"] and parts[3] == "refine-followup":
                try:
                    payload = self._read_json_body()
                    self._send_json(refine_research_followup_payload(service_factory(), parts[2], payload))
                except (json.JSONDecodeError, ValueError) as exc:
                    self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            if len(parts) == 4 and parts[:2] == ["api", "agent-runs"] and parts[3] == "reviews":
                try:
                    payload = self._read_json_body()
                    self._send_json(create_agent_run_review_payload(service_factory(), parts[2], payload))
                except (json.JSONDecodeError, ValueError) as exc:
                    self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
                except LookupError as exc:
                    self._send_error(HTTPStatus.NOT_FOUND, str(exc))
                return
            self._send_error(HTTPStatus.NOT_FOUND, "Not found")

        def log_message(self, format: str, *args: Any) -> None:
            return

        def _send_static(self, filename: str) -> None:
            safe_name = PurePosixPath(filename).name
            if not safe_name or safe_name != filename:
                self._send_error(HTTPStatus.BAD_REQUEST, "Invalid asset path")
                return
            static_root = resources.files(__package__).joinpath("command_center_static")
            asset = static_root.joinpath(safe_name)
            if not asset.is_file():
                self._send_error(HTTPStatus.NOT_FOUND, "Asset not found")
                return
            content = asset.read_bytes()
            content_type = _content_type(safe_name)
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def _send_json(self, payload: Mapping[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
            content = json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def _send_error(self, status: HTTPStatus, message: str) -> None:
            self._send_json({"error": message, "status": int(status)}, status=status)

        def _read_json_body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0") or "0")
            if length <= 0:
                return {}
            body = self.rfile.read(length).decode("utf-8")
            if not body.strip():
                return {}
            value = json.loads(body)
            if not isinstance(value, dict):
                raise ValueError("JSON body must be an object")
            return value

    return CommandCenterHandler


def _str_param(params: Mapping[str, list[str]], key: str) -> str | None:
    values = [value.strip() for value in params.get(key, []) if value.strip()]
    return values[0] if values else None


def _uuid_param(params: Mapping[str, list[str]], key: str) -> UUID | None:
    value = _str_param(params, key)
    return UUID(value) if value else None


def _list_param(params: Mapping[str, list[str]], key: str) -> list[str]:
    values: list[str] = []
    for raw in params.get(key, []):
        values.extend(value.strip() for value in raw.split(",") if value.strip())
    return values


def _payload_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = value.split(",")
    elif isinstance(value, list):
        values = value
    else:
        values = [value]
    return [str(item).strip() for item in values if str(item).strip()]


def _validation_autopilot_request_from_params(
    params: Mapping[str, list[str]],
    *,
    dry_run: bool,
) -> ValidationAutopilotRequest:
    return ValidationAutopilotRequest(
        enabled=_bool_param(params, "enabled", True),
        dry_run=dry_run,
        force=_bool_param(params, "force", False),
        manual_grace_period_hours=_float_param(params, "manual_grace_period_hours", 6.0),
        minimum_queue_age_hours=_float_param(params, "minimum_queue_age_hours", 1.0),
        max_per_run=_int_param(params, "max_per_run", 2),
        hourly_budget_usd=_float_param(params, "hourly_budget_usd", 0.25),
        daily_budget_usd=_float_param(params, "daily_budget_usd", 1.50),
        estimated_cost_per_item_usd=_float_param(params, "estimated_cost_per_item_usd", 0.03),
        allowed_task_types=_list_param(params, "allowed_task_type") or ["expert_review", "target_validation", "omics"],
        allowed_validation_types=_list_param(params, "allowed_validation_type") or ["expert_review", "homology", "omics"],
        source_keys=_list_param(params, "source"),
        model_profile=_str_param(params, "model_profile") or "openrouter_required",
    )


def _bool_param(params: Mapping[str, list[str]], key: str, default: bool) -> bool:
    value = _str_param(params, key)
    if value is None:
        return default
    return value.casefold() in {"1", "true", "yes", "on"}


def _int_param(params: Mapping[str, list[str]], key: str, default: int) -> int:
    value = _str_param(params, key)
    return default if value is None else int(value)


def _float_param(params: Mapping[str, list[str]], key: str, default: float) -> float:
    value = _str_param(params, key)
    return default if value is None else float(value)


def _content_type(filename: str) -> str:
    if filename.endswith(".css"):
        return "text/css; charset=utf-8"
    if filename.endswith(".js"):
        return "application/javascript; charset=utf-8"
    if filename.endswith(".html"):
        return "text/html; charset=utf-8"
    return "application/octet-stream"


def _action_priority(severity: str, fallback: int) -> int:
    return {"blocking": 10, "watch": 40, "info": 80}.get(severity, fallback)


def _status_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        key = str(item.get("status") or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _area_counts(items: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        key = str(item.get("area") or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


if __name__ == "__main__":
    main()
