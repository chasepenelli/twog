"""Local web command center for TWOG operations.

This is intentionally lightweight: a small stdlib HTTP server over the existing
service layer, with static assets bundled beside the package. The goal is a
usable management surface before committing to a heavier frontend stack.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
import html
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
    CommandCenterActivityEventRecord,
    CommandCenterBoardStageRecord,
    CommandCenterRequest,
    HypothesisPromotionReportRequest,
    PublicCandidateLibraryRequest,
    ResearchBriefQueueRequest,
    ResearchFollowupRefinementRequest,
    ResearchFollowupLoopRequest,
    ResearchBriefQualityReportRequest,
    ResearchProgramBoardRequest,
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

BIG_IDEA_STAGES = (
    "new_signal",
    "needs_evidence",
    "committee_ready",
    "validation_ready",
    "queued_running",
    "reviewed",
    "parked",
)

BIG_IDEA_STAGE_LABELS = {
    "new_signal": "New Signal",
    "needs_evidence": "Needs Evidence",
    "committee_ready": "Committee Ready",
    "validation_ready": "Validation Ready",
    "queued_running": "Queued / Running",
    "reviewed": "Reviewed",
    "parked": "Parked",
}


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
        include_evidence_addendum=_bool_param(params, "include_evidence_addendum", True),
        addendum_limit=_int_param(params, "addendum_limit", 25),
        queue_if_ready=_bool_param(params, "queue_if_ready", False),
        dry_run=_bool_param(params, "dry_run", True),
        max_tasks=_int_param(params, "max_tasks", 8),
        priority=_int_param(params, "priority", 40),
        limit=_int_param(params, "limit", 10),
    )
    return service.build_validation_packets(request).model_dump(mode="json")


def public_candidates_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Return public-proof candidate records for command center/public-page previews."""

    params = params or {}
    request = PublicCandidateLibraryRequest(
        candidate_id=_str_param(params, "candidate_id"),
        therapy_idea_id=_uuid_param(params, "therapy_idea_id"),
        public_status=_str_param(params, "status"),
        visibility=_str_param(params, "visibility"),
        candidate_kind=_str_param(params, "kind"),
        query=_str_param(params, "query"),
        limit=max(1, min(_int_param(params, "limit", 50), 500)),
    )
    result = service.list_public_candidates(request).model_dump(mode="json")
    return result


def public_candidate_payload(service: HSAResearchService, candidate_id: str) -> dict[str, Any]:
    """Return one candidate plus its latest snapshot and decision log."""

    candidate = service.get_public_candidate(candidate_id)
    if candidate is None:
        raise LookupError(f"public_candidate_not_found:{candidate_id}")
    snapshots = service.repository.list_public_candidate_snapshots(candidate_id=candidate_id, limit=1)
    events = service.repository.list_public_candidate_decision_events(candidate_id=candidate_id, limit=100)
    return {
        "candidate": candidate.model_dump(mode="json"),
        "latest_snapshot": snapshots[0].model_dump(mode="json") if snapshots else None,
        "decision_events": [event.model_dump(mode="json") for event in events],
    }


def _public_candidate_html(payload: Mapping[str, Any]) -> str:
    candidate = payload.get("candidate") if isinstance(payload.get("candidate"), dict) else {}
    snapshot = payload.get("latest_snapshot") if isinstance(payload.get("latest_snapshot"), dict) else {}
    body = snapshot.get("payload") if isinstance(snapshot.get("payload"), dict) else {}
    identity = body.get("identity") if isinstance(body.get("identity"), dict) else {}
    rationale = body.get("rationale") if isinstance(body.get("rationale"), dict) else {}
    biology = body.get("biology") if isinstance(body.get("biology"), dict) else {}
    evidence = body.get("evidence") if isinstance(body.get("evidence"), dict) else {}
    compute = body.get("computational_evidence") if isinstance(body.get("computational_evidence"), list) else []
    events = payload.get("decision_events") if isinstance(payload.get("decision_events"), list) else []
    title = str(candidate.get("title") or identity.get("title") or "TWOG Candidate")
    content_hash = str(candidate.get("content_hash") or snapshot.get("content_hash") or "")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)} | TWOG Candidate</title>
  <style>
    body {{ margin: 0; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #090b10; color: #f5f7fb; }}
    main {{ max-width: 1120px; margin: 0 auto; padding: 40px 20px 56px; }}
    .top {{ display: flex; gap: 16px; justify-content: space-between; align-items: flex-start; border-bottom: 1px solid #252b36; padding-bottom: 24px; }}
    .eyebrow {{ color: #9fb0c7; font-size: 13px; text-transform: uppercase; letter-spacing: .08em; }}
    h1 {{ margin: 10px 0; font-size: 42px; line-height: 1.05; letter-spacing: 0; }}
    .badge {{ display: inline-flex; align-items: center; border: 1px solid #3d4b60; color: #cde0ff; padding: 6px 10px; border-radius: 6px; font-size: 13px; background: #111722; }}
    .grid {{ display: grid; grid-template-columns: 1.2fr .8fr; gap: 20px; margin-top: 24px; }}
    section {{ background: #111722; border: 1px solid #252b36; border-radius: 8px; padding: 18px; }}
    h2 {{ font-size: 17px; margin: 0 0 12px; }}
    p {{ color: #d9e2f0; line-height: 1.55; }}
    .chips {{ display: flex; flex-wrap: wrap; gap: 8px; }}
    .chip {{ border: 1px solid #334155; border-radius: 999px; padding: 6px 9px; color: #d9e2f0; font-size: 13px; background: #0b1018; }}
    .muted {{ color: #9fb0c7; }}
    .list {{ display: grid; gap: 10px; }}
    .row {{ border-top: 1px solid #252b36; padding-top: 10px; }}
    code {{ color: #b7f7d4; overflow-wrap: anywhere; }}
    pre {{ background: #070a0f; border: 1px solid #252b36; border-radius: 6px; padding: 12px; overflow: auto; color: #cdd7e5; }}
    @media (max-width: 860px) {{ .grid, .top {{ grid-template-columns: 1fr; display: block; }} h1 {{ font-size: 32px; }} }}
  </style>
</head>
<body>
  <main>
    <div class="top">
      <div>
        <div class="eyebrow">{html.escape(str(candidate.get("display_id") or identity.get("display_id") or candidate.get("candidate_id") or ""))}</div>
        <h1>{html.escape(title)}</h1>
        <span class="badge">{html.escape(str(candidate.get("public_status") or identity.get("public_status") or ""))}</span>
        <span class="badge">{html.escape(str(candidate.get("candidate_kind") or identity.get("candidate_kind") or ""))}</span>
      </div>
      <div class="muted">Hash<br><code>{html.escape(content_hash[:16])}</code></div>
    </div>
    <div class="grid">
      <section>
        <h2>Rationale</h2>
        <p>{html.escape(str(rationale.get("hypothesis") or ""))}</p>
        <p>{html.escape(str(rationale.get("rationale_md") or ""))}</p>
      </section>
      <section>
        <h2>Targets And Therapies</h2>
        <div class="chips">{_html_chips([*_html_list_values(biology.get("targets")), *_html_list_values(biology.get("candidate_therapies"))])}</div>
      </section>
      <section>
        <h2>Evidence</h2>
        <div class="chips">{_html_chips(evidence.get("evidence_refs", []))}</div>
        <p class="muted">Strength: {html.escape(str(evidence.get("evidence_strength") or "unknown"))}</p>
      </section>
      <section>
        <h2>Risks And Next Experiments</h2>
        <div class="chips">{_html_chips([*_html_list_values(evidence.get("risks")), *_html_list_values(evidence.get("next_experiments"))])}</div>
      </section>
      <section>
        <h2>Computational Evidence</h2>
        <div class="list">{_html_compute_rows(compute)}</div>
      </section>
      <section>
        <h2>Decision Log</h2>
        <div class="list">{_html_decision_rows(events)}</div>
      </section>
    </div>
    <section style="margin-top:20px">
      <h2>Snapshot JSON</h2>
      <pre>{html.escape(json.dumps(snapshot, indent=2, sort_keys=True, default=str))}</pre>
    </section>
  </main>
</body>
</html>"""


def _html_chips(values: Any) -> str:
    if not isinstance(values, list):
        return ""
    return "".join(f'<span class="chip">{html.escape(str(value))}</span>' for value in values if str(value).strip())


def _html_list_values(values: Any) -> list[Any]:
    return values if isinstance(values, list) else []


def _html_compute_rows(values: Any) -> str:
    if not values:
        return '<p class="muted">No compute studies attached yet.</p>'
    rows = []
    for item in values:
        if not isinstance(item, dict):
            continue
        rows.append(
            '<div class="row">'
            f'<strong>{html.escape(str(item.get("title") or "Compute study"))}</strong><br>'
            f'<span class="muted">{html.escape(str(item.get("status") or ""))} · '
            f'{html.escape(str(item.get("validation_type") or item.get("runner_kind") or ""))}</span>'
            "</div>"
        )
    return "".join(rows)


def _html_decision_rows(values: Any) -> str:
    if not values:
        return '<p class="muted">No decision events recorded yet.</p>'
    rows = []
    for item in values:
        if not isinstance(item, dict):
            continue
        rows.append(
            '<div class="row">'
            f'<strong>{html.escape(str(item.get("action") or "event"))}</strong><br>'
            f'<span class="muted">{html.escape(str(item.get("occurred_at") or ""))}</span>'
            f'<p>{html.escape(str(item.get("rationale_md") or ""))}</p>'
            "</div>"
        )
    return "".join(rows)


def big_ideas_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Return the Big Ideas cockpit board payload."""

    params = params or {}
    limit = max(1, min(_int_param(params, "limit", 100), 300))
    query = (_str_param(params, "query") or "").casefold()
    stage_filter = _str_param(params, "stage")
    items = _collect_big_idea_items(service, limit=limit)
    if query:
        items = [item for item in items if query in _big_idea_search_text(item)]
    if stage_filter:
        items = [item for item in items if item.get("board_stage") == stage_filter]
    stage_counts: dict[str, int] = {stage: 0 for stage in BIG_IDEA_STAGES}
    for item in items:
        stage = str(item.get("board_stage") or "new_signal")
        stage_counts[stage] = stage_counts.get(stage, 0) + 1
    return {
        "stages": [{"key": key, "label": BIG_IDEA_STAGE_LABELS[key]} for key in BIG_IDEA_STAGES],
        "total": len(items),
        "stage_counts": stage_counts,
        "items": items,
        "updated_at": datetime.now(UTC).isoformat(),
    }


def activity_events_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Return explicit and derived command-center activity events."""

    params = params or {}
    limit = max(1, min(_int_param(params, "limit", 100), 300))
    entity_type = _str_param(params, "entity_type")
    entity_id = _str_param(params, "entity_id")
    source = _str_param(params, "source")
    explicit = [
        _command_center_activity_event(record.model_dump(mode="json"))
        for record in service.repository.list_command_center_activity_events(
            entity_type=entity_type,
            entity_id=entity_id,
            source=source,
            limit=limit,
        )
    ]
    derived = _derived_activity_events(service, limit=limit)
    if entity_type:
        derived = [item for item in derived if item.get("entity_type") == entity_type]
    if entity_id:
        derived = [item for item in derived if item.get("entity_id") == entity_id]
    if source:
        derived = [item for item in derived if item.get("source") == source]
    events = explicit + derived
    events.sort(key=lambda item: str(item.get("occurred_at") or ""), reverse=True)
    events = events[:limit]
    return {"total": len(events), "items": events}


def compute_jobs_payload(
    service: HSAResearchService,
    params: Mapping[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Return compute jobs for the redesigned command center."""

    params = params or {}
    items = service.repository.list_compute_jobs(
        status=_str_param(params, "status"),
        runner_kind=_str_param(params, "runner_kind"),
        limit=max(1, min(_int_param(params, "limit", 50), 200)),
    )
    status_counts: dict[str, int] = {}
    for job in service.repository.list_compute_jobs(limit=None):
        status_counts[str(job.status)] = status_counts.get(str(job.status), 0) + 1
    return {
        "total": sum(status_counts.values()),
        "visible": len(items),
        "status_counts": dict(sorted(status_counts.items())),
        "items": [item.model_dump(mode="json") for item in items],
    }


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


def update_big_idea_stage_payload(
    service: HSAResearchService,
    entity_type: str,
    entity_id: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist a command-center board stage for a Big Ideas card."""

    payload = payload or {}
    stage = str(payload.get("stage") or payload.get("board_stage") or "").strip()
    if stage not in BIG_IDEA_STAGES:
        raise ValueError("Invalid command-center board stage")
    entity_type = entity_type.strip()
    entity_id = entity_id.strip()
    if entity_type not in _command_center_entity_types():
        raise ValueError("Invalid command-center entity type")
    actor = str(payload.get("operator") or payload.get("actor") or "command_center_operator").strip()
    previous = service.repository.get_command_center_board_stage(entity_type, entity_id)
    previous_stage = previous.board_stage if previous else _infer_big_idea_stage(_find_big_idea_item(service, entity_type, entity_id))
    record = service.repository.upsert_command_center_board_stage(
        CommandCenterBoardStageRecord(
            entity_type=entity_type,  # type: ignore[arg-type]
            entity_id=entity_id,
            board_stage=stage,  # type: ignore[arg-type]
            actor=actor,
            note=str(payload.get("note") or "").strip() or None,
            metadata={"command_center": {"operator": actor}},
        )
    )
    event = _append_activity_event(
        service,
        event_type="idea.stage_changed",
        entity_type=entity_type,
        entity_id=entity_id,
        title=f"Moved to {BIG_IDEA_STAGE_LABELS[stage]}",
        summary=str(payload.get("note") or "").strip(),
        actor=actor,
        source="ui",
        severity="success",
        from_stage=previous_stage,
        to_stage=stage,
        payload={"stage": stage, "previous_stage": previous_stage},
    )
    item = _find_big_idea_item(service, entity_type, entity_id)
    if item is not None:
        item["board_stage"] = stage
        item["board_stage_label"] = BIG_IDEA_STAGE_LABELS[stage]
    return {
        "item": item,
        "stage": record.model_dump(mode="json"),
        "event": event.model_dump(mode="json"),
    }


def queue_big_idea_work_payload(
    service: HSAResearchService,
    entity_type: str,
    entity_id: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Create explicit follow-up work from a Big Ideas card."""

    payload = payload or {}
    action = str(payload.get("action") or "").strip()
    if action not in {"evidence_brief", "validation_packet", "park", "promote", "demote"}:
        raise ValueError("Invalid Big Ideas queue action")
    item = _find_big_idea_item(service, entity_type, entity_id)
    if item is None:
        raise LookupError("Big Ideas card not found")
    actor = str(payload.get("operator") or "command_center_operator").strip()
    if action in {"park", "promote", "demote"}:
        stage = {"park": "parked", "promote": "validation_ready", "demote": "needs_evidence"}[action]
        return update_big_idea_stage_payload(
            service,
            entity_type,
            entity_id,
            {"stage": stage, "operator": actor, "note": f"Action: {action}"},
        )
    if action == "evidence_brief":
        queue_item = service.queue_research_brief(
            ResearchBriefQueueRequest(
                topic=_big_idea_evidence_brief_topic(item),
                disease_scope=str(item.get("disease_scope") or "canine hemangiosarcoma and human angiosarcoma"),
                source_key=item.get("source_key") or None,
                priority=35,
                brief_style="technical",
                model_profile="research_brief",
                review_mode="openrouter_required",
                metadata={
                    "command_center": {
                        "queued_from": "big_ideas_cockpit",
                        "operator": actor,
                    },
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "card_title": item.get("title"),
                    "evidence_refs": item.get("evidence_refs") or [],
                    "missing_evidence": item.get("missing_evidence") or [],
                },
            )
        )
        event = _append_activity_event(
            service,
            event_type="idea.evidence_brief_queued",
            entity_type=entity_type,
            entity_id=entity_id,
            title="Queued evidence brief",
            summary=str(queue_item.topic),
            actor=actor,
            source="ui",
            severity="success",
            related={"queue_item_id": str(queue_item.queue_item_id)},
            payload={"queue_item": queue_item.model_dump(mode="json")},
        )
        return {"action": action, "queue_item": queue_item.model_dump(mode="json"), "event": event.model_dump(mode="json")}
    result = service.build_validation_packets(
        ValidationPacketRequest(
            candidate_id=item.get("candidate_id") if item.get("candidate_id") else None,
            therapy_idea_id=_uuid_from_value(item.get("therapy_idea_id")),
            topic_query=item.get("title") if not item.get("candidate_id") and not item.get("therapy_idea_id") else None,
            queue_if_ready=True,
            dry_run=False,
            priority=35,
            limit=1,
            metadata={
                "command_center": {
                    "queued_from": "big_ideas_cockpit",
                    "operator": actor,
                },
                "entity_type": entity_type,
                "entity_id": entity_id,
            },
        )
    )
    event = _append_activity_event(
        service,
        event_type="idea.validation_packet_queued",
        entity_type=entity_type,
        entity_id=entity_id,
        title="Queued validation packet work",
        summary=f"{result.queued_count} queued, {result.existing_queue_count} existing",
        actor=actor,
        source="ui",
        severity="success" if result.queued_count or result.existing_queue_count else "watch",
        payload=result.model_dump(mode="json"),
    )
    return {"action": action, "result": result.model_dump(mode="json"), "event": event.model_dump(mode="json")}


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


def _collect_big_idea_items(service: HSAResearchService, *, limit: int = 100) -> list[dict[str, Any]]:
    stage_overrides = {
        f"{record.entity_type}:{record.entity_id}": record
        for record in service.repository.list_command_center_board_stages(limit=None)
    }
    items: list[dict[str, Any]] = []
    programs = service.list_research_programs(ResearchProgramBoardRequest(limit=min(limit, 100))).programs
    for program in programs:
        item = {
            "entity_type": "research_program",
            "entity_id": str(program.program_id),
            "program_id": str(program.program_id),
            "type_label": "Research Program",
            "title": program.title,
            "summary": program.thesis,
            "hypothesis": program.thesis,
            "disease_scope": program.disease_scope,
            "status": program.status,
            "readiness": program.gate_decision,
            "score": program.confidence_score,
            "score_label": "confidence",
            "candidate_therapies": program.therapy_families,
            "targets": [],
            "biomarkers": [],
            "evidence_refs": program.evidence_refs,
            "missing_evidence": [task.objective for task in program.evidence_tasks if task.status in {"proposed", "queued"}][:8],
            "risks": program.confidence_decrease_criteria,
            "blockers": program.stop_criteria if program.gate_decision == "archive" else [],
            "next_experiments": program.downstream_therapy_opportunities,
            "next_action": _program_next_action(program.status, program.gate_decision),
            "created_at": program.created_at.isoformat(),
            "updated_at": program.updated_at.isoformat(),
            "payload": program.model_dump(mode="json"),
        }
        items.append(_with_board_stage(service, item, stage_overrides))

    therapy_result = service.list_therapy_ideas(TherapyIdeaLibraryRequest(limit=min(limit, 200)))
    for record in therapy_result.ideas:
        payload = _command_center_therapy_idea_record(record.model_dump(mode="json"))
        item = {
            **payload,
            "entity_type": "therapy_idea",
            "entity_id": payload["idea_id"],
            "therapy_idea_id": payload["idea_id"],
            "type_label": "Therapy Idea",
            "summary": payload.get("hypothesis") or payload.get("rationale") or "",
            "score": payload.get("priority_score"),
            "score_label": "priority",
            "readiness": payload.get("promotion_state") or payload.get("status"),
            "blockers": payload.get("risks") or [],
            "missing_evidence": [],
            "next_action": _therapy_idea_next_action(payload.get("status"), payload.get("promotion_state")),
            "payload": record.model_dump(mode="json"),
        }
        items.append(_with_board_stage(service, item, stage_overrides))

    promotion = service.build_hypothesis_promotion_report(
        HypothesisPromotionReportRequest(
            include_blocked=True,
            include_ready_for_committee=True,
            include_ready_for_validation=True,
            limit=min(limit, 100),
        )
    )
    for candidate in promotion.candidates:
        payload = candidate.model_dump(mode="json")
        item = {
            "entity_type": "promotion_candidate",
            "entity_id": candidate.candidate_id,
            "candidate_id": candidate.candidate_id,
            "therapy_idea_id": str(candidate.therapy_idea_id) if candidate.therapy_idea_id else None,
            "type_label": "Promotion Candidate",
            "title": candidate.title,
            "summary": candidate.hypothesis,
            "hypothesis": candidate.hypothesis,
            "disease_scope": (candidate.metadata or {}).get(
                "disease_scope", "canine hemangiosarcoma and human angiosarcoma"
            ),
            "status": candidate.promotion_state,
            "readiness": candidate.promotion_state,
            "score": candidate.score,
            "score_label": "promotion",
            "candidate_therapies": candidate.candidate_therapies,
            "targets": candidate.targets,
            "biomarkers": candidate.biomarkers,
            "evidence_refs": candidate.evidence_refs,
            "missing_evidence": candidate.blockers,
            "risks": candidate.risks,
            "blockers": candidate.blockers,
            "next_experiments": candidate.next_experiments,
            "next_action": candidate.recommended_next_action,
            "created_at": payload.get("created_at"),
            "updated_at": payload.get("updated_at"),
            "payload": payload,
        }
        items.append(_with_board_stage(service, item, stage_overrides))

    packets = service.build_validation_packets(ValidationPacketRequest(limit=min(limit, 100), dry_run=True)).packets
    for packet in packets:
        payload = packet.model_dump(mode="json")
        item = {
            "entity_type": "validation_packet",
            "entity_id": packet.packet_id,
            "packet_id": packet.packet_id,
            "candidate_id": packet.candidate_id,
            "therapy_idea_id": str(packet.therapy_idea_id) if packet.therapy_idea_id else None,
            "type_label": "Validation Packet",
            "title": packet.title,
            "summary": packet.hypothesis,
            "hypothesis": packet.hypothesis,
            "disease_scope": packet.disease_scope,
            "status": packet.status,
            "readiness": packet.readiness,
            "score": packet.score,
            "score_label": "packet",
            "candidate_therapies": packet.candidate_therapies,
            "targets": packet.targets,
            "biomarkers": packet.biomarkers,
            "evidence_refs": packet.evidence_refs,
            "direct_evidence_refs": packet.direct_evidence_refs,
            "analog_evidence_refs": packet.analog_evidence_refs,
            "missing_evidence": packet.missing_evidence,
            "risks": packet.risk_annotations or packet.safety_risks,
            "blockers": packet.dispatch_blockers or packet.protocol_blockers,
            "next_experiments": packet.next_experiments,
            "next_action": _validation_packet_next_action(packet.status, packet.readiness),
            "queue_items": [item.model_dump(mode="json") for item in packet.queue_items],
            "matched_tools": [match.model_dump(mode="json") for match in packet.matched_tools],
            "created_at": packet.created_at.isoformat(),
            "updated_at": packet.updated_at.isoformat(),
            "payload": payload,
        }
        items.append(_with_board_stage(service, item, stage_overrides))

    deduped: dict[str, dict[str, Any]] = {}
    for item in items:
        key = f"{item['entity_type']}:{item['entity_id']}"
        deduped[key] = item
    ordered = list(deduped.values())
    ordered.sort(key=lambda item: (float(item.get("score") or 0), str(item.get("updated_at") or "")), reverse=True)
    return ordered[:limit]


def _with_board_stage(
    service: HSAResearchService,
    item: dict[str, Any],
    stage_overrides: Mapping[str, CommandCenterBoardStageRecord],
) -> dict[str, Any]:
    key = f"{item['entity_type']}:{item['entity_id']}"
    override = stage_overrides.get(key)
    stage = override.board_stage if override else _infer_big_idea_stage(item)
    item["board_stage"] = stage
    item["board_stage_label"] = BIG_IDEA_STAGE_LABELS.get(stage, stage)
    if override:
        item["board_stage_actor"] = override.actor
        item["board_stage_updated_at"] = override.updated_at.isoformat()
    return item


def _infer_big_idea_stage(item: Mapping[str, Any] | None) -> str:
    if not item:
        return "new_signal"
    status = str(item.get("status") or "").casefold()
    readiness = str(item.get("readiness") or item.get("promotion_state") or "").casefold()
    if status in {"archived", "rejected"} or readiness in {"archive", "blocked"}:
        return "parked"
    if status in {"queued_for_validation", "queued", "running", "dispatched"} or "queued" in readiness:
        return "queued_running"
    if readiness in {"ready_for_validation_plan", "ready_for_validation_queue", "ready_for_validation_strategy"}:
        return "validation_ready"
    if readiness in {"ready_for_committee", "ready_for_therapy_ideas"} or status == "ready_for_promotion":
        return "committee_ready"
    if readiness in {"needs_more_evidence", "needs_citation_repair", "needs_one_more_pass"}:
        return "needs_evidence"
    return "new_signal"


def _find_big_idea_item(service: HSAResearchService, entity_type: str, entity_id: str) -> dict[str, Any] | None:
    for item in _collect_big_idea_items(service, limit=300):
        if item.get("entity_type") == entity_type and str(item.get("entity_id")) == entity_id:
            return item
    return None


def _big_idea_search_text(item: Mapping[str, Any]) -> str:
    fields = [
        item.get("title"),
        item.get("summary"),
        item.get("hypothesis"),
        item.get("status"),
        item.get("readiness"),
        " ".join(_string_list(item.get("candidate_therapies"))),
        " ".join(_string_list(item.get("targets"))),
        " ".join(_string_list(item.get("biomarkers"))),
        " ".join(_string_list(item.get("evidence_refs"))),
    ]
    return " ".join(str(field or "") for field in fields).casefold()


def _program_next_action(status: str, gate_decision: str) -> str:
    if gate_decision == "ready_for_therapy_ideas":
        return "Run or review therapy committee output."
    if gate_decision == "ready_for_validation_strategy":
        return "Build validation strategy from the program packet."
    if status == "needs_one_more_pass":
        return "Run the capped evidence loop, then force a gate decision."
    if status == "archived":
        return "Parked; reopen only with new evidence."
    return "Review thesis and decisive questions."


def _therapy_idea_next_action(status: Any, promotion_state: Any) -> str:
    if status == "queued_for_validation" or promotion_state == "queued_for_validation":
        return "Monitor validation queue and agent outputs."
    if promotion_state == "ready_for_validation_plan":
        return "Queue a validation packet."
    if promotion_state == "ready_for_committee":
        return "Route through committee review."
    if promotion_state in {"needs_more_evidence", "needs_citation_repair"}:
        return "Queue focused evidence work."
    if status in {"archived", "rejected"}:
        return "Parked; no action."
    return "Promote, demote, or queue evidence work."


def _validation_packet_next_action(status: str, readiness: str) -> str:
    if status == "queued_for_validation" or readiness == "queued_for_validation":
        return "Monitor queued validation work."
    if readiness in {"ready_for_validation_plan", "ready_for_validation_queue"}:
        return "Queue validation packet work."
    if readiness in {"needs_more_evidence", "needs_citation_repair"}:
        return "Queue evidence repair or follow-up brief."
    if status == "blocked":
        return "Inspect blockers before dispatch."
    return "Review packet and matched validation tools."


def _big_idea_evidence_brief_topic(item: Mapping[str, Any]) -> str:
    missing = _string_list(item.get("missing_evidence"))[:3]
    title = str(item.get("title") or "untitled idea").strip()
    if missing:
        return f"Evidence follow-up for {title}: {'; '.join(missing)}"
    return f"Evidence follow-up for {title}: strengthen provenance, contradictions, and validation readiness."


def _derived_activity_events(service: HSAResearchService, *, limit: int) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for run in service.repository.list_agent_runs(limit=min(limit, 50)):
        events.append(
            _derived_event(
                event_id=f"agent_run:{run.agent_run_id}:{run.status}",
                occurred_at=(run.completed_at or run.started_at).isoformat(),
                source="agent",
                event_type=f"agent.{run.status}",
                entity_type="agent_run",
                entity_id=str(run.agent_run_id),
                title=f"{run.agent_name} {run.status}",
                summary=_safe_summary_text(run.summary) or _safe_summary_text(run.output_payload),
                severity="blocked" if str(run.status) == "failed" else "success" if str(run.status) == "completed" else "info",
            )
        )
    for item in service.list_validation_request_queue_items(limit=min(limit, 50)):
        events.append(
            _derived_event(
                event_id=f"validation_request:{item.queue_item_id}:{item.status}",
                occurred_at=item.updated_at.isoformat(),
                source="system",
                event_type=f"validation_request.{item.status}",
                entity_type="validation_request",
                entity_id=str(item.queue_item_id),
                title=f"Validation {item.status}",
                summary=item.title,
                severity="blocked" if item.status in {"blocked", "failed"} else "success" if item.status == "completed" else "info",
            )
        )
    for item in service.list_research_brief_queue_items(limit=min(limit, 50)):
        events.append(
            _derived_event(
                event_id=f"research_brief:{item.queue_item_id}:{item.status}",
                occurred_at=item.updated_at.isoformat(),
                source="system",
                event_type=f"research_brief.{item.status}",
                entity_type="research_brief",
                entity_id=str(item.queue_item_id),
                title=f"Brief {item.status}",
                summary=item.topic,
                severity="blocked" if item.status == "failed" else "success" if item.status == "completed" else "info",
            )
        )
    for job in service.repository.list_compute_jobs(limit=min(limit, 50)):
        events.append(
            _derived_event(
                event_id=f"compute_job:{job.compute_job_id}:{job.status}",
                occurred_at=job.updated_at.isoformat(),
                source="system",
                event_type=f"compute.{job.status}",
                entity_type="compute_job",
                entity_id=str(job.compute_job_id),
                title=f"Compute {job.status}",
                summary=job.title,
                severity="blocked" if job.status in {"failed", "blocked"} else "success" if job.status == "completed" else "info",
            )
        )
    return events


def _derived_event(
    *,
    event_id: str,
    occurred_at: str,
    source: str,
    event_type: str,
    entity_type: str,
    entity_id: str,
    title: str,
    summary: str,
    severity: str,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "occurred_at": occurred_at,
        "actor": source,
        "source": source,
        "event_type": event_type,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "title": title,
        "summary": summary,
        "severity": severity,
        "related": {},
        "payload": {},
        "derived": True,
    }


def _append_activity_event(
    service: HSAResearchService,
    *,
    event_type: str,
    entity_type: str,
    entity_id: str,
    title: str,
    summary: str = "",
    actor: str = "command_center_operator",
    source: str = "ui",
    severity: str = "info",
    from_stage: str | None = None,
    to_stage: str | None = None,
    related: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> CommandCenterActivityEventRecord:
    return service.repository.append_command_center_activity_event(
        CommandCenterActivityEventRecord(
            actor=actor,
            source=source,  # type: ignore[arg-type]
            event_type=event_type,
            entity_type=entity_type,  # type: ignore[arg-type]
            entity_id=entity_id,
            title=title,
            summary=summary,
            from_stage=from_stage,  # type: ignore[arg-type]
            to_stage=to_stage,  # type: ignore[arg-type]
            severity=severity,  # type: ignore[arg-type]
            related=related or {},
            payload=payload or {},
        )
    )


def _command_center_activity_event(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "event_id": str(record.get("event_id")),
        "occurred_at": record.get("occurred_at"),
        "actor": record.get("actor"),
        "source": record.get("source"),
        "event_type": record.get("event_type"),
        "entity_type": record.get("entity_type"),
        "entity_id": record.get("entity_id"),
        "title": record.get("title"),
        "summary": record.get("summary"),
        "from_stage": record.get("from_stage"),
        "to_stage": record.get("to_stage"),
        "severity": record.get("severity"),
        "related": record.get("related") or {},
        "payload": record.get("payload") or {},
        "derived": False,
    }


def _command_center_entity_types() -> set[str]:
    return {
        "research_program",
        "therapy_idea",
        "promotion_candidate",
        "validation_packet",
        "research_lead",
        "research_brief",
        "agent_run",
        "validation_request",
        "compute_job",
    }


def _uuid_from_value(value: Any) -> UUID | None:
    if not value:
        return None
    try:
        return UUID(str(value))
    except ValueError:
        return None


def _safe_summary_text(payload: Any) -> str:
    if isinstance(payload, str):
        return payload[:240]
    if isinstance(payload, Mapping):
        for key in ("summary", "topic", "title", "decision_summary", "reason"):
            value = payload.get(key)
            if value:
                return str(value)[:240]
    return ""


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
            if parsed.path in {"/app.js", "/sortable.min.js", "/styles.css"}:
                self._send_static(parsed.path.removeprefix("/"))
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
            if parsed.path == "/api/big-ideas":
                self._send_json(big_ideas_payload(service_factory(), parse_qs(parsed.query)))
                return
            if parsed.path == "/api/activity-events":
                self._send_json(activity_events_payload(service_factory(), parse_qs(parsed.query)))
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
            if parsed.path == "/api/public-candidates":
                self._send_json(public_candidates_payload(service_factory(), parse_qs(parsed.query)))
                return
            if parsed.path == "/api/compute-jobs":
                self._send_json(compute_jobs_payload(service_factory(), parse_qs(parsed.query)))
                return
            parts = [part for part in PurePosixPath(parsed.path).parts if part != "/"]
            if len(parts) == 3 and parts[:2] == ["api", "public-candidates"]:
                try:
                    self._send_json(public_candidate_payload(service_factory(), parts[2]))
                except LookupError as exc:
                    self._send_error(HTTPStatus.NOT_FOUND, str(exc))
                return
            if len(parts) == 3 and parts[:2] == ["public", "candidates"]:
                try:
                    self._send_html(_public_candidate_html(public_candidate_payload(service_factory(), parts[2])))
                except LookupError as exc:
                    self._send_error(HTTPStatus.NOT_FOUND, str(exc))
                return
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
            if len(parts) == 5 and parts[:2] == ["api", "big-ideas"] and parts[4] == "stage":
                try:
                    payload = self._read_json_body()
                    self._send_json(update_big_idea_stage_payload(service_factory(), parts[2], parts[3], payload))
                except (json.JSONDecodeError, ValueError) as exc:
                    self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
                return
            if len(parts) == 5 and parts[:2] == ["api", "big-ideas"] and parts[4] == "queue-work":
                try:
                    payload = self._read_json_body()
                    self._send_json(queue_big_idea_work_payload(service_factory(), parts[2], parts[3], payload))
                except LookupError as exc:
                    self._send_error(HTTPStatus.NOT_FOUND, str(exc))
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
            try:
                self.wfile.write(content)
            except BrokenPipeError:
                return

        def _send_json(self, payload: Mapping[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
            content = json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            try:
                self.wfile.write(content)
            except BrokenPipeError:
                return

        def _send_html(self, html: str, status: HTTPStatus = HTTPStatus.OK) -> None:
            content = html.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            try:
                self.wfile.write(content)
            except BrokenPipeError:
                return

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
