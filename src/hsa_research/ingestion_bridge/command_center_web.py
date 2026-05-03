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

from .contracts import CommandCenterRequest, ValidationAutopilotRequest
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
            if len(parts) == 4 and parts[:2] == ["api", "research-leads"] and parts[3] == "status":
                try:
                    payload = self._read_json_body()
                    self._send_json(update_research_lead_status_payload(service_factory(), parts[2], payload))
                except (json.JSONDecodeError, ValueError) as exc:
                    self._send_error(HTTPStatus.BAD_REQUEST, str(exc))
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
