"""Read-only reporting for public candidate contribution intake."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, datetime
import os
from typing import Any
from uuid import UUID

import psycopg2
from psycopg2.extras import RealDictCursor


INTAKE_STATUSES = (
    "queued_for_intake",
    "triage_in_progress",
    "needs_more_information",
    "rejected",
    "accepted_for_evidence_review",
    "accepted_for_validation_queue",
    "accepted_for_compute_review",
    "archived",
)
PENDING_INTAKE_STATUSES = (
    "queued_for_intake",
    "triage_in_progress",
    "needs_more_information",
)
REQUESTED_SYSTEM_ACTIONS = (
    "evidence_review",
    "citation_repair",
    "validation_packet",
    "omics_readout",
    "docking_or_md_review",
    "no_action",
)


def candidate_contribution_database_url(environ: Mapping[str, str] | None = None) -> str | None:
    """Resolve the public-site intake database URL from TWOG/Vercel/Dagster env names."""

    values = environ or os.environ
    return (
        values.get("NEON_DATABASE_URL")
        or values.get("DATABASE_URL")
        or values.get("POSTGRES_URL")
        or values.get("HSA_DATABASE_URL")
    )


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return value


def _normalize_statuses(statuses: Sequence[str] | None) -> list[str]:
    if not statuses:
        return list(PENDING_INTAKE_STATUSES)
    normalized = []
    for status in statuses:
        value = str(status).strip()
        if value and value in INTAKE_STATUSES and value not in normalized:
            normalized.append(value)
    return normalized or list(PENDING_INTAKE_STATUSES)


def _bounded_limit(limit: int | None) -> int:
    if limit is None:
        return 50
    return max(1, min(int(limit), 500))


def _recommended_route(requested_action: str, status: str) -> tuple[str, str]:
    if status not in PENDING_INTAKE_STATUSES:
        return ("already_triaged", "This contribution is not in a pending intake state.")

    if requested_action == "evidence_review":
        return ("accepted_for_evidence_review", "Queue for citation/provenance review before synthesis.")
    if requested_action == "citation_repair":
        return ("accepted_for_evidence_review", "Route to citation repair and duplicate/source validation.")
    if requested_action == "validation_packet":
        return ("accepted_for_validation_queue", "Route to validation planning after evidence sufficiency check.")
    if requested_action == "omics_readout":
        return ("accepted_for_validation_queue", "Route to omics evidence packet review before validation.")
    if requested_action == "docking_or_md_review":
        return ("accepted_for_compute_review", "Route to compute review; keep expert gate before live compute.")
    if requested_action == "no_action":
        return ("archive_or_note", "No system action requested; keep as intake note unless operator promotes it.")
    return ("needs_human_review", "Requested action is missing or not recognized.")


def _compact_contributor(contributor: Mapping[str, Any] | None) -> dict[str, Any]:
    source = contributor or {}
    return {
        "name": source.get("name"),
        "affiliation": source.get("affiliation"),
        "contact": source.get("contact"),
    }


def _compact_row(row: Mapping[str, Any], include_packet: bool) -> dict[str, Any]:
    requested_action = str(row.get("requested_system_action") or "")
    status = str(row.get("status") or "")
    recommended_route, route_reason = _recommended_route(requested_action, status)
    packet = row.get("packet") if include_packet else None
    compact = {
        "contribution_id": str(row.get("contribution_id") or ""),
        "candidate_id": row.get("candidate_id"),
        "display_id": row.get("display_id"),
        "snapshot_content_hash": row.get("snapshot_content_hash"),
        "source_payload_url": row.get("source_payload_url"),
        "status": status,
        "contribution_type": row.get("contribution_type"),
        "relation_to_current_record": row.get("relation_to_current_record"),
        "requested_system_action": requested_action,
        "recommended_route": recommended_route,
        "route_reason": route_reason,
        "contributor": _compact_contributor(row.get("contributor") if isinstance(row.get("contributor"), dict) else {}),
        "evidence_count": len(row.get("evidence") or []),
        "artifact_count": len(row.get("artifacts") or []),
        "review_notes": row.get("review_notes"),
        "promoted_queue_id": row.get("promoted_queue_id"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "reviewed_at": row.get("reviewed_at"),
    }
    if include_packet:
        compact["packet"] = packet
    return _json_safe(compact)


def build_candidate_contribution_intake_report_from_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    storage_configured: bool = True,
    table_available: bool = True,
    statuses: Sequence[str] | None = None,
    candidate_ids: Sequence[str] | None = None,
    limit: int | None = None,
    include_packet: bool = False,
    errors: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Build a JSON-safe report from candidate contribution intake rows."""

    normalized_statuses = _normalize_statuses(statuses)
    bounded_limit = _bounded_limit(limit)
    compact_rows = [_compact_row(row, include_packet=include_packet) for row in rows][:bounded_limit]
    status_counts = Counter(row.get("status") or "unknown" for row in compact_rows)
    action_counts = Counter(row.get("requested_system_action") or "unknown" for row in compact_rows)
    route_counts = Counter(row.get("recommended_route") or "unknown" for row in compact_rows)
    candidate_counts = Counter(row.get("candidate_id") or "unknown" for row in compact_rows)
    pending_statuses = set(PENDING_INTAKE_STATUSES)
    actionable_rows = [
        row
        for row in compact_rows
        if row.get("status") in pending_statuses and row.get("requested_system_action") != "no_action"
    ]

    return {
        "storage_configured": storage_configured,
        "table_available": table_available,
        "filters": {
            "statuses": list(normalized_statuses),
            "candidate_ids": list(candidate_ids or []),
            "limit": bounded_limit,
            "include_packet": include_packet,
        },
        "summary": {
            "row_count": len(compact_rows),
            "queued_for_intake": int(status_counts.get("queued_for_intake", 0)),
            "triage_in_progress": int(status_counts.get("triage_in_progress", 0)),
            "needs_more_information": int(status_counts.get("needs_more_information", 0)),
            "actionable_count": len(actionable_rows),
            "no_action_count": int(action_counts.get("no_action", 0)),
            "candidate_count": len(candidate_counts),
        },
        "status_counts": dict(status_counts),
        "requested_action_counts": dict(action_counts),
        "recommended_route_counts": dict(route_counts),
        "candidate_counts": dict(candidate_counts),
        "rows": compact_rows,
        "errors": list(errors or []),
    }


def build_candidate_contribution_intake_report(
    *,
    database_url: str | None = None,
    statuses: Sequence[str] | None = None,
    candidate_ids: Sequence[str] | None = None,
    limit: int | None = None,
    include_packet: bool = False,
) -> dict[str, Any]:
    """Read candidate contribution intake rows from Postgres/Neon and summarize them."""

    connection_string = database_url or candidate_contribution_database_url()
    normalized_statuses = _normalize_statuses(statuses)
    bounded_limit = _bounded_limit(limit)
    if not connection_string:
        return build_candidate_contribution_intake_report_from_rows(
            [],
            storage_configured=False,
            table_available=False,
            statuses=normalized_statuses,
            candidate_ids=candidate_ids,
            limit=bounded_limit,
            include_packet=include_packet,
            errors=["Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL."],
        )

    where_clauses = ["status = any(%s)"]
    params: list[Any] = [normalized_statuses]
    candidate_filter = [str(value).strip() for value in (candidate_ids or []) if str(value).strip()]
    if candidate_filter:
        where_clauses.append("candidate_id = any(%s)")
        params.append(candidate_filter)
    params.append(bounded_limit)
    query = f"""
        select
            contribution_id::text,
            candidate_id,
            display_id,
            snapshot_content_hash,
            source_payload_url,
            status,
            contribution_type,
            relation_to_current_record,
            requested_system_action,
            contributor,
            evidence,
            artifacts,
            packet,
            review_notes,
            promoted_queue_id,
            created_at,
            updated_at,
            reviewed_at
        from candidate_contribution_intake
        where {" and ".join(where_clauses)}
        order by created_at desc
        limit %s
    """

    try:
        with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
    except psycopg2.errors.UndefinedTable:
        return build_candidate_contribution_intake_report_from_rows(
            [],
            storage_configured=True,
            table_available=False,
            statuses=normalized_statuses,
            candidate_ids=candidate_filter,
            limit=bounded_limit,
            include_packet=include_packet,
            errors=["candidate_contribution_intake table does not exist."],
        )

    return build_candidate_contribution_intake_report_from_rows(
        rows,
        storage_configured=True,
        table_available=True,
        statuses=normalized_statuses,
        candidate_ids=candidate_filter,
        limit=bounded_limit,
        include_packet=include_packet,
    )
