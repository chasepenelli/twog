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
CONTRIBUTION_TYPES = (
    "evidence_addition",
    "citation_repair",
    "claim_critique",
    "replication_result",
    "compute_artifact",
    "omics_note",
    "validation_proposal",
    "safety_or_translation_note",
    "candidate_demotion_case",
)
TRIAGE_ACTIONS = (
    "start_triage",
    "request_more_information",
    "reject",
    "accept_for_evidence_review",
    "accept_for_validation_queue",
    "accept_for_compute_review",
    "archive",
)
TRIAGE_ACTION_STATUSES = {
    "start_triage": "triage_in_progress",
    "request_more_information": "needs_more_information",
    "reject": "rejected",
    "accept_for_evidence_review": "accepted_for_evidence_review",
    "accept_for_validation_queue": "accepted_for_validation_queue",
    "accept_for_compute_review": "accepted_for_compute_review",
    "archive": "archived",
}
ACCEPTANCE_ACTIONS = {
    "accept_for_evidence_review",
    "accept_for_validation_queue",
    "accept_for_compute_review",
}


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


def _normalize_contribution_ids(contribution_ids: Sequence[str] | None) -> list[str]:
    normalized = []
    for contribution_id in contribution_ids or []:
        value = str(contribution_id).strip()
        if value and value not in normalized:
            normalized.append(value)
    return normalized[:50]


def _normalize_triage_action(action: str) -> str:
    value = str(action or "").strip()
    if value not in TRIAGE_ACTIONS:
        raise ValueError(f"action must be one of: {', '.join(TRIAGE_ACTIONS)}")
    return value


def _triage_promoted_queue_id(contribution_id: str, action: str) -> str | None:
    if action not in ACCEPTANCE_ACTIONS:
        return None
    route = action.removeprefix("accept_for_")
    return f"candidate_contribution:{contribution_id}:{route}"


def _triage_review_note(
    *,
    existing_note: str | None,
    operator: str,
    action: str,
    review_notes: str | None,
    timestamp: datetime,
    dry_run: bool,
) -> str:
    prefix = "DRY RUN" if dry_run else "TRIAGE"
    note = str(review_notes or "").strip() or "No operator note supplied."
    entry = f"[{timestamp.isoformat()}] {prefix} {operator}: {action} - {note}"
    existing = str(existing_note or "").strip()
    return f"{existing}\n{entry}" if existing else entry


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
        "handle": source.get("handle"),
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
        "contribution_content_hash": row.get("contribution_content_hash"),
        "source_payload_url": row.get("source_payload_url"),
        "status_url": (
            f"/api/contributions/{row.get('contribution_id')}/status"
            if row.get("contribution_id")
            else None
        ),
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


def build_candidate_contribution_triage_plan_from_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    contribution_ids: Sequence[str],
    action: str,
    operator: str,
    review_notes: str | None = None,
    dry_run: bool = True,
    timestamp: datetime | None = None,
    errors: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Build an operator triage plan/result from existing intake rows."""

    normalized_ids = _normalize_contribution_ids(contribution_ids)
    normalized_action = _normalize_triage_action(action)
    target_status = TRIAGE_ACTION_STATUSES[normalized_action]
    now = timestamp or datetime.utcnow()
    selected = [row for row in rows if str(row.get("contribution_id") or "") in normalized_ids]
    found_ids = {str(row.get("contribution_id") or "") for row in selected}
    missing_ids = [contribution_id for contribution_id in normalized_ids if contribution_id not in found_ids]

    planned_rows: list[dict[str, Any]] = []
    for row in selected:
        contribution_id = str(row.get("contribution_id") or "")
        old_status = str(row.get("status") or "")
        promoted_queue_id = row.get("promoted_queue_id") or _triage_promoted_queue_id(contribution_id, normalized_action)
        planned_rows.append(
            _json_safe(
                {
                    "contribution_id": contribution_id,
                    "candidate_id": row.get("candidate_id"),
                    "display_id": row.get("display_id"),
                    "contribution_content_hash": row.get("contribution_content_hash"),
                    "status_url": f"/api/contributions/{contribution_id}/status",
                    "old_status": old_status,
                    "new_status": target_status,
                    "action": normalized_action,
                    "operator": str(operator or "unknown"),
                    "promoted_queue_id": promoted_queue_id,
                    "would_update": old_status != target_status or row.get("promoted_queue_id") != promoted_queue_id,
                    "review_notes": _triage_review_note(
                        existing_note=row.get("review_notes"),
                        operator=str(operator or "unknown"),
                        action=normalized_action,
                        review_notes=review_notes,
                        timestamp=now,
                        dry_run=dry_run,
                    ),
                    "reviewed_at": None if target_status == "triage_in_progress" else now,
                    "updated_at": now,
                }
            )
        )

    return {
        "dry_run": dry_run,
        "action": normalized_action,
        "target_status": target_status,
        "operator": str(operator or "unknown"),
        "summary": {
            "requested_count": len(normalized_ids),
            "selected_count": len(selected),
            "missing_count": len(missing_ids),
            "updated_count": 0 if dry_run else len(planned_rows),
        },
        "missing_contribution_ids": missing_ids,
        "rows": planned_rows,
        "errors": list(errors or []),
    }


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
            contribution_content_hash,
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


def triage_candidate_contributions(
    *,
    database_url: str | None = None,
    contribution_ids: Sequence[str] | None = None,
    action: str,
    operator: str = "dagster",
    review_notes: str | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Apply one explicit operator triage decision to public candidate contribution rows."""

    connection_string = database_url or candidate_contribution_database_url()
    normalized_ids = _normalize_contribution_ids(contribution_ids)
    try:
        normalized_action = _normalize_triage_action(action)
    except ValueError as exc:
        return build_candidate_contribution_triage_plan_from_rows(
            [],
            contribution_ids=normalized_ids,
            action="start_triage",
            operator=operator,
            review_notes=review_notes,
            dry_run=True,
            errors=[str(exc)],
        )

    if not normalized_ids:
        return build_candidate_contribution_triage_plan_from_rows(
            [],
            contribution_ids=[],
            action=normalized_action,
            operator=operator,
            review_notes=review_notes,
            dry_run=True,
            errors=["At least one contribution_id is required."],
        )
    if not connection_string:
        return build_candidate_contribution_triage_plan_from_rows(
            [],
            contribution_ids=normalized_ids,
            action=normalized_action,
            operator=operator,
            review_notes=review_notes,
            dry_run=True,
            errors=["Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL."],
        )

    select_query = """
        select
            contribution_id::text,
            candidate_id,
            display_id,
            contribution_content_hash,
            status,
            review_notes,
            promoted_queue_id
        from candidate_contribution_intake
        where contribution_id::text = any(%s)
        order by created_at desc
    """
    update_query = """
        update candidate_contribution_intake
        set
            status = %s,
            review_notes = %s,
            promoted_queue_id = %s,
            updated_at = %s,
            reviewed_at = %s
        where contribution_id::text = %s
    """

    try:
        with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as connection:
            with connection.cursor() as cursor:
                cursor.execute(select_query, [normalized_ids])
                rows = cursor.fetchall()
                plan = build_candidate_contribution_triage_plan_from_rows(
                    rows,
                    contribution_ids=normalized_ids,
                    action=normalized_action,
                    operator=operator,
                    review_notes=review_notes,
                    dry_run=dry_run,
                )
                if dry_run or plan.get("errors"):
                    return plan
                for row in plan["rows"]:
                    cursor.execute(
                        update_query,
                        [
                            row["new_status"],
                            row["review_notes"],
                            row["promoted_queue_id"],
                            row["updated_at"],
                            row["reviewed_at"],
                            row["contribution_id"],
                        ],
                    )
                connection.commit()
                return plan
    except psycopg2.errors.UndefinedTable:
        return build_candidate_contribution_triage_plan_from_rows(
            [],
            contribution_ids=normalized_ids,
            action=normalized_action,
            operator=operator,
            review_notes=review_notes,
            dry_run=True,
            errors=["candidate_contribution_intake table does not exist."],
        )
