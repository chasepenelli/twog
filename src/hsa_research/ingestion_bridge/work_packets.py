"""Proof Network work packets — bounded checkout-able research tasks.

A work packet is a public, intake-only object: TWOG publishes it against a
candidate record, contributors check it out, and the work comes back as a
ProofCapsule for review. Public consumers can list and read; only the hosted
research pipeline creates or retires packets.

See deliverables/TWOG_Proof_Network_Techtree_Delta_Plan.md sections 3 and 11.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, datetime
import json
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg2
from psycopg2.errors import UndefinedTable
from psycopg2.extras import RealDictCursor

from .candidate_contribution_intake import candidate_contribution_database_url
from .contracts import (
    WorkPacketDifficulty,
    WorkPacketRecord,
    WorkPacketStatus,
    WorkPacketType,
)


WORK_PACKET_STATUSES: tuple[WorkPacketStatus, ...] = (
    "open",
    "in_progress",
    "completed",
    "retired",
)
OPEN_WORK_PACKET_STATUSES: tuple[WorkPacketStatus, ...] = ("open", "in_progress")
WORK_PACKET_TYPES: tuple[WorkPacketType, ...] = (
    "citation_repair",
    "claim_critique",
    "evidence_addition",
    "omics_note",
    "docking_replication",
    "md_review",
    "validation_proposal",
    "demotion_case",
    "methods_review",
)
WORK_PACKET_DIFFICULTIES: tuple[WorkPacketDifficulty, ...] = ("light", "moderate", "heavy")

# Default packet types where a notebook is the recommended artifact. Used when
# seeding and when the public surface decides whether to nudge contributors
# toward notebook-backed work. Lightweight packet types intentionally omit
# notebook nudges so citation/critique work stays low-friction.
NOTEBOOK_RECOMMENDED_TYPES: frozenset[WorkPacketType] = frozenset(
    {
        "omics_note",
        "docking_replication",
        "md_review",
        "methods_review",
        "validation_proposal",
    }
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
        return list(OPEN_WORK_PACKET_STATUSES)
    normalized: list[str] = []
    for status in statuses:
        value = str(status).strip()
        if value and value in WORK_PACKET_STATUSES and value not in normalized:
            normalized.append(value)
    return normalized or list(OPEN_WORK_PACKET_STATUSES)


def _bounded_limit(limit: int | None, *, default: int = 25, ceiling: int = 200) -> int:
    if limit is None:
        return default
    return max(1, min(int(limit), ceiling))


def _checkout_path(work_packet_id: str) -> str:
    return f"/api/work-packets/{work_packet_id}/checkout"


def _packet_url(work_packet_id: str) -> str:
    return f"/api/work-packets/{work_packet_id}"


def _decode_jsonish(value: Any) -> Any:
    """Postgres jsonb returns Python objects; bare text fields may be JSON strings."""

    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return value
    return value


def _string_list(value: Any) -> list[str]:
    decoded = _decode_jsonish(value)
    if not isinstance(decoded, list):
        return []
    return [str(item).strip() for item in decoded if isinstance(item, str) and item.strip()]


def _public_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Render the public-safe view of a work_packet row.

    Public boundary: this is the only shape exposed via the public Next.js
    surface. Operator audit fields (created_by, retired_reason, internal
    metadata) intentionally do not appear.
    """

    work_packet_id = str(row.get("work_packet_id") or "")
    return _json_safe(
        {
            "work_packet_id": work_packet_id,
            "candidate_id": row.get("candidate_id"),
            "packet_type": row.get("packet_type"),
            "title": row.get("title"),
            "question": row.get("question"),
            "why_it_matters": row.get("why_it_matters") or "",
            "target_claim_ids": _string_list(row.get("target_claim_ids")),
            "target_section": row.get("target_section"),
            "required_inputs": _string_list(row.get("required_inputs")),
            "suggested_methods": _string_list(row.get("suggested_methods")),
            "expected_outputs": _string_list(row.get("expected_outputs")),
            "acceptance_criteria": _string_list(row.get("acceptance_criteria")),
            "reward_hint": row.get("reward_hint") or "",
            "difficulty": row.get("difficulty") or "moderate",
            "status": row.get("status") or "open",
            "notebook_recommended": bool(row.get("notebook_recommended")),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "url": _packet_url(work_packet_id),
            "checkout_url": _checkout_path(work_packet_id),
        }
    )


def build_work_packet_report_from_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    storage_configured: bool = True,
    table_available: bool = True,
    statuses: Sequence[str] | None = None,
    candidate_ids: Sequence[str] | None = None,
    packet_types: Sequence[str] | None = None,
    limit: int | None = None,
    errors: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Render a JSON-safe public listing of work packets."""

    normalized_statuses = _normalize_statuses(statuses)
    bounded_limit = _bounded_limit(limit)
    public_rows = [_public_row(row) for row in rows][:bounded_limit]
    status_counts = Counter(row.get("status") or "unknown" for row in public_rows)
    type_counts = Counter(row.get("packet_type") or "unknown" for row in public_rows)
    candidate_counts = Counter(row.get("candidate_id") or "unknown" for row in public_rows)
    difficulty_counts = Counter(row.get("difficulty") or "moderate" for row in public_rows)
    open_count = sum(1 for row in public_rows if row.get("status") == "open")
    in_progress_count = sum(1 for row in public_rows if row.get("status") == "in_progress")

    return {
        "storage_configured": storage_configured,
        "table_available": table_available,
        "filters": {
            "statuses": list(normalized_statuses),
            "candidate_ids": list(candidate_ids or []),
            "packet_types": list(packet_types or []),
            "limit": bounded_limit,
        },
        "summary": {
            "row_count": len(public_rows),
            "open_count": open_count,
            "in_progress_count": in_progress_count,
            "candidate_count": len(candidate_counts),
        },
        "status_counts": dict(status_counts),
        "packet_type_counts": dict(type_counts),
        "candidate_counts": dict(candidate_counts),
        "difficulty_counts": dict(difficulty_counts),
        "rows": public_rows,
        "errors": list(errors or []),
    }


def list_work_packets(
    *,
    database_url: str | None = None,
    candidate_ids: Sequence[str] | None = None,
    statuses: Sequence[str] | None = None,
    packet_types: Sequence[str] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Read work_packet rows from Postgres/Neon and return a public listing."""

    connection_string = database_url or candidate_contribution_database_url()
    normalized_statuses = _normalize_statuses(statuses)
    bounded_limit = _bounded_limit(limit)
    candidate_filter = [str(value).strip() for value in (candidate_ids or []) if str(value).strip()]
    type_filter = [
        str(value).strip()
        for value in (packet_types or [])
        if str(value).strip() and str(value).strip() in WORK_PACKET_TYPES
    ]

    if not connection_string:
        return build_work_packet_report_from_rows(
            [],
            storage_configured=False,
            table_available=False,
            statuses=normalized_statuses,
            candidate_ids=candidate_filter,
            packet_types=type_filter,
            limit=bounded_limit,
            errors=["Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL."],
        )

    where_clauses = ["status = any(%s)"]
    params: list[Any] = [normalized_statuses]
    if candidate_filter:
        where_clauses.append("candidate_id = any(%s)")
        params.append(candidate_filter)
    if type_filter:
        where_clauses.append("packet_type = any(%s)")
        params.append(type_filter)
    params.append(bounded_limit)
    query = f"""
        select
            work_packet_id::text,
            candidate_id,
            packet_type,
            title,
            question,
            why_it_matters,
            target_claim_ids,
            target_section,
            required_inputs,
            suggested_methods,
            expected_outputs,
            acceptance_criteria,
            reward_hint,
            difficulty,
            status,
            notebook_recommended,
            created_at,
            updated_at
        from work_packets
        where {" and ".join(where_clauses)}
        order by
          case status when 'open' then 0 when 'in_progress' then 1 when 'completed' then 2 else 3 end,
          updated_at desc
        limit %s
    """

    try:
        with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
    except UndefinedTable:
        return build_work_packet_report_from_rows(
            [],
            storage_configured=True,
            table_available=False,
            statuses=normalized_statuses,
            candidate_ids=candidate_filter,
            packet_types=type_filter,
            limit=bounded_limit,
            errors=["work_packets table does not exist."],
        )

    return build_work_packet_report_from_rows(
        rows,
        storage_configured=True,
        table_available=True,
        statuses=normalized_statuses,
        candidate_ids=candidate_filter,
        packet_types=type_filter,
        limit=bounded_limit,
    )


def get_work_packet(
    work_packet_id: str,
    *,
    database_url: str | None = None,
) -> dict[str, Any] | None:
    """Read a single work_packet row by id; returns the public-safe view."""

    connection_string = database_url or candidate_contribution_database_url()
    if not connection_string:
        return None
    query = """
        select
            work_packet_id::text,
            candidate_id,
            packet_type,
            title,
            question,
            why_it_matters,
            target_claim_ids,
            target_section,
            required_inputs,
            suggested_methods,
            expected_outputs,
            acceptance_criteria,
            reward_hint,
            difficulty,
            status,
            notebook_recommended,
            created_at,
            updated_at
        from work_packets
        where work_packet_id::text = %s
        limit 1
    """
    try:
        with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, [str(work_packet_id).strip()])
                row = cursor.fetchone()
    except UndefinedTable:
        return None
    if row is None:
        return None
    return _public_row(row)


def _record_payload(record: WorkPacketRecord) -> dict[str, Any]:
    return _json_safe(record.model_dump(mode="json"))


def seed_work_packets(
    records: Sequence[WorkPacketRecord],
    *,
    database_url: str | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Idempotently insert work_packet rows from typed records.

    Each record is keyed by work_packet_id; existing rows are left untouched
    so the seed job is safe to run repeatedly.
    """

    connection_string = database_url or candidate_contribution_database_url()
    planned = [_record_payload(record) for record in records]

    if not connection_string:
        return {
            "dry_run": True,
            "storage_configured": False,
            "table_available": False,
            "planned_count": len(planned),
            "inserted_count": 0,
            "skipped_count": 0,
            "rows": planned,
            "errors": ["Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL."],
        }

    if dry_run:
        return {
            "dry_run": True,
            "storage_configured": True,
            "table_available": True,
            "planned_count": len(planned),
            "inserted_count": 0,
            "skipped_count": 0,
            "rows": planned,
            "errors": [],
        }

    insert_query = """
        insert into work_packets (
            work_packet_id,
            candidate_id,
            packet_type,
            title,
            question,
            why_it_matters,
            target_claim_ids,
            target_section,
            required_inputs,
            suggested_methods,
            expected_outputs,
            acceptance_criteria,
            reward_hint,
            difficulty,
            status,
            notebook_recommended,
            created_by,
            payload,
            created_at,
            updated_at
        )
        values (
            %s, %s, %s, %s, %s, %s,
            %s::jsonb, %s,
            %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
            %s, %s, %s, %s,
            %s, %s::jsonb,
            %s, %s
        )
        on conflict (work_packet_id) do nothing
    """

    inserted = 0
    skipped = 0
    errors: list[str] = []
    try:
        with psycopg2.connect(connection_string) as connection:
            with connection.cursor() as cursor:
                for record, payload in zip(records, planned, strict=True):
                    try:
                        cursor.execute(
                            insert_query,
                            [
                                str(record.work_packet_id),
                                record.candidate_id,
                                record.packet_type,
                                record.title,
                                record.question,
                                record.why_it_matters,
                                json.dumps(record.target_claim_ids),
                                record.target_section,
                                json.dumps(record.required_inputs),
                                json.dumps(record.suggested_methods),
                                json.dumps(record.expected_outputs),
                                json.dumps(record.acceptance_criteria),
                                record.reward_hint,
                                record.difficulty,
                                record.status,
                                record.notebook_recommended,
                                record.created_by,
                                json.dumps(payload),
                                record.created_at,
                                record.updated_at,
                            ],
                        )
                        if cursor.rowcount and cursor.rowcount > 0:
                            inserted += 1
                        else:
                            skipped += 1
                    except psycopg2.Error as exc:
                        errors.append(f"work_packet {record.work_packet_id}: {exc}")
                connection.commit()
    except UndefinedTable:
        return {
            "dry_run": True,
            "storage_configured": True,
            "table_available": False,
            "planned_count": len(planned),
            "inserted_count": 0,
            "skipped_count": 0,
            "rows": planned,
            "errors": ["work_packets table does not exist; apply migration 008_work_packets.sql."],
        }

    return {
        "dry_run": False,
        "storage_configured": True,
        "table_available": True,
        "planned_count": len(planned),
        "inserted_count": inserted,
        "skipped_count": skipped,
        "rows": planned,
        "errors": errors,
    }


def _curated_packet_id(candidate_id: str, packet_type: str) -> UUID:
    """Deterministic work_packet_id for the curated bootstrap set.

    Using uuid5 over (candidate_id, packet_type) means re-running
    ``seed_work_packets(curated_work_packets_for_candidate(...))`` is a
    no-op after the first apply: each curated packet has the same id, so
    the ``ON CONFLICT (work_packet_id) DO NOTHING`` insert skips it.
    Operators can still create non-curated packets with random UUIDs.
    """

    return uuid5(NAMESPACE_URL, f"twog:work_packet:{candidate_id}:{packet_type}:v1")


def curated_work_packets_for_candidate(candidate_id: str) -> list[WorkPacketRecord]:
    """Hand-curated initial packets for a public candidate.

    Delta plan section 11 requires at least three open packets per live
    candidate. This bootstrap returns a starter set covering the three
    lightweight types so the public surface is immediately usable.

    Packet text is intentionally generic — operators should refine titles
    and questions for specific candidates before publishing widely.

    Idempotent on (candidate_id, packet_type) via deterministic uuid5.
    """

    return [
        WorkPacketRecord(
            work_packet_id=_curated_packet_id(candidate_id, "citation_repair"),
            candidate_id=candidate_id,
            packet_type="citation_repair",
            title="Repair the strongest citation supporting the current rationale",
            question=(
                "Read the rationale on the candidate page, pick the load-bearing claim, "
                "and verify the cited source supports it. If the citation is weak, propose "
                "a stronger one with full provenance."
            ),
            why_it_matters=(
                "A single weak citation can quietly inflate confidence across the whole record. "
                "Catching it early prevents downstream validation work from compounding the error."
            ),
            required_inputs=[
                "candidate rationale_md",
                "literature dossier",
            ],
            suggested_methods=[
                "open the cited paper and verify the claim text",
                "search PubMed/Europe PMC for a stronger primary source",
            ],
            expected_outputs=[
                "specific claim under repair",
                "current citation handle",
                "proposed replacement citation with DOI/PMID",
                "one-paragraph justification",
            ],
            acceptance_criteria=[
                "claim text matches the candidate record",
                "replacement source is primary or peer-reviewed",
                "provenance is verifiable",
            ],
            reward_hint="citation_repair_credit",
            difficulty="light",
            notebook_recommended=False,
            created_by="proof_network_bootstrap",
        ),
        WorkPacketRecord(
            work_packet_id=_curated_packet_id(candidate_id, "claim_critique"),
            candidate_id=candidate_id,
            packet_type="claim_critique",
            title="Critique the strongest single claim in the candidate rationale",
            question=(
                "Identify one specific claim that the candidate's case rests on, and write a "
                "well-sourced critique: where might it be wrong, what evidence would change "
                "your mind, and what is the steel-manned counterargument?"
            ),
            why_it_matters=(
                "Public critique with provenance protects the record. A well-reasoned negative "
                "finding is as valuable as a positive one — the proof network rewards both."
            ),
            required_inputs=[
                "candidate rationale_md",
                "linked literature dossier",
            ],
            suggested_methods=[
                "trace the claim back to its evidence",
                "look for replication, negative results, or methodological concerns",
            ],
            expected_outputs=[
                "claim being critiqued",
                "structured critique with at least one source",
                "what evidence would update the critique",
            ],
            acceptance_criteria=[
                "critique cites a specific external source",
                "argument is internally consistent",
                "it is clear what would change the conclusion",
            ],
            reward_hint="claim_critique_credit",
            difficulty="moderate",
            notebook_recommended=False,
            created_by="proof_network_bootstrap",
        ),
        WorkPacketRecord(
            work_packet_id=_curated_packet_id(candidate_id, "evidence_addition"),
            candidate_id=candidate_id,
            packet_type="evidence_addition",
            title="Add one missing piece of evidence to the candidate record",
            question=(
                "Find one published result that the candidate record does not yet cite but "
                "should — supporting, challenging, or neighbouring the current case — and "
                "package it as a structured evidence addition with full provenance."
            ),
            why_it_matters=(
                "TWOG's records are only as strong as the evidence webbed into them. New "
                "primary sources tighten the case and surface neighbouring claims worth tracking."
            ),
            required_inputs=[
                "candidate rationale_md",
                "current evidence bundle",
            ],
            suggested_methods=[
                "search PubMed, bioRxiv, Europe PMC for relevant recent work",
                "compare against the candidate's existing evidence_refs",
            ],
            expected_outputs=[
                "new source citation",
                "summary of the evidence",
                "where in the candidate record it should be linked",
            ],
            acceptance_criteria=[
                "source is not already in the candidate record",
                "summary stays factual and citable",
                "linkage to the record is specific",
            ],
            reward_hint="evidence_addition_credit",
            difficulty="moderate",
            notebook_recommended=False,
            created_by="proof_network_bootstrap",
        ),
    ]


def summarize_work_packet_report(report: Mapping[str, Any]) -> dict[str, Any]:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    return {
        "row_count": int(summary.get("row_count") or 0),
        "open_count": int(summary.get("open_count") or 0),
        "in_progress_count": int(summary.get("in_progress_count") or 0),
        "candidate_count": int(summary.get("candidate_count") or 0),
    }
