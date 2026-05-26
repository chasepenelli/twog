"""Proof Network — proof capsules service.

A ProofCapsule is the inspectable work object submitted against a WorkPacket
(or as a freeform contribution to a candidate). This module handles:

- public-safe shape rendering (status payload, listing payload),
- submission persistence,
- review event recording,
- pure-function report builders for tests.

The public boundary is enforced at the shape level: contributor.contact,
contributor.agent_id, contributor.website, and any operator review notes
are never returned from public report builders. Operator code paths reach
into the row dict directly when they need those fields.
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
    AgentRunReviewVerdict,
    PROOF_CAPSULE_ACCEPTED_STATUSES,
    PROOF_CAPSULE_PENDING_STATUSES,
    ProofCapsuleContributor,
    ProofCapsuleRecord,
    ProofCapsuleReviewRecord,
    ProofCapsuleReviewVerdict,
    ProofCapsuleStatus,
    ProofCapsuleType,
    RewardEventRecord,
    RewardOutcomeBucket,
    RewardRoutingRecommendation,
)


PROOF_CAPSULE_STATUSES: tuple[ProofCapsuleStatus, ...] = (
    "submitted",
    "in_review",
    "needs_changes",
    "accepted",
    "rejected",
    "archived",
    "routed_to_validation",
    "routed_to_compute_review",
)
PROOF_CAPSULE_TYPES: tuple[ProofCapsuleType, ...] = (
    "citation_repair",
    "claim_critique",
    "evidence_addition",
    "omics_note",
    "docking_replication",
    "md_review",
    "validation_proposal",
    "demotion_case",
    "methods_review",
    "freeform",
)
PROOF_CAPSULE_VERDICTS: tuple[ProofCapsuleReviewVerdict, ...] = (
    "accepted",
    "needs_changes",
    "rejected",
    "archived",
    "routed_to_validation",
    "routed_to_compute_review",
)

# Verdict → resulting capsule status. Drives operator review wiring.
VERDICT_TARGET_STATUS: dict[ProofCapsuleReviewVerdict, ProofCapsuleStatus] = {
    "accepted": "accepted",
    "needs_changes": "needs_changes",
    "rejected": "rejected",
    "archived": "archived",
    "routed_to_validation": "routed_to_validation",
    "routed_to_compute_review": "routed_to_compute_review",
}


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


def _decode_jsonish(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return value
    return value


def _status_path(proof_capsule_id: str) -> str:
    return f"/api/proof-capsules/{proof_capsule_id}"


def _normalize_statuses(statuses: Sequence[str] | None) -> list[str]:
    if not statuses:
        return list(PROOF_CAPSULE_PENDING_STATUSES + PROOF_CAPSULE_ACCEPTED_STATUSES)
    normalized: list[str] = []
    for status in statuses:
        value = str(status).strip()
        if value and value in PROOF_CAPSULE_STATUSES and value not in normalized:
            normalized.append(value)
    return normalized or list(PROOF_CAPSULE_PENDING_STATUSES)


def _bounded_limit(limit: int | None, *, default: int = 25, ceiling: int = 200) -> int:
    if limit is None:
        return default
    return max(1, min(int(limit), ceiling))


def _public_contributor(value: Any) -> dict[str, Any]:
    """Drop sensitive contributor fields (contact, agent_id, website).

    See ProofCapsuleContributor.public_view() — this is the row-shaped twin
    that operates on already-deserialized JSONB values from psycopg2.
    """

    raw = _decode_jsonish(value)
    if not isinstance(raw, dict):
        raw = {}
    return {
        "kind": raw.get("kind") or "human",
        "name": raw.get("name"),
        "handle": raw.get("handle"),
        "affiliation": raw.get("affiliation"),
    }


def _public_artifact_manifest(value: Any) -> list[dict[str, Any]]:
    raw = _decode_jsonish(value)
    if not isinstance(raw, list):
        return []
    sanitized: list[dict[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        sanitized.append(
            {
                "label": entry.get("label"),
                "url": entry.get("url"),
                "content_hash": entry.get("content_hash"),
                "mime_type": entry.get("mime_type"),
                "size_bytes": entry.get("size_bytes"),
                "method_or_tool": entry.get("method_or_tool"),
            }
        )
    return sanitized


def _string_list(value: Any) -> list[str]:
    raw = _decode_jsonish(value)
    if not isinstance(raw, list):
        return []
    return [str(item).strip() for item in raw if isinstance(item, str) and item.strip()]


def _coerce_review_count(value: Any) -> int:
    """Coerce a row's review_count (may be None or string) to int."""

    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


# Content-quality smell flags. Computed at read time over capsule rows so
# operators can prioritize the review queue; agents see their own flags
# on status lookups and can self-correct before resubmitting. Rules are
# intentionally conservative: a flag is a *hint*, not a rejection.
QUALITY_FLAG_THIN_ANALYSIS = "thin_analysis"
QUALITY_FLAG_NO_SOURCE_TOKEN = "no_source_token"
QUALITY_FLAG_REPETITIVE_TEXT = "repetitive_text"
QUALITY_FLAG_MISSING_FINDINGS = "missing_findings"
QUALITY_FLAG_NO_ARTIFACT_FOR_REPLICATION = "no_artifact_for_replication"
QUALITY_FLAG_SHORT_CITATION_REPAIR = "short_citation_repair"


_SUBSTANTIVE_TYPES_REQUIRING_FINDINGS = frozenset(
    {"claim_critique", "validation_proposal", "methods_review", "demotion_case"}
)
_REPLICATION_TYPES_REQUIRING_ARTIFACT = frozenset(
    {"docking_replication", "md_review"}
)
_SOURCE_TOKEN_PATTERNS = (
    "doi:",
    "doi.org",
    "pmid:",
    "pmcid:",
    "ncbi.nlm.nih",
    "pubmed",
    "europepmc",
    "biorxiv",
    "arxiv",
    "10.",  # generic DOI prefix
    "http://",
    "https://",
)
_THIN_ANALYSIS_MIN_CHARS = 80
_THIN_ANALYSIS_MIN_WORDS = 12
_REPETITION_MIN_LEN = 6
_REPETITION_MIN_COUNT = 4


def _has_source_token(text: str) -> bool:
    lowered = text.lower()
    return any(token in lowered for token in _SOURCE_TOKEN_PATTERNS)


def _repetition_smell(text: str) -> bool:
    """Detect repetitive boilerplate like ``asdf asdf asdf asdf``.

    Looks for any 6+ character token that repeats four or more times.
    Returns ``True`` only on strong evidence of repetition, not natural
    English where short connectives recur.
    """

    tokens = [token for token in text.lower().split() if len(token) >= _REPETITION_MIN_LEN]
    if not tokens:
        return False
    counts: dict[str, int] = {}
    for token in tokens:
        counts[token] = counts.get(token, 0) + 1
    return any(count >= _REPETITION_MIN_COUNT for count in counts.values())


def compute_quality_flags(
    *,
    capsule_type: str,
    analysis_summary: str,
    findings: str,
    method_refs: Sequence[str],
    artifact_manifest: Sequence[Mapping[str, Any]],
    title: str,
) -> list[str]:
    """Return a list of smell flags for one capsule body.

    Pure function over the public-facing fields. Empty list means the
    capsule looks clean by current heuristics; flags are hints to
    reviewers, not blockers.
    """

    flags: list[str] = []
    summary = (analysis_summary or "").strip()
    finds = (findings or "").strip()
    combined = " ".join([title or "", summary, finds, " ".join(method_refs or [])])

    word_count = len(summary.split())
    if len(summary) < _THIN_ANALYSIS_MIN_CHARS or word_count < _THIN_ANALYSIS_MIN_WORDS:
        flags.append(QUALITY_FLAG_THIN_ANALYSIS)

    if not _has_source_token(combined) and not artifact_manifest:
        flags.append(QUALITY_FLAG_NO_SOURCE_TOKEN)

    if _repetition_smell(summary) or _repetition_smell(finds):
        flags.append(QUALITY_FLAG_REPETITIVE_TEXT)

    if capsule_type in _SUBSTANTIVE_TYPES_REQUIRING_FINDINGS and not finds:
        flags.append(QUALITY_FLAG_MISSING_FINDINGS)

    if capsule_type in _REPLICATION_TYPES_REQUIRING_ARTIFACT and not artifact_manifest:
        flags.append(QUALITY_FLAG_NO_ARTIFACT_FOR_REPLICATION)

    if capsule_type == "citation_repair" and (
        len(summary) < 120 or len(finds) < 20
    ):
        flags.append(QUALITY_FLAG_SHORT_CITATION_REPAIR)

    return flags


def _row_quality_flags(row: Mapping[str, Any]) -> list[str]:
    """Convenience: extract the fields a row carries and call compute_quality_flags."""

    return compute_quality_flags(
        capsule_type=str(row.get("capsule_type") or ""),
        analysis_summary=str(row.get("analysis_summary") or ""),
        findings=str(row.get("findings") or ""),
        method_refs=_string_list(row.get("method_refs")),
        artifact_manifest=_public_artifact_manifest(row.get("artifact_manifest")),
        title=str(row.get("title") or ""),
    )


def build_proof_capsule_public_view(row: Mapping[str, Any]) -> dict[str, Any]:
    """Public-safe view of a proof_capsule row.

    Public boundary: contributor.contact / agent_id / website never appear
    here, nor do operator review notes or raw payload JSON. Operators get
    the full payload via the Python intake report path only.

    ``review_count`` is surfaced so contributors can see how many review
    events landed on their capsule. Notes/identities are intentionally
    omitted; this is just an integer.
    """

    proof_capsule_id = str(row.get("proof_capsule_id") or "")
    work_packet_id = row.get("work_packet_id")
    return _json_safe(
        {
            "proof_capsule_id": proof_capsule_id,
            "work_packet_id": str(work_packet_id) if work_packet_id else None,
            "candidate_id": row.get("candidate_id"),
            "capsule_type": row.get("capsule_type"),
            "title": row.get("title"),
            "contributor": _public_contributor(row.get("contributor")),
            "candidate_snapshot_hash": row.get("candidate_snapshot_hash"),
            "evidence_bundle_hash": row.get("evidence_bundle_hash"),
            "method_refs": _string_list(row.get("method_refs")),
            "notebook_ref": row.get("notebook_ref"),
            "analysis_summary": row.get("analysis_summary"),
            "findings": row.get("findings") or "",
            "output_refs": _string_list(row.get("output_refs")),
            "artifact_manifest": _public_artifact_manifest(row.get("artifact_manifest")),
            "limitations": row.get("limitations") or "",
            "requested_review_route": row.get("requested_review_route"),
            "content_hash": row.get("content_hash"),
            "status": row.get("status") or "submitted",
            "submitted_at": row.get("submitted_at"),
            "updated_at": row.get("updated_at"),
            "reviewed_at": row.get("reviewed_at"),
            "review_count": _coerce_review_count(row.get("review_count")),
            "quality_flags": _row_quality_flags(row),
            "status_url": _status_path(proof_capsule_id),
        }
    )


def build_proof_capsule_list_report_from_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    storage_configured: bool = True,
    table_available: bool = True,
    statuses: Sequence[str] | None = None,
    candidate_ids: Sequence[str] | None = None,
    capsule_types: Sequence[str] | None = None,
    limit: int | None = None,
    errors: Sequence[str] | None = None,
) -> dict[str, Any]:
    normalized_statuses = _normalize_statuses(statuses)
    bounded_limit = _bounded_limit(limit)
    public_rows = [build_proof_capsule_public_view(row) for row in rows][:bounded_limit]
    status_counts = Counter(row.get("status") or "unknown" for row in public_rows)
    type_counts = Counter(row.get("capsule_type") or "unknown" for row in public_rows)
    candidate_counts = Counter(row.get("candidate_id") or "unknown" for row in public_rows)
    accepted = sum(
        1 for row in public_rows if row.get("status") in PROOF_CAPSULE_ACCEPTED_STATUSES
    )
    pending = sum(
        1 for row in public_rows if row.get("status") in PROOF_CAPSULE_PENDING_STATUSES
    )
    return {
        "storage_configured": storage_configured,
        "table_available": table_available,
        "filters": {
            "statuses": list(normalized_statuses),
            "candidate_ids": list(candidate_ids or []),
            "capsule_types": list(capsule_types or []),
            "limit": bounded_limit,
        },
        "summary": {
            "row_count": len(public_rows),
            "pending_count": pending,
            "accepted_count": accepted,
            "candidate_count": len(candidate_counts),
        },
        "status_counts": dict(status_counts),
        "capsule_type_counts": dict(type_counts),
        "candidate_counts": dict(candidate_counts),
        "rows": public_rows,
        "errors": list(errors or []),
    }


def _normalize_capsule_types(capsule_types: Sequence[str] | None) -> list[str]:
    return [
        str(value).strip()
        for value in (capsule_types or [])
        if str(value).strip() and str(value).strip() in PROOF_CAPSULE_TYPES
    ]


def _select_columns() -> str:
    # ``review_count`` is a correlated subquery against proof_capsule_reviews.
    # Operator notes and identities are intentionally absent — the only
    # publicly exposed fact is "this capsule has been reviewed N times."
    return """
        proof_capsule_id::text as proof_capsule_id,
        work_packet_id::text as work_packet_id,
        candidate_id,
        capsule_type,
        title,
        contributor,
        candidate_snapshot_hash,
        evidence_bundle_hash,
        method_refs,
        notebook_ref,
        analysis_summary,
        findings,
        output_refs,
        artifact_manifest,
        limitations,
        requested_review_route,
        content_hash,
        status,
        submitted_at,
        updated_at,
        reviewed_at,
        coalesce((
            select count(*)
            from proof_capsule_reviews r
            where r.proof_capsule_id = proof_capsules.proof_capsule_id
        ), 0) as review_count
    """


def list_proof_capsules(
    *,
    database_url: str | None = None,
    candidate_ids: Sequence[str] | None = None,
    statuses: Sequence[str] | None = None,
    capsule_types: Sequence[str] | None = None,
    work_packet_id: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Read proof_capsule rows from Postgres and return the public listing."""

    connection_string = database_url or candidate_contribution_database_url()
    normalized_statuses = _normalize_statuses(statuses)
    bounded_limit = _bounded_limit(limit)
    candidate_filter = [str(value).strip() for value in (candidate_ids or []) if str(value).strip()]
    type_filter = _normalize_capsule_types(capsule_types)
    work_packet_filter = str(work_packet_id).strip() if work_packet_id else None

    if not connection_string:
        return build_proof_capsule_list_report_from_rows(
            [],
            storage_configured=False,
            table_available=False,
            statuses=normalized_statuses,
            candidate_ids=candidate_filter,
            capsule_types=type_filter,
            limit=bounded_limit,
            errors=["Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL."],
        )

    where_clauses = ["status = any(%s)"]
    params: list[Any] = [normalized_statuses]
    if candidate_filter:
        where_clauses.append("candidate_id = any(%s)")
        params.append(candidate_filter)
    if type_filter:
        where_clauses.append("capsule_type = any(%s)")
        params.append(type_filter)
    if work_packet_filter:
        where_clauses.append("work_packet_id::text = %s")
        params.append(work_packet_filter)
    params.append(bounded_limit)
    query = f"""
        select {_select_columns()}
        from proof_capsules
        where {" and ".join(where_clauses)}
        order by submitted_at desc
        limit %s
    """
    try:
        with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
    except UndefinedTable:
        return build_proof_capsule_list_report_from_rows(
            [],
            storage_configured=True,
            table_available=False,
            statuses=normalized_statuses,
            candidate_ids=candidate_filter,
            capsule_types=type_filter,
            limit=bounded_limit,
            errors=["proof_capsules table does not exist."],
        )

    return build_proof_capsule_list_report_from_rows(
        rows,
        statuses=normalized_statuses,
        candidate_ids=candidate_filter,
        capsule_types=type_filter,
        limit=bounded_limit,
    )


def get_proof_capsule(
    proof_capsule_id: str,
    *,
    database_url: str | None = None,
) -> dict[str, Any] | None:
    connection_string = database_url or candidate_contribution_database_url()
    if not connection_string:
        return None
    query = f"""
        select {_select_columns()}
        from proof_capsules
        where proof_capsule_id::text = %s
        limit 1
    """
    try:
        with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, [str(proof_capsule_id).strip()])
                row = cursor.fetchone()
    except UndefinedTable:
        return None
    if row is None:
        return None
    return build_proof_capsule_public_view(row)


def submit_proof_capsule(
    record: ProofCapsuleRecord,
    *,
    database_url: str | None = None,
) -> dict[str, Any]:
    """Persist a ProofCapsule record. Returns the public status view.

    Idempotency: capsules with a previously-stored content_hash are not
    inserted twice. The original public view is returned so re-submitting
    the same work is a no-op rather than a duplicate.
    """

    connection_string = database_url or candidate_contribution_database_url()
    if not connection_string:
        return {
            "stored": False,
            "storage_configured": False,
            "errors": [
                "Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL.",
            ],
            "proof_capsule": build_proof_capsule_public_view(_record_to_row(record)),
        }

    payload = _json_safe(record.model_dump(mode="json"))
    select_existing = f"""
        select {_select_columns()}
        from proof_capsules
        where content_hash = %s
        limit 1
    """
    insert_query = """
        insert into proof_capsules (
            proof_capsule_id,
            work_packet_id,
            candidate_id,
            capsule_type,
            title,
            contributor,
            task_manifest,
            candidate_snapshot_hash,
            evidence_bundle_hash,
            method_refs,
            notebook_ref,
            analysis_summary,
            findings,
            output_refs,
            artifact_manifest,
            limitations,
            conflicts_or_disclosures,
            requested_review_route,
            content_hash,
            signature,
            status,
            payload,
            submitted_at,
            updated_at
        )
        values (
            %s, %s, %s, %s, %s,
            %s::jsonb, %s::jsonb,
            %s, %s,
            %s::jsonb, %s,
            %s, %s,
            %s::jsonb, %s::jsonb,
            %s, %s,
            %s, %s, %s, %s,
            %s::jsonb,
            %s, %s
        )
        on conflict (proof_capsule_id) do nothing
    """
    try:
        with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as connection:
            with connection.cursor() as cursor:
                cursor.execute(select_existing, [record.content_hash])
                existing = cursor.fetchone()
                if existing is not None:
                    return {
                        "stored": False,
                        "storage_configured": True,
                        "duplicate_of": str(existing.get("proof_capsule_id") or ""),
                        "errors": [],
                        "proof_capsule": build_proof_capsule_public_view(existing),
                    }
                cursor.execute(
                    insert_query,
                    [
                        str(record.proof_capsule_id),
                        str(record.work_packet_id) if record.work_packet_id else None,
                        record.candidate_id,
                        record.capsule_type,
                        record.title,
                        json.dumps(record.contributor.model_dump(mode="json")),
                        json.dumps(record.task_manifest),
                        record.candidate_snapshot_hash,
                        record.evidence_bundle_hash,
                        json.dumps(record.method_refs),
                        record.notebook_ref,
                        record.analysis_summary,
                        record.findings,
                        json.dumps(record.output_refs),
                        json.dumps(
                            [artifact.model_dump(mode="json") for artifact in record.artifact_manifest]
                        ),
                        record.limitations,
                        record.conflicts_or_disclosures,
                        record.requested_review_route,
                        record.content_hash,
                        record.signature,
                        record.status,
                        json.dumps(payload),
                        record.submitted_at,
                        record.updated_at,
                    ],
                )
                connection.commit()
    except UndefinedTable:
        return {
            "stored": False,
            "storage_configured": True,
            "table_available": False,
            "errors": ["proof_capsules table does not exist; apply migration 009_proof_capsules.sql."],
            "proof_capsule": build_proof_capsule_public_view(_record_to_row(record)),
        }
    except psycopg2.Error as exc:
        return {
            "stored": False,
            "storage_configured": True,
            "errors": [f"proof_capsule submit failed: {exc}"],
            "proof_capsule": build_proof_capsule_public_view(_record_to_row(record)),
        }

    return {
        "stored": True,
        "storage_configured": True,
        "errors": [],
        "proof_capsule": build_proof_capsule_public_view(_record_to_row(record)),
    }


def _record_to_row(record: ProofCapsuleRecord) -> dict[str, Any]:
    """Project a record to a row-shaped dict for shape rendering.

    Used so the public view function can take rows from psycopg2 *and*
    in-memory records without separate code paths.
    """

    return {
        "proof_capsule_id": str(record.proof_capsule_id),
        "work_packet_id": str(record.work_packet_id) if record.work_packet_id else None,
        "candidate_id": record.candidate_id,
        "capsule_type": record.capsule_type,
        "title": record.title,
        "contributor": record.contributor.model_dump(mode="json"),
        "candidate_snapshot_hash": record.candidate_snapshot_hash,
        "evidence_bundle_hash": record.evidence_bundle_hash,
        "method_refs": record.method_refs,
        "notebook_ref": record.notebook_ref,
        "analysis_summary": record.analysis_summary,
        "findings": record.findings,
        "output_refs": record.output_refs,
        "artifact_manifest": [artifact.model_dump(mode="json") for artifact in record.artifact_manifest],
        "limitations": record.limitations,
        "requested_review_route": record.requested_review_route,
        "content_hash": record.content_hash,
        "status": record.status,
        "submitted_at": record.submitted_at,
        "updated_at": record.updated_at,
        "reviewed_at": record.reviewed_at,
    }


_DEFAULT_FRESHNESS_MAX_AGE_HOURS = 72


def _stale_proof_capsule_rows(
    *,
    database_url: str,
    max_age_hours: float,
    limit: int,
) -> list[dict[str, Any]]:
    cutoff = datetime.utcnow() - _td_hours(max_age_hours)
    query = """
        select
            proof_capsule_id::text as proof_capsule_id,
            candidate_id,
            capsule_type,
            status,
            submitted_at,
            updated_at
        from proof_capsules
        where status in ('submitted', 'in_review')
          and updated_at < %s
        order by updated_at asc
        limit %s
    """
    try:
        with psycopg2.connect(database_url, cursor_factory=RealDictCursor) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, [cutoff, limit])
                return [dict(row) for row in cursor.fetchall()]
    except UndefinedTable:
        return []


def _td_hours(hours: float):
    from datetime import timedelta

    return timedelta(hours=float(hours))


def archive_stale_proof_capsules(
    *,
    database_url: str | None = None,
    max_age_hours: float = _DEFAULT_FRESHNESS_MAX_AGE_HOURS,
    limit: int = 200,
    dry_run: bool = True,
    reviewer_id: str = "twog_freshness_sla",
    rationale: str = "stale_intake_sla",
) -> dict[str, Any]:
    """Archive capsules sitting in submitted/in_review past the freshness SLA.

    Idempotent: capsules already past the SLA but in a terminal state are
    not touched; only ``submitted`` and ``in_review`` are candidates.
    Each archived capsule gets a system review row so the audit trail is
    consistent with operator-driven archives.
    """

    connection_string = database_url or candidate_contribution_database_url()
    if not connection_string:
        return {
            "dry_run": True,
            "storage_configured": False,
            "max_age_hours": max_age_hours,
            "scanned_count": 0,
            "archived_count": 0,
            "errors": [
                "Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL.",
            ],
            "rows": [],
        }

    rows = _stale_proof_capsule_rows(
        database_url=connection_string,
        max_age_hours=max_age_hours,
        limit=limit,
    )
    if dry_run:
        return {
            "dry_run": True,
            "storage_configured": True,
            "max_age_hours": max_age_hours,
            "scanned_count": len(rows),
            "archived_count": 0,
            "errors": [],
            "rows": [_json_safe(row) for row in rows],
        }

    archived: list[dict[str, Any]] = []
    errors: list[str] = []
    for row in rows:
        proof_capsule_id = row.get("proof_capsule_id")
        if not proof_capsule_id:
            continue
        review = ProofCapsuleReviewRecord(
            proof_capsule_id=UUID(str(proof_capsule_id)),
            reviewer_type="system",
            reviewer_id=reviewer_id,
            verdict="archived",
            confidence=1.0,
            rationale=rationale,
            required_changes="",
            metadata={
                "freshness_sla_hours": float(max_age_hours),
                "original_status": str(row.get("status") or ""),
                "stale_since": str(row.get("updated_at") or ""),
            },
        )
        result = record_proof_capsule_review(review, database_url=connection_string)
        if result.get("stored"):
            archived.append(_json_safe({
                "proof_capsule_id": proof_capsule_id,
                "original_status": row.get("status"),
                "review_id": str(review.review_id),
            }))
        else:
            errors.extend(result.get("errors", []))

    return {
        "dry_run": False,
        "storage_configured": True,
        "max_age_hours": max_age_hours,
        "scanned_count": len(rows),
        "archived_count": len(archived),
        "errors": errors,
        "rows": archived,
    }


def record_proof_capsule_review(
    review: ProofCapsuleReviewRecord,
    *,
    database_url: str | None = None,
) -> dict[str, Any]:
    """Persist a review event and transition the capsule status.

    The review is append-only. The capsule status is overwritten to the
    verdict's target status, with `updated_at` and `reviewed_at` advanced.
    Reward emission lives in B5 (`reward_events.py`) and reads back from
    `proof_capsule_reviews`.
    """

    connection_string = database_url or candidate_contribution_database_url()
    if not connection_string:
        return {
            "stored": False,
            "storage_configured": False,
            "errors": [
                "Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL.",
            ],
            "review": _json_safe(review.model_dump(mode="json")),
        }

    target_status = VERDICT_TARGET_STATUS[review.verdict]
    insert_query = """
        insert into proof_capsule_reviews (
            review_id,
            proof_capsule_id,
            reviewer_type,
            reviewer_id,
            verdict,
            confidence,
            scientific_usefulness,
            provenance_strength,
            reproducibility,
            actionability,
            novelty,
            clarity,
            downstream_impact,
            rationale,
            required_changes,
            linked_agent_run_id,
            payload,
            created_at
        )
        values (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s::jsonb, %s
        )
    """
    update_capsule = """
        update proof_capsules
        set status = %s,
            updated_at = %s,
            reviewed_at = %s
        where proof_capsule_id::text = %s
    """
    try:
        with psycopg2.connect(connection_string) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    insert_query,
                    [
                        str(review.review_id),
                        str(review.proof_capsule_id),
                        review.reviewer_type,
                        review.reviewer_id,
                        review.verdict,
                        review.confidence,
                        review.scientific_usefulness,
                        review.provenance_strength,
                        review.reproducibility,
                        review.actionability,
                        review.novelty,
                        review.clarity,
                        review.downstream_impact,
                        review.rationale,
                        review.required_changes,
                        str(review.linked_agent_run_id) if review.linked_agent_run_id else None,
                        json.dumps(_json_safe(review.model_dump(mode="json"))),
                        review.created_at,
                    ],
                )
                cursor.execute(
                    update_capsule,
                    [
                        target_status,
                        review.created_at,
                        review.created_at,
                        str(review.proof_capsule_id),
                    ],
                )
                connection.commit()
    except UndefinedTable:
        return {
            "stored": False,
            "storage_configured": True,
            "table_available": False,
            "errors": ["proof_capsules / proof_capsule_reviews tables do not exist."],
            "review": _json_safe(review.model_dump(mode="json")),
        }
    except psycopg2.Error as exc:
        return {
            "stored": False,
            "storage_configured": True,
            "errors": [f"proof_capsule review failed: {exc}"],
            "review": _json_safe(review.model_dump(mode="json")),
        }

    return {
        "stored": True,
        "storage_configured": True,
        "verdict": review.verdict,
        "target_status": target_status,
        "errors": [],
        "review": _json_safe(review.model_dump(mode="json")),
    }


# --- Reward emission from ProofCapsule reviews -------------------------
#
# Delta plan §9: reward events should be emitted only from reviewed
# capsules, never from raw submissions. This block is the bridge between
# the proof_capsule_reviews table and the existing reward_events ledger.
# Negative findings (a demotion case that is accepted, a non-reproducibility
# call that holds up) still earn positive signal — what we reward is the
# *work being accepted*, not the conclusion of the work.

# Verdict → primary reward score. Used as a fallback when a review has
# no rubric dimensions populated (legacy reviews written before the
# weighted-rubric path landed). New reviews are scored via
# compute_capsule_reward_from_rubric, which differentiates a load-bearing
# finding from a thin one by weighting the seven rubric dimensions.
PROOF_CAPSULE_VERDICT_REWARD_SCORES: dict[ProofCapsuleReviewVerdict, float] = {
    "accepted": 1.0,
    "routed_to_validation": 0.9,
    "routed_to_compute_review": 0.9,
    "needs_changes": 0.55,
    "rejected": 0.0,
    "archived": 0.3,
}

# Verdict gate multiplier for the rubric-weighted reward. Accepted gets
# full weight, routed gets a small haircut, needs_changes/rejected get
# zero (no proof points for unaccepted work).
_PROOF_CAPSULE_VERDICT_MULTIPLIER: dict[ProofCapsuleReviewVerdict, float] = {
    "accepted": 1.0,
    "routed_to_validation": 0.9,
    "routed_to_compute_review": 0.9,
    "needs_changes": 0.0,
    "rejected": 0.0,
    "archived": 0.3,
}

# Per-rubric weights for the weighted-sum reward. Sum to 1.0 so an
# all-1.0 capsule on an accepted verdict yields a 100 pp reward
# (matching the historical flat reward) — but a capsule that scores
# 0.5 across the board only earns 50 pp, and a high-quality one with
# strong scientific_usefulness + provenance + actionability can earn
# more than the legacy flat 100 pp.
_PROOF_CAPSULE_RUBRIC_WEIGHTS: dict[str, float] = {
    "scientific_usefulness": 0.25,
    "provenance_strength": 0.20,
    "actionability": 0.20,
    "reproducibility": 0.10,
    "novelty": 0.10,
    "downstream_impact": 0.10,
    "clarity": 0.05,
}


def compute_capsule_reward_from_rubric(
    review: "ProofCapsuleReviewRecord",
    verdict: ProofCapsuleReviewVerdict,
) -> float:
    """Per-rubric weighted reward, gated by verdict.

    Reads the seven rubric dimensions off the review (each in [0, 1]
    when present, None otherwise). Returns a score in [0, 1] that
    feeds the existing dimension_scores["overall"] → compute_proof_points
    pipeline (which multiplies by 100). When the review has no rubric
    dimensions populated at all, falls back to the flat
    PROOF_CAPSULE_VERDICT_REWARD_SCORES to preserve historical reward
    behavior for legacy reviews.
    """

    multiplier = _PROOF_CAPSULE_VERDICT_MULTIPLIER.get(verdict, 0.0)
    if multiplier == 0.0:
        return 0.0

    weighted_sum = 0.0
    weight_total = 0.0
    for dimension, weight in _PROOF_CAPSULE_RUBRIC_WEIGHTS.items():
        value = getattr(review, dimension, None)
        if value is None:
            continue
        clamped = max(0.0, min(1.0, float(value)))
        weighted_sum += weight * clamped
        weight_total += weight

    # No rubric dims populated at all → fall back to the flat lookup so
    # legacy reviews keep their historical reward. (Verdict gate is
    # already baked into the flat lookup.)
    if weight_total == 0.0:
        return PROOF_CAPSULE_VERDICT_REWARD_SCORES.get(verdict, 0.0)

    # Renormalize when some dimensions are missing so a review with
    # only 5 of 7 dims populated still scales to [0, 1] over the
    # weights that are present.
    base_score = weighted_sum / weight_total if weight_total > 0 else 0.0
    return round(base_score * multiplier, 4)

# Map the ProofCapsuleReviewVerdict onto the AgentRunReviewVerdict scheme so
# the reward ledger and existing report aggregations can reason uniformly.
_VERDICT_TO_AGENT_RUN_VERDICT: dict[ProofCapsuleReviewVerdict, AgentRunReviewVerdict] = {
    "accepted": "useful",
    "routed_to_validation": "useful",
    "routed_to_compute_review": "useful",
    "needs_changes": "needs_followup",
    "rejected": "bad",
    "archived": "unclear",
}

_VERDICT_TO_OUTCOME_BUCKET: dict[ProofCapsuleReviewVerdict, RewardOutcomeBucket] = {
    "accepted": "positive_signal",
    "routed_to_validation": "positive_signal",
    "routed_to_compute_review": "positive_signal",
    "needs_changes": "actionable_followup",
    "rejected": "negative_signal",
    "archived": "unclear_signal",
}

_OUTCOME_TO_ROUTING: dict[RewardOutcomeBucket, RewardRoutingRecommendation] = {
    "positive_signal": "prefer_lane",
    "actionable_followup": "queue_targeted_followup",
    "low_value_churn": "demote_or_rewrite",
    "negative_signal": "suppress_or_archive",
    "unclear_signal": "inspect_manually",
}

# Proof capsule rubric dimension fields. Kept here (not just in
# contracts.py) so the reward emitter is the single source of truth for
# which review fields participate in the reward dimension scores.
_PROOF_CAPSULE_RUBRIC_DIMENSIONS: tuple[str, ...] = (
    "scientific_usefulness",
    "provenance_strength",
    "reproducibility",
    "actionability",
    "novelty",
    "clarity",
    "downstream_impact",
)


def _churn_risk_score_for_capsule(
    outcome_bucket: RewardOutcomeBucket,
    actionability: float | None,
) -> float:
    if outcome_bucket == "negative_signal":
        return 1.0
    if outcome_bucket == "low_value_churn":
        if actionability is None:
            return 0.7
        return round(max(0.65, 1.0 - float(actionability)), 3)
    if outcome_bucket == "unclear_signal":
        return 0.6
    if outcome_bucket == "actionable_followup":
        return 0.25
    return 0.0


def reward_event_from_proof_capsule_review(
    review: ProofCapsuleReviewRecord,
    *,
    candidate_id: str,
    work_packet_id: UUID | None,
    capsule_type: str,
    contributor_handle: str | None,
    contributor_kind: str | None,
    created_by: str = "proof_capsule_review_sync",
) -> RewardEventRecord:
    """Deterministically convert one proof-capsule review into a reward event.

    Idempotency: the identity_key derives from review_id, so the same
    review always produces the same reward_event_id (uuid5 over the key).
    """

    # llm_evaluator reviews are advisory and do not award proof points.
    # They are stored as recommendations for the operator to adopt or
    # override; sync_reward_events_from_proof_capsule_reviews filters
    # them out so they never become reward events.
    score = compute_capsule_reward_from_rubric(review, review.verdict)
    identity_key = f"reward:proof_capsule_review:review:{review.review_id}"
    dimension_scores: dict[str, float] = {"overall": score}
    for dimension in _PROOF_CAPSULE_RUBRIC_DIMENSIONS:
        value = getattr(review, dimension, None)
        if value is not None:
            dimension_scores[dimension] = float(value)
    outcome_bucket = _VERDICT_TO_OUTCOME_BUCKET[review.verdict]
    actionability = review.actionability
    return RewardEventRecord(
        reward_event_id=uuid5(NAMESPACE_URL, identity_key),
        identity_key=identity_key,
        event_source="proof_capsule_review",
        score=score,
        dimension_scores=dimension_scores,  # type: ignore[arg-type]
        verdict=_VERDICT_TO_AGENT_RUN_VERDICT[review.verdict],
        agent_run_id=review.linked_agent_run_id,
        source_review_id=review.review_id,
        candidate_id=candidate_id,
        agent_name=(
            f"proof_capsule_contributor:{contributor_handle}"
            if contributor_handle and contributor_kind == "agent"
            else None
        ),
        task_type="proof_capsule_review",
        outcome_bucket=outcome_bucket,
        routing_recommendation=_OUTCOME_TO_ROUTING[outcome_bucket],
        churn_risk_score=_churn_risk_score_for_capsule(outcome_bucket, actionability),
        rationale=review.rationale,
        tags=[
            review.reviewer_type,
            capsule_type,
            f"capsule:{review.proof_capsule_id}",
        ],
        created_by=created_by,
        created_at=review.created_at,
        metadata={
            "proof_capsule_id": str(review.proof_capsule_id),
            "work_packet_id": str(work_packet_id) if work_packet_id else None,
            "capsule_type": capsule_type,
            "contributor_handle": contributor_handle,
            "contributor_kind": contributor_kind,
            "review_verdict": review.verdict,
            "reviewer_type": review.reviewer_type,
            "reviewer_id": review.reviewer_id,
            "required_changes": review.required_changes or None,
        },
    )


def list_proof_capsule_review_contexts(
    *,
    database_url: str | None = None,
    limit: int | None = None,
    reviewer_type: str | None = None,
) -> list[dict[str, Any]]:
    """Read proof_capsule_reviews joined with proof_capsules.

    Returns a list of context dicts that downstream code can convert into
    ProofCapsuleReviewRecord + capsule context for reward emission.
    """

    connection_string = database_url or candidate_contribution_database_url()
    if not connection_string:
        return []

    bounded_limit = max(1, min(int(limit or 500), 5000))
    where_clauses: list[str] = []
    params: list[Any] = []
    if reviewer_type:
        where_clauses.append("r.reviewer_type = %s")
        params.append(reviewer_type)
    params.append(bounded_limit)
    query = f"""
        select
            r.review_id::text as review_id,
            r.proof_capsule_id::text as proof_capsule_id,
            r.reviewer_type,
            r.reviewer_id,
            r.verdict,
            r.confidence,
            r.scientific_usefulness,
            r.provenance_strength,
            r.reproducibility,
            r.actionability,
            r.novelty,
            r.clarity,
            r.downstream_impact,
            r.rationale,
            r.required_changes,
            r.linked_agent_run_id::text as linked_agent_run_id,
            r.payload as review_payload,
            r.created_at,
            c.work_packet_id::text as work_packet_id,
            c.candidate_id,
            c.capsule_type,
            c.contributor
        from proof_capsule_reviews r
        join proof_capsules c on c.proof_capsule_id = r.proof_capsule_id
        {("where " + " and ".join(where_clauses)) if where_clauses else ""}
        order by r.created_at desc
        limit %s
    """
    try:
        with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
    except UndefinedTable:
        return []


def _review_record_from_context(context: Mapping[str, Any]) -> ProofCapsuleReviewRecord:
    """Hydrate a typed ProofCapsuleReviewRecord from a joined context dict."""

    def _float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    return ProofCapsuleReviewRecord(
        review_id=UUID(str(context["review_id"])),
        proof_capsule_id=UUID(str(context["proof_capsule_id"])),
        reviewer_type=context["reviewer_type"],
        reviewer_id=str(context["reviewer_id"]),
        verdict=context["verdict"],
        confidence=float(context.get("confidence") or 0.5),
        scientific_usefulness=_float(context.get("scientific_usefulness")),
        provenance_strength=_float(context.get("provenance_strength")),
        reproducibility=_float(context.get("reproducibility")),
        actionability=_float(context.get("actionability")),
        novelty=_float(context.get("novelty")),
        clarity=_float(context.get("clarity")),
        downstream_impact=_float(context.get("downstream_impact")),
        rationale=str(context.get("rationale") or ""),
        required_changes=str(context.get("required_changes") or ""),
        linked_agent_run_id=(
            UUID(str(context["linked_agent_run_id"]))
            if context.get("linked_agent_run_id")
            else None
        ),
        created_at=context.get("created_at") or datetime.now(),
        metadata={},
    )


def sync_reward_events_from_proof_capsule_reviews(
    repository: Any,
    *,
    database_url: str | None = None,
    limit: int = 500,
    reviewer_type: str | None = None,
    include_existing: bool = False,
    created_by: str = "proof_capsule_review_sync",
) -> dict[str, Any]:
    """Convert proof-capsule reviews into idempotent reward events.

    The repository persists the events through its normal create_reward_event
    path so SQLite local runs and Postgres hosted runs stay symmetric. Reads
    of the proof_capsule_reviews table happen via psycopg2 because that
    table is the public-site mirror, not part of the research repository
    schema.
    """

    contexts = list_proof_capsule_review_contexts(
        database_url=database_url,
        limit=limit,
        reviewer_type=reviewer_type,
    )
    scanned = len(contexts)
    eligible = 0
    created = 0
    skipped_existing = 0
    errors: list[str] = []
    reward_event_ids: list[str] = []

    for context in contexts:
        # llm_evaluator reviews are advisory recommendations, not source-of-truth
        # verdicts. They never award proof points; the operator's manual review
        # is what counts. Skip them in the reward-event sync.
        if str(context.get("reviewer_type") or "").lower() == "llm_evaluator":
            continue
        try:
            review = _review_record_from_context(context)
            contributor_raw = context.get("contributor")
            if isinstance(contributor_raw, str):
                try:
                    contributor_raw = json.loads(contributor_raw)
                except (TypeError, ValueError):
                    contributor_raw = {}
            if not isinstance(contributor_raw, dict):
                contributor_raw = {}
            work_packet_id = context.get("work_packet_id")
            event = reward_event_from_proof_capsule_review(
                review,
                candidate_id=str(context["candidate_id"]),
                work_packet_id=UUID(str(work_packet_id)) if work_packet_id else None,
                capsule_type=str(context["capsule_type"]),
                contributor_handle=contributor_raw.get("handle"),
                contributor_kind=contributor_raw.get("kind"),
                created_by=created_by,
            )
        except Exception as exc:  # noqa: BLE001 — capture and continue
            errors.append(f"review {context.get('review_id')}: {exc}")
            continue

        eligible += 1
        existing = repository.list_reward_events(source_review_id=review.review_id, limit=1)
        if existing and not include_existing:
            skipped_existing += 1
            continue
        try:
            persisted = repository.create_reward_event(event)
            created += 1
            reward_event_ids.append(str(persisted.reward_event_id))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"persist review {context.get('review_id')}: {exc}")

    return {
        "scanned_review_count": scanned,
        "eligible_review_count": eligible,
        "created_count": created,
        "skipped_existing_count": skipped_existing,
        "reward_event_ids": reward_event_ids,
        "errors": errors,
    }
