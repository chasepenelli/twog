"""Proof Network — contributor profiles.

Contributor identity is captured inline on ProofCapsule submissions
(handle, name, affiliation). This module derives a profile and proof-point
ledger view for a given handle by aggregating the contributor's accepted
proof capsules and the reward events emitted from their reviewed work.

v1 has no separate contributor_profiles table on purpose: every fact about
a contributor can be reconstructed from the proof_capsules and
reward_events tables. A registration table can be added later without
touching this aggregation logic.

See deliverables/TWOG_Proof_Network_Techtree_Delta_Plan.md section 6 and
TWOG_Nookplot_Aligned_Proof_Network_Vision.md "Reputation Model" for the
intended semantics.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, datetime
import json
from typing import Any
from uuid import UUID

import psycopg2
from psycopg2.errors import UndefinedTable
from psycopg2.extras import RealDictCursor

from .candidate_contribution_intake import candidate_contribution_database_url


CONTRIBUTOR_TIERS: tuple[str, ...] = (
    "observer",
    "scout",
    "citation_repairer",
    "record_builder",
    "replication_contributor",
    "validation_contributor",
    "trusted_reviewer",
    "proof_partner",
)

# Capsule types that elevate a contributor into a specialty tier.
_REPLICATION_TYPES = {"docking_replication", "md_review"}
_VALIDATION_TYPES = {"validation_proposal"}
_CITATION_TYPES = {"citation_repair"}


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


def _contributor_view(value: Any) -> dict[str, Any]:
    raw = _decode_jsonish(value)
    if not isinstance(raw, dict):
        raw = {}
    return {
        "kind": raw.get("kind") or "human",
        "name": raw.get("name"),
        "handle": raw.get("handle"),
        "affiliation": raw.get("affiliation"),
    }


def _public_capsule_row(row: Mapping[str, Any]) -> dict[str, Any]:
    capsule_id = str(row.get("proof_capsule_id") or "")
    return _json_safe(
        {
            "proof_capsule_id": capsule_id,
            "candidate_id": row.get("candidate_id"),
            "capsule_type": row.get("capsule_type"),
            "title": row.get("title"),
            "status": row.get("status"),
            "submitted_at": row.get("submitted_at"),
            "reviewed_at": row.get("reviewed_at"),
            "status_url": f"/api/proof-capsules/{capsule_id}" if capsule_id else None,
        }
    )


def compute_proof_points(reward_event_scores: Sequence[float]) -> int:
    """Proof points = round(100 * sum(reward scores)).

    A single accepted capsule with score 1.0 → 100 proof points. Routed
    capsules contribute 90 each. Rejected work contributes 0. The unit is
    intentionally chunky so reputation differences read cleanly in the UI.
    """

    if not reward_event_scores:
        return 0
    total = sum(float(score) for score in reward_event_scores)
    return int(round(total * 100))


def compute_tier(
    *,
    accepted_count: int,
    routed_count: int,
    distinct_candidates: int,
    capsule_type_counts: Mapping[str, int],
    proof_points: int,
) -> str:
    """Map contributor stats to a named tier.

    Tiers are ordered most-prestigious-first; the function returns the
    highest tier the contributor qualifies for. The intent is that a
    contributor's tier should never decrease as they add accepted work,
    so each rule is monotonic in the inputs.
    """

    positive_total = accepted_count + routed_count
    has_replication = any(capsule_type_counts.get(t, 0) > 0 for t in _REPLICATION_TYPES)
    has_validation = any(capsule_type_counts.get(t, 0) > 0 for t in _VALIDATION_TYPES)
    has_citation = any(capsule_type_counts.get(t, 0) > 0 for t in _CITATION_TYPES)
    if positive_total >= 20 and proof_points >= 1500:
        return "proof_partner"
    if positive_total >= 10:
        return "trusted_reviewer"
    if has_validation and positive_total >= 2:
        return "validation_contributor"
    if has_replication and positive_total >= 2:
        return "replication_contributor"
    if positive_total >= 5 and distinct_candidates >= 2:
        return "record_builder"
    if has_citation and positive_total >= 3:
        return "citation_repairer"
    if positive_total >= 1:
        return "scout"
    return "observer"


def build_contributor_profile_from_rows(
    *,
    handle: str,
    capsule_rows: Sequence[Mapping[str, Any]],
    reward_event_rows: Sequence[Mapping[str, Any]],
    storage_configured: bool = True,
    table_available: bool = True,
    errors: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Pure aggregator. Renders the public contributor profile."""

    normalized_handle = handle.strip()
    accepted_rows: list[dict[str, Any]] = []
    routed_rows: list[dict[str, Any]] = []
    capsule_type_counts: Counter[str] = Counter()
    candidate_ids: set[str] = set()
    display_name: str | None = None
    affiliation: str | None = None
    kind: str | None = None

    for row in capsule_rows:
        contributor = _contributor_view(row.get("contributor"))
        if contributor.get("handle") != normalized_handle:
            continue
        status = str(row.get("status") or "")
        if status == "accepted":
            accepted_rows.append(_public_capsule_row(row))
        elif status in ("routed_to_validation", "routed_to_compute_review"):
            routed_rows.append(_public_capsule_row(row))
        else:
            continue
        candidate_id = row.get("candidate_id")
        if candidate_id:
            candidate_ids.add(str(candidate_id))
        capsule_type = str(row.get("capsule_type") or "")
        if capsule_type:
            capsule_type_counts[capsule_type] += 1
        if display_name is None and contributor.get("name"):
            display_name = contributor.get("name")
        if affiliation is None and contributor.get("affiliation"):
            affiliation = contributor.get("affiliation")
        if kind is None and contributor.get("kind"):
            kind = contributor.get("kind")

    reward_scores: list[float] = []
    reward_outcome_counts: Counter[str] = Counter()
    for event in reward_event_rows:
        metadata = _decode_jsonish(event.get("metadata"))
        if isinstance(metadata, dict) and metadata.get("contributor_handle") == normalized_handle:
            try:
                reward_scores.append(float(event.get("score") or 0.0))
            except (TypeError, ValueError):
                continue
            bucket = event.get("outcome_bucket")
            if isinstance(bucket, str):
                reward_outcome_counts[bucket] += 1

    proof_points = compute_proof_points(reward_scores)
    accepted_total = len(accepted_rows) + len(routed_rows)
    tier = compute_tier(
        accepted_count=len(accepted_rows),
        routed_count=len(routed_rows),
        distinct_candidates=len(candidate_ids),
        capsule_type_counts=capsule_type_counts,
        proof_points=proof_points,
    )

    strongest = max(
        (event for event in reward_event_rows
         if isinstance(_decode_jsonish(event.get("metadata")), dict)
         and (_decode_jsonish(event.get("metadata")) or {}).get("contributor_handle") == normalized_handle),
        key=lambda event: float(event.get("score") or 0.0),
        default=None,
    )
    strongest_summary: dict[str, Any] | None = None
    if strongest is not None:
        meta = _decode_jsonish(strongest.get("metadata")) or {}
        strongest_summary = {
            "score": float(strongest.get("score") or 0.0),
            "proof_capsule_id": meta.get("proof_capsule_id"),
            "capsule_type": meta.get("capsule_type"),
            "candidate_id": strongest.get("candidate_id"),
            "rationale": strongest.get("rationale"),
        }

    accepted_rows.sort(key=lambda row: row.get("reviewed_at") or row.get("submitted_at") or "", reverse=True)
    routed_rows.sort(key=lambda row: row.get("reviewed_at") or row.get("submitted_at") or "", reverse=True)

    return _json_safe(
        {
            "schema_version": "twog-contributor-profile-v1",
            "storage_configured": storage_configured,
            "table_available": table_available,
            "handle": normalized_handle,
            "display_name": display_name,
            "affiliation": affiliation,
            "kind": kind or "human",
            "tier": tier,
            "proof_points": proof_points,
            "summary": {
                "accepted_capsule_count": len(accepted_rows),
                "routed_capsule_count": len(routed_rows),
                "candidate_count": len(candidate_ids),
                "reward_event_count": len(reward_scores),
                "outcome_counts": dict(reward_outcome_counts),
            },
            "capsule_type_counts": dict(capsule_type_counts),
            "candidates": sorted(candidate_ids),
            "strongest_accepted_work": strongest_summary,
            "accepted_capsules": accepted_rows,
            "routed_capsules": routed_rows,
            "profile_url": f"/contributors/{normalized_handle}",
            "errors": list(errors or []),
        }
    )


def get_contributor_profile_by_handle(
    handle: str,
    *,
    database_url: str | None = None,
    capsule_limit: int = 100,
    reward_limit: int = 500,
) -> dict[str, Any]:
    """Read accepted capsules + reward events for one contributor handle."""

    normalized_handle = (handle or "").strip()
    if not normalized_handle:
        return build_contributor_profile_from_rows(
            handle="",
            capsule_rows=[],
            reward_event_rows=[],
            storage_configured=False,
            table_available=False,
            errors=["handle is required"],
        )

    connection_string = database_url or candidate_contribution_database_url()
    if not connection_string:
        return build_contributor_profile_from_rows(
            handle=normalized_handle,
            capsule_rows=[],
            reward_event_rows=[],
            storage_configured=False,
            table_available=False,
            errors=["Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL."],
        )

    capsule_query = """
        select
            proof_capsule_id::text as proof_capsule_id,
            candidate_id,
            capsule_type,
            title,
            contributor,
            status,
            submitted_at,
            reviewed_at
        from proof_capsules
        where contributor ->> 'handle' = %s
          and status in ('accepted', 'routed_to_validation', 'routed_to_compute_review')
        order by reviewed_at desc nulls last, submitted_at desc
        limit %s
    """
    # NOTE: reward_events stores candidate_id / outcome_bucket / rationale
    # inside the ``payload`` jsonb (per postgres_store.py); they are not
    # columns on the table. Extract them via jsonb accessors so the SELECT
    # matches the actual schema.
    reward_query = """
        select
            reward_event_id::text as reward_event_id,
            event_source,
            score,
            (payload ->> 'candidate_id') as candidate_id,
            agent_name,
            source_review_id::text as source_review_id,
            (payload ->> 'outcome_bucket') as outcome_bucket,
            (payload ->> 'rationale') as rationale,
            payload as metadata
        from reward_events
        where event_source = 'proof_capsule_review'
          and (payload -> 'metadata' ->> 'contributor_handle') = %s
        order by created_at desc
        limit %s
    """

    capsule_rows: list[dict[str, Any]] = []
    reward_rows: list[dict[str, Any]] = []
    errors: list[str] = []
    table_available = True

    try:
        with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as connection:
            with connection.cursor() as cursor:
                try:
                    cursor.execute(capsule_query, [normalized_handle, capsule_limit])
                    capsule_rows = [dict(row) for row in cursor.fetchall()]
                except UndefinedTable:
                    errors.append("proof_capsules table does not exist.")
                    table_available = False
                try:
                    cursor.execute(reward_query, [normalized_handle, reward_limit])
                    raw_rows = cursor.fetchall()
                    # The payload column carries the full RewardEventRecord
                    # JSON; lift its nested metadata up so the aggregator can
                    # read it through a uniform `metadata` key.
                    for row in raw_rows:
                        merged = dict(row)
                        payload = _decode_jsonish(row.get("metadata"))
                        if isinstance(payload, dict):
                            merged["metadata"] = payload.get("metadata") or {}
                        else:
                            merged["metadata"] = {}
                        reward_rows.append(merged)
                except UndefinedTable:
                    errors.append("reward_events table does not exist.")
    except psycopg2.Error as exc:
        errors.append(f"contributor profile query failed: {exc}")

    return build_contributor_profile_from_rows(
        handle=normalized_handle,
        capsule_rows=capsule_rows,
        reward_event_rows=reward_rows,
        storage_configured=True,
        table_available=table_available,
        errors=errors,
    )


def tier_label(tier: str) -> str:
    """Human-readable tier label (used by the UI)."""

    return {
        "observer": "Observer",
        "scout": "Scout",
        "citation_repairer": "Citation Repairer",
        "record_builder": "Record Builder",
        "replication_contributor": "Replication Contributor",
        "validation_contributor": "Validation Contributor",
        "trusted_reviewer": "Trusted Reviewer",
        "proof_partner": "Proof Partner",
    }.get(tier, tier.replace("_", " ").title())
