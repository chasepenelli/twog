"""Client-side ProofCapsule content_hash.

Mirrors the server's hashing algorithm in
``hsa_research.ingestion_bridge.contracts._proof_capsule_content_hash`` so
that an agent can compute the same canonical digest *before* submission
and sign it. The contract test
``test_twog_agent_client_hash_matches_server`` pins these two
implementations together — if either drifts, the test breaks.

Inputs participate in the hash exactly as they do server-side:

- text fields are stripped
- list fields are normalized (strip + casefold-deduped, order preserved)
- ``None`` is preserved as ``null`` in the digest payload
- artifact manifest entries contribute label / content_hash / mime_type / size_bytes

Anything not in this list (e.g. ``conflicts_or_disclosures``,
``task_manifest``, timestamps, signatures) is **not** part of the hash
and can change without invalidating the capsule identity.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def _strip_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _strip_or_empty(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalized_unique_strings(values: Any) -> list[str]:
    """Strip, casefold-dedupe, preserve original order.

    Mirrors ``contracts._normalized_unique_strings``.
    """

    if not isinstance(values, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        normalized = str(raw).strip()
        key = normalized.casefold()
        if not normalized or key in seen:
            continue
        out.append(normalized)
        seen.add(key)
    return out


def _artifact_manifest_digest(artifact_manifest: Any) -> list[dict[str, Any]]:
    if not isinstance(artifact_manifest, list):
        return []
    out: list[dict[str, Any]] = []
    for artifact in artifact_manifest:
        if not isinstance(artifact, dict):
            continue
        size = artifact.get("size_bytes")
        out.append(
            {
                "label": _strip_or_empty(artifact.get("label")),
                "content_hash": _strip_or_empty(artifact.get("content_hash")),
                "mime_type": _strip_or_none(artifact.get("mime_type")),
                "size_bytes": int(size) if isinstance(size, int) else None,
            }
        )
    return out


def compute_proof_capsule_content_hash(body: dict[str, Any]) -> str:
    """Compute the canonical content_hash for a capsule submission body.

    Accepts the same shape ``POST /api/proof-capsules`` accepts. The body
    may be the un-normalized submission packet — we apply the same
    normalization here as the server does.
    """

    contributor = body.get("contributor") or {}
    contributor_handle = (
        _strip_or_none(contributor.get("handle")) if isinstance(contributor, dict) else None
    )
    work_packet_id = body.get("work_packet_id")
    digest_payload = {
        "candidate_id": _strip_or_empty(body.get("candidate_id")),
        "work_packet_id": str(work_packet_id) if work_packet_id else None,
        "capsule_type": _strip_or_empty(body.get("capsule_type")),
        "title": _strip_or_empty(body.get("title")),
        "analysis_summary": _strip_or_empty(body.get("analysis_summary")),
        "findings": _strip_or_empty(body.get("findings")),
        "limitations": _strip_or_empty(body.get("limitations")),
        "candidate_snapshot_hash": _strip_or_none(body.get("candidate_snapshot_hash")),
        "evidence_bundle_hash": _strip_or_none(body.get("evidence_bundle_hash")),
        "method_refs": _normalized_unique_strings(body.get("method_refs")),
        "output_refs": _normalized_unique_strings(body.get("output_refs")),
        "notebook_ref": _strip_or_none(body.get("notebook_ref")),
        "artifact_manifest": _artifact_manifest_digest(body.get("artifact_manifest")),
        "contributor_handle": contributor_handle,
    }
    serialized = json.dumps(digest_payload, sort_keys=True, default=str)
    return "sha256:" + hashlib.sha256(serialized.encode("utf-8")).hexdigest()
