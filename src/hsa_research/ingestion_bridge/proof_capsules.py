"""ProofCapsule check-in validation for research workspaces."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
import hashlib
import json
import re
from typing import Any

from .contracts import (
    ProofCapsuleLibraryRequest,
    ProofCapsuleLibraryResult,
    ProofCapsuleRecord,
    ProofCapsuleSubmitRequest,
    ProofCapsuleSubmitResult,
    ResearchWorkspaceRecord,
)
from .repository import ResearchRepository


_EXTERNAL_PAYLOAD_MAX_BYTES = 1_000_000  # cap on a BYOC capsule's freeform payload+metadata (results, not bulk data)

_SECRET_PATTERNS = [
    re.compile(r"\bpostgres(?:ql)?://", re.IGNORECASE),
    re.compile(r"\b(?:database_url|postgres_url|neon_database_url|openrouter_api_key|provider_api_key|gh_token)\b", re.IGNORECASE),
    re.compile(r"\brpa_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bnapi_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bnpg_[A-Za-z0-9]{8,}\b"),
]


def submit_proof_capsule(
    repository: ResearchRepository,
    request: ProofCapsuleSubmitRequest,
    *,
    external_submission: bool = False,
) -> ProofCapsuleSubmitResult:
    """Validate and optionally persist a structured workspace check-in.

    `external_submission=True` marks a capsule produced OUTSIDE our trusted compute (a collaborator's
    own compute / BYOC, Phase B): it must clear the authenticated-submit gate (registered + scoped +
    leased + Ed25519-signed). Operator-hosted compute (the internal validation flow) leaves this False
    — its trust comes from running on our infra + the terminal human accept/promote gate."""

    now = datetime.now(UTC)
    workspace = repository.get_research_workspace(request.workspace_id)
    errors = _validate_request_against_workspace(request, workspace)
    if _contains_raw_secret(request.model_dump(mode="json")):
        errors.append("ProofCapsule payload appears to contain a raw secret or database URL.")
    if errors:
        return ProofCapsuleSubmitResult(
            workspace=workspace,
            accepted=False,
            persisted=False,
            errors=errors,
            created_at=now,
        )

    assert workspace is not None
    if external_submission:
        # A BYOC submitter signs capsule_content_hash(request) over the REQUEST's OWN values. Applying
        # the workspace fallback here would make the stored/verified hash diverge from what they signed
        # — falsely rejecting a legitimate submit that simply omitted an optional field. So for external
        # submissions the hash is request-only (the collaborator signs exactly what the gate verifies).
        work_packet_id = request.work_packet_id
        candidate_snapshot_hash = request.candidate_snapshot_hash
    else:
        work_packet_id = request.work_packet_id or workspace.work_packet_id
        candidate_snapshot_hash = request.candidate_snapshot_hash or workspace.candidate_snapshot_hash
    content_hash = _capsule_content_hash(request, work_packet_id, candidate_snapshot_hash)

    # Phase B trust boundary: a capsule produced on a collaborator's OWN compute (external_submission)
    # is admitted ONLY from an authenticated, leased, signing submitter. Operator-hosted compute (the
    # internal validation flow, external_submission=False) is unaffected — the solo-operator path is
    # unchanged. The signature is verified over the content_hash computed just above.
    if external_submission:
        gate_errors = _enforce_external_submit_gate(repository, request, content_hash, now)
        if gate_errors:
            return ProofCapsuleSubmitResult(
                workspace=workspace,
                accepted=False,
                persisted=False,
                errors=gate_errors,
                created_at=now,
            )
        # Idempotency: a re-submit of byte-identical content (same content_hash) returns the EXISTING
        # capsule rather than inserting a duplicate. Without this an external party could flood the
        # ledger with copies of one supporting capsule to manufacture apparent evidence weight. Scoped
        # to external submissions so the internal operator flow is unchanged.
        if request.persist:
            duplicate = next(
                (
                    existing
                    for existing in repository.list_proof_capsules(candidate_id=request.candidate_id, limit=500)
                    if existing.content_hash == content_hash
                ),
                None,
            )
            if duplicate is not None:
                return ProofCapsuleSubmitResult(
                    capsule=duplicate, workspace=workspace, accepted=True, persisted=True,
                    errors=[], created_at=now,
                )

    capsule = ProofCapsuleRecord(
        workspace_id=request.workspace_id,
        checkout_manifest_hash=request.checkout_manifest_hash,
        candidate_id=request.candidate_id,
        candidate_snapshot_hash=candidate_snapshot_hash,
        work_packet_id=work_packet_id,
        packet_type=request.packet_type,
        requested_action=request.requested_action,
        producer=request.producer,
        submitted_by=request.submitted_by,  # Phase 4: actor provenance (excluded from content_hash)
        # provenance wrappers (excluded from content_hash by construction): edit-chain link + signature
        parent_content_hash=request.parent_content_hash,
        lineage_index=request.lineage_index,
        signature=request.signature,
        target=request.target,
        summary=request.summary,
        payload=request.payload,
        artifacts=request.artifacts,
        source_refs=request.source_refs,
        conflicts=request.conflicts,
        limitations=request.limitations,
        content_hash=content_hash,
        created_at=now,
        updated_at=now,
        metadata={
            **request.metadata,
            "source_workspace_status": workspace.status,
            "source_workspace_provider": workspace.provider,
        },
    )

    persisted = False
    if request.persist:
        capsule = repository.upsert_proof_capsule(capsule)
        workspace = repository.upsert_research_workspace(
            workspace.model_copy(
                update={
                    "status": "submitted",
                    "submitted_proof_capsule_id": capsule.capsule_id,
                    "updated_at": now,
                    "metadata": {
                        **workspace.metadata,
                        "proof_capsule": {
                            "capsule_id": str(capsule.capsule_id),
                            "content_hash": capsule.content_hash,
                            "packet_type": capsule.packet_type,
                            "requested_action": capsule.requested_action,
                            "submitted_at": now.isoformat(),
                        },
                    },
                }
            )
        )
        persisted = True

    return ProofCapsuleSubmitResult(
        capsule=capsule,
        workspace=workspace,
        accepted=True,
        persisted=persisted,
        errors=[],
        created_at=now,
    )


def _enforce_external_submit_gate(
    repository: ResearchRepository,
    request: ProofCapsuleSubmitRequest,
    content_hash: str,
    now: datetime,
) -> list[str]:
    """Phase B write-gate for external_collaborator workspaces. The submitter must (1) be a registered
    ACTIVE collaborator that (2) holds the `submit_capsule` scope, (3) currently hold the workspace's
    lease, and (4) sign the content_hash with the Ed25519 private key matching their registered
    public_key. Returns a list of human-readable errors ([] = pass). This is what makes opening the
    system safe: a foreign capsule cannot enter unless its authorship + binding are cryptographically
    proven. Validity (is the science right?) stays the job of the confound/provenance auditors + the
    human accept gate — this only proves WHO produced it and that it is bound to a leased sandbox."""
    from . import provenance

    # DoS guard: an external capsule carries bounded structured RESULTS, not bulk data — large artifacts
    # belong by reference (URL/hash), not inlined. Cap the freeform payload+metadata so an open
    # submitter cannot flood storage with a giant blob. (The internal operator flow is uncapped.)
    payload_bytes = len(json.dumps(request.payload, default=str)) + len(json.dumps(request.metadata, default=str))
    if payload_bytes > _EXTERNAL_PAYLOAD_MAX_BYTES:
        return [
            f"external capsule payload is too large ({payload_bytes} bytes > {_EXTERNAL_PAYLOAD_MAX_BYTES} "
            f"limit); reference bulk artifacts by URL/hash instead of inlining them."
        ]

    principal = (request.submitted_by or "").strip()
    if not principal:
        return ["external_collaborator workspace requires submitted_by (a registered principal)."]
    collaborator = repository.get_collaborator_by_principal(principal)
    if collaborator is None:
        return [f"submitter '{principal}' is not a registered collaborator (access is deny-by-default here)."]

    errors: list[str] = []
    if collaborator.status != "active":
        errors.append(f"collaborator '{principal}' is '{collaborator.status}', not active.")
    if not collaborator.has_scope("submit_capsule"):  # type: ignore[arg-type]
        errors.append(f"collaborator '{principal}' lacks the 'submit_capsule' scope.")

    ws = repository.get_research_workspace(request.workspace_id)
    lease_held = bool(
        ws is not None
        and ws.leased_by == principal
        and ws.lease_expires_at is not None
        and ws.lease_expires_at > now
    )
    if not lease_held:
        errors.append(f"submitter '{principal}' does not hold an active lease on workspace {request.workspace_id}.")

    if not request.signature:
        errors.append("external_collaborator submit requires an Ed25519 signature over the content_hash.")
    elif not collaborator.public_key:
        errors.append(f"collaborator '{principal}' has no registered public_key to verify the signature against.")
    elif not provenance.verify(content_hash, request.signature, collaborator.public_key):
        errors.append("capsule signature does not verify against the submitter's registered public_key.")

    return errors


def build_proof_capsule_library(
    repository: ResearchRepository,
    request: ProofCapsuleLibraryRequest | None = None,
) -> ProofCapsuleLibraryResult:
    """Return persisted ProofCapsules with compact rollups."""

    request = request or ProofCapsuleLibraryRequest()
    if request.capsule_id:
        record = repository.get_proof_capsule(request.capsule_id)
        records = [record] if record else []
    else:
        records = repository.list_proof_capsules(
            workspace_id=request.workspace_id,
            checkout_manifest_hash=request.checkout_manifest_hash,
            candidate_id=request.candidate_id,
            work_packet_id=request.work_packet_id,
            packet_type=request.packet_type,
            requested_action=request.requested_action,
            status=request.status,
            statuses=list(request.statuses) if request.statuses else None,
            limit=request.limit,
        )
    return ProofCapsuleLibraryResult(
        capsule_count=len(records),
        status_counts=dict(sorted(Counter(record.status for record in records).items())),
        packet_type_counts=dict(sorted(Counter(record.packet_type for record in records).items())),
        requested_action_counts=dict(sorted(Counter(record.requested_action for record in records).items())),
        capsules=records,
        errors=[],
    )


def _validate_request_against_workspace(
    request: ProofCapsuleSubmitRequest,
    workspace: ResearchWorkspaceRecord | None,
) -> list[str]:
    errors: list[str] = []
    if workspace is None:
        return [f"Research workspace not found: {request.workspace_id}"]
    if workspace.status in {"destroyed", "archived"}:
        errors.append(f"Workspace status {workspace.status!r} cannot accept ProofCapsule check-ins.")
    if workspace.candidate_id != request.candidate_id:
        errors.append(
            f"candidate_id mismatch: workspace has {workspace.candidate_id!r}, request has {request.candidate_id!r}."
        )
    if not workspace.checkout_manifest_hash:
        errors.append("Workspace has no checkout_manifest_hash; build a checkout manifest before check-in.")
    elif workspace.checkout_manifest_hash != request.checkout_manifest_hash:
        errors.append(
            "checkout_manifest_hash mismatch: "
            f"workspace has {workspace.checkout_manifest_hash!r}, request has {request.checkout_manifest_hash!r}."
        )
    if workspace.work_packet_id and request.work_packet_id and workspace.work_packet_id != request.work_packet_id:
        errors.append(
            f"work_packet_id mismatch: workspace has {workspace.work_packet_id!r}, request has {request.work_packet_id!r}."
        )
    if (
        workspace.candidate_snapshot_hash
        and request.candidate_snapshot_hash
        and workspace.candidate_snapshot_hash != request.candidate_snapshot_hash
    ):
        errors.append(
            "candidate_snapshot_hash mismatch: "
            f"workspace has {workspace.candidate_snapshot_hash!r}, request has {request.candidate_snapshot_hash!r}."
        )
    allowed_task_types = workspace.checkout_manifest.get("allowed_task_types")
    if isinstance(allowed_task_types, list) and allowed_task_types and request.packet_type not in allowed_task_types:
        errors.append(
            f"packet_type {request.packet_type!r} is not allowed by checkout manifest task types."
        )
    return errors


def _contains_raw_secret(value: Any) -> bool:
    raw = json.dumps(value, sort_keys=True, default=str)
    return any(pattern.search(raw) for pattern in _SECRET_PATTERNS)


def _stable_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return f"sha256:{hashlib.sha256(raw.encode('utf-8')).hexdigest()}"


def _capsule_content_hash(
    request: ProofCapsuleSubmitRequest, work_packet_id: str | None, candidate_snapshot_hash: str | None
) -> str:
    """The canonical content_hash over a capsule's scientific content (provenance wrappers — signature,
    lineage, submitted_by — are deliberately excluded so they don't alter the hash)."""
    return _stable_hash(
        {
            "capsule_version": "proof-capsule-v1",
            "workspace_id": str(request.workspace_id),
            "checkout_manifest_hash": request.checkout_manifest_hash,
            "candidate_id": request.candidate_id,
            "candidate_snapshot_hash": candidate_snapshot_hash,
            "work_packet_id": work_packet_id,
            "packet_type": request.packet_type,
            "requested_action": request.requested_action,
            "producer": request.producer.model_dump(mode="json"),
            "target": request.target.model_dump(mode="json"),
            "summary": request.summary.model_dump(mode="json"),
            "payload": request.payload,
            "artifacts": [artifact.model_dump(mode="json") for artifact in request.artifacts],
            "source_refs": [source.model_dump(mode="json") for source in request.source_refs],
            "conflicts": request.conflicts,
            "limitations": request.limitations,
            "metadata": request.metadata,
        }
    )


def capsule_content_hash(request: ProofCapsuleSubmitRequest) -> str:
    """The content_hash a BYOC submitter must sign over (Phase B). Uses the request's OWN
    work_packet_id / candidate_snapshot_hash with no workspace fallback — so an external submitter
    supplies them explicitly and signs exactly what the gate will recompute and verify."""
    return _capsule_content_hash(request, request.work_packet_id, request.candidate_snapshot_hash)
