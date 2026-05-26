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


_SECRET_PATTERNS = [
    re.compile(r"\bpostgres(?:ql)?://", re.IGNORECASE),
    re.compile(r"\b(?:database_url|postgres_url|neon_database_url|openrouter_api_key|runpod_api_key|gh_token)\b", re.IGNORECASE),
    re.compile(r"\brpa_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bnapi_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bnpg_[A-Za-z0-9]{8,}\b"),
]


def submit_proof_capsule(
    repository: ResearchRepository,
    request: ProofCapsuleSubmitRequest,
) -> ProofCapsuleSubmitResult:
    """Validate and optionally persist a structured workspace check-in."""

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
    work_packet_id = request.work_packet_id or workspace.work_packet_id
    candidate_snapshot_hash = request.candidate_snapshot_hash or workspace.candidate_snapshot_hash
    content_hash = _stable_hash(
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
    capsule = ProofCapsuleRecord(
        workspace_id=request.workspace_id,
        checkout_manifest_hash=request.checkout_manifest_hash,
        candidate_id=request.candidate_id,
        candidate_snapshot_hash=candidate_snapshot_hash,
        work_packet_id=work_packet_id,
        packet_type=request.packet_type,
        requested_action=request.requested_action,
        producer=request.producer,
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
