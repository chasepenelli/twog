"""Research workspace allocation helpers.

This module owns the first provider-specific allocator: Neon branch creation
for isolated WorkPacket state. Sandbox providers (E2B, Daytona, Coder, DevPod)
can be layered on top of the same ResearchWorkspaceRecord later.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Protocol

from .contracts import (
    DaytonaWorkspaceRequest,
    DaytonaWorkspaceResult,
    NeonBranchWorkspaceRequest,
    NeonBranchWorkspaceResult,
    ResearchWorkspaceCleanupCandidate,
    ResearchWorkspaceCleanupRequest,
    ResearchWorkspaceCleanupResult,
    ResearchWorkspaceCheckoutManifest,
    ResearchWorkspaceCheckoutManifestRequest,
    ResearchWorkspaceCheckoutManifestResult,
    ResearchWorkspaceRecord,
    ResearchWorkspaceSkillProfile,
)
from .repository import ResearchRepository


K_DENSE_WORKSPACE_SKILL_PRESETS: dict[ResearchWorkspaceSkillProfile, list[str]] = {
    "core": [],
    "literature_and_citation": [
        "K-Dense-AI/scientific-agent-skills:literature-review",
        "K-Dense-AI/scientific-agent-skills:citation-management",
        "K-Dense-AI/scientific-agent-skills:paper-lookup",
        "K-Dense-AI/scientific-agent-skills:parallel-web",
    ],
    "database_lookup": [
        "K-Dense-AI/scientific-agent-skills:database-lookup",
        "K-Dense-AI/scientific-agent-skills:bioservices",
        "K-Dense-AI/scientific-agent-skills:gget",
        "K-Dense-AI/scientific-agent-skills:biopython",
    ],
    "chemistry": [
        "K-Dense-AI/scientific-agent-skills:rdkit",
        "K-Dense-AI/scientific-agent-skills:datamol",
        "K-Dense-AI/scientific-agent-skills:medchem",
        "K-Dense-AI/scientific-agent-skills:molfeat",
    ],
    "omics": [
        "K-Dense-AI/scientific-agent-skills:scanpy",
        "K-Dense-AI/scientific-agent-skills:pydeseq2",
        "K-Dense-AI/scientific-agent-skills:anndata",
        "K-Dense-AI/scientific-agent-skills:cellxgene-census",
        "K-Dense-AI/scientific-agent-skills:gget",
    ],
    "md_review": [
        "K-Dense-AI/scientific-agent-skills:molecular-dynamics",
        "K-Dense-AI/scientific-agent-skills:rdkit",
        "K-Dense-AI/scientific-agent-skills:diffdock",
    ],
    "validation_planning": [
        "K-Dense-AI/scientific-agent-skills:scientific-critical-thinking",
        "K-Dense-AI/scientific-agent-skills:peer-review",
        "K-Dense-AI/scientific-agent-skills:hypothesis-generation",
    ],
    "k_dense_biomed": [
        "K-Dense-AI/scientific-agent-skills:database-lookup",
        "K-Dense-AI/scientific-agent-skills:literature-review",
        "K-Dense-AI/scientific-agent-skills:citation-management",
        "K-Dense-AI/scientific-agent-skills:rdkit",
        "K-Dense-AI/scientific-agent-skills:gget",
        "K-Dense-AI/scientific-agent-skills:bioservices",
    ],
}

DEFAULT_CHECKOUT_METHOD_REFS = [
    "candidate-record-v1",
    "research-workspace-v1",
    "contribution-packet-v1",
]

DEFAULT_CHECKOUT_TASK_TYPES = [
    "evidence_addition",
    "citation_repair",
    "claim_critique",
    "replication_result",
    "compute_artifact",
    "omics_note",
    "validation_proposal",
    "safety_or_translation_note",
    "candidate_demotion_case",
]

DEFAULT_CHECKOUT_EXPECTED_OUTPUTS = [
    "proof_capsule_json",
    "targeted_claim_or_section",
    "method_notes",
    "evidence_or_artifact_refs",
    "conflicts_and_limitations",
    "requested_action",
]

DEFAULT_CHECKOUT_INSTRUCTIONS = [
    "Work from the supplied candidate snapshot, evidence bundle, method refs, and workspace database secret reference.",
    "Return a contribution packet or ProofCapsule that cites the exact claim or section being changed.",
    "Include method notes, source/artifact references, conflicts, limitations, and the requested system action.",
    "Do not mutate candidate records directly; submitted work enters operator-gated intake.",
]

DEFAULT_CHECKOUT_BOUNDARIES = [
    "Public or sandbox workspaces never receive production write credentials.",
    "A checkout manifest is a read/work handoff, not approval to mutate public candidate state.",
    "GPU compute is approval-first and never triggered by public check-in alone.",
    "LLMs may argue and synthesize; operator approval is the write gate.",
]


class NeonBranchClient(Protocol):
    def list_branches(self, *, project_id: str, search: str | None = None) -> list[dict[str, Any]]:
        """Return Neon branches for a project."""

    def create_branch(
        self,
        *,
        project_id: str,
        branch: dict[str, Any],
        endpoints: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Create a Neon branch and optional compute endpoint."""

    def get_connection_uri(
        self,
        *,
        project_id: str,
        branch_id: str,
        database_name: str,
        role_name: str,
        endpoint_id: str | None = None,
    ) -> dict[str, Any]:
        """Retrieve a branch connection URI. Callers must not persist the URI."""

    def delete_branch(self, *, project_id: str, branch_id: str) -> dict[str, Any]:
        """Delete a Neon branch."""


class NeonApiClient:
    """Small Neon API client.

    The allocator stores only branch IDs and secret references. The retrieved
    connection URI proves the branch is connectable but is deliberately not
    written into ResearchWorkspaceRecord.
    """

    def __init__(self, api_key: str, *, api_host: str = "https://console.neon.tech/api/v2") -> None:
        self.api_key = api_key
        self.api_host = api_host.rstrip("/")

    def _request(self, method: str, path: str, *, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = urllib.request.Request(
            f"{self.api_host}{path}",
            data=body,
            method=method,
            headers={
                "accept": "application/json",
                "authorization": f"Bearer {self.api_key}",
                "content-type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Neon API {method} {path} failed with {exc.code}: {detail}") from exc
        return json.loads(raw) if raw else {}

    def list_branches(self, *, project_id: str, search: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, str] = {"limit": "100"}
        if search:
            params["search"] = search
        query = urllib.parse.urlencode(params)
        data = self._request("GET", f"/projects/{project_id}/branches?{query}")
        branches = data.get("branches", [])
        return branches if isinstance(branches, list) else []

    def create_branch(
        self,
        *,
        project_id: str,
        branch: dict[str, Any],
        endpoints: list[dict[str, Any]],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"branch": branch}
        if endpoints:
            payload["endpoints"] = endpoints
        return self._request("POST", f"/projects/{project_id}/branches", payload=payload)

    def get_connection_uri(
        self,
        *,
        project_id: str,
        branch_id: str,
        database_name: str,
        role_name: str,
        endpoint_id: str | None = None,
    ) -> dict[str, Any]:
        params = {
            "branch_id": branch_id,
            "database_name": database_name,
            "role_name": role_name,
        }
        if endpoint_id:
            params["endpoint_id"] = endpoint_id
        query = urllib.parse.urlencode(params)
        return self._request("GET", f"/projects/{project_id}/connection_uri?{query}")

    def delete_branch(self, *, project_id: str, branch_id: str) -> dict[str, Any]:
        return self._request("DELETE", f"/projects/{project_id}/branches/{urllib.parse.quote(branch_id, safe='')}")


def provision_neon_branch_workspace(
    repository: ResearchRepository,
    request: NeonBranchWorkspaceRequest,
    *,
    client: NeonBranchClient | None = None,
) -> NeonBranchWorkspaceResult:
    """Create or plan a Neon-backed ResearchWorkspaceRecord.

    `dry_run=True` records a requested workspace without touching Neon. For
    live allocation, pass a client; the function creates/reuses a branch by
    name, retrieves a connection URI to verify access, and persists only a
    secret reference.
    """

    now = datetime.now(UTC)
    expires_at = now + timedelta(hours=request.ttl_hours)
    branch_name = request.branch_name or _workspace_branch_name(request, now)
    installed_skill_refs = _merge_skill_refs(request.skill_profile, request.installed_skill_refs)
    operation_ids: list[str] = []
    endpoint_id: str | None = None
    branch_id: str | None = None
    branch_created = False
    branch_reused = False
    status = "requested" if request.dry_run else "provisioning"
    errors: list[str] = []

    project_id = request.project_id or "dry-run-project"
    if request.dry_run:
        database_secret_ref = _database_secret_ref(
            project_id=project_id,
            branch_id=branch_name,
            database_name=request.database_name,
            role_name=request.role_name,
        )
    else:
        if client is None:
            raise ValueError("client is required when dry_run is false")
        if not request.project_id:
            raise ValueError("project_id is required when dry_run is false")
        parent_branch_id = request.parent_branch_id
        if not parent_branch_id and request.parent_branch_name:
            parent = _find_existing_branch(
                client.list_branches(project_id=request.project_id, search=request.parent_branch_name),
                request.parent_branch_name,
            )
            if parent is None or not parent.get("id"):
                raise RuntimeError(f"Neon parent branch not found: {request.parent_branch_name}")
            parent_branch_id = str(parent["id"])
        existing = _find_existing_branch(
            client.list_branches(project_id=request.project_id, search=branch_name),
            branch_name,
        )
        if existing:
            branch_reused = True
            branch_id = str(existing.get("id") or "")
        else:
            branch_body: dict[str, Any] = {
                "name": branch_name,
                "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
            }
            if parent_branch_id:
                branch_body["parent_id"] = parent_branch_id

            endpoint_body: dict[str, Any] = {"type": "read_write"}
            if request.suspend_timeout_seconds is not None:
                endpoint_body["suspend_timeout_seconds"] = request.suspend_timeout_seconds
            created = client.create_branch(
                project_id=request.project_id,
                branch=branch_body,
                endpoints=[endpoint_body],
            )
            branch_created = True
            branch_payload = created.get("branch") if isinstance(created.get("branch"), dict) else {}
            branch_id = str(branch_payload.get("id") or "")
            endpoints = created.get("endpoints") if isinstance(created.get("endpoints"), list) else []
            endpoint_id = _first_endpoint_id(endpoints)
            operation_ids = _operation_ids(created.get("operations"))
        if not branch_id:
            raise RuntimeError("Neon branch allocation did not return a branch id")

        connection = client.get_connection_uri(
            project_id=request.project_id,
            branch_id=branch_id,
            database_name=request.database_name,
            role_name=request.role_name,
            endpoint_id=endpoint_id,
        )
        if not _connection_uri_present(connection):
            errors.append("Neon connection URI lookup returned no URI; workspace recorded as failed.")
            status = "failed"
        else:
            status = "ready"
        database_secret_ref = _database_secret_ref(
            project_id=request.project_id,
            branch_id=branch_id,
            database_name=request.database_name,
            role_name=request.role_name,
        )

    workspace = ResearchWorkspaceRecord(
        work_packet_id=request.work_packet_id,
        candidate_id=request.candidate_id,
        candidate_snapshot_hash=request.candidate_snapshot_hash,
        evidence_bundle_hash=request.evidence_bundle_hash,
        checkout_manifest_hash=request.checkout_manifest_hash,
        checkout_manifest=request.checkout_manifest,
        provider="neon",
        provider_workspace_id=branch_id,
        git_repo=request.git_repo,
        git_ref=request.git_ref,
        git_branch=request.git_branch,
        neon_branch_id=branch_id,
        neon_branch_name=branch_name,
        neon_parent_branch=request.parent_branch_id or request.parent_branch_name,
        database_secret_ref=database_secret_ref,
        artifact_root=request.artifact_root,
        skill_profile=request.skill_profile,
        installed_skill_refs=installed_skill_refs,
        recommended_source_refs=request.recommended_source_refs,
        status=status,
        expires_at=expires_at,
        errors=errors,
        metadata={
            **request.metadata,
            "neon_project_id": project_id,
            "database_name": request.database_name,
            "role_name": request.role_name,
            "branch_created": branch_created,
            "branch_reused": branch_reused,
            "endpoint_id": endpoint_id,
        },
    )
    if request.persist:
        workspace = repository.upsert_research_workspace(workspace)

    return NeonBranchWorkspaceResult(
        workspace=workspace,
        dry_run=request.dry_run,
        branch_created=branch_created,
        branch_reused=branch_reused,
        branch_id=branch_id,
        branch_name=branch_name,
        endpoint_id=endpoint_id,
        database_secret_ref=database_secret_ref,
        operation_ids=operation_ids,
        errors=errors,
    )


def cleanup_neon_research_workspaces(
    repository: ResearchRepository,
    request: ResearchWorkspaceCleanupRequest,
    *,
    project_id: str = "",
    client: NeonBranchClient | None = None,
    protected_branch_ids: set[str] | None = None,
) -> ResearchWorkspaceCleanupResult:
    """Plan or execute cleanup for expired Neon-backed workspaces.

    Dry-runs report the branches that would be deleted. Live cleanup deletes
    only eligible Neon branches and clears the stored database secret reference
    after the provider confirms deletion.
    """

    now = datetime.now(UTC)
    expired_before = request.expired_before or now
    protected = {value.strip() for value in (protected_branch_ids or set()) if value and value.strip()}
    records = _cleanup_records(repository, request)
    candidates: list[ResearchWorkspaceCleanupCandidate] = []
    skipped: list[ResearchWorkspaceCleanupCandidate] = []
    updated_workspaces: list[ResearchWorkspaceRecord] = []
    deleted_branch_ids: list[str] = []
    errors: list[str] = []

    for workspace in records:
        candidate = _cleanup_candidate(
            workspace,
            request=request,
            expired_before=expired_before,
            protected_branch_ids=protected,
        )
        if candidate.eligible:
            action = "dry_run" if request.dry_run else "delete_neon_branch"
            candidates.append(candidate.model_copy(update={"action": action}))
        else:
            skipped.append(candidate)

    if not request.dry_run and candidates:
        missing: list[str] = []
        if not project_id:
            missing.append("NEON_PROJECT_ID")
        if client is None:
            missing.append("NEON_API_KEY")
        if missing:
            errors.extend(f"{name} is required when dry_run is false." for name in missing)
        else:
            for candidate in candidates:
                workspace = candidate.workspace
                branch_id = workspace.neon_branch_id or ""
                try:
                    client.delete_branch(project_id=project_id, branch_id=branch_id)
                except Exception as exc:  # pragma: no cover - live Neon failures are environment-dependent
                    error = f"Neon branch cleanup failed for {branch_id}: {exc}"
                    errors.append(error)
                    failed_workspace = workspace.model_copy(
                        update={
                            "errors": [*workspace.errors, error],
                            "updated_at": now,
                            "metadata": {
                                **workspace.metadata,
                                "cleanup": {
                                    **_cleanup_metadata(request, now, branch_id),
                                    "deleted": False,
                                    "error": str(exc),
                                },
                            },
                        }
                    )
                    repository.upsert_research_workspace(failed_workspace)
                    updated_workspaces.append(failed_workspace)
                    skipped.append(candidate.model_copy(update={"eligible": False, "action": "skip", "reason": error}))
                    continue

                deleted_branch_ids.append(branch_id)
                updated = workspace.model_copy(
                    update={
                        "status": "expired",
                        "database_secret_ref": None,
                        "updated_at": now,
                        "metadata": {
                            **workspace.metadata,
                            "cleanup": {
                                **_cleanup_metadata(request, now, branch_id),
                                "deleted": True,
                            },
                        },
                    }
                )
                repository.upsert_research_workspace(updated)
                updated_workspaces.append(updated)

    return ResearchWorkspaceCleanupResult(
        dry_run=request.dry_run,
        workspace_count=len(records),
        candidate_count=len(candidates),
        skipped_count=len(skipped),
        deleted_count=len(deleted_branch_ids),
        updated_count=len(updated_workspaces),
        candidates=candidates,
        skipped=skipped,
        deleted_branch_ids=deleted_branch_ids,
        updated_workspaces=updated_workspaces,
        errors=errors,
        created_at=now,
    )


def build_research_workspace_checkout_manifest(
    repository: ResearchRepository,
    request: ResearchWorkspaceCheckoutManifestRequest,
) -> ResearchWorkspaceCheckoutManifestResult:
    """Build and optionally attach a public-safe workspace checkout manifest."""

    now = datetime.now(UTC)
    errors: list[str] = []
    workspace: ResearchWorkspaceRecord | None = None
    if request.workspace_id:
        workspace = repository.get_research_workspace(request.workspace_id)
        if workspace is None:
            errors.append(f"Research workspace not found: {request.workspace_id}")

    candidate_id = request.candidate_id or (workspace.candidate_id if workspace else None)
    if not candidate_id:
        errors.append("candidate_id is required when workspace_id cannot be resolved.")
        candidate_id = "missing-candidate"

    work_packet_id = request.work_packet_id or (workspace.work_packet_id if workspace else None)
    candidate_snapshot_hash = request.candidate_snapshot_hash or (
        workspace.candidate_snapshot_hash if workspace else None
    )
    evidence_bundle_hash = request.evidence_bundle_hash or (workspace.evidence_bundle_hash if workspace else None)
    git_repo = request.git_repo or (workspace.git_repo if workspace else None)
    git_ref = request.git_ref or (workspace.git_ref if workspace else None)
    git_branch = request.git_branch or (workspace.git_branch if workspace else None)
    database_secret_ref = request.database_secret_ref or (workspace.database_secret_ref if workspace else None)
    skill_profile = workspace.skill_profile if workspace and request.skill_profile == "core" else request.skill_profile
    installed_skill_refs = _merge_skill_refs(
        skill_profile,
        [
            *(workspace.installed_skill_refs if workspace else []),
            *request.installed_skill_refs,
        ],
    )
    recommended_source_refs = _unique_strings(
        [
            *(workspace.recommended_source_refs if workspace else []),
            *request.recommended_source_refs,
        ]
    )
    method_refs = request.method_refs or DEFAULT_CHECKOUT_METHOD_REFS
    allowed_task_types = request.allowed_task_types or DEFAULT_CHECKOUT_TASK_TYPES
    expected_outputs = request.expected_outputs or DEFAULT_CHECKOUT_EXPECTED_OUTPUTS
    checkout_instructions = DEFAULT_CHECKOUT_INSTRUCTIONS
    boundaries = DEFAULT_CHECKOUT_BOUNDARIES

    hash_payload = {
        "manifest_version": "research-workspace-checkout-v1",
        "workspace_id": str(workspace.workspace_id) if workspace else (str(request.workspace_id) if request.workspace_id else None),
        "candidate_id": candidate_id,
        "work_packet_id": work_packet_id,
        "candidate_snapshot_hash": candidate_snapshot_hash,
        "evidence_bundle_hash": evidence_bundle_hash,
        "method_refs": method_refs,
        "open_questions": request.open_questions,
        "allowed_task_types": allowed_task_types,
        "expected_outputs": expected_outputs,
        "artifact_refs": request.artifact_refs,
        "git_repo": git_repo,
        "git_ref": git_ref,
        "git_branch": git_branch,
        "database_secret_ref": database_secret_ref,
        "skill_profile": skill_profile,
        "installed_skill_refs": installed_skill_refs,
        "recommended_source_refs": recommended_source_refs,
        "checkout_instructions": checkout_instructions,
        "boundaries": boundaries,
        "metadata": request.metadata,
    }
    content_hash = _stable_hash(hash_payload)
    manifest = ResearchWorkspaceCheckoutManifest(
        workspace_id=workspace.workspace_id if workspace else request.workspace_id,
        candidate_id=candidate_id,
        work_packet_id=work_packet_id,
        candidate_snapshot_hash=candidate_snapshot_hash,
        evidence_bundle_hash=evidence_bundle_hash,
        method_refs=method_refs,
        open_questions=request.open_questions,
        allowed_task_types=allowed_task_types,
        expected_outputs=expected_outputs,
        artifact_refs=request.artifact_refs,
        git_repo=git_repo,
        git_ref=git_ref,
        git_branch=git_branch,
        database_secret_ref=database_secret_ref,
        skill_profile=skill_profile,
        installed_skill_refs=installed_skill_refs,
        recommended_source_refs=recommended_source_refs,
        checkout_instructions=checkout_instructions,
        boundaries=boundaries,
        content_hash=content_hash,
        created_at=now,
        metadata={
            **request.metadata,
            "source_workspace_status": workspace.status if workspace else None,
            "source_provider": workspace.provider if workspace else None,
        },
    )

    persisted = False
    if workspace and request.persist_to_workspace and not errors:
        workspace = workspace.model_copy(
            update={
                "checkout_manifest_hash": manifest.content_hash,
                "checkout_manifest": manifest.model_dump(mode="json"),
                "candidate_snapshot_hash": candidate_snapshot_hash,
                "evidence_bundle_hash": evidence_bundle_hash,
                "updated_at": now,
                "metadata": {
                    **workspace.metadata,
                    "checkout_manifest": {
                        "content_hash": manifest.content_hash,
                        "manifest_version": manifest.manifest_version,
                        "updated_at": now.isoformat(),
                    },
                },
            }
        )
        workspace = repository.upsert_research_workspace(workspace)
        persisted = True

    return ResearchWorkspaceCheckoutManifestResult(
        manifest=manifest,
        workspace=workspace,
        persisted=persisted,
        errors=errors,
        created_at=now,
    )


def _cleanup_records(
    repository: ResearchRepository,
    request: ResearchWorkspaceCleanupRequest,
) -> list[ResearchWorkspaceRecord]:
    if request.workspace_id:
        workspace = repository.get_research_workspace(request.workspace_id)
        return [workspace] if workspace else []
    return repository.list_research_workspaces(
        work_packet_id=request.work_packet_id,
        candidate_id=request.candidate_id,
        provider=request.provider,
        include_expired=True,
        limit=request.limit,
    )


def _cleanup_candidate(
    workspace: ResearchWorkspaceRecord,
    *,
    request: ResearchWorkspaceCleanupRequest,
    expired_before: datetime,
    protected_branch_ids: set[str],
) -> ResearchWorkspaceCleanupCandidate:
    explicit_workspace = request.workspace_id is not None
    branch_id = workspace.neon_branch_id
    if workspace.provider != "neon":
        return ResearchWorkspaceCleanupCandidate(
            workspace=workspace,
            eligible=False,
            action="skip",
            reason="workspace_provider_is_not_neon",
        )
    if not branch_id:
        return ResearchWorkspaceCleanupCandidate(
            workspace=workspace,
            eligible=False,
            action="skip",
            reason="workspace_has_no_neon_branch_id",
        )
    if branch_id in protected_branch_ids:
        return ResearchWorkspaceCleanupCandidate(
            workspace=workspace,
            eligible=False,
            action="skip",
            reason="workspace_branch_is_configured_parent_branch",
        )
    cleanup_metadata = workspace.metadata.get("cleanup") if isinstance(workspace.metadata, dict) else None
    if (
        workspace.database_secret_ref is None
        and isinstance(cleanup_metadata, dict)
        and cleanup_metadata.get("deleted") is True
    ):
        return ResearchWorkspaceCleanupCandidate(
            workspace=workspace,
            eligible=False,
            action="skip",
            reason="workspace_branch_already_cleaned",
        )
    if not explicit_workspace and workspace.status not in {"failed", "expired"}:
        if workspace.expires_at is None:
            return ResearchWorkspaceCleanupCandidate(
                workspace=workspace,
                eligible=False,
                action="skip",
                reason="workspace_has_no_expiration",
            )
        if workspace.expires_at > expired_before:
            return ResearchWorkspaceCleanupCandidate(
                workspace=workspace,
                eligible=False,
                action="skip",
                reason="workspace_not_expired",
            )
    return ResearchWorkspaceCleanupCandidate(
        workspace=workspace,
        eligible=True,
        action="dry_run" if request.dry_run else "delete_neon_branch",
        reason="eligible_for_neon_branch_cleanup",
    )


def _cleanup_metadata(
    request: ResearchWorkspaceCleanupRequest,
    cleaned_at: datetime,
    branch_id: str,
) -> dict[str, Any]:
    return {
        **request.metadata,
        "branch_id": branch_id,
        "cleaned_at": cleaned_at.isoformat(),
        "dry_run": request.dry_run,
        "reason": request.reason,
    }


def plan_daytona_workspace(
    repository: ResearchRepository,
    request: DaytonaWorkspaceRequest,
) -> DaytonaWorkspaceResult:
    """Persist a Daytona workspace handoff plan.

    This intentionally stops before provider API execution. The returned
    provider_payload is the reviewed shape the live adapter should submit once
    Daytona credentials/client code are added.
    """

    errors: list[str] = []
    if not request.dry_run:
        errors.append("Daytona provider client is not configured; live provisioning remains gated.")
        if not request.database_secret_ref:
            errors.append("database_secret_ref is required before live Daytona provisioning.")
        if not request.checkout_manifest_hash:
            errors.append("checkout_manifest_hash is required before live Daytona provisioning.")
        if not request.git_repo:
            errors.append("git_repo is required before live Daytona provisioning.")
    installed_skill_refs = _merge_skill_refs(request.skill_profile, request.installed_skill_refs)
    provider_payload = {
        "workspace_name": request.workspace_name
        or f"twog-{_slug(request.candidate_id)}-{_slug(request.work_packet_id or 'workspace')}",
        "provider_template": request.provider_template or "twog-research-workspace",
        "candidate_id": request.candidate_id,
        "work_packet_id": request.work_packet_id,
        "neon_workspace_id": str(request.neon_workspace_id) if request.neon_workspace_id else None,
        "database_secret_ref": request.database_secret_ref,
        "checkout_manifest_hash": request.checkout_manifest_hash,
        "checkout_manifest": request.checkout_manifest,
        "candidate_snapshot_hash": request.candidate_snapshot_hash,
        "evidence_bundle_hash": request.evidence_bundle_hash,
        "git": {
            "repo": request.git_repo,
            "ref": request.git_ref,
            "branch": request.git_branch,
        },
        "artifact_root": request.artifact_root,
        "skill_profile": request.skill_profile,
        "installed_skill_refs": installed_skill_refs,
        "recommended_source_refs": request.recommended_source_refs,
        "forbidden_secrets": ["OPENROUTER_API_KEY", "RUNPOD_API_KEY", "GH_TOKEN", "production_database_url"],
    }
    ready_for_provider_dispatch = bool(
        request.dry_run
        and request.database_secret_ref
        and request.checkout_manifest_hash
        and request.git_repo
    )
    workspace = ResearchWorkspaceRecord(
        work_packet_id=request.work_packet_id,
        candidate_id=request.candidate_id,
        candidate_snapshot_hash=request.candidate_snapshot_hash,
        evidence_bundle_hash=request.evidence_bundle_hash,
        checkout_manifest_hash=request.checkout_manifest_hash,
        checkout_manifest=request.checkout_manifest,
        provider="daytona",
        provider_workspace_id=None,
        git_repo=request.git_repo,
        git_ref=request.git_ref,
        git_branch=request.git_branch,
        database_secret_ref=request.database_secret_ref,
        artifact_root=request.artifact_root,
        skill_profile=request.skill_profile,
        installed_skill_refs=installed_skill_refs,
        recommended_source_refs=request.recommended_source_refs,
        status="requested" if not errors else "failed",
        errors=errors,
        metadata={
            **request.metadata,
            "neon_workspace_id": str(request.neon_workspace_id) if request.neon_workspace_id else None,
            "provider_stub": True,
            "live_provisioning_enabled": False,
            "ready_for_provider_dispatch": ready_for_provider_dispatch,
            "provider_template": provider_payload["provider_template"],
        },
    )
    if request.persist:
        workspace = repository.upsert_research_workspace(workspace)
    return DaytonaWorkspaceResult(
        workspace=workspace,
        dry_run=request.dry_run,
        ready_for_provider_dispatch=ready_for_provider_dispatch,
        provider_payload=provider_payload,
        errors=errors,
    )


def _workspace_branch_name(request: NeonBranchWorkspaceRequest, now: datetime) -> str:
    pieces = [
        request.branch_name_prefix,
        _slug(request.candidate_id),
        _slug(request.work_packet_id or "workspace"),
        now.strftime("%Y%m%d%H%M%S"),
    ]
    return "-".join(piece for piece in pieces if piece)[:63].strip("-")


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")[:32]


def _merge_skill_refs(
    profile: ResearchWorkspaceSkillProfile,
    requested_refs: list[str],
) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for ref in [*K_DENSE_WORKSPACE_SKILL_PRESETS.get(profile, []), *requested_refs]:
        normalized = str(ref).strip()
        key = normalized.casefold()
        if normalized and key not in seen:
            merged.append(normalized)
            seen.add(key)
    return merged


def _unique_strings(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = str(raw).strip()
        key = value.casefold()
        if value and key not in seen:
            unique.append(value)
            seen.add(key)
    return unique


def _stable_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return f"sha256:{hashlib.sha256(raw.encode('utf-8')).hexdigest()}"


def _find_existing_branch(branches: list[dict[str, Any]], branch_name: str) -> dict[str, Any] | None:
    for branch in branches:
        if str(branch.get("name") or "") == branch_name:
            return branch
    return None


def _first_endpoint_id(endpoints: list[Any]) -> str | None:
    for endpoint in endpoints:
        if isinstance(endpoint, dict) and endpoint.get("id"):
            return str(endpoint["id"])
    return None


def _operation_ids(operations: Any) -> list[str]:
    if not isinstance(operations, list):
        return []
    out: list[str] = []
    for operation in operations:
        if isinstance(operation, dict) and operation.get("id"):
            out.append(str(operation["id"]))
    return out


def _connection_uri_present(connection: dict[str, Any]) -> bool:
    return bool(connection.get("uri") or connection.get("connection_uri"))


def _database_secret_ref(
    *,
    project_id: str,
    branch_id: str,
    database_name: str,
    role_name: str,
) -> str:
    return f"neon://{project_id}/{branch_id}/{database_name}/{role_name}"
