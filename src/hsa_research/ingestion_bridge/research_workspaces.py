"""Research workspace allocation helpers.

This module owns the first provider-specific allocator: Neon branch creation
for isolated WorkPacket state. Sandbox providers (E2B, Daytona, Coder, DevPod)
can be layered on top of the same ResearchWorkspaceRecord later.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
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


def plan_daytona_workspace(
    repository: ResearchRepository,
    request: DaytonaWorkspaceRequest,
) -> DaytonaWorkspaceResult:
    """Persist a non-live Daytona workspace request for the next sandbox slice."""

    errors: list[str] = []
    if not request.dry_run:
        errors.append("Daytona provider is stubbed in this slice; live provisioning is not enabled.")
    if not request.database_secret_ref:
        errors.append("database_secret_ref is recommended before a Daytona workspace is executable.")
    workspace = ResearchWorkspaceRecord(
        work_packet_id=request.work_packet_id,
        candidate_id=request.candidate_id,
        candidate_snapshot_hash=request.candidate_snapshot_hash,
        evidence_bundle_hash=request.evidence_bundle_hash,
        checkout_manifest_hash=request.checkout_manifest_hash,
        checkout_manifest=request.checkout_manifest,
        provider="daytona",
        git_repo=request.git_repo,
        git_ref=request.git_ref,
        git_branch=request.git_branch,
        database_secret_ref=request.database_secret_ref,
        artifact_root=request.artifact_root,
        skill_profile=request.skill_profile,
        installed_skill_refs=_merge_skill_refs(request.skill_profile, request.installed_skill_refs),
        recommended_source_refs=request.recommended_source_refs,
        status="requested" if not errors else "failed",
        errors=errors,
        metadata={
            **request.metadata,
            "neon_workspace_id": str(request.neon_workspace_id) if request.neon_workspace_id else None,
            "provider_stub": True,
            "live_provisioning_enabled": False,
        },
    )
    if request.persist:
        workspace = repository.upsert_research_workspace(workspace)
    return DaytonaWorkspaceResult(workspace=workspace, dry_run=request.dry_run, errors=errors)


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
