"""Phase B / B2 — the isolated collaborator sandbox + the contribution-artifact model.

An active collaborator opens an isolated sandbox (an operator-provisioned workspace leased to them,
which seals it as external_collaborator). The bundle exposes the lane sandbox manifest + the
contribution modes (what they can DO), and carries NO operator secrets. Pending/revoked/unknown
principals are denied; one collaborator cannot open another's held sandbox. Offline.
"""

from __future__ import annotations

import json

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository

import pytest

from hsa_research.ingestion_bridge import provenance
from hsa_research.ingestion_bridge.contracts import ResearchWorkspaceRecord
from hsa_research.ingestion_bridge.service import CollaboratorAccessError, WorkspaceLeaseError

CANDIDATE = "twog-candidate-sbx01"


def _workspace(repo, **over):
    fields = dict(
        candidate_id=CANDIDATE, work_packet_id="wp-sbx", provider="neon",
        neon_branch_id="br-sbx", neon_branch_name="twog-sbx", provider_workspace_id="br-sbx",
        database_secret_ref="neon://project/br-sbx/neondb/neondb_owner_SECRET",
        checkout_manifest_hash="sha256:" + "c" * 56, status="ready",
    )
    fields.update(over)
    return repo.upsert_research_workspace(ResearchWorkspaceRecord(**fields))


def _service_with_active(tmp_path, name="sbx", principal="dr.vet"):
    repo = SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False)
    service = HSAResearchService(repo)
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    _, pub = provenance.generate_keypair()
    applicant = service.request_collaborator_access(principal=principal, name="Dr Vet", public_key=pub)
    service.approve_collaborator(applicant.collaborator_id, approved_by="chase")
    return repo, service


def test_active_collaborator_opens_sealed_sandbox(tmp_path):
    repo, service = _service_with_active(tmp_path)
    ws = _workspace(repo)
    bundle = service.open_collaborator_sandbox(
        "dr.vet", ws.workspace_id, validation_type="docking", candidate_id=CANDIDATE
    )
    assert bundle is not None
    assert bundle["leased_by"] == "dr.vet"
    assert bundle["gate_policy"] == "external_collaborator"  # sealed: write gate stays operator-held
    assert bundle["lease_expires_at"] is not None
    # the contribution-artifact model is surfaced (the "what can they do" answer)
    modes = {m["mode"] for m in bundle["contribution_modes"]}
    assert {"evidence_capsule", "target_library_entry", "candidate_proposal"} <= modes
    # NO operator secret leaks into the collaborator's bundle
    blob = json.dumps(bundle)
    assert "database_secret_ref" not in blob and "SECRET" not in blob and "neondb_owner" not in blob


def test_pending_applicant_cannot_open_sandbox(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "pend.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _, pub = provenance.generate_keypair()
    service.request_collaborator_access(principal="dr.vet", name="Dr Vet", public_key=pub)  # pending
    ws = _workspace(repo)
    with pytest.raises(CollaboratorAccessError):
        service.open_collaborator_sandbox("dr.vet", ws.workspace_id, validation_type="docking")


def test_revoked_and_unknown_cannot_open_sandbox(tmp_path):
    repo, service = _service_with_active(tmp_path, name="rev")
    ws = _workspace(repo)
    revoked = service.resolve_principal("dr.vet")
    service.revoke_collaborator(revoked.collaborator_id)
    with pytest.raises(CollaboratorAccessError):
        service.open_collaborator_sandbox("dr.vet", ws.workspace_id, validation_type="docking")
    with pytest.raises(CollaboratorAccessError):
        service.open_collaborator_sandbox("ghost", ws.workspace_id, validation_type="docking")


def test_collaborator_cannot_open_anothers_held_sandbox(tmp_path):
    repo, service = _service_with_active(tmp_path, name="iso")
    # a second active collaborator
    _, pub2 = provenance.generate_keypair()
    other = service.request_collaborator_access(principal="vet2", name="Vet Two", public_key=pub2)
    service.approve_collaborator(other.collaborator_id, approved_by="chase")

    ws = _workspace(repo)
    service.open_collaborator_sandbox("dr.vet", ws.workspace_id, validation_type="docking")
    # vet2 cannot open the sandbox dr.vet currently holds
    with pytest.raises(WorkspaceLeaseError):
        service.open_collaborator_sandbox("vet2", ws.workspace_id, validation_type="docking")


def test_open_sandbox_missing_workspace_returns_none(tmp_path):
    from uuid import uuid4

    repo, service = _service_with_active(tmp_path, name="missing")
    assert service.open_collaborator_sandbox("dr.vet", uuid4(), validation_type="docking") is None
