"""Phase 4.1 + 4.2 — trusted-collaborator principals + actor provenance."""

from __future__ import annotations

from uuid import UUID, uuid4

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import (  # noqa: F401
    HSAResearchService,
    SQLiteResearchRepository,
    ValidationAssayContext,
    ValidationRequest,
    ValidationRequestQueueItem,
)
from tests.test_candidates import _seed_validation_ready_candidate

import pytest

from hsa_research.ingestion_bridge.contracts import ResearchWorkspaceRecord
from hsa_research.ingestion_bridge.service import CollaboratorAccessError, WorkspaceLeaseError


# ---- 4.1 principal model ----------------------------------------------------------------------
def test_register_collaborator_role_scopes(tmp_path):
    service = HSAResearchService(SQLiteResearchRepository(tmp_path / "c.sqlite3", seed=False))
    op = service.register_collaborator(principal="chase", name="Chase", role="operator")
    collab = service.register_collaborator(principal="vet1", name="Dr Vet", role="collaborator")
    assert op.has_scope("promote_candidate") and op.has_scope("accept_capsule")
    assert collab.has_scope("submit_capsule") and collab.has_scope("submit_compute")
    # a collaborator can never hold the write-gate scopes, even if asked for them
    sneaky = service.register_collaborator(
        principal="vet2", name="Sneaky", role="collaborator", scopes=["promote_candidate", "submit_capsule"]
    )
    assert not sneaky.has_scope("promote_candidate")
    assert sneaky.has_scope("submit_capsule")


def test_register_collaborator_idempotent_on_principal(tmp_path):
    service = HSAResearchService(SQLiteResearchRepository(tmp_path / "c.sqlite3", seed=False))
    first = service.register_collaborator(principal="vet1", name="Dr Vet", role="collaborator")
    again = service.register_collaborator(principal="vet1", name="Dr Vet Renamed", role="operator")
    assert again.collaborator_id == first.collaborator_id  # same principal -> updated in place
    assert again.role == "operator" and again.name == "Dr Vet Renamed"
    assert len(service.list_collaborators()) == 1


def test_revoke_collaborator_drops_scopes(tmp_path):
    service = HSAResearchService(SQLiteResearchRepository(tmp_path / "c.sqlite3", seed=False))
    collab = service.register_collaborator(principal="vet1", name="Dr Vet", role="collaborator")
    assert service.resolve_principal("vet1").has_scope("submit_capsule")
    revoked = service.revoke_collaborator(collab.collaborator_id)
    assert revoked.status == "revoked"
    assert not service.resolve_principal("vet1").has_scope("submit_capsule")
    assert service.resolve_principal("nobody") is None


# ---- 4.2 actor provenance ---------------------------------------------------------------------
def _docking_queue_item(repo):
    return repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(), task_id=uuid4(), brief_id=uuid4(),
            topic="Dock candidate against KDR", task_type="docking",
            title="Dock candidate against KDR", objective="Run a mock docking compute.",
            rationale="Provenance test for the collaborator loop.",
            validation_request=ValidationRequest(
                validation_type="docking", target_name="KDR", candidate_name="candidate",
                objective="Dock candidate against KDR.", require_approval=True,
                assay_context=ValidationAssayContext(
                    disease_context="canine hemangiosarcoma and human angiosarcoma",
                    species=["canine", "human"],
                    model_system="Computational structure model with explicit provenance.",
                    assay_type="in silico structural validation",
                    readout="binding plausibility", endpoint="computational plausibility",
                ),
            ),
        )
    )


def test_flow_records_submitted_by_on_job_and_capsule(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "p4.sqlite3", seed=False)
    service = HSAResearchService(repo)
    candidate_id = _seed_validation_ready_candidate(repo, candidate_id="vr-p4", ready=True)
    item = _docking_queue_item(repo)

    flow = service.run_compute_validation_flow(
        candidate_id, item.queue_item_id, runner_kind="mock", submitted_by="vet1"
    )
    assert flow["errors"] == [], flow
    job = repo.get_compute_job(UUID(flow["compute_job_id"]))
    assert job.submitted_by == "vet1"  # actor stamped on the job
    capsule = repo.get_proof_capsule(UUID(flow["capsule_id"]))
    assert capsule.submitted_by == "vet1"  # carried onto the capsule


def test_accept_sets_first_class_reviewed_by(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "p4b.sqlite3", seed=False)
    service = HSAResearchService(repo)
    candidate_id = _seed_validation_ready_candidate(repo, candidate_id="vr-p4b", ready=True)
    item = _docking_queue_item(repo)
    flow = service.run_compute_validation_flow(candidate_id, item.queue_item_id, runner_kind="mock")
    capsule_id = UUID(flow["capsule_id"])

    accepted = service.accept_proof_capsule(capsule_id, reviewer="chase")
    assert accepted.reviewed_by == "chase"  # first-class field, not just metadata
    assert accepted.metadata["reviewed_by"] == "chase"  # back-compat metadata still set


# ---- 4.3 workspace leasing --------------------------------------------------------------------
def _bare_workspace(repo, candidate_id="vr-lease"):
    return repo.upsert_research_workspace(
        ResearchWorkspaceRecord(candidate_id=candidate_id, provider="manual", status="ready")
    )


def test_lease_acquire_conflict_steal_release(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "lease.sqlite3", seed=False)
    service = HSAResearchService(repo)
    service.register_collaborator(principal="vet1", name="Dr Vet", role="collaborator")
    service.register_collaborator(principal="vet2", name="Dr Two", role="collaborator")
    ws = _bare_workspace(repo)

    leased = service.lease_workspace(ws.workspace_id, "vet1", ttl_seconds=3600)
    assert leased.leased_by == "vet1" and service.workspace_lease_active(leased)
    assert leased.gate_policy == "external_collaborator"  # collaborator hold re-gates the workspace

    # another principal cannot take an active lease...
    with pytest.raises(WorkspaceLeaseError):
        service.lease_workspace(ws.workspace_id, "vet2")
    # ...unless stealing
    stolen = service.lease_workspace(ws.workspace_id, "vet2", steal=True)
    assert stolen.leased_by == "vet2"

    # the non-holder cannot release without force
    with pytest.raises(WorkspaceLeaseError):
        service.release_workspace(ws.workspace_id, "vet1")
    released = service.release_workspace(ws.workspace_id, "vet2")
    assert released.leased_by is None and released.status == "ready"


def test_expired_lease_is_reacquirable(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "lease2.sqlite3", seed=False)
    service = HSAResearchService(repo)
    service.register_collaborator(principal="vet1", name="Dr Vet", role="collaborator")
    service.register_collaborator(principal="vet2", name="Dr Two", role="collaborator")
    ws = _bare_workspace(repo)
    expired = service.lease_workspace(ws.workspace_id, "vet1", ttl_seconds=-1)  # already expired
    assert not service.workspace_lease_active(expired)
    # expired lease -> another principal can acquire without stealing
    reacquired = service.lease_workspace(ws.workspace_id, "vet2")
    assert reacquired.leased_by == "vet2"


def test_revoked_principal_cannot_lease(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "lease3.sqlite3", seed=False)
    service = HSAResearchService(repo)
    collab = service.register_collaborator(principal="vet1", name="Dr Vet", role="collaborator")
    service.revoke_collaborator(collab.collaborator_id)
    ws = _bare_workspace(repo)
    with pytest.raises(CollaboratorAccessError):
        service.lease_workspace(ws.workspace_id, "vet1")


# ---- 4.4 write-gate enforcement ---------------------------------------------------------------
def test_collaborator_cannot_accept_or_promote_operator_can(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "gate.sqlite3", seed=False)
    service = HSAResearchService(repo)
    service.register_collaborator(principal="vet1", name="Dr Vet", role="collaborator")
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    candidate_id = _seed_validation_ready_candidate(repo, candidate_id="vr-gate", ready=True)
    item = _docking_queue_item(repo)
    flow = service.run_compute_validation_flow(
        candidate_id, item.queue_item_id, runner_kind="mock", submitted_by="vet1"
    )
    capsule_id = UUID(flow["capsule_id"])

    # collaborator is blocked at the write gate
    with pytest.raises(CollaboratorAccessError):
        service.accept_proof_capsule(capsule_id, reviewer="vet1")
    # operator passes
    accepted = service.accept_proof_capsule(capsule_id, reviewer="chase")
    assert accepted.status == "accepted_for_evidence_review"

    with pytest.raises(CollaboratorAccessError):
        service.promote_proof_capsule_to_candidate(capsule_id, reviewer="vet1")
    promotion = service.promote_proof_capsule_to_candidate(capsule_id, reviewer="chase")
    assert promotion["promoted"] is True, promotion


def test_revoked_principal_cannot_run_compute(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "gate3.sqlite3", seed=False)
    service = HSAResearchService(repo)
    collab = service.register_collaborator(principal="vet1", name="Dr Vet", role="collaborator")
    service.revoke_collaborator(collab.collaborator_id)
    candidate_id = _seed_validation_ready_candidate(repo, candidate_id="vr-gate3", ready=True)
    item = _docking_queue_item(repo)
    with pytest.raises(CollaboratorAccessError):
        service.run_compute_validation_flow(
            candidate_id, item.queue_item_id, runner_kind="mock", submitted_by="vet1"
        )


def test_atomic_lease_acquire_semantics(tmp_path):
    """The row-locked lease (Postgres hardening): acquire iff free / expired / own / steal — exercised
    via the repository's atomic acquire_workspace_lease and the service wrapper."""
    from datetime import UTC, datetime, timedelta

    repo = SQLiteResearchRepository(tmp_path / "lease.sqlite3", seed=False)
    service = HSAResearchService(repo)
    service.register_collaborator(principal="vet1", name="Vet One", role="collaborator")
    service.register_collaborator(principal="vet2", name="Vet Two", role="collaborator")
    ws = repo.upsert_research_workspace(
        ResearchWorkspaceRecord(
            candidate_id="twog-candidate-lease01", work_packet_id="wp-l", provider="neon",
            neon_branch_id="br-l", neon_branch_name="twog-l", provider_workspace_id="br-l",
            database_secret_ref="neon://p/br-l/db/owner", status="ready",
        )
    )
    # free -> vet1 acquires
    a = service.lease_workspace(ws.workspace_id, "vet1", ttl_seconds=3600)
    assert a is not None and a.leased_by == "vet1" and a.gate_policy == "external_collaborator"
    # held by vet1 -> vet2 is rejected (no double-grant), and steal=False raises
    import pytest as _pytest

    with _pytest.raises(WorkspaceLeaseError):
        service.lease_workspace(ws.workspace_id, "vet2")
    # owner renews
    assert service.lease_workspace(ws.workspace_id, "vet1").leased_by == "vet1"
    # direct repo: an expired lease is acquirable by another
    repo.acquire_workspace_lease(
        ws.workspace_id, "vet1", lease_expires_at=datetime.now(UTC) - timedelta(seconds=1)
    )
    got = repo.acquire_workspace_lease(
        ws.workspace_id, "vet2", lease_expires_at=datetime.now(UTC) + timedelta(hours=1)
    )
    assert got is not None and got.leased_by == "vet2"
    # contended (held by vet2, unexpired) -> None from the atomic acquire
    assert repo.acquire_workspace_lease(
        ws.workspace_id, "vet1", lease_expires_at=datetime.now(UTC) + timedelta(hours=1)
    ) is None
    # steal overrides
    stolen = repo.acquire_workspace_lease(
        ws.workspace_id, "vet1", lease_expires_at=datetime.now(UTC) + timedelta(hours=1), steal=True
    )
    assert stolen is not None and stolen.leased_by == "vet1"


def test_collaborator_run_marks_external_collaborator_gate(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "co.sqlite3", seed=False)
    service = HSAResearchService(repo)
    service.register_collaborator(principal="vet1", name="Dr Vet", role="collaborator")
    cid = _seed_validation_ready_candidate(repo, candidate_id="vr-checkout", ready=True)
    flow = service.run_compute_validation_flow(
        cid, _docking_queue_item(repo).queue_item_id, runner_kind="mock", submitted_by="vet1"
    )
    assert flow["errors"] == [], flow
    assert flow["gate_policy"] == "external_collaborator"  # collaborator origin recorded
    ws = repo.get_research_workspace(UUID(flow["workspace_id"]))
    assert ws.gate_policy == "external_collaborator"


def test_operator_run_stays_trusted_gate(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "co3.sqlite3", seed=False)
    service = HSAResearchService(repo)
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    cid = _seed_validation_ready_candidate(repo, candidate_id="vr-op", ready=True)
    flow = service.run_compute_validation_flow(
        cid, _docking_queue_item(repo).queue_item_id, runner_kind="mock", submitted_by="chase"
    )
    assert flow["errors"] == [], flow
    assert flow["gate_policy"] == "trusted_operator"  # operator runs stay trusted


def test_full_collaborator_workflow_end_to_end(tmp_path):
    """Capstone: operator onboards a collaborator; the collaborator runs compute and produces a
    capsule but is blocked from the write gate; the operator reviews and promotes; the audit trail
    records both principals."""
    repo = SQLiteResearchRepository(tmp_path / "capstone.sqlite3", seed=False)
    service = HSAResearchService(repo)
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    service.register_collaborator(principal="dr.vet", name="Dr Vet", role="collaborator")
    cid = _seed_validation_ready_candidate(repo, candidate_id="vr-capstone", ready=True)

    # collaborator runs the compute->capsule middle of the loop
    flow = service.run_compute_validation_flow(
        cid, _docking_queue_item(repo).queue_item_id, runner_kind="mock", submitted_by="dr.vet"
    )
    assert flow["errors"] == [], flow
    assert flow["gate_policy"] == "external_collaborator"
    capsule_id = UUID(flow["capsule_id"])
    capsule = repo.get_proof_capsule(capsule_id)
    assert capsule.status == "submitted" and capsule.submitted_by == "dr.vet"

    # the collaborator cannot cross the write gate
    with pytest.raises(CollaboratorAccessError):
        service.accept_proof_capsule(capsule_id, reviewer="dr.vet")
    with pytest.raises(CollaboratorAccessError):
        service.promote_proof_capsule_to_candidate(capsule_id, reviewer="dr.vet")

    # the operator reviews and promotes
    accepted = service.accept_proof_capsule(capsule_id, reviewer="chase")
    assert accepted.reviewed_by == "chase"
    before = repo.get_public_candidate(cid)
    promotion = service.promote_proof_capsule_to_candidate(capsule_id, reviewer="chase")
    assert promotion["promoted"] is True, promotion

    # candidate grew + full provenance is on the audit trail
    after = repo.get_public_candidate(cid)
    assert len(after.evidence_refs) > len(before.evidence_refs)
    assert repo.get_proof_capsule(capsule_id).status == "archived"
    final = repo.get_proof_capsule(capsule_id)
    assert final.submitted_by == "dr.vet" and final.reviewed_by == "chase"
    events = repo.list_public_candidate_decision_events(candidate_id=cid)
    assert any(e.action == "evidence_added" and e.actor == "chase" and e.related_capsule_id == capsule_id
               for e in events)


def test_revoked_operator_loses_write_access(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "revop.sqlite3", seed=False)
    service = HSAResearchService(repo)
    op = service.register_collaborator(principal="chase", name="Chase", role="operator")
    cid = _seed_validation_ready_candidate(repo, candidate_id="vr-revop", ready=True)
    flow = service.run_compute_validation_flow(cid, _docking_queue_item(repo).queue_item_id, runner_kind="mock")
    capsule_id = UUID(flow["capsule_id"])
    # active operator can accept...
    service.accept_proof_capsule(capsule_id, reviewer="chase")
    # ...but once revoked, even an operator is blocked from the write gate
    service.revoke_collaborator(op.collaborator_id)
    with pytest.raises(CollaboratorAccessError):
        service.promote_proof_capsule_to_candidate(capsule_id, reviewer="chase")


def test_unregistered_actor_still_allowed_backcompat(tmp_path):
    # the solo-operator model: an unregistered reviewer string is trusted by default
    repo = SQLiteResearchRepository(tmp_path / "gate2.sqlite3", seed=False)
    service = HSAResearchService(repo)
    candidate_id = _seed_validation_ready_candidate(repo, candidate_id="vr-gate2", ready=True)
    item = _docking_queue_item(repo)
    flow = service.run_compute_validation_flow(candidate_id, item.queue_item_id, runner_kind="mock")
    capsule_id = UUID(flow["capsule_id"])
    assert service.accept_proof_capsule(capsule_id, reviewer="twog_operator").status == "accepted_for_evidence_review"
    assert service.promote_proof_capsule_to_candidate(capsule_id, reviewer="twog_operator")["promoted"] is True
