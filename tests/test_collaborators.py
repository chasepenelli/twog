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
