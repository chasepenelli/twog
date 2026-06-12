"""Provenance Auditor pre-gate (increment 4) — integrity of claimed runs.

Verifies a capsule's CLAIMED compute job (id, candidate, workspace/snapshot hashes, validation type,
completion) against the real linked job — the "claimed V100, ran H100" catch. A capsule with no claim
verifies trivially; a fabricated or drifted claim is blocked at accept.
"""

from __future__ import annotations

from uuid import UUID, uuid4

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository
from tests.test_candidates import _seed_validation_ready_candidate
from tests.test_collaborators import _docking_queue_item
from tests.test_falsification_planner import _seed_capsule

from hsa_research.ingestion_bridge.contracts import (
    ComputeJobRecord,
    ProofCapsuleRecord,
    ProofCapsuleSummary,
    ProofCapsuleTarget,
)


def _svc(tmp_path, name, cid):
    repo = SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _seed_validation_ready_candidate(repo, candidate_id=cid)
    return service, repo


def _capsule_claiming(repo, cid, job_id, *, validation_type="omics", checkout_manifest_hash="sha256:" + "b" * 24):
    cap = ProofCapsuleRecord(
        workspace_id=uuid4(),
        checkout_manifest_hash=checkout_manifest_hash,
        candidate_id=cid,
        candidate_snapshot_hash=None,
        packet_type="compute_artifact",
        requested_action="docking_or_md_review",
        target=ProofCapsuleTarget(section=validation_type),
        summary=ProofCapsuleSummary(
            title="claim capsule",
            finding="claims provenance from a compute job",
            why_it_matters="provenance audit fixture",
            limitations=["synthetic fixture"],
        ),
        payload={"compute_job_id": str(job_id), "validation_type": validation_type, "signal": "neutral"},
        content_hash="c" * 40,
    )
    return repo.upsert_proof_capsule(cap)


# ---- verdicts --------------------------------------------------------------------------------
def test_no_claim_capsule_verifies_trivially(tmp_path):
    service, repo = _svc(tmp_path, "nc", "vr-nc")
    cap = _seed_capsule(repo, "vr-nc", signal="neutral", validation_type="omics")  # no compute_job_id
    verdict = service.audit_capsule_provenance(cap.capsule_id)
    assert verdict.ok and verdict.status == "no_claim"


def test_flow_built_capsule_is_verified_and_accepts(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "flow.sqlite3", seed=False)
    service = HSAResearchService(repo)
    cid = _seed_validation_ready_candidate(repo, candidate_id="vr-flow", ready=True)
    item = _docking_queue_item(repo)
    flow = service.run_compute_validation_flow(cid, item.queue_item_id, runner_kind="mock")
    assert flow["errors"] == [], flow

    cap_id = UUID(flow["capsule_id"])
    verdict = service.audit_capsule_provenance(cap_id)
    assert verdict.ok and verdict.status == "verified"
    assert "compute_job_completed" in verdict.checks_passed
    # provenance verified + signal neutral (confound gate n/a) => accept proceeds
    accepted = service.accept_proof_capsule(cap_id, reviewer="chase")
    assert accepted.status == "accepted_for_evidence_review"


def test_fabricated_claim_is_unresolved_and_blocks_accept(tmp_path):
    service, repo = _svc(tmp_path, "fab", "vr-fab")
    cap = _capsule_claiming(repo, "vr-fab", uuid4())  # claims a job that does not exist
    verdict = service.audit_capsule_provenance(cap.capsule_id)
    assert not verdict.ok and verdict.status == "claim_unresolved"

    blocked = service.accept_proof_capsule(cap.capsule_id, reviewer="chase")
    assert blocked.status == "submitted"  # gate held
    assert blocked.metadata["provenance_gate"]["verdict"] == "claim_unresolved"


def test_hash_mismatch_is_caught_and_blocks_accept(tmp_path):
    service, repo = _svc(tmp_path, "mm", "vr-mm")
    job = repo.upsert_compute_job(
        ComputeJobRecord(
            title="omics job",
            objective="run omics",
            status="completed",
            validation_type="omics",
            candidate_id="vr-mm",
            checkout_manifest_hash="sha256:" + "a" * 24,  # differs from the capsule's claim
            candidate_snapshot_hash=None,
        )
    )
    cap = _capsule_claiming(repo, "vr-mm", job.compute_job_id, checkout_manifest_hash="sha256:" + "b" * 24)
    verdict = service.audit_capsule_provenance(cap.capsule_id)
    assert not verdict.ok and verdict.status == "mismatch"
    assert any("checkout_manifest_hash" in m for m in verdict.mismatches)

    blocked = service.accept_proof_capsule(cap.capsule_id, reviewer="chase")
    assert blocked.status == "submitted"


def test_provenance_verdict_persists_without_mutating_content_hash(tmp_path):
    service, repo = _svc(tmp_path, "persist", "vr-pp")
    cap = _seed_capsule(repo, "vr-pp", signal="neutral", validation_type="omics")
    service.audit_capsule_provenance(cap.capsule_id)
    reloaded = repo.get_proof_capsule(cap.capsule_id)
    assert reloaded.metadata["provenance_verdict"]["status"] == "no_claim"
    assert reloaded.content_hash == cap.content_hash  # metadata excluded from content hash
