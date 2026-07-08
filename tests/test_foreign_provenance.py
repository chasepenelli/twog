"""Phase B / B4 — foreign-compute provenance for BYOC container capsules.

A capsule claiming a container (BYOC) compute job is admissible ONLY if its claimed image DIGEST and
runner identity match the linked job AND it carries a valid Ed25519 signature from the runner. The
provenance auditor verifies digest + identity; the accept gate additionally requires the signature for
foreign capsules. Operator-hosted capsules are unaffected (no signature required). Offline.
"""

from __future__ import annotations

from uuid import uuid4

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository

from hsa_research.ingestion_bridge import provenance
from hsa_research.ingestion_bridge.contracts import (
    ComputeJobRecord,
    ProofCapsuleRecord,
    ProofCapsuleSummary,
    ProofCapsuleTarget,
)

CANDIDATE = "twog-candidate-fp01"
MANIFEST = "sha256:" + "e" * 56
SNAPSHOT = "sha256:" + "f" * 56
DIGEST = "ghcr.io/twog/gnina@sha256:" + "a" * 64
MUTABLE = "ghcr.io/twog/gnina:latest"


def _setup(tmp_path, name):
    repo = SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False)
    service = HSAResearchService(repo)
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    priv, pub = provenance.generate_keypair()
    service.register_collaborator(principal="dr.vet", name="Dr Vet", role="collaborator", public_key=pub)
    return repo, service, priv


def _job(repo, *, runner_kind="container", image=DIGEST, principal="dr.vet", status="completed"):
    return repo.upsert_compute_job(
        ComputeJobRecord(
            status=status, runner_kind=runner_kind, compute_profile="gpu_a100",
            validation_type="docking", title="BYOC dock", objective="container dock",
            candidate_id=CANDIDATE, checkout_manifest_hash=MANIFEST, candidate_snapshot_hash=SNAPSHOT,
            container_image=image, submitted_by=principal,
        )
    )


def _capsule(repo, service, priv, job, *, image=DIGEST, runner_principal="dr.vet", sign=True, signer_priv=None):
    capsule = ProofCapsuleRecord(
        workspace_id=uuid4(),
        checkout_manifest_hash=MANIFEST,
        candidate_id=CANDIDATE,
        candidate_snapshot_hash=SNAPSHOT,
        packet_type="compute_artifact",
        requested_action="evidence_review",
        target=ProofCapsuleTarget(section="docking"),
        summary=ProofCapsuleSummary(
            title="BYOC dock result", finding="binds", why_it_matters="engagement", limitations=["in silico"],
        ),
        submitted_by="dr.vet",
        payload={
            "compute_job_id": str(job.compute_job_id),
            "validation_type": "docking",
            "container_image": image,
            "runner_principal": runner_principal,
            "signal": "neutral",
            "provider": "container",
        },
        content_hash="byoc" + "0" * 36,
        status="submitted",
    )
    if sign:
        capsule = capsule.model_copy(
            update={"signature": service.sign_capsule_content(capsule.content_hash, signer_priv or priv)}
        )
    return repo.upsert_proof_capsule(capsule)


# ---- the auditor verifies container image digest + runner identity ---------------------------
def test_matching_container_capsule_verifies(tmp_path):
    repo, service, priv = _setup(tmp_path, "ok")
    job = _job(repo)
    cap = _capsule(repo, service, priv, job)
    verdict = service.audit_capsule_provenance(cap.capsule_id)
    assert verdict.status == "verified" and verdict.ok is True
    assert "container_image_digest_pinned" in verdict.checks_passed
    assert verdict.signature_valid is True


def test_swapped_image_digest_is_caught(tmp_path):
    repo, service, priv = _setup(tmp_path, "swap")
    job = _job(repo)  # job ran image A...
    other = "ghcr.io/twog/gnina@sha256:" + "b" * 64
    cap = _capsule(repo, service, priv, job, image=other)  # ...capsule claims image B
    verdict = service.audit_capsule_provenance(cap.capsule_id)
    assert verdict.ok is False and verdict.status == "mismatch"
    assert any("container_image" in m for m in verdict.mismatches)


def test_runner_identity_mismatch_is_caught(tmp_path):
    repo, service, priv = _setup(tmp_path, "ident")
    job = _job(repo, principal="dr.vet")
    cap = _capsule(repo, service, priv, job, runner_principal="someone.else")
    verdict = service.audit_capsule_provenance(cap.capsule_id)
    assert verdict.ok is False
    assert any("runner_principal" in m for m in verdict.mismatches)


def test_mutable_image_on_job_is_caught(tmp_path):
    repo, service, priv = _setup(tmp_path, "mut")
    job = _job(repo, image=MUTABLE)
    cap = _capsule(repo, service, priv, job, image=MUTABLE)
    verdict = service.audit_capsule_provenance(cap.capsule_id)
    assert verdict.ok is False
    assert any("container_image_not_digest_pinned" in m for m in verdict.mismatches)


# ---- the accept gate requires a valid signature for foreign capsules -------------------------
def test_foreign_capsule_accepted_when_signed_and_matched(tmp_path):
    repo, service, priv = _setup(tmp_path, "acc")
    job = _job(repo)
    cap = _capsule(repo, service, priv, job)
    out = service.accept_proof_capsule(cap.capsule_id, reviewer="chase", enforce_confound_gate=False)
    assert out.status == "accepted_for_evidence_review"


def test_foreign_capsule_blocked_when_unsigned(tmp_path):
    repo, service, priv = _setup(tmp_path, "unsigned")
    job = _job(repo)
    cap = _capsule(repo, service, priv, job, sign=False)  # provenance matches, but NO signature
    out = service.accept_proof_capsule(cap.capsule_id, reviewer="chase", enforce_confound_gate=False)
    assert out.status == "submitted"  # blocked, not advanced
    gate = out.metadata["provenance_gate"]
    assert gate["status"] == "blocked"
    assert "foreign_capsule_signature_invalid" in gate["mismatches"]


def test_foreign_capsule_blocked_when_signed_by_wrong_key(tmp_path):
    repo, service, priv = _setup(tmp_path, "wrongkey")
    job = _job(repo)
    other_priv, _ = provenance.generate_keypair()
    cap = _capsule(repo, service, priv, job, signer_priv=other_priv)
    out = service.accept_proof_capsule(cap.capsule_id, reviewer="chase", enforce_confound_gate=False)
    assert out.status == "submitted"
    assert "foreign_capsule_signature_invalid" in out.metadata["provenance_gate"]["mismatches"]
