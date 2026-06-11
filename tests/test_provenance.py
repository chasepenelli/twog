"""Cryptographic provenance — tamper-evident lineage (proof of change) + Ed25519 signatures."""

from __future__ import annotations

from uuid import UUID

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository
from tests.test_candidates import _seed_validation_ready_candidate
from tests.test_collaborators import _docking_queue_item

from hsa_research.ingestion_bridge import provenance


# ---- pure primitives --------------------------------------------------------------------------
def test_sign_verify_roundtrip():
    priv, pub = provenance.generate_keypair()
    h = "a" * 64
    sig = provenance.sign(h, priv)
    assert provenance.verify(h, sig, pub) is True
    # tampered content fails
    assert provenance.verify("b" * 64, sig, pub) is False
    # wrong key fails
    _, other_pub = provenance.generate_keypair()
    assert provenance.verify(h, sig, other_pub) is False


def test_merkle_root_deterministic_and_sensitive():
    a = provenance.merkle_root(["h1", "h2", "h3"])
    assert a == provenance.merkle_root(["h1", "h2", "h3"])  # deterministic
    assert a != provenance.merkle_root(["h1", "h2", "h4"])  # changes if any leaf changes
    assert provenance.merkle_root([]) is None
    assert provenance.merkle_root(["only"]) is not None


def test_verify_lineage_detects_breaks():
    # intact chain: H0 (genesis) <- H1 <- H2
    ok, problems = provenance.verify_lineage([("H0", None, 0), ("H1", "H0", 1), ("H2", "H1", 2)])
    assert ok and not problems
    # broken link
    ok, problems = provenance.verify_lineage([("H0", None, 0), ("H1", "WRONG", 1)])
    assert not ok and any("broken link" in p for p in problems)
    # genesis must have no parent
    ok, problems = provenance.verify_lineage([("H0", "x", 0)])
    assert not ok


# ---- service integration ----------------------------------------------------------------------
def _flow_capsule(tmp_path, name):
    repo = SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False)
    service = HSAResearchService(repo)
    cid = _seed_validation_ready_candidate(repo, candidate_id=f"vr-{name}", ready=True)
    flow = service.run_compute_validation_flow(
        cid, _docking_queue_item(repo).queue_item_id, runner_kind="mock", submitted_by="vet1"
    )
    return repo, service, cid, repo.get_proof_capsule(UUID(flow["capsule_id"]))


def test_signed_capsule_verifies_against_registered_pubkey(tmp_path):
    repo, service, cid, capsule = _flow_capsule(tmp_path, "sign")
    priv, pub = provenance.generate_keypair()
    service.register_collaborator(principal="vet1", name="Dr Vet", role="collaborator", public_key=pub)

    sig = service.sign_capsule_content(capsule.content_hash, priv)
    signed = capsule.model_copy(update={"signature": sig})
    repo.upsert_proof_capsule(signed)

    res = service.verify_capsule_provenance(signed)
    assert res["signed"] and res["signature_valid"] and res["signer"] == "vet1"

    # a forged content_hash no longer verifies under the same signature
    forged = signed.model_copy(update={"content_hash": signed.content_hash[:-1] + ("0" if signed.content_hash[-1] != "0" else "1")})
    assert service.verify_capsule_provenance(forged)["signature_valid"] is False


def test_capsule_lineage_proof_of_change(tmp_path):
    repo, service, cid, v0 = _flow_capsule(tmp_path, "lineage")
    # v1 supersedes v0: parent_content_hash links to v0's content_hash
    v1 = v0.model_copy(update={
        "capsule_id": __import__("uuid").uuid4(),
        "content_hash": "deadbeef" * 8,
        "parent_content_hash": v0.content_hash,
        "lineage_index": 1,
    })
    res = service.verify_capsule_lineage([v0, v1])
    assert res["ok"] and res["versions"] == 2
    # tamper v1's parent link -> detected
    broken = v1.model_copy(update={"parent_content_hash": "00" * 8})
    assert service.verify_capsule_lineage([v0, broken])["ok"] is False


def test_evidence_merkle_root_changes_with_evidence(tmp_path):
    repo, service, cid, capsule = _flow_capsule(tmp_path, "root")
    root1 = service.evidence_merkle_root(cid)
    assert root1 is not None and len(root1) == 64  # sha256 hex
    # add another capsule version -> root changes (the value you'd anchor)
    v2 = capsule.model_copy(update={"capsule_id": __import__("uuid").uuid4(), "content_hash": "f00dcafe" * 8})
    repo.upsert_proof_capsule(v2)
    assert service.evidence_merkle_root(cid) != root1
