"""Phase B / B0 — the authenticated external-submit gate + deny-unknown access flag.

A capsule produced on a collaborator's OWN compute (BYOC) enters via submit_external_proof_capsule and
must clear the gate: the submitter is a registered, active collaborator holding the submit_capsule
scope, currently holds the target workspace's lease, and signs the content_hash with the Ed25519 key
matching their registered public_key. Operator-hosted compute (submit_proof_capsule, the internal
flow) is unaffected. Offline; no network/GPU.
"""

from __future__ import annotations

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository

import pytest

from hsa_research.ingestion_bridge import provenance
from hsa_research.ingestion_bridge.contracts import (
    ProofCapsuleSubmitRequest,
    ProofCapsuleSummary,
    ProofCapsuleTarget,
    ResearchWorkspaceRecord,
)
from hsa_research.ingestion_bridge.service import CollaboratorAccessError

CANDIDATE = "twog-candidate-phaseb01"
MANIFEST_HASH = "sha256:" + "a" * 56


def _workspace(repo, **over):
    fields = dict(
        candidate_id=CANDIDATE, work_packet_id="wp-b0", provider="neon",
        neon_branch_id="br-b0", neon_branch_name="twog-b0",
        provider_workspace_id="br-b0", database_secret_ref="neon://p/br-b0/db/owner",
        checkout_manifest_hash=MANIFEST_HASH, status="ready",
    )
    fields.update(over)
    return repo.upsert_research_workspace(ResearchWorkspaceRecord(**fields))


def _request(ws):
    return ProofCapsuleSubmitRequest(
        workspace_id=ws.workspace_id, checkout_manifest_hash=MANIFEST_HASH,
        candidate_id=CANDIDATE, work_packet_id="wp-b0",
        packet_type="evidence_addition", requested_action="evidence_review",
        submitted_by="vet1",
        target=ProofCapsuleTarget(section="Docking"),
        summary=ProofCapsuleSummary(
            title="BYOC dock", finding="ligand binds the verified pocket",
            why_it_matters="target engagement", limitations=["in silico"],
        ),
        payload={"signal": "supports", "validation_type": "docking"},
    )


def _setup(tmp_path, name, *, public_key=None):
    repo = SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False)
    service = HSAResearchService(repo)
    priv, pub = provenance.generate_keypair()
    service.register_collaborator(
        principal="vet1", name="Dr Vet", role="collaborator", public_key=public_key or pub
    )
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "vet1", ttl_seconds=3600)
    ws = repo.get_research_workspace(ws.workspace_id)
    assert ws.gate_policy == "external_collaborator"  # collaborator lease re-gates the workspace
    return repo, service, ws, priv


def _signed(service, ws, priv, *, signer_priv=None):
    req = _request(ws)
    content_hash = service.capsule_content_hash_for_submission(req)
    sig = service.sign_capsule_content(content_hash, signer_priv or priv)
    return req.model_copy(update={"signature": sig})


# ---- happy path: registered + scoped + leased + signed -> accepted ---------------------------
def test_authenticated_signed_byoc_submit_is_accepted(tmp_path):
    repo, service, ws, priv = _setup(tmp_path, "happy")
    res = service.submit_external_proof_capsule(_signed(service, ws, priv))
    assert res.accepted is True and res.persisted is True
    assert res.errors == []
    # and the stored capsule's signature verifies against the registered public key
    assert service.verify_capsule_provenance(res.capsule)["signature_valid"] is True


# ---- a legit submit that OMITS the optional work_packet_id still verifies (regression) -------
def test_signed_submit_with_omitted_work_packet_id_is_accepted(tmp_path):
    # the workspace HAS a work_packet_id; the collaborator leaves it unset (it is optional). The gate
    # must verify the signature over request-only values, NOT the workspace fallback — else a
    # legitimate submit is wrongly rejected (caught by scripts/stress_phase_b.py).
    repo, service, ws, priv = _setup(tmp_path, "omitwp")
    assert ws.work_packet_id == "wp-b0"
    req = _request(ws).model_copy(update={"work_packet_id": None})
    content_hash = service.capsule_content_hash_for_submission(req)
    req = req.model_copy(update={"signature": service.sign_capsule_content(content_hash, priv)})
    res = service.submit_external_proof_capsule(req)
    assert res.accepted is True, res.errors


# ---- each missing factor is refused ----------------------------------------------------------
def test_unsigned_byoc_submit_is_refused(tmp_path):
    repo, service, ws, priv = _setup(tmp_path, "unsigned")
    res = service.submit_external_proof_capsule(_request(ws))  # no signature
    assert res.accepted is False
    assert any("signature" in e for e in res.errors)


def test_unregistered_submitter_is_refused(tmp_path):
    repo, service, ws, priv = _setup(tmp_path, "ghost")
    req = _signed(service, ws, priv).model_copy(update={"submitted_by": "ghost"})
    res = service.submit_external_proof_capsule(req)
    assert res.accepted is False
    assert any("not a registered collaborator" in e for e in res.errors)


def test_submit_without_holding_lease_is_refused(tmp_path):
    repo, service, ws, priv = _setup(tmp_path, "nolease")
    service.release_workspace(ws.workspace_id, "vet1")  # drop the lease
    res = service.submit_external_proof_capsule(_signed(service, ws, priv))
    assert res.accepted is False
    assert any("does not hold an active lease" in e for e in res.errors)


def test_oversized_external_payload_is_refused(tmp_path):
    """DoS guard (found by scripts/stress_phase_b.py): an external capsule's freeform payload is
    size-capped — bulk data belongs by reference, not inlined."""
    repo, service, ws, priv = _setup(tmp_path, "huge")
    big = _request(ws).model_copy(update={"payload": {"signal": "supports", "blob": "A" * 2_000_000}})
    content_hash = service.capsule_content_hash_for_submission(big)
    big = big.model_copy(update={"signature": service.sign_capsule_content(content_hash, priv)})
    res = service.submit_external_proof_capsule(big)
    assert res.accepted is False
    assert any("too large" in e for e in res.errors)


def test_signature_from_wrong_key_is_refused(tmp_path):
    repo, service, ws, priv = _setup(tmp_path, "wrongkey")
    other_priv, _ = provenance.generate_keypair()
    res = service.submit_external_proof_capsule(_signed(service, ws, priv, signer_priv=other_priv))
    assert res.accepted is False
    assert any("does not verify" in e for e in res.errors)


# ---- re-submitting identical content is idempotent (no ledger flooding) ----------------------
def test_duplicate_external_submit_is_idempotent(tmp_path):
    from hsa_research.ingestion_bridge.contracts import ProofCapsuleLibraryRequest

    repo, service, ws, priv = _setup(tmp_path, "dup")
    signed = _signed(service, ws, priv)
    first = service.submit_external_proof_capsule(signed)
    second = service.submit_external_proof_capsule(signed)  # byte-identical re-submit
    assert first.accepted and second.accepted
    assert first.capsule.content_hash == second.capsule.content_hash
    assert first.capsule.capsule_id == second.capsule.capsule_id  # same row, not a duplicate
    ledger = service.list_proof_capsules(ProofCapsuleLibraryRequest(candidate_id=CANDIDATE, limit=50)).capsules
    assert len(ledger) == 1


# ---- the internal operator-hosted path is unaffected by the external gate --------------------
def test_internal_submit_on_external_workspace_needs_no_signature(tmp_path):
    repo, service, ws, priv = _setup(tmp_path, "internal")
    # external_submission defaults False: operator-hosted compute submits without a signature, even on
    # an external_collaborator workspace (trust = our infra + the terminal human accept/promote gate).
    res = service.submit_proof_capsule(_request(ws))
    assert res.accepted is True and res.persisted is True


# ---- deny-unknown access flag ----------------------------------------------------------------
def test_deny_unknown_flag_blocks_unregistered_principal(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "strict.sqlite3", seed=False)
    service = HSAResearchService(repo)
    ws = _workspace(repo)
    # default: an unregistered principal is trusted (solo-operator model)
    assert service.require_registered_principals is False
    assert service.lease_workspace(ws.workspace_id, "stranger") is not None

    # strict mode: an unregistered principal is denied at the authorize gate
    service.require_registered_principals = True
    ws2 = _workspace(
        repo, neon_branch_id="br-b0b", neon_branch_name="twog-b0b", provider_workspace_id="br-b0b"
    )
    with pytest.raises(CollaboratorAccessError):
        service.lease_workspace(ws2.workspace_id, "stranger")
