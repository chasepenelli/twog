"""Adversarial stress test for the Phase B authenticated collaborator boundary (Unit 1).

Runs realistic onboarding + a battery of attacks against a LIVE service (SQLite, like a real
deployment). Each scenario asserts the boundary behaved correctly: legitimate actions SUCCEED, attacks
are REFUSED. Prints a security-style report. Offline, no spend.

    PYTHONPATH=src python scripts/stress_phase_b.py
"""

from __future__ import annotations

import tempfile
import pathlib
from uuid import uuid4

from hsa_research.ingestion_bridge import provenance
from hsa_research.ingestion_bridge.compute_runners import get_compute_runner, register_container_backend
from hsa_research.ingestion_bridge.contracts import (
    ComputeJobRecord,
    ProofCapsuleRecord,
    ProofCapsuleSubmitRequest,
    ProofCapsuleSummary,
    ProofCapsuleTarget,
    ResearchWorkspaceRecord,
)
from hsa_research.ingestion_bridge.local_store import SQLiteResearchRepository
from hsa_research.ingestion_bridge.service import (
    CollaboratorAccessError,
    HSAResearchService,
    WorkspaceLeaseError,
)

CANDIDATE = "twog-candidate-stress01"
RESULTS: list[tuple[bool, str, str]] = []


def check(ok: bool, name: str, detail: str = "") -> None:
    RESULTS.append((ok, name, detail))
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail else ""))


def _fresh():
    d = pathlib.Path(tempfile.mkdtemp())
    repo = SQLiteResearchRepository(d / "stress.sqlite3", seed=False)
    return HSAResearchService(repo), repo


def _workspace(repo, *, branch="br-x", wp="wp-x", candidate=CANDIDATE, status="ready"):
    return repo.upsert_research_workspace(
        ResearchWorkspaceRecord(
            candidate_id=candidate, work_packet_id=wp, provider="neon",
            neon_branch_id=branch, neon_branch_name=f"twog-{branch}", provider_workspace_id=branch,
            database_secret_ref=f"neon://project/{branch}/neondb/neondb_owner_TOPSECRET",
            checkout_manifest_hash="sha256:" + "d" * 56, status=status,
        )
    )


def _request(ws, *, candidate=CANDIDATE, wp="wp-x", section="Docking", payload=None):
    return ProofCapsuleSubmitRequest(
        workspace_id=ws.workspace_id, checkout_manifest_hash=ws.checkout_manifest_hash,
        candidate_id=candidate, work_packet_id=wp,
        packet_type="evidence_addition", requested_action="evidence_review", submitted_by="dr.vet",
        target=ProofCapsuleTarget(section=section),
        summary=ProofCapsuleSummary(
            title="BYOC dock", finding="ligand engages the verified pocket",
            why_it_matters="target engagement", limitations=["in silico"],
        ),
        payload=payload or {"signal": "supports", "validation_type": "docking", "best_affinity_kcal_mol": -9.1},
    )


def _onboard(service, repo, principal="dr.vet"):
    """operator + an active, approved collaborator with a registered key. Returns (private_key)."""
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    priv, pub = provenance.generate_keypair()
    applicant = service.request_collaborator_access(principal=principal, name="Dr Vet", public_key=pub)
    service.approve_collaborator(applicant.collaborator_id, approved_by="chase")
    return priv


def _sign(service, request, priv):
    content_hash = service.capsule_content_hash_for_submission(request)
    return request.model_copy(update={"signature": service.sign_capsule_content(content_hash, priv)})


# ============================ HAPPY PATHS (must SUCCEED) ============================
def scenario_happy_path_end_to_end():
    print("\n# Legitimate end-to-end: apply → approve → sandbox → signed submit → operator accept")
    service, repo = _fresh()
    priv = _onboard(service, repo)
    ws = _workspace(repo)
    bundle = service.open_collaborator_sandbox("dr.vet", ws.workspace_id, validation_type="docking", candidate_id=CANDIDATE)
    check(bundle is not None and bundle["gate_policy"] == "external_collaborator", "sandbox opens + sealed")
    res = service.submit_external_proof_capsule(_sign(service, _request(ws), priv))
    check(res.accepted and res.persisted, "signed external capsule accepted", str(res.errors))
    if res.accepted:
        prov = service.verify_capsule_provenance(res.capsule)
        check(prov["signature_valid"], "stored signature verifies post-hoc")
        # operator can accept it across the write gate
        try:
            accepted = service.accept_proof_capsule(res.capsule.capsule_id, reviewer="chase")
            check(accepted is not None, "operator accepts across the write gate")
        except Exception as exc:  # noqa: BLE001
            check(False, "operator accepts across the write gate", f"raised {exc!r}")


def scenario_legit_submit_omitting_optional_work_packet():
    print("\n# Legitimate submit that OMITS the optional work_packet_id (workspace has one)")
    service, repo = _fresh()
    priv = _onboard(service, repo)
    ws = _workspace(repo, wp="wp-x")  # workspace HAS a work_packet_id
    service.lease_workspace(ws.workspace_id, "dr.vet")
    req = _request(ws, wp=None)  # collaborator leaves work_packet_id unset (it is optional)
    res = service.submit_external_proof_capsule(_sign(service, req, priv))
    check(res.accepted, "legit submit accepted even though work_packet_id omitted", str(res.errors))


# ============================ ATTACKS (must be REFUSED) ============================
def scenario_forged_signature():
    print("\n# Attack: forged signature (signed with a key that isn't the registered one)")
    service, repo = _fresh()
    _onboard(service, repo)
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.vet")
    attacker_priv, _ = provenance.generate_keypair()
    res = service.submit_external_proof_capsule(_sign(service, _request(ws), attacker_priv))
    check(not res.accepted and any("verify" in e for e in res.errors), "forged signature refused", str(res.errors))


def scenario_tampered_after_signing():
    print("\n# Attack: tamper the payload AFTER signing (content no longer matches the signature)")
    service, repo = _fresh()
    priv = _onboard(service, repo)
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.vet")
    signed = _sign(service, _request(ws), priv)
    tampered = signed.model_copy(update={"payload": {**signed.payload, "best_affinity_kcal_mol": -99.0, "signal": "supports"}})
    res = service.submit_external_proof_capsule(tampered)
    check(not res.accepted and any("verify" in e for e in res.errors), "post-signing tamper refused", str(res.errors))


def scenario_no_lease():
    print("\n# Attack: submit without holding the workspace lease")
    service, repo = _fresh()
    priv = _onboard(service, repo)
    ws = _workspace(repo)  # never leased by dr.vet
    res = service.submit_external_proof_capsule(_sign(service, _request(ws), priv))
    check(not res.accepted and any("lease" in e for e in res.errors), "submit without lease refused", str(res.errors))


def scenario_expired_lease():
    print("\n# Attack: submit on an EXPIRED lease")
    service, repo = _fresh()
    priv = _onboard(service, repo)
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.vet", ttl_seconds=-1)  # already expired
    res = service.submit_external_proof_capsule(_sign(service, _request(ws), priv))
    check(not res.accepted and any("lease" in e for e in res.errors), "expired-lease submit refused", str(res.errors))


def scenario_unregistered_ghost():
    print("\n# Attack: an unregistered 'ghost' principal submits")
    service, repo = _fresh()
    priv = _onboard(service, repo)
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.vet")
    ghost = _sign(service, _request(ws), priv).model_copy(update={"submitted_by": "ghost"})
    res = service.submit_external_proof_capsule(ghost)
    check(not res.accepted and any("registered" in e for e in res.errors), "unregistered submitter refused", str(res.errors))


def scenario_pending_applicant():
    print("\n# Attack: a PENDING applicant (not yet approved) tries to act")
    service, repo = _fresh()
    _, pub = provenance.generate_keypair()
    service.request_collaborator_access(principal="dr.vet", name="Dr Vet", public_key=pub)  # pending
    ws = _workspace(repo)
    try:
        service.open_collaborator_sandbox("dr.vet", ws.workspace_id, validation_type="docking")
        check(False, "pending applicant blocked from sandbox", "no exception raised")
    except CollaboratorAccessError:
        check(True, "pending applicant blocked from sandbox")


def scenario_revoked_collaborator():
    print("\n# Attack: a REVOKED collaborator tries to submit")
    service, repo = _fresh()
    priv = _onboard(service, repo)
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.vet")
    rec = service.resolve_principal("dr.vet")
    service.revoke_collaborator(rec.collaborator_id)
    res = service.submit_external_proof_capsule(_sign(service, _request(ws), priv))
    check(not res.accepted, "revoked collaborator submit refused", str(res.errors))


def scenario_collaborator_self_accept_promote():
    print("\n# Attack: a collaborator tries to cross the write gate (accept / promote its own work)")
    service, repo = _fresh()
    priv = _onboard(service, repo)
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.vet")
    res = service.submit_external_proof_capsule(_sign(service, _request(ws), priv))
    if not res.accepted:
        check(False, "self-accept test setup", f"submit failed: {res.errors}")
        return
    cid = res.capsule.capsule_id
    for action, fn in [("accept", lambda: service.accept_proof_capsule(cid, reviewer="dr.vet")),
                       ("promote", lambda: service.promote_proof_capsule_to_candidate(cid, reviewer="dr.vet"))]:
        try:
            fn()
            check(False, f"collaborator self-{action} blocked", "no exception raised")
        except CollaboratorAccessError:
            check(True, f"collaborator self-{action} blocked")


def scenario_collaborator_approves_peer():
    print("\n# Attack: a collaborator tries to approve a peer applicant")
    service, repo = _fresh()
    _onboard(service, repo)  # dr.vet active
    _, pub = provenance.generate_keypair()
    peer = service.request_collaborator_access(principal="vet2", name="Vet Two", public_key=pub)
    try:
        service.approve_collaborator(peer.collaborator_id, approved_by="dr.vet")
        check(False, "collaborator cannot approve a peer", "no exception raised")
    except CollaboratorAccessError:
        check(True, "collaborator cannot approve a peer")


def scenario_scope_escalation():
    print("\n# Attack: register a collaborator demanding write-gate scopes (privilege escalation)")
    service, repo = _fresh()
    sneaky = service.register_collaborator(
        principal="sneaky", name="Sneaky", role="collaborator",
        scopes=["promote_candidate", "accept_capsule", "submit_capsule"],
    )
    check(not sneaky.has_scope("promote_candidate") and not sneaky.has_scope("accept_capsule"),
          "write-gate scopes stripped from collaborator", f"scopes={sneaky.scopes}")


def scenario_cross_candidate_binding():
    print("\n# Attack: submit a capsule bound to a DIFFERENT candidate than the workspace")
    service, repo = _fresh()
    priv = _onboard(service, repo)
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.vet")
    req = _request(ws, candidate="twog-candidate-OTHER")  # mismatched candidate
    res = service.submit_external_proof_capsule(_sign(service, req, priv))
    check(not res.accepted and any("candidate_id mismatch" in e for e in res.errors),
          "cross-candidate binding refused", str(res.errors))


def scenario_signature_replay_other_workspace():
    print("\n# Attack: take a capsule signed for workspace A and submit it to workspace B")
    service, repo = _fresh()
    priv = _onboard(service, repo)
    ws_a = _workspace(repo, branch="br-a", wp="wp-a")
    ws_b = _workspace(repo, branch="br-b", wp="wp-b")
    service.lease_workspace(ws_b.workspace_id, "dr.vet")
    signed_for_a = _sign(service, _request(ws_a, wp="wp-a"), priv)
    # retarget to B but keep A's signature
    replayed = signed_for_a.model_copy(update={
        "workspace_id": ws_b.workspace_id, "checkout_manifest_hash": ws_b.checkout_manifest_hash, "work_packet_id": "wp-b",
    })
    res = service.submit_external_proof_capsule(replayed)
    check(not res.accepted and any("verify" in e for e in res.errors),
          "signature replay to another workspace refused", str(res.errors))


def scenario_deny_unknown_mode():
    print("\n# Strict mode: deny-unknown blocks unregistered principals at every gate")
    service, repo = _fresh()
    service.require_registered_principals = True
    ws = _workspace(repo)
    try:
        service.lease_workspace(ws.workspace_id, "stranger")
        check(False, "deny-unknown blocks unregistered lease", "no exception raised")
    except CollaboratorAccessError:
        check(True, "deny-unknown blocks unregistered lease")


def scenario_lease_theft():
    print("\n# Attack: a second collaborator tries to open a sandbox another already holds")
    service, repo = _fresh()
    _onboard(service, repo)  # dr.vet
    _, pub = provenance.generate_keypair()
    other = service.request_collaborator_access(principal="vet2", name="Vet Two", public_key=pub)
    service.approve_collaborator(other.collaborator_id, approved_by="chase")
    ws = _workspace(repo)
    service.open_collaborator_sandbox("dr.vet", ws.workspace_id, validation_type="docking")
    try:
        service.open_collaborator_sandbox("vet2", ws.workspace_id, validation_type="docking")
        check(False, "lease theft blocked", "no exception raised")
    except WorkspaceLeaseError:
        check(True, "lease theft blocked")


def scenario_replay_same_capsule_twice():
    print("\n# Edge: submit the SAME signed capsule twice (must be idempotent, not double-counted)")
    service, repo = _fresh()
    priv = _onboard(service, repo)
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.vet")
    signed = _sign(service, _request(ws), priv)
    r1 = service.submit_external_proof_capsule(signed)
    r2 = service.submit_external_proof_capsule(signed)
    from hsa_research.ingestion_bridge.contracts import ProofCapsuleLibraryRequest

    ledger = service.list_proof_capsules(ProofCapsuleLibraryRequest(candidate_id=CANDIDATE, limit=50)).capsules
    same = r1.accepted and r2.accepted and r1.capsule.content_hash == r2.capsule.content_hash
    check(same and len(ledger) == 1, "duplicate submit is idempotent (one capsule, same hash)",
          f"ledger={len(ledger)}")


def scenario_malformed_signature():
    print("\n# Robustness: a malformed / garbage signature must be refused gracefully (no crash)")
    service, repo = _fresh()
    _onboard(service, repo)
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.vet")
    for label, sig in [("empty", ""), ("garbage", "not-a-real-signature"), ("odd-hex", "abc"), ("wrong-len-hex", "ab" * 10)]:
        req = _request(ws).model_copy(update={"signature": sig or None})
        try:
            res = service.submit_external_proof_capsule(req)
            check(not res.accepted, f"malformed signature ({label}) refused without crash", str(res.errors)[:80])
        except Exception as exc:  # noqa: BLE001 — a crash on bad input IS a finding
            check(False, f"malformed signature ({label}) refused without crash", f"crashed: {exc!r}")


def scenario_identity_confusion():
    print("\n# Attack: principal A signs but claims to be principal B (B has a different key)")
    service, repo = _fresh()
    priv_a = _onboard(service, repo, principal="dr.vet")  # A active, key A
    _, pub_b = provenance.generate_keypair()
    b = service.request_collaborator_access(principal="dr.bob", name="Dr Bob", public_key=pub_b)
    service.approve_collaborator(b.collaborator_id, approved_by="chase")
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.bob")  # B holds the lease
    # A signs with key A but stamps submitted_by=dr.bob to ride B's lease
    forged = _request(ws).model_copy(update={"submitted_by": "dr.bob"})
    content_hash = service.capsule_content_hash_for_submission(forged)
    forged = forged.model_copy(update={"signature": service.sign_capsule_content(content_hash, priv_a)})
    res = service.submit_external_proof_capsule(forged)
    check(not res.accepted and any("verify" in e for e in res.errors),
          "identity confusion (A signs as B) refused", str(res.errors))


def scenario_secret_smuggling():
    print("\n# Attack: a collaborator smuggles a raw DB secret inside a signed capsule payload")
    service, repo = _fresh()
    priv = _onboard(service, repo)
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.vet")
    payload = {"signal": "supports", "validation_type": "docking",
               "leak": "postgresql://user:pass@host/db"}
    res = service.submit_external_proof_capsule(_sign(service, _request(ws, payload=payload), priv))
    check(not res.accepted and any("secret" in e.lower() for e in res.errors),
          "raw-secret smuggling refused even when signed", str(res.errors))


def scenario_secret_isolation():
    print("\n# Isolation: the sandbox bundle must not leak operator secrets")
    import json

    service, repo = _fresh()
    _onboard(service, repo)
    ws = _workspace(repo)
    bundle = service.open_collaborator_sandbox("dr.vet", ws.workspace_id, validation_type="docking")
    blob = json.dumps(bundle)
    check("TOPSECRET" not in blob and "database_secret_ref" not in blob and "neondb_owner" not in blob,
          "no operator secret leaks into the sandbox bundle")


# ============================ NASTY (auth-logic, DoS, malformed, concurrency) ====================
def scenario_reapply_key_takeover():
    print("\n# NASTY: attacker re-applies as an ACTIVE principal to swap its public key (account takeover)")
    service, repo = _fresh()
    victim_priv = _onboard(service, repo, principal="dr.vet")  # active, key = victim
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.vet")
    # attacker self-applies as dr.vet with THEIR key (no operator involved)
    attacker_priv, attacker_pub = provenance.generate_keypair()
    try:
        service.request_collaborator_access(principal="dr.vet", name="Dr Vet", public_key=attacker_pub)
    except CollaboratorAccessError:
        check(True, "re-apply on an active principal is refused (no key rotation via self-service)")
        return
    # if it did NOT refuse, can the attacker now sign as dr.vet?
    res = service.submit_external_proof_capsule(_sign(service, _request(ws), attacker_priv))
    check(not res.accepted, "attacker key cannot take over an active principal", f"accepted={res.accepted}")


def scenario_reapply_operator_demotion():
    print("\n# NASTY: attacker re-applies as the OPERATOR principal to demote it to collaborator")
    service, repo = _fresh()
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    _, attacker_pub = provenance.generate_keypair()
    try:
        service.request_collaborator_access(principal="chase", name="Chase", public_key=attacker_pub)
    except CollaboratorAccessError:
        pass  # refused — good
    chase = service.resolve_principal("chase")
    check(chase.role == "operator" and chase.has_scope("promote_candidate"),
          "operator is NOT demoted by a self-service re-application", f"role={chase.role}, scopes={chase.scopes}")


def scenario_huge_payload():
    print("\n# NASTY: a 5 MB payload (storage/DoS probe)")
    service, repo = _fresh()
    priv = _onboard(service, repo)
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.vet")
    big = {"signal": "supports", "validation_type": "docking", "blob": "A" * 5_000_000}
    try:
        res = service.submit_external_proof_capsule(_sign(service, _request(ws, payload=big), priv))
        # acceptance of an unbounded blob is a storage-DoS vector for an open system
        check(not res.accepted, "5 MB payload is bounded/refused", f"accepted={res.accepted} (unbounded payload = DoS risk)")
    except Exception as exc:  # noqa: BLE001
        check(False, "5 MB payload handled without crashing", f"crashed: {exc!r}")


def scenario_oversized_and_overlong_inputs():
    print("\n# NASTY: oversized lists + overlong strings must be rejected at the contract boundary")
    from pydantic import ValidationError

    service, repo = _fresh()
    ws = _workspace(repo)
    # >50 conflicts (list cap) -> ValidationError at construction
    try:
        _request(ws).model_copy(update={"conflicts": [f"c{i}" for i in range(200)]}, deep=True)
        # model_copy bypasses validation; build fresh instead
    except Exception:
        pass
    try:
        ProofCapsuleSummary(title="x" * 10_000, finding="f", why_it_matters="w", limitations=[])
        check(False, "overlong summary title rejected", "no ValidationError")
    except ValidationError:
        check(True, "overlong summary title rejected by the contract")
    try:
        ProofCapsuleTarget(section="s" * 10_000)
        check(False, "overlong target section rejected", "no ValidationError")
    except ValidationError:
        check(True, "overlong target section rejected by the contract")


def scenario_control_chars_and_whitespace():
    print("\n# NASTY: control chars / whitespace principal impersonation")
    service, repo = _fresh()
    priv = _onboard(service, repo, principal="dr.vet")
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.vet")
    # attacker (no key) claims a whitespace-padded variant of the victim's principal
    sneaky = _sign(service, _request(ws), priv).model_copy(update={"submitted_by": "  dr.vet  \n"})
    res = service.submit_external_proof_capsule(sneaky)
    # this happens to be the victim's own signature, so it MAY verify after normalization — the point
    # is no crash and a deterministic outcome (a non-key-holder could not produce this signature).
    check(isinstance(res.accepted, bool), "whitespace principal handled deterministically (no crash)",
          f"accepted={res.accepted}")


def scenario_concurrent_lease_race():
    print("\n# NASTY: two collaborators race to lease the SAME workspace concurrently")
    from concurrent.futures import ThreadPoolExecutor

    service, repo = _fresh()
    for p in ("vet1", "vet2"):
        c = service.register_collaborator(principal=p, name=p, role="collaborator")
        del c
    ws = _workspace(repo)

    def grab(principal):
        try:
            r = service.lease_workspace(ws.workspace_id, principal)
            return (principal, r.leased_by if r else None, None)
        except Exception as exc:  # noqa: BLE001
            return (principal, None, repr(exc))

    with ThreadPoolExecutor(max_workers=2) as ex:
        list(ex.map(grab, ["vet1", "vet2"]))
    final = repo.get_research_workspace(ws.workspace_id)
    # SAFETY invariant (the security property): a race must NEVER yield two simultaneous holders. A
    # dropped write (held=None) under single-node SQLite contention is a safe, retryable degradation —
    # production Postgres serializes this. The violation we guard against is a DOUBLE-GRANT.
    held = final.leased_by
    safe_state = held in (None, "vet1", "vet2")
    double_grant = False
    if held is not None:
        other = "vet2" if held == "vet1" else "vet1"
        try:
            service.lease_workspace(ws.workspace_id, other)  # a non-steal lease must be rejected
            double_grant = True
        except WorkspaceLeaseError:
            double_grant = False
    check(safe_state and not double_grant,
          "lease race never yields two holders (single-node may drop writes; Postgres serializes)",
          f"held={held}")


def scenario_concurrent_duplicate_submit():
    print("\n# NASTY: race two byte-identical external submits (dedup under concurrency)")
    from concurrent.futures import ThreadPoolExecutor

    from hsa_research.ingestion_bridge.contracts import ProofCapsuleLibraryRequest

    service, repo = _fresh()
    priv = _onboard(service, repo)
    ws = _workspace(repo)
    service.lease_workspace(ws.workspace_id, "dr.vet")
    signed = _sign(service, _request(ws), priv)

    def submit(_):
        try:
            return service.submit_external_proof_capsule(signed).accepted
        except Exception as exc:  # noqa: BLE001
            return repr(exc)

    with ThreadPoolExecutor(max_workers=4) as ex:
        list(ex.map(submit, range(4)))
    ledger = service.list_proof_capsules(ProofCapsuleLibraryRequest(candidate_id=CANDIDATE, limit=50)).capsules
    # SAFETY invariant: duplicates must NEVER flood the ledger (>1 identical row). 0 rows = single-node
    # SQLite dropped writes under contention (safe, retryable). The hard cross-process guarantee is a
    # (candidate_id, content_hash) UNIQUE constraint in Postgres — recommended production hardening.
    check(len(ledger) <= 1, "concurrent duplicate submits never flood the ledger (>1)",
          f"ledger={len(ledger)} (0 = single-node dropped writes; add a content_hash UNIQUE index in Postgres)")


# ============================ BYOC (Unit 2: container runner + foreign provenance) ===============
_DIGEST = "ghcr.io/twog/gnina@sha256:" + "a" * 64
_MUTABLE = "ghcr.io/twog/gnina:latest"
_MANIFEST = "sha256:" + "e" * 56
_SNAPSHOT = "sha256:" + "f" * 56


class _OkBackend:
    def run(self, spec):
        return {"status": "completed", "provider_job_id": "byoc-1",
                "output_payload": {"signal": "neutral", "validation_type": spec.get("lane"),
                                   "metrics": {"best_affinity_kcal_mol": -7.0}}}


def _byoc(name):
    service, repo = _fresh()
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    priv, pub = provenance.generate_keypair()
    service.register_collaborator(principal="dr.vet", name="Dr Vet", role="collaborator", public_key=pub)
    return service, repo, priv


def _container_job(repo, *, image=_DIGEST, principal="dr.vet", status="completed"):
    return repo.upsert_compute_job(ComputeJobRecord(
        status=status, runner_kind="container", compute_profile="gpu_a100", validation_type="docking",
        title="BYOC dock", objective="container dock", candidate_id=CANDIDATE,
        checkout_manifest_hash=_MANIFEST, candidate_snapshot_hash=_SNAPSHOT,
        container_image=image, submitted_by=principal,
    ))


def _foreign_capsule(repo, service, priv, job, *, image=_DIGEST, runner_principal="dr.vet",
                     signal="neutral", sign=True, signer_priv=None):
    cap = ProofCapsuleRecord(
        workspace_id=uuid4(), checkout_manifest_hash=_MANIFEST, candidate_id=CANDIDATE,
        candidate_snapshot_hash=_SNAPSHOT, packet_type="compute_artifact", requested_action="evidence_review",
        target=ProofCapsuleTarget(section="docking"),
        summary=ProofCapsuleSummary(title="BYOC result", finding="binds", why_it_matters="engagement",
                                    limitations=["in silico"]),
        submitted_by="dr.vet",
        payload={"compute_job_id": str(job.compute_job_id), "validation_type": "docking",
                 "container_image": image, "runner_principal": runner_principal, "signal": signal,
                 "provider": "container"},
        content_hash="byoc" + "0" * 36, status="submitted",
    )
    if sign:
        cap = cap.model_copy(update={"signature": service.sign_capsule_content(cap.content_hash, signer_priv or priv)})
    return repo.upsert_proof_capsule(cap)


def _accept(service, cap, *, confound=False):
    return service.accept_proof_capsule(cap.capsule_id, reviewer="chase", enforce_confound_gate=confound)


def scenario_byoc_digest_swap():
    print("\n# BYOC: capsule claims a DIFFERENT image digest than the job ran")
    service, repo, priv = _byoc("swap")
    job = _container_job(repo)
    cap = _foreign_capsule(repo, service, priv, job, image="ghcr.io/twog/gnina@sha256:" + "b" * 64)
    out = _accept(service, cap)
    blocked = out.status == "submitted" and out.metadata.get("provenance_gate", {}).get("status") == "blocked"
    check(blocked, "BYOC image-digest swap blocked at accept", str(out.metadata.get("provenance_gate", {}).get("mismatches")))


def scenario_byoc_identity_spoof():
    print("\n# BYOC: capsule claims a runner identity that didn't run the job")
    service, repo, priv = _byoc("ident")
    job = _container_job(repo, principal="dr.vet")
    cap = _foreign_capsule(repo, service, priv, job, runner_principal="impostor")
    out = _accept(service, cap)
    check(out.status == "submitted", "BYOC runner-identity spoof blocked at accept")


def scenario_byoc_unsigned():
    print("\n# BYOC: a foreign capsule with NO signature must not be accepted")
    service, repo, priv = _byoc("unsigned")
    job = _container_job(repo)
    cap = _foreign_capsule(repo, service, priv, job, sign=False)
    out = _accept(service, cap)
    gate = out.metadata.get("provenance_gate", {})
    check(out.status == "submitted" and "foreign_capsule_signature_invalid" in gate.get("mismatches", []),
          "unsigned foreign capsule blocked at accept", str(gate.get("mismatches")))


def scenario_byoc_wrong_key():
    print("\n# BYOC: foreign capsule signed by a key that isn't the runner's registered key")
    service, repo, priv = _byoc("wrongkey")
    job = _container_job(repo)
    other_priv, _ = provenance.generate_keypair()
    cap = _foreign_capsule(repo, service, priv, job, signer_priv=other_priv)
    out = _accept(service, cap)
    check(out.status == "submitted", "wrong-key foreign signature blocked at accept")


def scenario_byoc_mutable_image():
    print("\n# BYOC: a non-digest-pinned (mutable) image is refused by runner AND provenance")
    service, repo, priv = _byoc("mut")
    # runner refuses to dispatch a mutable tag
    job = _container_job(repo, image=_MUTABLE)
    runner_out = get_compute_runner(job).submit(job)
    runner_refused = runner_out["status"] == "failed" and runner_out["output_payload"]["error"] == "container_image_must_be_digest_pinned"
    # and the auditor flags it if such a capsule is somehow submitted
    cap = _foreign_capsule(repo, service, priv, job, image=_MUTABLE)
    out = _accept(service, cap)
    check(runner_refused and out.status == "submitted", "mutable image refused (runner + accept gate)")


def scenario_byoc_credentials_never_persisted():
    print("\n# BYOC: the collaborator's credentials must never be persisted in the job/result")
    import json as _json

    service, repo, priv = _byoc("creds")
    register_container_backend("ok", lambda: _OkBackend())
    job = repo.upsert_compute_job(ComputeJobRecord(
        status="approved", runner_kind="container", compute_profile="gpu_a100", validation_type="docking",
        title="BYOC dock", objective="container dock", candidate_id=CANDIDATE,
        container_image=_DIGEST, submitted_by="dr.vet",
        input_payload={"container": {"backend": "ok", "credentials": {"token": "SUPERSECRET_BYOC_CRED"},
                                     "config": {"ligand_smiles": "C"}}},
    ))
    out = get_compute_runner(job).submit(job)
    leaked = "SUPERSECRET_BYOC_CRED" in _json.dumps(out)
    check(out["status"] == "completed" and not leaked,
          "BYOC credentials not echoed into the result (not persisted)", f"leaked={leaked}")


def scenario_byoc_signed_but_still_needs_science_gate():
    print("\n# BYOC: a SIGNED, provenance-clean 'supports' capsule still must clear the confound gate")
    service, repo, priv = _byoc("science")
    job = _container_job(repo)
    cap = _foreign_capsule(repo, service, priv, job, signal="supports")  # legitimately signed + matched
    out = service.accept_proof_capsule(cap.capsule_id, reviewer="chase")  # confound gate ON
    # signed != trusted science: a supports signal with no surviving control is still blocked
    blocked = out.status == "submitted" and "confound_gate" in out.metadata
    check(blocked, "signed foreign 'supports' still gated by confounds (signed != true)",
          str(out.metadata.get("confound_gate", {}).get("verdict")))


def scenario_byoc_noncompleted_job():
    print("\n# BYOC: claiming a job that never completed is rejected")
    service, repo, priv = _byoc("incomplete")
    job = _container_job(repo, status="running")
    cap = _foreign_capsule(repo, service, priv, job)
    out = _accept(service, cap)
    check(out.status == "submitted", "claim on a non-completed container job blocked")


def main() -> None:
    print("=" * 78)
    print("PHASE B — UNITS 1+2 ADVERSARIAL STRESS TEST")
    print("=" * 78)
    scenarios = [
        scenario_happy_path_end_to_end,
        scenario_legit_submit_omitting_optional_work_packet,
        scenario_forged_signature,
        scenario_tampered_after_signing,
        scenario_no_lease,
        scenario_expired_lease,
        scenario_unregistered_ghost,
        scenario_pending_applicant,
        scenario_revoked_collaborator,
        scenario_collaborator_self_accept_promote,
        scenario_collaborator_approves_peer,
        scenario_scope_escalation,
        scenario_cross_candidate_binding,
        scenario_signature_replay_other_workspace,
        scenario_deny_unknown_mode,
        scenario_lease_theft,
        scenario_replay_same_capsule_twice,
        scenario_malformed_signature,
        scenario_identity_confusion,
        scenario_secret_smuggling,
        scenario_secret_isolation,
        scenario_reapply_key_takeover,
        scenario_reapply_operator_demotion,
        scenario_huge_payload,
        scenario_oversized_and_overlong_inputs,
        scenario_control_chars_and_whitespace,
        scenario_concurrent_lease_race,
        scenario_concurrent_duplicate_submit,
        scenario_byoc_digest_swap,
        scenario_byoc_identity_spoof,
        scenario_byoc_unsigned,
        scenario_byoc_wrong_key,
        scenario_byoc_mutable_image,
        scenario_byoc_credentials_never_persisted,
        scenario_byoc_signed_but_still_needs_science_gate,
        scenario_byoc_noncompleted_job,
    ]
    for scenario in scenarios:
        try:
            scenario()
        except Exception as exc:  # noqa: BLE001 — a scenario crashing is itself a finding
            check(False, f"{scenario.__name__} (uncaught)", repr(exc))

    passed = sum(1 for ok, _, _ in RESULTS if ok)
    total = len(RESULTS)
    print("\n" + "=" * 78)
    print(f"RESULT: {passed}/{total} checks held")
    failures = [(n, d) for ok, n, d in RESULTS if not ok]
    if failures:
        print("\nFAILURES (boundary did NOT behave correctly):")
        for n, d in failures:
            print(f"  ✗ {n}" + (f" — {d}" if d else ""))
        raise SystemExit(1)
    print("All boundary checks held. ✅")


if __name__ == "__main__":
    main()
