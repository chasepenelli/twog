"""Phase B / B1 — the semi-open collaborator lifecycle: apply -> operator approves -> scoped key.

An applicant self-registers as PENDING (powerless until approved); an operator approves them into an
active, scoped collaborator. A pending or revoked principal cannot lease/submit. Offline.
"""

from __future__ import annotations

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository

import pytest

from hsa_research.ingestion_bridge import provenance
from hsa_research.ingestion_bridge.service import CollaboratorAccessError


def _service(tmp_path, name="life"):
    return HSAResearchService(SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False))


def test_apply_creates_powerless_pending_record(tmp_path):
    service = _service(tmp_path)
    _, pub = provenance.generate_keypair()
    rec = service.request_collaborator_access(principal="dr.vet", name="Dr Vet", public_key=pub)
    assert rec.status == "pending" and rec.role == "collaborator"
    # pending holds NO effective scopes — has_scope is False until active
    assert not rec.has_scope("submit_capsule") and not rec.has_scope("lease_workspace")
    # and the registry resolves it but the authorize gate still denies a scoped action
    resolved = service.resolve_principal("dr.vet")
    assert resolved is not None and resolved.status == "pending"


def test_apply_requires_public_key(tmp_path):
    service = _service(tmp_path)
    with pytest.raises(ValueError):
        service.request_collaborator_access(principal="dr.vet", name="Dr Vet", public_key="")


def test_operator_approves_pending_into_active_scoped(tmp_path):
    service = _service(tmp_path)
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    _, pub = provenance.generate_keypair()
    applicant = service.request_collaborator_access(principal="dr.vet", name="Dr Vet", public_key=pub)

    approved = service.approve_collaborator(applicant.collaborator_id, approved_by="chase")
    assert approved.status == "active"
    assert approved.has_scope("submit_capsule") and approved.has_scope("lease_workspace")
    # never the write-gate scopes
    assert not approved.has_scope("accept_capsule") and not approved.has_scope("promote_candidate")
    # the public key carried through onboarding (so signed contributions verify)
    assert approved.public_key == pub


def test_collaborator_cannot_approve_a_peer(tmp_path):
    service = _service(tmp_path)
    # an active collaborator lacks promote_candidate, so cannot approve applicants
    _, pub = provenance.generate_keypair()
    peer = service.register_collaborator(principal="vet1", name="Vet One", role="collaborator")
    assert peer.has_scope("submit_capsule")
    applicant = service.request_collaborator_access(principal="dr.vet", name="Dr Vet", public_key=pub)
    with pytest.raises(CollaboratorAccessError):
        service.approve_collaborator(applicant.collaborator_id, approved_by="vet1")


def test_revoked_collaborator_loses_all_scopes(tmp_path):
    service = _service(tmp_path)
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    _, pub = provenance.generate_keypair()
    applicant = service.request_collaborator_access(principal="dr.vet", name="Dr Vet", public_key=pub)
    approved = service.approve_collaborator(applicant.collaborator_id, approved_by="chase")
    revoked = service.revoke_collaborator(approved.collaborator_id)
    assert revoked.status == "revoked"
    assert not service.resolve_principal("dr.vet").has_scope("submit_capsule")


def test_reapply_cannot_take_over_active_principal(tmp_path):
    """SECURITY (found by scripts/stress_phase_b.py): self-service re-application must NOT mutate an
    existing active principal — otherwise anyone could re-apply with their own key (account takeover)."""
    service = _service(tmp_path)
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    _, victim_pub = provenance.generate_keypair()
    applicant = service.request_collaborator_access(principal="dr.vet", name="Dr Vet", public_key=victim_pub)
    service.approve_collaborator(applicant.collaborator_id, approved_by="chase")

    _, attacker_pub = provenance.generate_keypair()
    with pytest.raises(CollaboratorAccessError):
        service.request_collaborator_access(principal="dr.vet", name="Dr Vet", public_key=attacker_pub)
    # the victim's key is unchanged
    assert service.resolve_principal("dr.vet").public_key == victim_pub


def test_reapply_cannot_demote_operator(tmp_path):
    """SECURITY: re-applying as the operator principal must not downgrade its role/scopes."""
    service = _service(tmp_path)
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    _, attacker_pub = provenance.generate_keypair()
    with pytest.raises(CollaboratorAccessError):
        service.request_collaborator_access(principal="chase", name="Chase", public_key=attacker_pub)
    chase = service.resolve_principal("chase")
    assert chase.role == "operator" and chase.has_scope("promote_candidate")


def test_reapply_while_pending_is_allowed(tmp_path):
    """A still-pending applicant may re-submit (e.g. fix a typo) before approval."""
    service = _service(tmp_path)
    _, pub1 = provenance.generate_keypair()
    first = service.request_collaborator_access(principal="dr.vet", name="Typo", public_key=pub1)
    _, pub2 = provenance.generate_keypair()
    second = service.request_collaborator_access(principal="dr.vet", name="Dr Vet", public_key=pub2)
    assert second.collaborator_id == first.collaborator_id  # same pending record updated in place
    assert second.status == "pending" and second.public_key == pub2 and second.name == "Dr Vet"


def test_auth_subject_maps_external_identity_to_principal(tmp_path):
    """The web boundary's account→principal link: a WorkOS user id (auth_subject) captured at apply
    time resolves to the collaborator's principal, which then feeds _authorize/scopes."""
    service = _service(tmp_path)
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    _, pub = provenance.generate_keypair()
    applicant = service.request_collaborator_access(
        principal="dr.vet", name="Dr Vet", public_key=pub, auth_subject="workos_user_01ABC"
    )
    assert applicant.auth_subject == "workos_user_01ABC"
    # resolves both before and after approval; resolution is by the stable external id, not email
    assert service.resolve_collaborator_by_auth_subject("workos_user_01ABC").principal == "dr.vet"
    service.approve_collaborator(applicant.collaborator_id, approved_by="chase")
    assert service.resolve_collaborator_by_auth_subject("workos_user_01ABC").status == "active"
    # unmapped / blank identities resolve to nobody (→ no access)
    assert service.resolve_collaborator_by_auth_subject("workos_user_UNKNOWN") is None
    assert service.resolve_collaborator_by_auth_subject(None) is None


def test_list_collaborators_filters_by_status(tmp_path):
    service = _service(tmp_path)
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    _, pub = provenance.generate_keypair()
    service.request_collaborator_access(principal="pend1", name="Pending One", public_key=pub)
    pending = service.list_collaborators(status="pending")
    assert [c.principal for c in pending] == ["pend1"]
    actives = {c.principal for c in service.list_collaborators(status="active")}
    assert "chase" in actives and "pend1" not in actives
