"""Phase B — the web JSON API (dispatch routing + auth mapping).

dispatch() is pure over (service, method, path, principal, body). It resolves identity upstream and
delegates AUTHZ to the service's hardened _authorize, mapping exceptions to HTTP status. These tests
exercise routing + the 401/403/404 mapping; deep authz behavior is covered by the service tests.
"""

from __future__ import annotations

from uuid import uuid4

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository

from hsa_research.ingestion_bridge import provenance, web_api
from hsa_research.ingestion_bridge.web_api import dispatch, resolve_principal_from_token


def _setup(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "api.sqlite3", seed=False)
    service = HSAResearchService(repo)
    operator = service.register_collaborator(
        principal="chase", name="Chase", role="operator", auth_subject="workos_op"
    )
    _, pub = provenance.generate_keypair()
    applicant = service.request_collaborator_access(
        principal="dr.vet", name="Dr Vet", public_key=pub, auth_subject="workos_vet"
    )
    collaborator = service.approve_collaborator(applicant.collaborator_id, approved_by="chase")
    return service, operator, collaborator


def test_unauthenticated_is_401(tmp_path):
    service, *_ = _setup(tmp_path)
    assert dispatch(service, method="GET", path="/me", principal=None)[0] == 401
    assert dispatch(service, method="GET", path="/candidates", principal=None)[0] == 401


def test_public_routes_need_no_auth(tmp_path):
    """The public read surface (STATE/EVIDENCE/RUNS) must serve with principal=None."""
    service, *_ = _setup(tmp_path)
    for path in ("/public/state", "/public/capsules", "/public/campaigns", "/public/candidates"):
        status, _payload = dispatch(service, method="GET", path=path, principal=None)
        assert status == 200, f"{path} should be public, got {status}"
    assert isinstance(dispatch(service, method="GET", path="/public/state", principal=None)[1], dict)
    assert isinstance(dispatch(service, method="GET", path="/public/capsules", principal=None)[1], list)


def test_public_state_has_display_contract(tmp_path):
    service, *_ = _setup(tmp_path)
    _, state = dispatch(service, method="GET", path="/public/state", principal=None)
    assert state["online"] is True
    assert {"hypothesesFalsified", "validatedResults", "computeLanes", "testsPassing"} <= set(state["headline"])
    assert len(state["loop"]) == 5


def test_public_unknown_id_is_404(tmp_path):
    service, *_ = _setup(tmp_path)
    assert dispatch(service, method="GET", path=f"/public/capsules/{uuid4()}", principal=None)[0] == 404
    assert dispatch(service, method="GET", path=f"/public/campaigns/{uuid4()}", principal=None)[0] == 404


def test_me_returns_principal(tmp_path):
    service, _operator, collaborator = _setup(tmp_path)
    status, payload = dispatch(service, method="GET", path="/me", principal=collaborator)
    assert status == 200 and payload["principal"] == "dr.vet" and payload["role"] == "collaborator"


def test_pending_applicant_is_403_on_reads(tmp_path):
    service, *_ = _setup(tmp_path)
    _, pub = provenance.generate_keypair()
    pending = service.request_collaborator_access(principal="newbie", name="New", public_key=pub)
    assert dispatch(service, method="GET", path="/candidates", principal=pending)[0] == 403


def test_operator_only_list_collaborators(tmp_path):
    service, operator, collaborator = _setup(tmp_path)
    assert dispatch(service, method="GET", path="/collaborators", principal=collaborator)[0] == 403
    status, payload = dispatch(service, method="GET", path="/collaborators", principal=operator)
    assert status == 200 and {c["principal"] for c in payload} >= {"chase", "dr.vet"}


def test_apply_is_open_to_unmapped_user(tmp_path):
    service, *_ = _setup(tmp_path)
    _, pub = provenance.generate_keypair()
    status, payload = dispatch(
        service, method="POST", path="/collaborators/apply", principal=None,
        body={"principal": "applicant1", "name": "Applicant", "public_key": pub, "auth_subject": "workos_new"},
    )
    assert status == 201 and payload["status"] == "pending"


def test_candidate_proposal_contribution(tmp_path):
    service, _operator, collaborator = _setup(tmp_path)
    status, payload = dispatch(
        service, method="POST", path="/contributions", principal=collaborator,
        body={"kind": "candidate_proposal", "candidate_id": "prop-x", "title": "Test a target",
              "targets": ["KDR"]},
    )
    assert status == 201 and payload["public_status"] == "proposed"
    # operator sees it in the review queue; a collaborator cannot
    assert dispatch(service, method="GET", path="/proposals", principal=collaborator)[0] == 403
    op_status, proposals = dispatch(service, method="GET", path="/proposals", principal=_operator)
    assert op_status == 200 and any(p["candidate_id"] == "prop-x" for p in proposals)


def test_write_gate_scope_enforced_by_service(tmp_path):
    service, _operator, collaborator = _setup(tmp_path)
    # a collaborator hitting the operator write gate → the service's _authorize denies → 403
    status, _ = dispatch(
        service, method="POST", path=f"/capsules/{uuid4()}/accept", principal=collaborator
    )
    assert status == 403


def test_unknown_route_is_404(tmp_path):
    service, _operator, collaborator = _setup(tmp_path)
    assert dispatch(service, method="GET", path="/nope", principal=collaborator)[0] == 404


def test_token_resolution_maps_auth_subject_to_principal(tmp_path):
    service, *_ = _setup(tmp_path)
    # the WorkOS verify seam is injected; a token verifying to an auth_subject resolves to a principal
    verify = lambda tok: {"tok_vet": "workos_vet", "tok_op": "workos_op"}.get(tok)
    assert resolve_principal_from_token(service, "tok_vet", verify_token=verify).principal == "dr.vet"
    assert resolve_principal_from_token(service, "tok_op", verify_token=verify).role == "operator"
    assert resolve_principal_from_token(service, None, verify_token=verify) is None
    assert resolve_principal_from_token(service, "bogus", verify_token=verify) is None
