"""Phase B / Unit 3 (B5) — the no-compute contribution path: candidate proposals.

A collaborator proposes a candidate/hypothesis (zero compute); it enters as a 'proposed' public
candidate awaiting an operator decision. Operator approve → validation-ready (RUNNABLE, but not run —
dispatch stays a separate operator-only-spend action); reject → archived. Operator-gated. Offline.
"""

from __future__ import annotations

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository

import pytest

from hsa_research.ingestion_bridge.service import CollaboratorAccessError


def _service(tmp_path, name="prop"):
    repo = SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False)
    service = HSAResearchService(repo)
    service.register_collaborator(principal="chase", name="Chase", role="operator")
    service.register_collaborator(principal="dr.vet", name="Dr Vet", role="collaborator")
    return service


def _propose(service, cid="prop-pik3ca"):
    return service.submit_candidate_proposal(
        "dr.vet", candidate_id=cid, title="Test PIK3CA in a new HSA subset",
        rationale="worth a falsification run", targets=["PIK3CA"],
        candidate_therapies=["alpelisib"], evidence_refs=["PMID:123"],
    )


def test_proposal_enters_as_proposed_not_runnable(tmp_path):
    service = _service(tmp_path)
    rec = _propose(service)
    assert rec.public_status == "proposed"
    assert rec.validation_ready is not True  # NOT runnable until an operator approves
    assert rec.metadata["proposal"]["proposed_by"] == "dr.vet"
    assert rec.targets == ["PIK3CA"]


def test_list_candidate_proposals(tmp_path):
    service = _service(tmp_path)
    _propose(service, "prop-a")
    _propose(service, "prop-b")
    proposals = service.list_candidate_proposals()
    assert {p.candidate_id for p in proposals} == {"prop-a", "prop-b"}


def test_revoked_principal_cannot_propose(tmp_path):
    service = _service(tmp_path)
    revoked = service.resolve_principal("dr.vet")
    service.revoke_collaborator(revoked.collaborator_id)
    with pytest.raises(CollaboratorAccessError):
        _propose(service)


def test_collaborator_cannot_decide_a_proposal(tmp_path):
    service = _service(tmp_path)
    _propose(service)
    with pytest.raises(CollaboratorAccessError):
        service.decide_candidate_proposal("prop-pik3ca", approved_by="dr.vet", approve=True)


def test_operator_approve_makes_runnable_but_does_not_run(tmp_path):
    service = _service(tmp_path)
    _propose(service)
    out = service.decide_candidate_proposal("prop-pik3ca", approved_by="chase", approve=True)
    assert out is not None
    cand = service.get_public_candidate("prop-pik3ca")
    assert cand.validation_ready is True  # now runnable...
    # ...but approval did NOT auto-run it: no campaign manifest, no capsules exist yet
    from hsa_research.ingestion_bridge.contracts import ProofCapsuleLibraryRequest

    assert service.list_proof_capsules(ProofCapsuleLibraryRequest(candidate_id="prop-pik3ca")).capsules == []
    assert service.list_run_manifests(manifest_type="falsification_campaign") == []


def test_operator_reject_archives_with_reason(tmp_path):
    service = _service(tmp_path)
    _propose(service)
    out = service.decide_candidate_proposal(
        "prop-pik3ca", approved_by="chase", approve=False, note="out of scope"
    )
    assert out.public_status == "archived"
    assert out.metadata["proposal_decision"] == {"by": "chase", "approved": False, "note": "out of scope"}
    assert service.get_public_candidate("prop-pik3ca").validation_ready is not True
