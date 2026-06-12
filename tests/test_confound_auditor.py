"""Confound Auditor pre-gate (increment 3) — adversarial gate on supporting evidence.

A `supports` capsule cannot be accepted onto a candidate until its known confounds each have a
surviving control. The verdict is never "true"; best is "survived known confounds". Missing controls
=> "unauditable" (blocked); a control that shows the effect vanishes => "refuted" (blocked).
"""

from __future__ import annotations

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository
from tests.test_candidates import _seed_validation_ready_candidate
from tests.test_falsification_planner import _seed_capsule

OMICS_CONFOUNDS = ["tumor_purity", "batch", "cell_composition", "normalization"]


def _svc(tmp_path, name, cid):
    repo = SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _seed_validation_ready_candidate(repo, candidate_id=cid)
    return service, repo


# ---- audit verdicts --------------------------------------------------------------------------
def test_supports_capsule_without_controls_is_unauditable(tmp_path):
    service, repo = _svc(tmp_path, "ua", "vr-ua")
    cap = _seed_capsule(repo, "vr-ua", signal="supports", validation_type="omics", confidence=0.8)

    verdict = service.audit_capsule_confounds(cap.capsule_id)
    assert verdict.status == "unauditable"  # NOT "clean"
    assert "tumor_purity" in verdict.missing_controls
    assert verdict.demanded_control_plan is not None  # reuses the planner to emit the owed control
    assert verdict.status != "survived_known_confounds"  # verdict is never "true"


def test_all_surviving_controls_yield_survived(tmp_path):
    service, repo = _svc(tmp_path, "ok", "vr-ok")
    cap = _seed_capsule(repo, "vr-ok", signal="supports", validation_type="omics", confidence=0.8)
    for confound in OMICS_CONFOUNDS:
        _seed_capsule(repo, "vr-ok", signal="supports", validation_type="omics", controls_confound=confound)

    verdict = service.audit_capsule_confounds(cap.capsule_id)
    assert verdict.status == "survived_known_confounds"
    assert sorted(verdict.checked_confounds) == sorted(OMICS_CONFOUNDS)


def test_refuting_control_yields_refuted(tmp_path):
    service, repo = _svc(tmp_path, "ref", "vr-ref")
    cap = _seed_capsule(repo, "vr-ref", signal="supports", validation_type="omics", confidence=0.8)
    # a tumor-purity control whose own signal goes neutral => the effect vanished under purity
    _seed_capsule(repo, "vr-ref", signal="neutral", validation_type="omics", controls_confound="tumor_purity")
    for confound in ["batch", "cell_composition", "normalization"]:
        _seed_capsule(repo, "vr-ref", signal="supports", validation_type="omics", controls_confound=confound)

    verdict = service.audit_capsule_confounds(cap.capsule_id)
    assert verdict.status == "refuted"
    assert "tumor_purity" in verdict.refuted_by


def test_neutral_capsule_audit_not_required(tmp_path):
    service, repo = _svc(tmp_path, "nr", "vr-nr")
    cap = _seed_capsule(repo, "vr-nr", signal="neutral", validation_type="omics", confidence=0.0)
    assert service.audit_capsule_confounds(cap.capsule_id).status == "not_required"


def test_audit_persists_verdict_without_mutating_content_hash(tmp_path):
    service, repo = _svc(tmp_path, "persist", "vr-persist")
    cap = _seed_capsule(repo, "vr-persist", signal="supports", validation_type="omics", confidence=0.8)

    service.audit_capsule_confounds(cap.capsule_id)
    reloaded = repo.get_proof_capsule(cap.capsule_id)
    assert reloaded.metadata["confound_verdict"]["status"] == "unauditable"
    assert reloaded.content_hash == cap.content_hash  # metadata is excluded from the content hash


# ---- the accept pre-gate ---------------------------------------------------------------------
def test_accept_blocks_unaudited_supports_capsule(tmp_path):
    service, repo = _svc(tmp_path, "blk", "vr-blk")
    cap = _seed_capsule(repo, "vr-blk", signal="supports", validation_type="omics", confidence=0.8)

    result = service.accept_proof_capsule(cap.capsule_id, reviewer="chase")
    assert result.status == "submitted"  # NOT accepted — the gate held
    assert result.metadata["confound_gate"]["status"] == "blocked"
    assert result.metadata["confound_gate"]["verdict"] == "unauditable"


def test_accept_blocks_refuted_supports_capsule(tmp_path):
    service, repo = _svc(tmp_path, "blkref", "vr-blkref")
    cap = _seed_capsule(repo, "vr-blkref", signal="supports", validation_type="omics", confidence=0.8)
    _seed_capsule(repo, "vr-blkref", signal="refutes", validation_type="omics", controls_confound="tumor_purity")
    for confound in ["batch", "cell_composition", "normalization"]:
        _seed_capsule(repo, "vr-blkref", signal="supports", validation_type="omics", controls_confound=confound)

    blocked = service.accept_proof_capsule(cap.capsule_id, reviewer="chase")
    assert blocked.status == "submitted"
    assert blocked.metadata["confound_gate"]["verdict"] == "refuted"


def test_accept_succeeds_when_confounds_survive(tmp_path):
    service, repo = _svc(tmp_path, "okacc", "vr-okacc")
    cap = _seed_capsule(repo, "vr-okacc", signal="supports", validation_type="omics", confidence=0.8)
    for confound in OMICS_CONFOUNDS:
        _seed_capsule(repo, "vr-okacc", signal="supports", validation_type="omics", controls_confound=confound)

    accepted = service.accept_proof_capsule(cap.capsule_id, reviewer="chase")
    assert accepted.status == "accepted_for_evidence_review"
    assert accepted.reviewed_by == "chase"


def test_neutral_capsule_accepts_unimpeded(tmp_path):
    service, repo = _svc(tmp_path, "nracc", "vr-nracc")
    cap = _seed_capsule(repo, "vr-nracc", signal="neutral", validation_type="omics", confidence=0.0)
    accepted = service.accept_proof_capsule(cap.capsule_id, reviewer="chase")
    assert accepted.status == "accepted_for_evidence_review"  # gate skipped for non-supports
