"""Falsification round (increment 2) — pre-registration + mock auto-dispatch.

The loop closes from "proposes" to "proposes AND runs": the agent pre-registers its kill-criterion
(content-hashed BEFORE dispatch), runs the test on the MockComputeRunner, and reads the result against
the pre-registration. The resulting capsule is never auto-accepted or auto-promoted — the human
write-gate stays terminal.
"""

from __future__ import annotations

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository
from tests.test_candidates import _seed_validation_ready_candidate
from tests.test_falsification_planner import _seed_capsule


def _ready_with_supports(tmp_path, name, cid):
    repo = SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _seed_validation_ready_candidate(repo, candidate_id=cid)
    _seed_capsule(repo, cid, signal="supports", validation_type="omics", confidence=0.8)
    return service, repo


# ---- pre-registration lock -------------------------------------------------------------------
def test_register_freezes_kill_criterion_on_approved_queue_item(tmp_path):
    service, _ = _ready_with_supports(tmp_path, "reg", "vr-reg")
    plan = service.propose_next_falsification("vr-reg").proposed
    assert plan is not None

    prereg = service.register_falsification_test(plan)
    assert len(prereg.preregistration_hash) == 64  # sha256 hex
    item = service.get_validation_request_queue_item(prereg.queue_item_id)
    assert item is not None and item.status == "approved"
    frozen = item.validation_request.metadata["falsification_preregistration"]
    assert frozen["preregistration_hash"] == prereg.preregistration_hash
    assert frozen["kill_criterion"] == plan.kill_criterion.model_dump(mode="json")


def test_round_capsule_carries_unchanged_kill_criterion(tmp_path):
    service, repo = _ready_with_supports(tmp_path, "lock", "vr-lock")
    result = service.run_falsification_round("vr-lock")
    assert result is not None and result.errors == [], result.errors if result else None
    assert result.plan is not None and result.plan.lane == "omics"
    assert result.capsule_id is not None

    capsule = repo.get_proof_capsule(result.capsule_id)
    frozen = capsule.payload["falsification_preregistration"]
    # the capsule's kill-criterion equals the planned one byte-for-byte ...
    assert frozen["kill_criterion"] == result.plan.kill_criterion.model_dump(mode="json")
    # ... and its hash equals the hash committed at registration, BEFORE the result existed.
    recomputed = service._falsification_preregistration_hash(
        candidate_id="vr-lock",
        lane="omics",
        kill_criterion=result.plan.kill_criterion,
        expected_signal=result.plan.expected_signal_if_alive,
    )
    assert frozen["preregistration_hash"] == result.preregistration.preregistration_hash == recomputed


# ---- write-gate stays terminal ---------------------------------------------------------------
def test_round_does_not_auto_accept_or_promote(tmp_path):
    service, repo = _ready_with_supports(tmp_path, "noacc", "vr-noacc")
    cand_before = service.get_public_candidate("vr-noacc")

    result = service.run_falsification_round("vr-noacc")
    assert result.promoted is False
    capsule = repo.get_proof_capsule(result.capsule_id)
    assert capsule.status == "submitted"  # agent-produced; not accepted, not promoted

    cand_after = service.get_public_candidate("vr-noacc")
    assert cand_after.evidence_refs == cand_before.evidence_refs  # candidate gained no evidence


# ---- result evaluation -----------------------------------------------------------------------
def test_kill_criterion_met_when_purity_control_goes_neutral(tmp_path):
    service, _ = _ready_with_supports(tmp_path, "eval", "vr-eval")
    result = service.run_falsification_round("vr-eval")
    # the mock omics lane returns a neutral signal; the purity-control kill-criterion kills on neutral,
    # so the originally-supporting signal is flagged as a possible composition artifact.
    assert result.observed_signal == "neutral"
    assert result.plan.kill_criterion.observed_signal_kills == "neutral"
    assert result.kill_criterion_met is True


# ---- guards ----------------------------------------------------------------------------------
def test_round_none_for_unknown_candidate(tmp_path):
    service = HSAResearchService(SQLiteResearchRepository(tmp_path / "u.sqlite3", seed=False))
    assert service.run_falsification_round("nonexistent-candidate-xyz") is None


def test_round_with_no_runnable_proposal_runs_nothing(tmp_path):
    service, repo = _ready_with_supports(tmp_path, "noplan", "vr-noplan")
    result = service.run_falsification_round("vr-noplan", lane_allowlist=["admet"])  # no lane registered
    assert result is not None
    assert result.plan is None and result.capsule_id is None
    assert "no_runnable_proposal" in result.errors
    assert "no_runnable_lane" in result.blockers
    assert len(repo.list_compute_jobs()) == 0  # pre-registered nothing, dispatched nothing
