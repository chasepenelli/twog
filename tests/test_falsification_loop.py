"""Multi-round falsification loop (increment 5) — the autonomous campaign + termination.

Chains rounds until a pre-registered kill-criterion is met, the planner exhausts runnable tests, the
budget runs out, or the round cap is hit. NEVER auto-promotes: a refuting/neutral outcome terminates
WITHOUT promotion (the Megquier shape — the hypothesis is refuted and never becomes evidence).
"""

from __future__ import annotations

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository
from tests.test_candidates import _seed_validation_ready_candidate
from tests.test_falsification_planner import _seed_capsule


def _svc(tmp_path, name, cid, *, signal, validation_type="omics"):
    repo = SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _seed_validation_ready_candidate(repo, candidate_id=cid)
    _seed_capsule(
        repo, cid, signal=signal, validation_type=validation_type,
        confidence=0.8 if signal == "supports" else 0.0,
    )
    return service, repo


# ---- refuted in one round (the Megquier shape) -----------------------------------------------
def test_supports_signal_is_refuted_by_its_purity_control_and_terminates(tmp_path):
    service, repo = _svc(tmp_path, "ref", "vr-loop-ref", signal="supports")
    cand_before = service.get_public_candidate("vr-loop-ref")

    result = service.run_falsification_loop("vr-loop-ref")
    # round 1 proposes the tumor-purity control; the mock returns neutral, meeting the kill-criterion
    assert result.terminal_reason == "hypothesis_refuted"
    assert result.leading_hypothesis_status == "refuted"
    assert result.rounds_run == 1
    assert result.rounds[0].kill_criterion_met is True
    # the refutation terminates WITHOUT promotion — the candidate gains no evidence
    assert result.promoted is False
    cand_after = service.get_public_candidate("vr-loop-ref")
    assert cand_after.evidence_refs == cand_before.evidence_refs


# ---- a standing hypothesis: exhaust the runnable test space without a kill --------------------
def test_neutral_signal_runs_rounds_until_test_space_exhausted(tmp_path):
    # seed on 'homology' (no lane) so both ungated runnable lanes (omics, docking) are untested
    service, repo = _svc(tmp_path, "stand", "vr-loop-stand", signal="neutral", validation_type="homology")
    result = service.run_falsification_loop("vr-loop-stand")
    # the loop runs the ungated lanes cheapest-first (omics, docking, cofolding; none kill), then has
    # nothing left. MD is expert-gated, so the unattended loop never proposes it.
    assert result.terminal_reason == "no_runnable_proposal"
    assert result.leading_hypothesis_status == "standing"
    assert result.rounds_run == 3
    lanes = [r.plan.lane for r in result.rounds if r.plan is not None]
    assert lanes == ["omics", "docking", "cofolding"]
    assert "md" not in lanes  # expert-gated lane excluded from the autonomous loop


# ---- termination knobs -----------------------------------------------------------------------
def test_budget_caps_the_campaign(tmp_path):
    service, repo = _svc(tmp_path, "budget", "vr-loop-budget", signal="neutral", validation_type="homology")
    result = service.run_falsification_loop("vr-loop-budget", budget_usd=0.005)
    # round 1 (omics, ~$0.01 calibrated) pushes spend past the $0.005 ceiling -> stop before round 2
    assert result.terminal_reason == "budget_exhausted"
    assert result.rounds_run == 1
    assert result.leading_hypothesis_status == "underpowered"
    assert result.total_est_cost_usd >= 0.005


def test_max_rounds_caps_the_campaign(tmp_path):
    service, repo = _svc(tmp_path, "cap", "vr-loop-cap", signal="neutral", validation_type="homology")
    result = service.run_falsification_loop("vr-loop-cap", max_rounds=1)
    assert result.terminal_reason == "max_rounds"
    assert result.rounds_run == 1
    assert result.leading_hypothesis_status == "underpowered"


def test_loop_none_for_unknown_candidate(tmp_path):
    service = HSAResearchService(SQLiteResearchRepository(tmp_path / "u.sqlite3", seed=False))
    assert service.run_falsification_loop("nonexistent-candidate-xyz") is None


# ---- the controls_confound thread lets rounds advance ----------------------------------------
def test_dispatched_control_marks_confound_controlled(tmp_path):
    service, repo = _svc(tmp_path, "ctrl", "vr-loop-ctrl", signal="supports")
    result = service.run_falsification_loop("vr-loop-ctrl")
    # the round-1 purity-control capsule records that it controls the tumor_purity confound
    capsule_id = result.rounds[0].capsule_id
    capsule = repo.get_proof_capsule(capsule_id)
    assert capsule.payload.get("controls_confound") == "tumor_purity"


# ---- a REAL runner must not dispatch a proposal whose inputs don't resolve --------------------
def test_real_runner_does_not_dispatch_inputs_unready_proposal(tmp_path):
    """The termination fix: on a real runner (modal/local), the best remaining proposal having
    inputs_ready=False means the RUNNABLE test space is exhausted — the round reports
    no_runnable_proposal WITHOUT dispatching (no no-op round, no compute job, no capsule). The mock
    runner still dispatches the same candidate (it fabricates signals without real inputs)."""
    repo = SQLiteResearchRepository(tmp_path / "ir.sqlite3", seed=False)
    service = HSAResearchService(repo)
    # target NOT in the curated library + no lane_inputs + no network resolvers -> every lane is
    # inputs_unresolved, so the planner's best proposal is flagged inputs_ready=False.
    service.seed_validation_ready_candidate(
        "vr-no-inputs", title="No resolvable inputs", evidence_refs=["x"], targets=["ZZZ_NO_STRUCTURE"]
    )

    rnd_real = service.run_falsification_round("vr-no-inputs", runner_kind="modal")
    assert rnd_real.plan is None  # not dispatched
    assert "no_runnable_proposal" in rnd_real.errors
    assert rnd_real.compute_job_id is None and rnd_real.capsule_id is None

    # the loop then terminates cleanly (no wasted rounds capping out at max_rounds)
    loop = service.run_falsification_loop("vr-no-inputs", runner_kind="modal", max_rounds=3)
    assert loop.terminal_reason == "no_runnable_proposal"
    assert loop.rounds_run == 0  # nothing was runnable, so nothing ran

    # under mock, the SAME candidate still produces a dispatched plan (inputs not needed)
    rnd_mock = service.run_falsification_round("vr-no-inputs", runner_kind="mock")
    assert rnd_mock.plan is not None
