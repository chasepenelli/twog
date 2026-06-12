"""Active Falsification Planner (increment 1) — twog's first autonomous discovery move.

Read-only: the agent reads a candidate's signed proof-capsule ledger and proposes the next cheapest
test that could KILL the leading hypothesis, pre-registering a kill-criterion and a runnable lane —
touching zero compute and zero writes, provenance-logged via AgentRunner.
"""

from __future__ import annotations

import json
from typing import get_args

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import (
    HSAResearchService,
    ProofCapsuleLibraryRequest,
    ProofCapsuleRecord,
    ProofCapsuleSummary,
    ProofCapsuleTarget,
    SQLiteResearchRepository,
    uuid4,
)
from tests.test_candidates import _seed_validation_ready_candidate

from hsa_research.ingestion_bridge.contracts import (
    FalsificationLane,
    KillCriterion,
    ValidationRequest,
)


def _service(tmp_path, name):
    repo = SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False)
    return HSAResearchService(repo), repo


def _seed_capsule(
    repo,
    candidate_id,
    *,
    signal=None,
    validation_type=None,
    confidence=None,
    packet_type="compute_artifact",
    controls_confound=None,
    section=None,
):
    """Seed a proof capsule directly (bypassing workspace validation, as test_provenance.py does)."""
    payload = {}
    if signal is not None:
        payload["signal"] = signal
    if validation_type is not None:
        payload["validation_type"] = validation_type
    if confidence is not None:
        payload["confidence"] = confidence
    if controls_confound is not None:
        payload["controls_confound"] = controls_confound
    capsule = ProofCapsuleRecord(
        workspace_id=uuid4(),
        checkout_manifest_hash="sha256:" + "b" * 24,
        candidate_id=candidate_id,
        packet_type=packet_type,
        requested_action="no_action",
        target=ProofCapsuleTarget(section=section or validation_type or "abstract"),
        summary=ProofCapsuleSummary(
            title="seed capsule",
            finding="seeded finding for the falsification planner test",
            why_it_matters="exercises the planner against a real ledger row",
            limitations=["synthetic test fixture"],
        ),
        payload=payload,
        content_hash="c" * 40,
    )
    return repo.upsert_proof_capsule(capsule)


# ---- contracts -------------------------------------------------------------------------------
def test_falsification_lane_matches_validation_type():
    """The lane Literal can never silently drift from the real validation_type set."""
    lane_members = set(get_args(FalsificationLane))
    vt_members = set(get_args(ValidationRequest.model_fields["validation_type"].annotation))
    assert lane_members == vt_members


def test_kill_criterion_serializes_byte_stable():
    """KillCriterion is hash-ready (deterministic) so increment 2 can content-hash the pre-registration."""
    a = KillCriterion(metric="ligand_rmsd", comparator="<", threshold=1.0, rationale="enough chars here")
    b = KillCriterion(metric="ligand_rmsd", comparator="<", threshold=1.0, rationale="enough chars here")
    assert a.model_dump(mode="json") == b.model_dump(mode="json")
    assert json.dumps(a.model_dump(mode="json")) == json.dumps(b.model_dump(mode="json"))


# ---- runnable lanes (the lane-vs-provider fix) -----------------------------------------------
def test_runnable_lanes_are_science_lanes_not_providers(tmp_path):
    service, _ = _service(tmp_path, "runnable")
    runnable = service._runnable_falsification_lanes()
    assert runnable <= {"docking", "md", "omics"}
    assert "cofolding" not in runnable  # registered lane, but not in the validation_type Literal
    assert runnable.isdisjoint({"mock", "modal", "local", "checkpoint", "modal_checkpoint"})
    assert {"docking", "omics"} <= runnable  # registered AND in the Literal


# ---- belief distillation ---------------------------------------------------------------------
def test_distill_belief_reads_signal_only_from_signalful_capsules(tmp_path):
    service, repo = _service(tmp_path, "belief")
    cid = _seed_validation_ready_candidate(repo, candidate_id="vr-belief")
    _seed_capsule(repo, cid, signal="supports", validation_type="omics", confidence=0.7)
    _seed_capsule(repo, cid, packet_type="evidence_addition", section="abstract")  # no signal

    belief = service.propose_next_falsification(cid).belief_state
    assert belief.net_signal == "supports"
    assert belief.signalful_capsule_count == 1
    assert belief.net_confidence <= 0.3  # thin signal base caps confidence (anti ungrounded-confidence)
    assert belief.ledger_capsule_count == 2
    assert "omics" in belief.tested_lanes


# ---- cost estimator --------------------------------------------------------------------------
def test_lane_cost_is_ordinal_and_labeled(tmp_path):
    service, repo = _service(tmp_path, "cost")
    assert service._estimate_lane_cost("omics") < service._estimate_lane_cost("md")
    cid = _seed_validation_ready_candidate(repo, candidate_id="vr-cost")
    _seed_capsule(repo, cid, signal="neutral", validation_type="omics", confidence=0.5)
    proposed = service.propose_next_falsification(cid).proposed
    assert proposed is not None
    assert proposed.est_cost_source == "lane_cost_table"


# ---- confound branch + de-dup ----------------------------------------------------------------
def test_supports_signal_triggers_grounded_purity_control(tmp_path):
    service, repo = _service(tmp_path, "confound")
    cid = _seed_validation_ready_candidate(repo, candidate_id="vr-confound")
    cap = _seed_capsule(repo, cid, signal="supports", validation_type="omics", confidence=0.8)

    result = service.propose_next_falsification(cid)
    flags = [f for f in result.belief_state.open_confounds if f.kind == "tumor_purity"]
    assert len(flags) == 1 and flags[0].status == "open"
    assert flags[0].refutes_capsule_id == cap.capsule_id

    plan = result.proposed
    assert plan is not None and plan.addresses_confound is not None
    assert plan.addresses_confound.kind == "tumor_purity"
    assert plan.targets_capsule_ids == [cap.capsule_id]  # names the SPECIFIC capsule it would refute
    assert plan.kill_criterion.observed_signal_kills in ("neutral", "refutes")
    assert plan.lane == "omics"


def test_controlled_purity_confound_is_not_reproposed(tmp_path):
    service, repo = _service(tmp_path, "dedup")
    cid = _seed_validation_ready_candidate(repo, candidate_id="vr-dedup")
    _seed_capsule(repo, cid, signal="supports", validation_type="omics", confidence=0.8)
    _seed_capsule(repo, cid, validation_type="omics", controls_confound="tumor_purity")  # the control

    result = service.propose_next_falsification(cid)
    assert all(
        not (f.kind == "tumor_purity" and f.status == "open") for f in result.belief_state.open_confounds
    )
    assert result.proposed is None or result.proposed.addresses_confound is None


# ---- ranking: cost tiebreak + lane gate ------------------------------------------------------
def test_equal_voi_breaks_toward_cheaper_lane(tmp_path):
    service, repo = _service(tmp_path, "tiebreak")
    cid = _seed_validation_ready_candidate(repo, candidate_id="vr-tie")
    _seed_capsule(repo, cid, signal="neutral", validation_type="omics", confidence=0.5)

    plan = service.propose_next_falsification(cid).proposed
    # docking ($2.50) and md ($40) are the two untested generic lanes at equal VOI -> docking wins
    assert plan is not None and plan.lane == "docking"
    assert "$" in plan.rank_rationale


def test_lane_allowlist_gates_proposed_lane(tmp_path):
    service, repo = _service(tmp_path, "gate")
    cid = _seed_validation_ready_candidate(repo, candidate_id="vr-gate")
    _seed_capsule(repo, cid, signal="supports", validation_type="omics", confidence=0.8)

    only_docking = service.propose_next_falsification(cid, lane_allowlist=["docking"])
    assert only_docking.proposed is not None and only_docking.proposed.lane == "docking"
    assert "docking" in only_docking.runnable_lanes

    no_lane = service.propose_next_falsification(cid, lane_allowlist=["admet"])  # in Literal, no lane
    assert no_lane.proposed is None
    assert "no_runnable_lane" in no_lane.blockers


# ---- provenance + zero-write honesty ---------------------------------------------------------
def test_proposal_is_provenance_logged(tmp_path):
    service, repo = _service(tmp_path, "prov")
    cid = _seed_validation_ready_candidate(repo, candidate_id="vr-prov")
    _seed_capsule(repo, cid, signal="supports", validation_type="omics", confidence=0.8)

    result = service.propose_next_falsification(cid)
    assert result.agent_run_id is not None  # AgentRunner stamped the durable run id (top-level field)

    runs = repo.list_agent_runs(agent_name="active_falsification_planner")
    assert len(runs) == 1
    run = repo.get_agent_run(runs[0].agent_run_id)
    assert run is not None and run.status == "completed"
    assert run.output_payload.get("candidate_id") == cid
    assert result.proposed.provenance_flag == "agent_proposed_unconfirmed"


def test_planner_performs_no_writes(tmp_path):
    service, repo = _service(tmp_path, "nowrite")
    cid = _seed_validation_ready_candidate(repo, candidate_id="vr-nowrite")
    _seed_capsule(repo, cid, signal="supports", validation_type="omics", confidence=0.8)

    caps_before = service.list_proof_capsules(ProofCapsuleLibraryRequest(candidate_id=cid)).capsule_count
    jobs_before = len(repo.list_compute_jobs())
    cand_before = service.get_public_candidate(cid)

    service.propose_next_falsification(cid)

    caps_after = service.list_proof_capsules(ProofCapsuleLibraryRequest(candidate_id=cid)).capsule_count
    jobs_after = len(repo.list_compute_jobs())
    cand_after = service.get_public_candidate(cid)
    assert caps_after == caps_before
    assert jobs_after == jobs_before
    assert cand_after.validation_ready == cand_before.validation_ready
    assert cand_after.evidence_refs == cand_before.evidence_refs
    # unknown candidate -> None (no write-gate path touched)
    assert service.propose_next_falsification("nonexistent-candidate-xyz") is None
