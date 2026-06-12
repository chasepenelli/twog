"""Lane input resolution (increment 7) — proposed tests carry real compute inputs, or fail honestly.

A candidate curates its real inputs at metadata['lane_inputs'][<lane>]; the resolver turns them into
the lane config and register_falsification_test rides it onto the validation request so a Modal
dispatch runs real compute. No inputs => resolved=False, no fabrication.
"""

from __future__ import annotations

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository
from tests.test_candidates import _seed_validation_ready_candidate

from hsa_research.ingestion_bridge.contracts import FalsificationPlan, KillCriterion

DOCK_INPUTS = {"receptor_pdb": "ATOM  ...minimal...", "ligand_smiles": "CCO"}


def _svc(tmp_path, name):
    repo = SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False)
    return HSAResearchService(repo), repo


def _candidate_with_inputs(repo, service, cid, lane_inputs_bag):
    _seed_validation_ready_candidate(repo, candidate_id=cid)
    cand = service.get_public_candidate(cid)
    repo.upsert_public_candidate(cand.model_copy(update={"metadata": {"lane_inputs": lane_inputs_bag}}))


def _docking_plan(cid):
    return FalsificationPlan(
        candidate_id=cid,
        test_objective="Dock the candidate therapy against the target.",
        lane="docking",
        validation_type="docking",
        kill_criterion=KillCriterion(metric="cnn_affinity", comparator="<", threshold=4.0, rationale="no engagement kills it"),
        expected_signal_if_alive="supports",
        est_cost_usd=0.10,
        value_of_information=0.5,
    )


# ---- resolution -------------------------------------------------------------------------------
def test_resolve_docking_inputs_from_candidate(tmp_path):
    service, repo = _svc(tmp_path, "dock")
    _candidate_with_inputs(repo, service, "vr-li", {"docking": DOCK_INPUTS})
    res = service.resolve_lane_inputs("vr-li", "docking")
    assert res.resolved is True
    assert res.config_key == "docking"  # the metadata key the gnina lane reads
    assert res.config["ligand_smiles"] == "CCO"


def test_resolve_omics_inputs_maps_to_omics_review_key(tmp_path):
    service, repo = _svc(tmp_path, "om")
    _candidate_with_inputs(repo, service, "vr-om", {"omics": {"expression": {"g": {"m0": 1.0}}, "strata": {"m0": "mutant"}}})
    res = service.resolve_lane_inputs("vr-om", "omics")
    assert res.resolved is True and res.config_key == "omics_review"


def test_resolve_unresolved_without_inputs(tmp_path):
    service, repo = _svc(tmp_path, "none")
    _seed_validation_ready_candidate(repo, candidate_id="vr-none")
    res = service.resolve_lane_inputs("vr-none", "docking")
    assert res.resolved is False and res.missing


def test_resolve_incomplete_inputs_reports_missing_key(tmp_path):
    service, repo = _svc(tmp_path, "inc")
    _candidate_with_inputs(repo, service, "vr-inc", {"docking": {"receptor_pdb": "ATOM"}})  # no ligand_smiles
    res = service.resolve_lane_inputs("vr-inc", "docking")
    assert res.resolved is False
    assert "ligand_smiles" in res.missing


def test_resolve_none_for_unknown_candidate(tmp_path):
    service, _ = _svc(tmp_path, "unk")
    assert service.resolve_lane_inputs("nonexistent-candidate", "docking") is None


# ---- registration attaches resolved inputs ----------------------------------------------------
def test_register_rides_resolved_inputs_onto_the_request(tmp_path):
    service, repo = _svc(tmp_path, "reg")
    _candidate_with_inputs(repo, service, "vr-reg", {"docking": DOCK_INPUTS})
    prereg = service.register_falsification_test(_docking_plan("vr-reg"))

    item = service.get_validation_request_queue_item(prereg.queue_item_id)
    meta = item.validation_request.metadata
    assert meta["docking"] == DOCK_INPUTS  # the gnina lane will read its real inputs here
    assert meta["falsification_preregistration"]["inputs_resolved"] is True


def test_register_records_unresolved_and_fabricates_nothing(tmp_path):
    service, repo = _svc(tmp_path, "unres")
    _seed_validation_ready_candidate(repo, candidate_id="vr-unres")
    prereg = service.register_falsification_test(_docking_plan("vr-unres"))

    item = service.get_validation_request_queue_item(prereg.queue_item_id)
    meta = item.validation_request.metadata
    assert "docking" not in meta  # no fabricated lane config
    assert meta["falsification_preregistration"]["inputs_resolved"] is False
