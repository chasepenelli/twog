"""Lane input resolution (increment 7) — proposed tests carry real compute inputs, or fail honestly.

A candidate curates its real inputs at metadata['lane_inputs'][<lane>]; the resolver turns them into
the lane config and register_falsification_test rides it onto the validation request so a Modal
dispatch runs real compute. No inputs => resolved=False, no fabrication.
"""

from __future__ import annotations

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository
from tests.test_candidates import _seed_validation_ready_candidate

from tests.test_falsification_planner import _seed_capsule

from hsa_research.ingestion_bridge.contracts import BeliefState, FalsificationPlan, KillCriterion
from hsa_research.ingestion_bridge.falsification_planner import rank_falsification_tests

DOCK_INPUTS = {"receptor_pdb": "ATOM  ...minimal...", "ligand_smiles": "CCO"}

_COST = {"omics": 0.01, "docking": 0.10, "md": 40.0}


def _cost(lane):
    return _COST.get(lane, 100.0)


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
    # omics still reports missing keys from incomplete curated metadata (docking now goes through the
    # curated-library gate instead — see test_target_library.py).
    service, repo = _svc(tmp_path, "inc")
    _candidate_with_inputs(repo, service, "vr-inc", {"omics": {"expression": {"g": {"m0": 1.0}}}})  # no strata
    res = service.resolve_lane_inputs("vr-inc", "omics")
    assert res.resolved is False
    assert "strata" in res.missing


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


# ---- input-aware planning: prefer lanes the candidate can really run --------------------------
def test_rank_prefers_input_ready_lane_over_cheaper_unresolved():
    belief = BeliefState(candidate_id="c", net_signal="neutral", net_confidence=0.2, tested_lanes=[])
    plans = rank_falsification_tests(belief, {"docking", "omics"}, _cost, inputs_unresolved=frozenset({"omics"}))
    # omics is cheaper (higher VOI/$) but its inputs are unresolved -> docking (ready) ranks first
    assert plans[0].lane == "docking" and plans[0].inputs_ready is True
    omics = next(p for p in plans if p.lane == "omics")
    assert omics.inputs_ready is False and "inputs unresolved" in omics.novelty_note


def test_propose_prefers_lane_with_resolved_inputs(tmp_path):
    service, repo = _svc(tmp_path, "ia")
    _candidate_with_inputs(repo, service, "vr-ia", {"docking": DOCK_INPUTS})  # docking inputs only
    _seed_capsule(repo, "vr-ia", signal="neutral", validation_type="homology")  # docking + omics untested

    result = service.propose_next_falsification("vr-ia")
    assert result.proposed.lane == "docking"  # preferred despite omics being far cheaper
    assert result.proposed.inputs_ready is True
    omics_alt = next((p for p in result.alternatives if p.lane == "omics"), None)
    assert omics_alt is not None and omics_alt.inputs_ready is False


# ---- network input resolution (v0.2): name a target + therapy, fetch the inputs -----------------
class _FakeResolvers:
    """Stands in for NetworkInputResolvers (PubChem + RCSB) — no network in CI."""

    def __init__(self, smiles="CCO", structure=None, sequence="MQIFVKTLTGK"):
        self._smiles = smiles
        self._sequence = sequence
        self._structure = structure if structure is not None else {
            "receptor_pdb": "ATOM      1  N   ALA A   1      11.1  13.2   8.6  1.00\nEND\n",
            "pdb_id": "9XYZ", "center_x": 1.0, "center_y": 2.0, "center_z": 3.0,
        }

    def compound_smiles(self, name):
        return self._smiles

    def target_structure(self, target):
        return self._structure

    def protein_sequence(self, target):
        return self._sequence


def _candidate_named(repo, service, cid, *, targets, therapies, lane_inputs_bag=None):
    _seed_validation_ready_candidate(repo, candidate_id=cid)
    cand = service.get_public_candidate(cid)
    update = {"targets": targets, "candidate_therapies": therapies}
    if lane_inputs_bag is not None:
        update["metadata"] = {"lane_inputs": lane_inputs_bag}
    repo.upsert_public_candidate(cand.model_copy(update=update))


def _verified_library(target="PIK3CA"):
    return {"version": "t", "entries": {target: {
        "target": target, "uniprot": "P", "pdb_id": "4JPS", "chain": "A",
        "box": {"center_x": 0.0, "center_y": 0.0, "center_z": 0.0, "size_x": 20, "size_y": 20, "size_z": 20},
        "receptor_pdb": "ATOM      1  N   ALA A   1      11.1  13.2   8.6  1.00\nEND\n",
        "verified": True, "redock_rmsd": 1.6,
    }}}


def test_curated_library_resolves_docking_for_verified_target(tmp_path):
    from hsa_research.ingestion_bridge import lane_inputs

    service, repo = _svc(tmp_path, "lib")
    _candidate_named(repo, service, "vr-lib", targets=["PIK3CA"], therapies=["alpelisib"])
    cand = service.get_public_candidate("vr-lib")
    res = lane_inputs.resolve(cand, "docking", resolvers=_FakeResolvers(), target_library=_verified_library("PIK3CA"))
    assert res.resolved is True and res.source == "curated_library"
    assert res.config["ligand_smiles"] == "CCO" and res.config["receptor_pdb"].startswith("ATOM")
    assert res.config["target"] == "PIK3CA" and res.config["source_pdb"] == "4JPS"


def test_docking_refused_for_unverified_target(tmp_path):
    from hsa_research.ingestion_bridge import lane_inputs

    service, repo = _svc(tmp_path, "unver")
    _candidate_named(repo, service, "vr-unver", targets=["NOSUCH"], therapies=["alpelisib"])
    cand = service.get_public_candidate("vr-unver")
    # target not in the (verified) library -> refuse, no dirty fallback
    res = lane_inputs.resolve(cand, "docking", resolvers=_FakeResolvers(), target_library=_verified_library("PIK3CA"))
    assert res.resolved is False and res.source == "curated_library" and res.missing


def test_network_resolves_cofolding_sequence_from_named_target(tmp_path):
    from hsa_research.ingestion_bridge import lane_inputs

    service, repo = _svc(tmp_path, "cofold")
    _candidate_named(repo, service, "vr-cofold", targets=["PIK3CA"], therapies=["alpelisib"])
    cand = service.get_public_candidate("vr-cofold")
    res = lane_inputs.resolve(cand, "cofolding", resolvers=_FakeResolvers())
    assert res.resolved is True and res.source == "network"
    assert res.config["protein_sequence"] == "MQIFVKTLTGK" and res.config["ligand_smiles"] == "CCO"


def test_curated_inputs_win_over_network(tmp_path):
    from hsa_research.ingestion_bridge import lane_inputs

    service, repo = _svc(tmp_path, "win")
    _candidate_named(repo, service, "vr-win", targets=["PIK3CA"], therapies=["alpelisib"], lane_inputs_bag={"docking": DOCK_INPUTS})
    cand = service.get_public_candidate("vr-win")
    res = lane_inputs.resolve(cand, "docking", resolvers=_FakeResolvers())
    assert res.resolved is True and res.source == "candidate.metadata"  # curated wins


def test_no_resolvers_means_curated_only(tmp_path):
    from hsa_research.ingestion_bridge import lane_inputs

    service, repo = _svc(tmp_path, "nores")
    _candidate_named(repo, service, "vr-nores", targets=["PIK3CA"], therapies=["alpelisib"])
    cand = service.get_public_candidate("vr-nores")
    assert lane_inputs.resolve(cand, "docking").resolved is False  # no resolvers -> curated only


def test_network_unresolved_on_resolver_miss(tmp_path):
    from hsa_research.ingestion_bridge import lane_inputs

    service, repo = _svc(tmp_path, "miss")
    _candidate_named(repo, service, "vr-miss", targets=["PIK3CA"], therapies=["alpelisib"])
    cand = service.get_public_candidate("vr-miss")
    res = lane_inputs.resolve(cand, "docking", resolvers=_FakeResolvers(smiles=None))  # PubChem miss
    assert res.resolved is False


def test_service_uses_injected_resolvers_for_input_aware_planning(tmp_path, monkeypatch):
    from hsa_research.ingestion_bridge import target_library

    service, repo = _svc(tmp_path, "svc-net")
    _candidate_named(repo, service, "vr-svc-net", targets=["PIK3CA"], therapies=["alpelisib"])
    _seed_capsule(repo, "vr-svc-net", signal="neutral", validation_type="homology")
    service.input_resolvers = _FakeResolvers()  # PubChem SMILES resolver
    monkeypatch.setattr(target_library, "load_target_library", lambda *a, **k: _verified_library("PIK3CA"))

    result = service.propose_next_falsification("vr-svc-net")
    # docking resolves from the verified library (receptor) + PubChem ligand -> ready and preferred
    assert result.proposed.lane == "docking" and result.proposed.inputs_ready is True


# --- Stage-0 frontier/design lanes: first-class + honest, but never runnable (contract only) --------

from typing import get_args as _get_args

from hsa_research.ingestion_bridge import lane_inputs as _li
from hsa_research.ingestion_bridge import frontier_research_policy as _frp
from hsa_research.ingestion_bridge.lanes import available_lanes as _available_lanes
from hsa_research.ingestion_bridge.compute_runners import _MODAL_LANES
from hsa_research.ingestion_bridge.contracts import FalsificationLane as _FL, ValidationRequest as _VR


def test_stage0_design_lanes_are_first_class_but_never_runnable(tmp_path):
    service, _ = _svc(tmp_path, "s0-contract")
    fl = set(_get_args(_FL))
    vt = set(_get_args(_VR.model_fields["validation_type"].annotation))
    runnable = service._runnable_falsification_lanes()
    auto = service._autonomously_runnable_lanes()
    assert _li._DESIGN_STAGE0_LANES, "expected a non-empty Stage-0 lane set"
    for lane in _li._DESIGN_STAGE0_LANES:
        # first-class in BOTH Literals (the lockstep the suite enforces)
        assert lane in fl and lane in vt
        # carries an honest, non-empty input checklist + a config key
        assert _li._REQUIRED_KEY_SETS.get(lane)
        assert lane in _li.LANE_CONFIG_KEY
        # THE safety floor: no registered runner => can never be proposed, dispatched, or fabricate
        assert lane not in _available_lanes()
        assert lane not in _MODAL_LANES
        assert lane not in runnable
        assert lane not in auto


def test_stage0_resolve_refuses_with_checklist(tmp_path):
    service, repo = _svc(tmp_path, "s0-refuse")
    _seed_validation_ready_candidate(repo, candidate_id="s0-cand")
    for lane in sorted(_li._DESIGN_STAGE0_LANES):
        res = service.resolve_lane_inputs("s0-cand", lane)
        assert res.resolved is False
        assert res.source == "frontier_design_stage0"
        assert list(res.missing) == list(_li._REQUIRED_KEY_SETS[lane][0])
        assert "design concept" in (res.notes or "")


def test_stage0_list_frontier_design_lanes_maps_modality(tmp_path):
    service, repo = _svc(tmp_path, "s0-map")
    _seed_validation_ready_candidate(repo, candidate_id="s0-degrader")
    cand = service.get_public_candidate("s0-degrader")
    repo.upsert_public_candidate(cand.model_copy(update={"title": "Peptide-PROTAC degrader of vimentin"}))
    lanes = service.list_frontier_design_lanes("s0-degrader")
    assert lanes is not None
    assert {r.lane for r in lanes} == {"degrader_design"}
    assert all(r.resolved is False for r in lanes)
    # a conventional small molecule implies NO design lane
    _seed_validation_ready_candidate(repo, candidate_id="s0-conv")
    conv = service.get_public_candidate("s0-conv")
    repo.upsert_public_candidate(
        conv.model_copy(update={"title": "alpelisib small-molecule PI3Ka inhibitor", "candidate_therapies": []})
    )
    assert service.list_frontier_design_lanes("s0-conv") == []
    # unknown candidate -> None
    assert service.list_frontier_design_lanes("does-not-exist") is None


def test_design_lanes_for_text_pure_mapping():
    assert _frp.design_lanes_for_text("base editing to fix a driver mutation") == {"genome_edit"}
    assert _frp.design_lanes_for_text("CAR-T against a tumor-selective antigen") == {"cell_therapy"}
    assert _frp.design_lanes_for_text("personalized neoantigen mRNA vaccine") == {"mrna_vaccine"}
    assert _frp.design_lanes_for_text("a plain kinase inhibitor") == set()
