"""Cofolding (Boltz-2) lane engine + sandbox-environment resolution."""

from __future__ import annotations

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository
from tests.test_candidates import _seed_validation_ready_candidate

from hsa_research.ingestion_bridge.cofolding import build_cofolding_result, parse_boltz_outputs
from hsa_research.ingestion_bridge.lanes import resolve_sandbox_environment


# ---- cofolding engine -------------------------------------------------------------------------
def test_parse_boltz_outputs():
    scores = parse_boltz_outputs(
        {"confidence_score": 0.82, "iptm": 0.71, "ptm": 0.8, "ligand_iptm": 0.66, "complex_plddt": 0.9},
        {"affinity_pred_value": -1.2, "affinity_probability_binary": 0.78},
    )
    assert scores["iptm"] == 0.71
    assert scores["affinity_probability_binary"] == 0.78
    assert scores["affinity_pred_value"] == -1.2


def test_cofolding_supports_confident_binder():
    scores = parse_boltz_outputs({"iptm": 0.74, "confidence_score": 0.8}, {"affinity_probability_binary": 0.8})
    r = build_cofolding_result(scores, target="PI3Ka", ligand="alpelisib")
    assert r["signal"] == "supports"
    assert r["confidence"] > 0.0
    assert r["metrics"]["iptm"] == 0.74


def test_cofolding_refutes_poor_interface():
    scores = parse_boltz_outputs({"iptm": 0.25, "confidence_score": 0.3}, {"affinity_probability_binary": 0.2})
    r = build_cofolding_result(scores, target="PI3Ka", ligand="weak")
    assert r["signal"] == "refutes"


def test_cofolding_neutral_midrange():
    scores = parse_boltz_outputs({"iptm": 0.5, "confidence_score": 0.5}, {"affinity_probability_binary": 0.5})
    r = build_cofolding_result(scores, target="PI3Ka", ligand="mid")
    assert r["signal"] == "neutral"


def test_cofolding_no_scores_is_neutral():
    r = build_cofolding_result(parse_boltz_outputs({}), target="X", ligand="Y")
    assert r["signal"] == "neutral" and r["confidence"] == 0.0


# ---- sandbox environment ----------------------------------------------------------------------
def test_resolve_sandbox_environment_cofolding():
    env = resolve_sandbox_environment("cofolding")
    assert env is not None
    assert env["gpu"] == "A100" and "boltz" in env["tools"]
    assert "boltz2-weights" in env["data_refs"]
    assert env["validation_type"] == "cofolding"


def test_resolve_sandbox_environment_unknown_is_none():
    assert resolve_sandbox_environment("does-not-exist") is None
    assert resolve_sandbox_environment(None) is None


def test_cofolding_dispatches_through_modal_runner(monkeypatch):
    from hsa_research.ingestion_bridge import compute_runners as cr
    from hsa_research.ingestion_bridge.contracts import ComputeJobRecord

    def fake_boltz(config):
        assert config["protein_sequence"] == "MELENIV"  # lane config reached the call
        # flat result (signal at top), matching run_boltz_remote's contract
        return {"provider": "boltz2", "signal": "supports", "confidence": 0.6, "findings": "ok",
                "source_refs": [], "limitations": ["mock"], "metrics": {}}

    monkeypatch.setattr(cr, "_call_modal_boltz", fake_boltz)
    record = ComputeJobRecord(
        runner_kind="modal", compute_profile="gpu", validation_type="cofolding",
        status="approved", title="cofold", objective="cofold a complex",
        input_payload={"cofolding": {"protein_sequence": "MELENIV", "ligand_smiles": "C"}},
    )
    result = cr.get_compute_runner(record).submit(record)
    assert result["status"] == "completed"
    assert result["output_payload"]["signal"] == "supports"  # runner wraps flat result -> output_payload


def test_describe_sandbox_environment_folds_candidate_data(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "sb.sqlite3", seed=False)
    service = HSAResearchService(repo)
    cid = _seed_validation_ready_candidate(repo, candidate_id="vr-sandbox", ready=True)
    manifest = service.describe_sandbox_environment("docking", candidate_id=cid)
    assert manifest is not None
    assert "gnina" in manifest["tools"] and manifest["gpu"] == "A100"
    assert manifest["candidate_id"] == cid
    # candidate evidence refs are folded in as data to stage (superset of the lane defaults)
    candidate = repo.get_public_candidate(cid)
    for ref in candidate.evidence_refs:
        assert ref in manifest["data_refs"]


# ---- Phase 0: manifest -> concrete image plan (lock-down) --------------------------------------
def test_lane_image_plans_are_concrete_and_pinned():
    from hsa_research.ingestion_bridge.lanes import lane_image_plan, UTILITY_IMAGE_PLANS

    cof = lane_image_plan("cofolding")
    assert cof["builder"] == "debian_slim" and cof["gpu"] == "A100"
    assert "boltz==2.2.1" in cof["pip"]  # pinned, MIT

    md = lane_image_plan("md")
    assert md["builder"] == "micromamba"
    assert "openmm=8.5.2" in md["conda"] and "openmmforcefields=0.16.0" in md["conda"]
    assert "pdbfixer" in md["conda"] and "openff-toolkit" in md["conda"]

    dock = lane_image_plan("docking")
    assert dock["builder"] == "registry" and dock["base"] == "gnina/gnina:v1.3.1"
    assert "rdkit" in dock["pip"]

    assert lane_image_plan("nope") is None

    # the one-shot R/Seurat .rds -> h5ad converter utility image
    rds = UTILITY_IMAGE_PLANS["rds_convert"]
    assert rds["builder"] == "micromamba"
    assert "r-seurat" in rds["conda"] and "r-sceasy" in rds["conda"]
    assert "bioconda" in rds["channels"]
