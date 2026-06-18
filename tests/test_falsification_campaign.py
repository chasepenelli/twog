"""Offline campaign test (no network / no GPU).

Seeds candidates via the product seeder `service.seed_validation_ready_candidate`, runs the
budget-capped `run_falsification_campaign` under the mock runner, and asserts the persisted
RunManifestRecord(manifest_type='falsification_campaign') aggregates one row per candidate + a
roll-up, NEVER auto-promotes, the budget cap stops the campaign, and candidate_ids=None defaults to
the validation-ready set. Also asserts inline omics inputs resolve (the crux candidate's real-data path).
"""

from __future__ import annotations

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository
from tests.test_falsification_planner import _seed_capsule


def _service(tmp_path, name="campaign"):
    repo = SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False)
    return HSAResearchService(repo), repo


def _seed(service, repo, cid, *, signal="neutral", validation_type="homology", lane_inputs=None):
    """Seed a validation-ready candidate via the PRODUCT seeder, then a capsule that fixes the mock
    signal. validation_type='homology' (no lane) leaves the runnable lanes untested -> the loop runs
    them and terminates 'no_runnable_proposal'/'standing' deterministically under the mock runner."""
    readiness = service.seed_validation_ready_candidate(
        cid,
        title=f"Campaign candidate {cid}",
        evidence_refs=["PMID:00000000"],
        targets=["PIK3CA"],
        candidate_therapies=["alpelisib"],
        biomarkers=["PIK3CA mutation"],
        lane_inputs=lane_inputs,
    )
    _seed_capsule(repo, cid, signal=signal, validation_type=validation_type)
    return readiness


# ---- the product seeder makes an honestly validation-ready candidate -------------------------
def test_seeder_makes_candidate_validation_ready(tmp_path):
    service, repo = _service(tmp_path)
    readiness = _seed(service, repo, "camp-ready")
    assert readiness.ready is True
    cand = service.get_public_candidate("camp-ready")
    assert cand.validation_ready is True
    assert cand.targets == ["PIK3CA"]
    assert cand.candidate_therapies == ["alpelisib"]


# ---- the campaign aggregates rows + a roll-up and persists a retrievable manifest -------------
def test_campaign_aggregates_rows_and_rollup_and_persists(tmp_path):
    service, repo = _service(tmp_path)
    for cid in ("camp-a", "camp-b"):
        _seed(service, repo, cid)

    manifest = service.run_falsification_campaign(
        candidate_ids=["camp-a", "camp-b"], runner_kind="mock", persist=True
    )

    assert manifest.manifest_type == "falsification_campaign"
    assert manifest.status == "completed"

    # persisted + retrievable both by id and by the type filter
    assert service.get_run_manifest(manifest.manifest_id) is not None
    listed = service.list_run_manifests(manifest_type="falsification_campaign")
    assert any(m.manifest_id == manifest.manifest_id for m in listed)

    rows = manifest.output_refs["rows"]
    rollup = manifest.output_refs["rollup"]
    assert [r["candidate_id"] for r in rows] == ["camp-a", "camp-b"]  # one row per candidate, in order
    assert rollup["candidates_selected"] == 2
    assert rollup["candidates_processed"] == 2
    assert rollup["budget_exhausted"] is False
    # the honesty invariant: NOTHING is ever auto-promoted
    assert rollup["any_promoted"] is False
    assert all(r["promoted"] is False for r in rows)
    # roll-up counts are consistent with the per-candidate rows
    assert sum(rollup["terminal_reasons"].values()) == 2
    assert sum(rollup["leading_hypothesis_status"].values()) == 2
    assert rollup["total_est_cost_usd"] == round(sum(r["total_est_cost_usd"] for r in rows), 4)


# ---- dedup + candidate_ids=None defaults to the validation-ready set -------------------------
def test_campaign_defaults_to_validation_ready_and_dedups(tmp_path):
    service, repo = _service(tmp_path)
    _seed(service, repo, "camp-vr1")
    _seed(service, repo, "camp-vr2")
    # a non-ready candidate must NOT be selected
    service.seed_validation_ready_candidate(
        "camp-notready", title="not ready", evidence_refs=[], ready=False
    )

    manifest = service.run_falsification_campaign(candidate_ids=None, runner_kind="mock")
    selected = {r["candidate_id"] for r in manifest.output_refs["rows"]}
    assert selected == {"camp-vr1", "camp-vr2"}
    assert "camp-notready" not in selected

    # explicit duplicates collapse to one row
    dup = service.run_falsification_campaign(candidate_ids=["camp-vr1", "camp-vr1"], runner_kind="mock")
    assert [r["candidate_id"] for r in dup.output_refs["rows"]] == ["camp-vr1"]


# ---- the campaign budget cap stops before processing the whole roster ------------------------
def test_campaign_budget_cap_stops_early(tmp_path):
    service, repo = _service(tmp_path)
    for cid in ("camp-cap1", "camp-cap2", "camp-cap3"):
        _seed(service, repo, cid)

    # a budget below one candidate's spend: the first candidate runs, then the running cost trips the
    # cap before the second is dispatched (the cap is checked at the top of each candidate iteration).
    manifest = service.run_falsification_campaign(
        candidate_ids=["camp-cap1", "camp-cap2", "camp-cap3"],
        runner_kind="mock",
        campaign_budget_usd=0.001,
    )
    rollup = manifest.output_refs["rollup"]
    assert rollup["candidates_processed"] == 1
    assert rollup["budget_exhausted"] is True
    assert len(manifest.output_refs["rows"]) == 1


# ---- max_candidates caps processing without flagging budget exhaustion -----------------------
def test_campaign_max_candidates_caps_processing(tmp_path):
    service, repo = _service(tmp_path)
    for cid in ("camp-m1", "camp-m2", "camp-m3"):
        _seed(service, repo, cid)
    manifest = service.run_falsification_campaign(
        candidate_ids=["camp-m1", "camp-m2", "camp-m3"], runner_kind="mock", max_candidates=2
    )
    rollup = manifest.output_refs["rollup"]
    assert rollup["candidates_processed"] == 2
    assert rollup["budget_exhausted"] is False


# ---- inline omics inputs resolve (the crux candidate's real-data path) -----------------------
def test_inline_omics_inputs_resolve(tmp_path):
    from hsa_research.ingestion_bridge import lane_inputs

    service, repo = _service(tmp_path)
    omics = {
        "expression": {"S1": {"GENE_A": 1.0}, "S2": {"GENE_A": 2.0}},
        "strata": {"S1": "mutant", "S2": "wt"},
        "signatures": {"immune": ["GENE_A"]},
        "direction_hypothesis": "higher_in_mutant",
        "min_n_per_stratum": 1,
        "source_refs": ["GSE95183"],
    }
    _seed(service, repo, "camp-omics", lane_inputs={"omics": omics})
    cand = service.get_public_candidate("camp-omics")
    resolution = lane_inputs.resolve(cand, "omics")
    assert resolution.resolved is True
    assert resolution.config["strata"] == omics["strata"]
    assert resolution.missing == []
