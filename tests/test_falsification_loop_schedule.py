"""Increment 6b: the autonomous falsification-loop scheduler (Dagster).

A RUNNING-by-default hourly schedule materializes `falsification_loop_report`, which (optionally)
generates NEW dockable candidates and then runs budget-capped falsification campaigns for
VALIDATION-READY public candidates. It NEVER promotes — a refuting/neutral outcome terminates without
promotion (the Megquier shape); the human write-gate stays terminal.

CI-safe: no daemon, no GPU, no cloud. The loop runs with runner_kind="mock".
"""

from __future__ import annotations

import dagster as dg

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository
from tests.test_candidates import _seed_validation_ready_candidate
from tests.test_falsification_planner import _seed_capsule

from hsa_research.ingestion_bridge.dagster_resources import ResearchRepositoryResource
from hsa_research.ingestion_bridge import dagster_assets as assets_module


def _seed_ready(repo, candidate_id, *, signal="supports", validation_type="omics"):
    """Seed a validation-ready public candidate with a supports/neutral omics capsule."""
    _seed_validation_ready_candidate(repo, candidate_id=candidate_id)
    _seed_capsule(
        repo,
        candidate_id,
        signal=signal,
        validation_type=validation_type,
        confidence=0.8 if signal == "supports" else 0.0,
    )
    record = repo.get_public_candidate(candidate_id)
    repo.upsert_public_candidate(record.model_copy(update={"validation_ready": True}))
    return candidate_id


def _repo(tmp_path, name):
    return SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False)


def _resource(repo):
    return ResearchRepositoryResource(storage_backend="sqlite", sqlite_path=str(repo.db_path))


def _materialize(repo, config, *, with_checks=False):
    assets = [assets_module.falsification_loop_report]
    if with_checks:
        assets = [assets_module.falsification_loop_report, assets_module.falsification_loop_never_promotes]
    return dg.materialize(
        assets,
        resources={"research_repository": _resource(repo)},
        run_config={"ops": {"falsification_loop_report": {"config": config}}},
    )


# ---- 0. the generate→dock wiring: generation runs as part of the tick ------------------------
def test_loop_tick_runs_generation_when_enabled(tmp_path):
    repo = _repo(tmp_path, "gen")
    # generate=True flips the autonomous idea flow on. With the mock runner no network resolver is
    # set, so nothing seeds (inputs unresolvable) — but generation RAN, proving the wiring path.
    result = _materialize(repo, {"enabled": True, "dry_run": False, "runner_kind": "mock", "generate": True})
    assert result.success
    report = result.asset_value("falsification_loop_report")
    assert report["generation"]["ran"] is True
    assert report["any_promoted"] is False  # generation never promotes


# ---- 1. a real loop tick never promotes -------------------------------------------------------
def test_loop_tick_runs_campaigns_and_never_promotes(tmp_path):
    repo = _repo(tmp_path, "tick")
    _seed_ready(repo, "vr-loop-tick", signal="supports")
    before = HSAResearchService(repo).get_public_candidate("vr-loop-tick")

    result = _materialize(repo, {"enabled": True, "dry_run": False, "runner_kind": "mock"})
    assert result.success

    report = result.asset_value("falsification_loop_report")
    assert report["any_promoted"] is False
    assert report["candidates_processed"] >= 1
    assert any(row.get("terminal_reason") for row in report["rows"])

    # the loop NEVER promotes — the candidate's evidence is unchanged
    after = HSAResearchService(repo).get_public_candidate("vr-loop-tick")
    assert after.evidence_refs == before.evidence_refs


# ---- 2. the tick budget hard-caps the campaign ------------------------------------------------
def test_tick_budget_caps_candidates_processed(tmp_path):
    repo = _repo(tmp_path, "budget")
    # each neutral/homology candidate costs ~$0.11 (omics $0.01 + docking $0.10, calibrated); a
    # $0.05 tick cap admits one candidate then short-circuits the rest.
    for i in range(4):
        _seed_ready(repo, f"vr-loop-budget-{i}", signal="neutral", validation_type="homology")

    result = _materialize(
        repo,
        {
            "enabled": True,
            "dry_run": False,
            "runner_kind": "mock",
            "max_candidates_per_tick": 10,
            "budget_usd_per_candidate": 0.50,
            "tick_budget_usd": 0.05,
        },
    )
    assert result.success
    report = result.asset_value("falsification_loop_report")
    assert report["candidates_available"] == 4
    assert report["budget_exhausted"] is True
    assert report["candidates_processed"] < report["candidates_available"]
    assert report["any_promoted"] is False


# ---- 3. dry_run never calls run_falsification_loop --------------------------------------------
def test_dry_run_lists_intended_candidates_without_running(tmp_path, monkeypatch):
    repo = _repo(tmp_path, "dry")
    _seed_ready(repo, "vr-loop-dry-a", signal="supports")
    _seed_ready(repo, "vr-loop-dry-b", signal="supports")

    calls = {"n": 0}
    original = HSAResearchService.run_falsification_loop

    def _counting(self, *args, **kwargs):
        calls["n"] += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(HSAResearchService, "run_falsification_loop", _counting)

    result = _materialize(repo, {"enabled": True, "dry_run": True, "runner_kind": "mock"})
    assert result.success
    report = result.asset_value("falsification_loop_report")

    assert calls["n"] == 0  # dry_run never runs a loop
    assert report["dry_run"] is True
    assert report["candidates_processed"] == 2
    ids = {row["candidate_id"] for row in report["rows"]}
    assert ids == {"vr-loop-dry-a", "vr-loop-dry-b"}
    assert report["any_promoted"] is False


# ---- 4. the asset_check asserts nothing was promoted -----------------------------------------
def test_never_promotes_asset_check_passes(tmp_path):
    repo = _repo(tmp_path, "check")
    _seed_ready(repo, "vr-loop-check", signal="supports")

    result = _materialize(repo, {"enabled": True, "dry_run": False, "runner_kind": "mock"}, with_checks=True)
    assert result.success

    check_evals = result.get_asset_check_evaluations()
    assert check_evals, "expected the falsification_loop_never_promotes check to run"
    assert all(evaluation.passed for evaluation in check_evals)

    # the check is also a pure function of the report
    report = result.asset_value("falsification_loop_report")
    assert assets_module.falsification_loop_never_promotes(report).passed is True
    promoted_report = dict(report, any_promoted=True)
    assert assets_module.falsification_loop_never_promotes(promoted_report).passed is False


# ---- 5. import-safety: the new names are wired into the Definitions ---------------------------
def test_definitions_register_falsification_loop():
    assert assets_module.falsification_loop_report is not None
    assert assets_module.falsification_loop_job is not None
    schedule = assets_module.falsification_loop_hourly_schedule
    assert schedule is not None
    # RUNNING by default: the autonomous generate→dock cycle is switched on (was STOPPED pre-launch).
    assert schedule.default_status == dg.DefaultScheduleStatus.RUNNING
    assert schedule.cron_schedule == "0 * * * *"

    defs = assets_module.defs
    assert any(job.name == "falsification_loop_job" for job in defs.jobs)
    assert any(s.name == "falsification_loop_hourly_schedule" for s in defs.schedules)
