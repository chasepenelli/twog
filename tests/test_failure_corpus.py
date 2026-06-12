"""Failure Corpus (increment 6) — queryable negative knowledge + the planner novelty penalty.

Derived from the persistent capsule ledger: a capsule whose pre-registered kill-criterion was met is a
refuted test; a capsule that controlled a confound is a caught confound. The planner reads it to
deprioritize already-settled approaches.
"""

from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository
from tests.test_candidates import _seed_validation_ready_candidate
from tests.test_falsification_planner import _seed_capsule

from hsa_research.ingestion_bridge import failure_corpus
from hsa_research.ingestion_bridge.contracts import BeliefState
from hsa_research.ingestion_bridge.falsification_planner import rank_falsification_tests

_COST = {"omics": 0.5, "docking": 2.5, "md": 40.0}


def _cost(lane):
    return _COST.get(lane, 100.0)


# ---- derivation from the ledger --------------------------------------------------------------
def test_refuting_loop_populates_failure_corpus(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "fc.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _seed_validation_ready_candidate(repo, candidate_id="vr-fc")
    _seed_capsule(repo, "vr-fc", signal="supports", validation_type="omics", confidence=0.8)

    result = service.run_falsification_loop("vr-fc")
    assert result.terminal_reason == "hypothesis_refuted"

    corpus = service.get_failure_corpus("vr-fc")
    kinds = {e.kind for e in corpus}
    assert "refuted_test" in kinds  # the purity control met its kill-criterion
    assert "caught_confound" in kinds  # ... and recorded that it controlled tumor_purity

    refuted = next(e for e in corpus if e.kind == "refuted_test")
    assert refuted.lane == "omics"
    assert refuted.preregistration_hash and len(refuted.preregistration_hash) == 64
    assert refuted.related_capsule_id is not None
    confound = next(e for e in corpus if e.kind == "caught_confound")
    assert confound.confound == "tumor_purity"


def test_corpus_empty_without_any_run_test(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "fce.sqlite3", seed=False)
    service = HSAResearchService(repo)
    _seed_validation_ready_candidate(repo, candidate_id="vr-fce")
    _seed_capsule(repo, "vr-fce", signal="supports", validation_type="omics", confidence=0.8)
    assert service.get_failure_corpus("vr-fce") == []  # a raw seed carries no pre-registration/control


def test_ruled_out_lanes_from_met_killcriterion():
    cap = SimpleNamespace(
        capsule_id=uuid4(),
        payload={
            "signal": "refutes",
            "falsification_preregistration": {
                "lane": "docking",
                "validation_type": "docking",
                "preregistration_hash": "h" * 64,
                "kill_criterion": {"observed_signal_kills": "refutes"},
            },
        },
    )
    assert failure_corpus.ruled_out_lanes([cap]) == {"docking"}
    # a capsule whose observed signal did NOT meet the kill-criterion is not ruled out
    cap.payload["signal"] = "neutral"
    assert failure_corpus.ruled_out_lanes([cap]) == set()


# ---- the planner novelty penalty -------------------------------------------------------------
def test_novelty_penalty_halves_voi_and_annotates_ruled_out_lane():
    belief = BeliefState(candidate_id="c", net_signal="neutral", net_confidence=0.2, tested_lanes=[])
    base = rank_falsification_tests(belief, {"docking", "omics"}, _cost)
    penalized = rank_falsification_tests(belief, {"docking", "omics"}, _cost, ruled_out=frozenset({"omics"}))

    omics_base = next(p for p in base if p.lane == "omics")
    omics_pen = next(p for p in penalized if p.lane == "omics")
    assert omics_pen.value_of_information == round(omics_base.value_of_information * 0.5, 6)
    assert "novelty penalty" in omics_pen.novelty_note.lower()
    # the un-ruled-out lane is untouched
    docking_base = next(p for p in base if p.lane == "docking")
    docking_pen = next(p for p in penalized if p.lane == "docking")
    assert docking_pen.value_of_information == docking_base.value_of_information
