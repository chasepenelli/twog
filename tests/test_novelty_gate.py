"""Novelty / prior-art gate — the input analog of the docking spend-gate.

Pure scoring is tested offline by injecting a fake `count_fn` (no network). Service-level tests verify
the verdict is cached in the AgentRun ledger keyed by the triple (so agents reuse it + accumulate a
queryable explored-vs-white-space map) and that the generation gate refuses already-known biochemistry.
"""

from __future__ import annotations

import json

import pytest

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository

from hsa_research.ingestion_bridge import novelty_gate, target_library
from hsa_research.ingestion_bridge.contracts import PublicCandidateLibraryRequest


class _FixedCount:
    """count_fn stub: returns a fixed count for every query; records how many times it was called."""

    def __init__(self, n: int) -> None:
        self.n = n
        self.calls = 0

    def __call__(self, query: str) -> int:
        self.calls += 1
        return self.n


# ---- pure scoring -----------------------------------------------------------------------------
def test_score_decreases_with_prior_art():
    assert novelty_gate.score_from_counts(0, 0, 0, 0) == 1.0
    s_few = novelty_gate.score_from_counts(0, 1, 1, 1)
    s_many = novelty_gate.score_from_counts(50, 80, 80, 80)
    assert 0.0 < s_many < s_few < 1.0


def test_absent_triple_is_novel():
    v = novelty_gate.assess("novelcompound-x", "ORPHANTARGET", "canine hemangiosarcoma", count_fn=_FixedCount(0))
    assert v["is_novel"] is True
    assert v["novelty_score"] == 1.0
    assert v["prior_art"]["n_ctd"] == 0


def test_exact_triple_prior_art_kills_novelty():
    # even one paper on the exact compound×target×disease => not novel (someone did it)
    counts = {"n_ctd": 2}
    fn = lambda q: 2 if q.count(" AND ") == 2 else 0  # only the 3-term (ctd) query has hits
    v = novelty_gate.assess("alpelisib", "PIK3CA", "angiosarcoma", count_fn=fn)
    assert v["prior_art"]["n_ctd"] == 2
    assert v["is_novel"] is False


def test_well_studied_triple_is_not_novel():
    v = novelty_gate.assess("alpelisib", "PIK3CA", "angiosarcoma", count_fn=_FixedCount(40))
    assert v["is_novel"] is False
    assert v["novelty_score"] < 0.5


def test_translational_gap_flag():
    # known in human AS (ctd hits when 'angiosarcoma' present), absent in canine HSA
    def fn(q: str) -> int:
        if "angiosarcoma" in q and "canine hemangiosarcoma" not in q and q.count(" AND ") == 2:
            return 5  # human triple has prior art
        if "canine hemangiosarcoma" in q and q.count(" AND ") == 2:
            return 0  # canine triple is empty
        return 0
    v = novelty_gate.assess_translational("sometkinib", "KDR", count_fn=fn)
    assert v["translational_gap"] is True
    assert v["is_novel"] is True  # novel on the canine frontier


def test_triple_key_stable():
    a = novelty_gate.triple_key("Copanlisib", "PIK3CA", "Canine Hemangiosarcoma")
    b = novelty_gate.triple_key("copanlisib", "pik3ca", "canine hemangiosarcoma")
    assert a == b == "novelty:copanlisib|pik3ca|canine hemangiosarcoma"


# ---- service: caching in the ledger + the generation gate -------------------------------------
def _service(tmp_path, count, name="nov"):
    svc = HSAResearchService(SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False))
    svc.novelty_count_fn = count
    return svc


def test_assess_novelty_caches_in_agent_run_ledger(tmp_path):
    count = _FixedCount(0)
    svc = _service(tmp_path, count)
    v1 = svc.assess_novelty("copanlisib", "PIK3CA", "canine hemangiosarcoma")
    calls_after_first = count.calls
    assert calls_after_first == 4  # ctd, ct, cd, td
    # second call for the same triple is served from the ledger — no new queries
    v2 = svc.assess_novelty("copanlisib", "PIK3CA", "canine hemangiosarcoma")
    assert count.calls == calls_after_first  # cache hit, count_fn NOT called again
    assert v2["novelty_score"] == v1["novelty_score"]
    # and it's discoverable by any agent via the triple source_key
    key = novelty_gate.triple_key("copanlisib", "PIK3CA", "canine hemangiosarcoma")
    runs = svc.repository.list_agent_runs(agent_name="novelty_gate", source_key=key, limit=5)
    assert runs and runs[0].output_payload["is_novel"] is True


_VERIFIED_LIB = {
    "version": "v", "entries": {"PIK3CA": {
        "verified": True, "pdb_id": "4JPS", "redock_rmsd": 0.7,
        "receptor_pdb": "ATOM      1  CA  ALA A   1       0.000   0.000   0.000\nEND\n",
        "box": {"center_x": 0.0, "center_y": 0.0, "center_z": 0.0, "size_x": 20.0, "size_y": 20.0, "size_z": 20.0},
    }}}


class _Resolver:
    def compound_smiles(self, name): return "C1=CC=CC=C1"
    def target_structure(self, t): return None
    def protein_sequence(self, t): return None


def test_generation_gate_refuses_known_biochem(tmp_path, monkeypatch):
    monkeypatch.setattr(target_library, "load_target_library", lambda *a, **k: _VERIFIED_LIB)
    seed = tmp_path / "seed.json"
    seed.write_text(json.dumps({"candidates": [{"compound": "alpelisib", "target": "PIK3CA"}]}))

    # high prior art => the gate rejects it as not-novel, even though it's dockable
    svc = _service(tmp_path, _FixedCount(40), name="known")
    svc.input_resolvers = _Resolver()
    m = svc.generate_candidate_ideas(source="curated_seed", seed_path=str(seed), check_novelty=True)
    assert m.output_refs["rollup"]["seeded"] == 0
    assert m.output_refs["rollup"]["rejected"] == 1
    assert m.output_refs["rejected"][0]["reason"] == "not_novel"

    # same idea, but no prior art => novel => seeded
    svc2 = _service(tmp_path, _FixedCount(0), name="novel")
    svc2.input_resolvers = _Resolver()
    m2 = svc2.generate_candidate_ideas(source="curated_seed", seed_path=str(seed), check_novelty=True)
    assert m2.output_refs["rollup"]["seeded"] == 1
    assert svc2.get_public_candidate("alpelisib-pik3ca") is not None
