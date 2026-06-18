"""Cross-species white-space discovery — the novel-hypothesis frontier.

Hermetic: Open Targets + Ensembl + literature-count are all injected (no network). Verifies the
white-space logic (human-AS-associated + conserved canine ortholog + no canine-HSA literature) and that
the service caches the report in the AgentRun ledger so agents reuse + accumulate it.
"""

from __future__ import annotations

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository

from hsa_research.ingestion_bridge import cross_species, target_library


def _ot_fetch(targets):
    """Fake Open Targets: build the GraphQL response shape from (symbol, ensembl_id, score) tuples."""
    def fetch(query, variables):
        return {"data": {"disease": {"associatedTargets": {"rows": [
            {"score": s, "target": {"approvedSymbol": sym, "id": eid}} for sym, eid, s in targets
        ]}}}}
    return fetch


def _ens_fetch(orthologs):
    """Fake Ensembl: map symbol -> perc_id (None => no canine ortholog)."""
    def fetch(symbol):
        pid = orthologs.get(symbol)
        return [] if pid is None else [{"target": {"id": f"ENSCAF-{symbol}", "perc_id": pid}}]
    return fetch


def _lit(hits_by_symbol):
    """Fake canine-HSA literature count: symbol -> hit count."""
    def count(query):
        for sym, n in hits_by_symbol.items():
            if f'"{sym}"' in query:
                return n
        return 0
    return count


def test_white_space_logic():
    rows = cross_species.white_space_targets(
        "EFO_0003968",
        ot_fetch=_ot_fetch([("KDR", "ENSG1", 0.53), ("PLCG1", "ENSG2", 0.51), ("ORPHANX", "ENSG3", 0.40)]),
        ens_fetch=_ens_fetch({"KDR": 92.0, "PLCG1": 97.9, "ORPHANX": None}),  # ORPHANX has no canine ortholog
        canine_lit_count=_lit({"KDR": 6, "PLCG1": 0}),  # KDR studied in dog HSA, PLCG1 not
        verified_targets={"KDR"},  # KDR is in the verified library; PLCG1 is not
    )
    by = {r["symbol"]: r for r in rows}
    assert "ORPHANX" not in by  # skipped: no conserved canine ortholog
    assert by["KDR"]["is_white_space"] is False  # has canine-HSA literature => not white space
    assert by["PLCG1"]["is_white_space"] is True  # conserved + untested in canine HSA => white space
    assert by["PLCG1"]["dockable"] is False  # not in the verified library
    assert by["PLCG1"]["perc_id"] == 97.9


def _service(tmp_path, *, targets, orthologs, hits, name="ws"):
    svc = HSAResearchService(SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False))
    svc.ot_fetch = _ot_fetch(targets)
    svc.ens_fetch = _ens_fetch(orthologs)
    svc.novelty_count_fn = _lit(hits)
    return svc


def test_discover_reports_white_space_and_queue(tmp_path, monkeypatch):
    monkeypatch.setattr(target_library, "load_target_library",
                        lambda *a, **k: {"entries": {"KDR": {"verified": True}}})
    svc = _service(
        tmp_path,
        targets=[("KDR", "ENSG1", 0.53), ("PLCG1", "ENSG2", 0.51), ("TOP2A", "ENSG4", 0.56)],
        orthologs={"KDR": 92.0, "PLCG1": 97.9, "TOP2A": 88.0},
        hits={"KDR": 6, "PLCG1": 0, "TOP2A": 0},
    )
    report = svc.discover_cross_species_white_space()
    assert report["targets_examined"] == 3
    assert set(s for s in report["verification_queue"]) == {"PLCG1", "TOP2A"}  # novel, not yet verified
    assert report["dockable_white_space"] == []  # KDR isn't white space (studied in dog HSA)
    assert report["white_space_count"] == 2


def test_discover_caches_in_ledger(tmp_path, monkeypatch):
    monkeypatch.setattr(target_library, "load_target_library", lambda *a, **k: {"entries": {}})
    calls = {"n": 0}
    svc = _service(tmp_path, targets=[("PLCG1", "ENSG2", 0.51)], orthologs={"PLCG1": 97.9}, hits={"PLCG1": 0})
    orig = svc.ot_fetch
    def counting(q, v):
        calls["n"] += 1
        return orig(q, v)
    svc.ot_fetch = counting

    svc.discover_cross_species_white_space()
    assert calls["n"] == 1
    svc.discover_cross_species_white_space()  # cached — no second Open Targets call
    assert calls["n"] == 1
