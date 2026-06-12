"""Run real Modal lane jobs to baseline per-lane cost for LANE_COST_USD calibration.

Runs the actual compute the falsification loop dispatches (omics CPU, gnina docking A100, Boltz-2
cofolding A100) with real minimal inputs, timing each end-to-end wall-clock (which includes one-time
image build / cold start on the first run of a lane). Estimated $ is wall-seconds x a labeled Modal
rate; the AUTHORITATIVE per-run cost is in the Modal dashboard.

Usage:  python scripts/modal_cost_baseline.py omics docking cofolding
        python scripts/modal_cost_baseline.py omics      # one lane at a time (controlled spend)
"""

from __future__ import annotations

import sys
import time
import urllib.request

# Labeled, APPROXIMATE Modal rates (confirm exact billed $ in the dashboard).
RATE_USD_PER_SEC = {"cpu": 0.0000131, "A100": 0.001097}  # ~ $0.047/core-hr ; ~ $3.95/hr A100

UBIQUITIN_SEQ = "MQIFVKTLTGKTITLEVEPSDTIENVKAKIQDKEGIPPDQQRLIFAGKQLEDGRTLSDYNIQKESTLHLVLRLRGG"


def _fetch_receptor_pdb() -> str:
    """Fetch a real small receptor (ubiquitin, 1UBQ) from RCSB for a representative gnina run."""
    with urllib.request.urlopen("https://files.rcsb.org/download/1UBQ.pdb", timeout=60) as fh:
        return fh.read().decode("utf-8")


def _omics_config() -> dict:
    from tests.test_candidates import _omics_fixture_expression  # the real inline fixture

    mut = [f"m{i}" for i in range(6)]
    wt = [f"w{i}" for i in range(6)]
    return {
        "expression": _omics_fixture_expression(mut, wt),
        "strata": {**{s: "mutant" for s in mut}, **{s: "wt" for s in wt}},
        "source_refs": ["baseline-synthetic"],
    }


def _docking_config() -> dict:
    return {
        "receptor_pdb": _fetch_receptor_pdb(),
        "ligand_smiles": "CCO",  # trivial ligand — we are baselining COST, not affinity
        "target": "ubiquitin_1UBQ",
        "ligand_name": "ethanol",
        "source_refs": ["baseline:1UBQ"],
    }


def _cofolding_config() -> dict:
    return {
        "protein_sequence": UBIQUITIN_SEQ,
        "ligand_smiles": "CCO",
        "target": "ubiquitin",
        "ligand_name": "ethanol",
        "diffusion_samples": 1,  # cost-controlled smoke
        "source_refs": ["baseline:ubiquitin"],
    }


LANES = {
    "omics": ("cpu", "_call_modal_omics", _omics_config),
    "docking": ("A100", "_call_modal_gnina", _docking_config),
    "cofolding": ("A100", "_call_modal_boltz", _cofolding_config),
}


def run_lane(name: str) -> dict:
    from hsa_research.ingestion_bridge import compute_runners

    gpu, fn_name, cfg_fn = LANES[name]
    print(f"\n=== {name} ({gpu}) — building config + dispatching to Modal ...", flush=True)
    config = cfg_fn()
    call = getattr(compute_runners, fn_name)
    t0 = time.monotonic()
    status, signal, detail = "ok", None, ""
    try:
        result = call(config)
        signal = result.get("signal")
        detail = str(result.get("findings") or result.get("error") or "")[:160]
        if result.get("error"):
            status = "lane_error"
    except Exception as exc:  # noqa: BLE001 — capture cost even on failure
        status = "exception"
        detail = f"{type(exc).__name__}: {exc}"[:200]
    wall = time.monotonic() - t0
    est = wall * RATE_USD_PER_SEC[gpu]
    row = {"lane": name, "gpu": gpu, "wall_s": round(wall, 1), "est_usd": round(est, 4),
           "status": status, "signal": signal, "detail": detail}
    print(f"    -> {row}", flush=True)
    return row


def main() -> None:
    lanes = sys.argv[1:] or ["omics"]
    rows = [run_lane(name) for name in lanes if name in LANES]
    print("\n===== BASELINE SUMMARY (wall-clock incl. one-time build; dashboard = exact $) =====")
    total = 0.0
    for r in rows:
        total += r["est_usd"]
        print(f"  {r['lane']:<10} {r['gpu']:<5} {r['wall_s']:>7}s  ~${r['est_usd']:<8} {r['status']:<10} signal={r['signal']}")
    print(f"  {'TOTAL':<10} {'':<5} {'':>7}   ~${round(total, 4)}")


if __name__ == "__main__":
    main()
