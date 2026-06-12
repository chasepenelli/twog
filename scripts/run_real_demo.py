"""Real end-to-end autonomous-discovery demo (Neon + Modal).

Seeds ONE real validation-ready candidate for the alpelisib -> PI3Ka engagement hypothesis (real
PI3Ka structure from RCSB + real alpelisib SMILES from PubChem), then lets the autonomous loop drive
it: propose a falsification -> resolve the real inputs -> dispatch real gnina docking on Modal ->
evaluate the pre-registered kill-criterion. Nothing is auto-promoted (the human write-gate is terminal).

    PYTHONPATH=src:. python scripts/run_real_demo.py
"""

from __future__ import annotations

import os
import pathlib
import urllib.request

from hsa_research.ingestion_bridge.postgres_store import PostgresResearchRepository
from hsa_research.ingestion_bridge.service import HSAResearchService
from tests.test_candidates import _seed_validation_ready_candidate

CANDIDATE_ID = "alpelisib-pi3ka-demo"
PDB_ID = "4JPS"  # PI3Ka (p110a) co-crystal with alpelisib (BYL719)
_NON_LIGAND_HET = {"HOH", "SO4", "PO4", "GOL", "EDO", "CL", "NA", "MG", "ZN", "CA", "K", "ACT", "DMS", "PEG"}


def _database_url() -> str:
    for key in ("NEON_DATABASE_URL", "DATABASE_URL", "POSTGRES_URL", "HSA_DATABASE_URL"):
        if os.getenv(key):
            return os.environ[key].strip()
    env = pathlib.Path(__file__).resolve().parents[1] / ".env"
    if env.exists():
        for line in env.read_text().splitlines():
            if line.startswith("NEON_DATABASE_URL="):
                return line.split("=", 1)[1].strip()
    raise SystemExit("No NEON_DATABASE_URL (env or .env).")


def fetch_alpelisib_smiles() -> str:
    url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/alpelisib/property/CanonicalSMILES/TXT"
    with urllib.request.urlopen(url, timeout=60) as fh:
        return fh.read().decode().strip().splitlines()[0]


def fetch_pdb(pdb_id: str) -> str:
    with urllib.request.urlopen(f"https://files.rcsb.org/download/{pdb_id}.pdb", timeout=60) as fh:
        return fh.read().decode("utf-8")


def receptor_and_box(pdb_text: str) -> tuple[str, dict]:
    """Strip HETATM -> apo receptor; box center = centroid of the largest drug-like ligand."""
    receptor_lines, het_by_res = [], {}
    for line in pdb_text.splitlines():
        rec = line[:6].strip()
        if rec == "ATOM":
            receptor_lines.append(line)
        elif rec == "HETATM":
            resn = line[17:20].strip()
            if resn in _NON_LIGAND_HET:
                continue
            try:
                xyz = (float(line[30:38]), float(line[38:46]), float(line[46:54]))
            except ValueError:
                continue
            het_by_res.setdefault((resn, line[21], line[22:26]), []).append(xyz)
    receptor_lines.append("END")
    receptor = "\n".join(receptor_lines) + "\n"
    if not het_by_res:
        return receptor, {}  # whole-receptor autobox fallback
    biggest = max(het_by_res.values(), key=len)  # the drug-like ligand (most atoms)
    n = len(biggest)
    cx, cy, cz = (sum(c[i] for c in biggest) / n for i in range(3))
    return receptor, {"center_x": round(cx, 2), "center_y": round(cy, 2), "center_z": round(cz, 2),
                      "size_x": 22, "size_y": 22, "size_z": 22}


def main() -> None:
    repo = PostgresResearchRepository(_database_url(), seed=False)
    service = HSAResearchService(repo)

    print("Fetching real inputs (PubChem alpelisib SMILES + RCSB PI3Ka 4JPS) ...", flush=True)
    smiles = fetch_alpelisib_smiles()
    receptor, box = receptor_and_box(fetch_pdb(PDB_ID))
    print(f"  alpelisib SMILES: {smiles}")
    print(f"  receptor: {receptor.count(chr(10))} ATOM lines; box center: {box.get('center_x')},{box.get('center_y')},{box.get('center_z')}")

    _seed_validation_ready_candidate(repo, candidate_id=CANDIDATE_ID, ready=True)
    cand = service.get_public_candidate(CANDIDATE_ID)
    docking_inputs = {"receptor_pdb": receptor, "ligand_smiles": smiles,
                      "target": "PI3Ka_4JPS", "ligand_name": "alpelisib", **box}
    repo.upsert_public_candidate(cand.model_copy(update={
        "targets": ["PIK3CA"], "candidate_therapies": ["alpelisib"],
        "metadata": {"lane_inputs": {"docking": docking_inputs}},
    }))
    print(f"\nSeeded validation-ready candidate '{CANDIDATE_ID}' with real docking inputs.\n")

    print("Running the autonomous falsification loop (runner_kind='modal', real gnina on A100) ...", flush=True)
    result = service.run_falsification_loop(CANDIDATE_ID, lane_allowlist=["docking"], runner_kind="modal", max_rounds=2)

    print("\n===== AUTONOMOUS DISCOVERY RESULT =====")
    print(f"  terminal_reason          : {result.terminal_reason}")
    print(f"  leading_hypothesis_status: {result.leading_hypothesis_status}")
    print(f"  rounds_run               : {result.rounds_run}")
    print(f"  promoted (must be False) : {result.promoted}")
    print(f"  total_est_cost_usd       : ${result.total_est_cost_usd}")
    for i, rnd in enumerate(result.rounds, 1):
        print(f"\n  --- round {i} ---")
        if rnd.plan:
            print(f"    proposed lane     : {rnd.plan.lane} (inputs_ready={rnd.plan.inputs_ready})")
            print(f"    test_objective    : {rnd.plan.test_objective}")
            print(f"    kill_criterion    : {rnd.plan.kill_criterion.metric} {rnd.plan.kill_criterion.comparator} "
                  f"{rnd.plan.kill_criterion.threshold}  (kills on '{rnd.plan.kill_criterion.observed_signal_kills}')")
        print(f"    observed_signal   : {rnd.observed_signal}")
        print(f"    kill_criterion_met: {rnd.kill_criterion_met}")
        print(f"    errors            : {rnd.errors}")
        if rnd.capsule_id:
            cap = repo.get_proof_capsule(rnd.capsule_id)
            print(f"    capsule status    : {cap.status}  metrics: {cap.payload.get('metrics')}")
            print(f"    finding           : {str(cap.payload.get('output_payload', {}).get('findings'))[:200]}")


if __name__ == "__main__":
    main()
