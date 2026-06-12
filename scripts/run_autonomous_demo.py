"""Fully-autonomous discovery demo (v0.2): name a hypothesis, the loop fetches its own inputs.

Seeds a validation-ready candidate that ONLY NAMES a target + therapy (no curated inputs), turns on the
network resolvers, and runs the loop: it auto-fetches the PI3Ka structure (RCSB) + alpelisib SMILES
(PubChem), dispatches real gnina docking on Modal, and evaluates the pre-registered kill-criterion.
Nothing is auto-promoted.

    PYTHONPATH=src:. python scripts/run_autonomous_demo.py
"""

from __future__ import annotations

from hsa_research.ingestion_bridge.input_resolvers import NetworkInputResolvers
from hsa_research.ingestion_bridge.postgres_store import PostgresResearchRepository
from hsa_research.ingestion_bridge.service import HSAResearchService

# reuse the Neon-url + candidate-seed helpers from the curated demo
from scripts.run_real_demo import _database_url
from tests.test_candidates import _seed_validation_ready_candidate

CANDIDATE_ID = "alpelisib-pi3ka-auto"


def main() -> None:
    repo = PostgresResearchRepository(_database_url(), seed=False)
    service = HSAResearchService(repo)
    service.input_resolvers = NetworkInputResolvers()  # turn ON live PubChem + RCSB resolution

    _seed_validation_ready_candidate(repo, candidate_id=CANDIDATE_ID, ready=True)
    cand = service.get_public_candidate(CANDIDATE_ID)
    repo.upsert_public_candidate(cand.model_copy(update={
        "targets": ["PIK3CA"], "candidate_therapies": ["alpelisib"], "metadata": {},  # NO curated inputs
    }))
    print(f"Seeded '{CANDIDATE_ID}' naming ONLY target=PIK3CA, therapy=alpelisib — no curated inputs.\n")

    print("Running the loop (network resolvers ON: RCSB structure + PubChem SMILES; real gnina on Modal)...", flush=True)
    result = service.run_falsification_loop(CANDIDATE_ID, lane_allowlist=["docking"], runner_kind="modal", max_rounds=2)

    print("\n===== FULLY-AUTONOMOUS RESULT (inputs self-resolved) =====")
    print(f"  terminal_reason          : {result.terminal_reason}")
    print(f"  leading_hypothesis_status: {result.leading_hypothesis_status}")
    print(f"  promoted (must be False) : {result.promoted}")
    print(f"  total_est_cost_usd       : ${result.total_est_cost_usd}")
    for i, rnd in enumerate(result.rounds, 1):
        if rnd.plan is None:
            continue
        print(f"\n  round {i}: lane={rnd.plan.lane} inputs_ready={rnd.plan.inputs_ready}")
        print(f"    observed_signal={rnd.observed_signal} kill_met={rnd.kill_criterion_met} errors={rnd.errors}")
        if rnd.capsule_id:
            cap = repo.get_proof_capsule(rnd.capsule_id)
            inp = cap.payload.get("falsification_preregistration", {})
            print(f"    inputs_resolved={inp.get('inputs_resolved')}  capsule={cap.status}")
            print(f"    finding: {str(cap.payload.get('output_payload', {}).get('findings'))[:200]}")


if __name__ == "__main__":
    main()
