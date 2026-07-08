"""Run a REAL, budget-capped falsification campaign on Modal GPU against Neon (production).

Docking-focused: docks never-tested validation-ready candidates against the verified PIK3CA/KDR pockets,
refuses any whose SMILES won't resolve (no spend), writes real signed capsules to Neon, never auto-promotes.
Hard-capped by campaign_budget_usd. Invoked explicitly (not by the scheduler).

    NEON_DATABASE_URL=... PYTHONPATH=src python scripts/run_real_campaign.py
"""

from __future__ import annotations

import os

from hsa_research.ingestion_bridge.input_catalog import CatalogResolvers, InputCatalog
from hsa_research.ingestion_bridge.input_resolvers import NetworkInputResolvers
from hsa_research.ingestion_bridge.postgres_store import PostgresResearchRepository
from hsa_research.ingestion_bridge.service import HSAResearchService

from scripts.run_web_api import _database_url

CAMPAIGN_BUDGET_USD = float(os.environ.get("CAMPAIGN_BUDGET_USD", "5.00"))
PER_CANDIDATE_USD = float(os.environ.get("PER_CANDIDATE_USD", "0.50"))
MAX_ROUNDS = int(os.environ.get("MAX_ROUNDS", "2"))
# Lane allowlist — restricts what the campaign can dispatch (the cost+safety gate). Defaults to docking;
# set LANES=cofolding for the orthogonal engagement sweep. NEVER include 'md' here (md is $40/run and
# expert-gated) unless you mean it.
LANES = [s.strip() for s in os.environ.get("LANES", "docking").split(",") if s.strip()]


def main() -> None:
    url = _database_url()
    print(f"Neon: {url.split('@')[-1].split('/')[0]}", flush=True)
    svc = HSAResearchService(PostgresResearchRepository(url, seed=False))
    # read-through catalog so resolved SMILES/sequences are reused (resolve-once) and persisted
    svc.input_resolvers = CatalogResolvers(NetworkInputResolvers(), InputCatalog())

    assert "md" not in LANES, "refusing to autodispatch the md lane ($40/run) from a campaign"
    print(
        f"campaign: runner=modal · lanes={LANES} · max_rounds={MAX_ROUNDS} · "
        f"per_candidate=${PER_CANDIDATE_USD:.2f} · CAP=${CAMPAIGN_BUDGET_USD:.2f}",
        flush=True,
    )
    manifest = svc.run_falsification_campaign(
        candidate_ids=None,  # all validation-ready candidates; already-tested lanes are $0 no-ops
        max_rounds=MAX_ROUNDS,
        budget_usd_per_candidate=PER_CANDIDATE_USD,
        campaign_budget_usd=CAMPAIGN_BUDGET_USD,
        lane_allowlist=LANES,
        runner_kind="modal",
        submitted_by="chasepenelli",
        persist=True,
    )
    refs = manifest.output_refs or {}
    rollup = refs.get("rollup") or {}
    rows = refs.get("rows") or []
    dispatched = [r for r in rows if r.get("compute_job_ids")]
    print("\n=== CAMPAIGN COMPLETE ===", flush=True)
    print(f"manifest_id: {manifest.manifest_id}", flush=True)
    print(f"candidates_selected: {rollup.get('candidates_selected')} · processed: {rollup.get('candidates_processed')}", flush=True)
    print(f"total_est_cost_usd: ${float(rollup.get('total_est_cost_usd', 0)):.2f} · budget_exhausted: {rollup.get('budget_exhausted')}", flush=True)
    print(f"any_promoted: {rollup.get('any_promoted')}  (must be False)", flush=True)
    print(f"terminal_reasons: {rollup.get('terminal_reasons')}", flush=True)
    print(f"leading_hypothesis_status: {rollup.get('leading_hypothesis_status')}", flush=True)
    print(f"rows that dispatched real compute: {len(dispatched)}", flush=True)
    for r in dispatched[:25]:
        print(f"  - {r.get('candidate_id')}: {r.get('leading_hypothesis_status')} "
              f"({r.get('terminal_reason')}) ${float(r.get('total_est_cost_usd', 0)):.2f}", flush=True)


if __name__ == "__main__":
    main()
