"""Run the autonomous falsification campaign across the seeded real roster + print the report.

Defaults to runner_kind=mock (free, no GPU). Use --runner-kind modal for the REAL run — verify the
target library first (scripts/verify_target_library.py) and seed the roster (scripts/seed_real_roster.py).
The persisted report is a RunManifestRecord(manifest_type=falsification_campaign), also readable via
`python -m hsa_research.ingestion_bridge.cli run-manifests --manifest-type falsification_campaign`.

    PYTHONPATH=src:. python scripts/run_campaign.py --runner-kind modal --lane docking --lane omics
"""

from __future__ import annotations

import argparse

from hsa_research.ingestion_bridge.input_resolvers import NetworkInputResolvers
from hsa_research.ingestion_bridge.postgres_store import PostgresResearchRepository
from hsa_research.ingestion_bridge.service import HSAResearchService

from scripts.run_real_demo import _database_url


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runner-kind", default="mock")
    ap.add_argument("--max-rounds", type=int, default=2)
    ap.add_argument("--budget-usd-per-candidate", type=float, default=0.50)
    ap.add_argument("--campaign-budget-usd", type=float, default=2.0)
    ap.add_argument("--max-candidates", type=int, default=None)
    ap.add_argument("--lane", action="append", default=[], help="restrict to lane(s); repeat")
    ap.add_argument("--candidate-id", action="append", default=[], help="specific candidate(s); repeat")
    ap.add_argument("--submitted-by", default="chasepenelli")
    args = ap.parse_args()

    repo = PostgresResearchRepository(_database_url(), seed=False)
    service = HSAResearchService(repo)
    if args.runner_kind != "mock":
        service.input_resolvers = NetworkInputResolvers()  # PubChem ligand for docking-from-library

    result = service.run_falsification_campaign(
        candidate_ids=args.candidate_id or None,
        max_rounds=args.max_rounds,
        budget_usd_per_candidate=args.budget_usd_per_candidate,
        campaign_budget_usd=args.campaign_budget_usd,
        max_candidates=args.max_candidates,
        lane_allowlist=args.lane or None,
        runner_kind=args.runner_kind,
        submitted_by=args.submitted_by,
    )

    rollup = result.output_refs.get("rollup", {})
    print(f"\n=== CAMPAIGN {result.manifest_id} (runner={args.runner_kind}) ===")
    print(f"  processed         : {rollup.get('candidates_processed')}/{rollup.get('candidates_selected')}")
    print(f"  total_est_cost_usd: ${rollup.get('total_est_cost_usd')}   budget_exhausted={rollup.get('budget_exhausted')}")
    print(f"  any_promoted      : {rollup.get('any_promoted')}   (must be False)")
    print(f"  terminal_reasons  : {rollup.get('terminal_reasons')}")
    print(f"  hypothesis_status : {rollup.get('leading_hypothesis_status')}")
    for row in result.output_refs.get("rows", []):
        if "skipped" in row:
            print(f"   - {row['candidate_id']:<32} SKIPPED ({row['skipped']})")
            continue
        print(
            f"   - {row['candidate_id']:<32} {row.get('terminal_reason', '?'):<22} "
            f"status={row.get('leading_hypothesis_status', '?'):<11} "
            f"cost=${row.get('total_est_cost_usd', 0)} capsules={len(row.get('capsule_ids', []))}"
        )


if __name__ == "__main__":
    main()
