"""Autonomous generate→dock cycle — the local "keep it going" driver.

Each tick: generate NEW dockable candidates ($0 GPU, PubChem only), then run a budget-capped
falsification campaign that docks the validation-ready ones. Bounded three ways: per-candidate cap,
per-pass campaign cap, and a HARD total kill-switch across the whole run, plus a wall-clock deadline.
Fresh DB connection per tick (Neon drops idle connections). This is the same generate→dock flow the
Dagster `falsification_loop` schedule runs on the deploy — this driver is for running it now, locally.

    PYTHONPATH=src:. python scripts/run_autonomous_cycle.py --minutes 240 --total-budget-usd 6
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

from hsa_research.ingestion_bridge.input_resolvers import NetworkInputResolvers
from hsa_research.ingestion_bridge.postgres_store import PostgresResearchRepository
from hsa_research.ingestion_bridge.service import HSAResearchService

from scripts.run_real_demo import _database_url


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=240.0)
    ap.add_argument("--total-budget-usd", type=float, default=6.0, help="HARD kill-switch across all ticks")
    ap.add_argument("--interval-min", type=float, default=10.0)
    ap.add_argument("--generate-max-new", type=int, default=3)
    ap.add_argument("--per-candidate", type=float, default=0.50)
    ap.add_argument("--campaign-budget-usd", type=float, default=2.0)
    ap.add_argument("--max-rounds", type=int, default=2)
    ap.add_argument("--runner-kind", default="modal")
    ap.add_argument("--lane", action="append", default=["docking"], help="restrict lanes; repeat. MD excluded by the per-candidate budget anyway.")
    ap.add_argument("--seed-path", default=None, help="generation seed JSON (e.g. data/repurposing_seed.json); default = built-in curated seed")
    ap.add_argument("--check-novelty", action="store_true", help="prior-art gate generated ideas (keep only novel)")
    args = ap.parse_args()

    db_url = _database_url()

    def _fresh_service() -> HSAResearchService:
        svc = HSAResearchService(PostgresResearchRepository(db_url, seed=False))
        if args.runner_kind != "mock":
            svc.input_resolvers = NetworkInputResolvers()
        return svc

    _log(f"AUTONOMOUS CYCLE start · runner={args.runner_kind} · {args.minutes:.0f} min · "
         f"HARD total cap ${args.total_budget_usd:.2f} · generate {args.generate_max_new}/tick · lanes={args.lane}")

    start = time.monotonic()
    deadline = start + args.minutes * 60
    cumulative = 0.0
    tick = 0

    while time.monotonic() < deadline:
        if cumulative >= args.total_budget_usd:
            _log(f"⛔ HARD total budget ${args.total_budget_usd:.2f} reached (${cumulative:.4f}) — stopping.")
            break
        tick += 1
        pass_budget = min(args.campaign_budget_usd, args.total_budget_usd - cumulative)
        try:
            service = _fresh_service()
            gen = service.generate_candidate_ideas(
                source="curated_seed", seed_path=args.seed_path, max_new=args.generate_max_new,
                check_novelty=args.check_novelty, submitted_by="autonomous_cycle",
            )
            groll = gen.output_refs.get("rollup", {})
            camp = service.run_falsification_campaign(
                max_rounds=args.max_rounds, budget_usd_per_candidate=args.per_candidate,
                campaign_budget_usd=pass_budget, lane_allowlist=args.lane or None,
                runner_kind=args.runner_kind, submitted_by="autonomous_cycle",
            )
            croll = camp.output_refs.get("rollup", {})
        except Exception as exc:
            _log(f"  tick {tick} ERROR: {type(exc).__name__}: {exc}")
            time.sleep(min(args.interval_min * 60, max(0, deadline - time.monotonic())))
            continue

        cost = float(croll.get("total_est_cost_usd") or 0.0)
        cumulative += cost
        _log(f"tick {tick} · generated {groll.get('seeded', 0)} (skip {groll.get('skipped_exists', 0)}, "
             f"rej {groll.get('rejected', 0)}) · docked {croll.get('candidates_processed')}/{croll.get('candidates_selected')} "
             f"· ${cost:.4f} · CUMULATIVE ${cumulative:.4f} · status={croll.get('leading_hypothesis_status')}")

        if time.monotonic() + args.interval_min * 60 >= deadline:
            break
        time.sleep(args.interval_min * 60)

    mins = (time.monotonic() - start) / 60
    _log(f"AUTONOMOUS CYCLE done · {tick} tick(s) · {mins:.1f} min · TOTAL est spend ${cumulative:.4f}")


if __name__ == "__main__":
    main()
