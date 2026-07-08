"""Time-boxed, budget-capped spend probe — run real falsification campaigns for ~an hour and log the
actual estimated spend per pass + cumulative, so we can decide on the real autonomous cadence.

THREE layers of cost protection: (1) per-candidate cap, (2) per-pass campaign cap, (3) a HARD total
kill-switch across all passes — the probe stops the instant cumulative est-cost reaches it, regardless
of time remaining. runner_kind=modal does real GPU docking; the est figures are the engine's estimate
(cross-check the Modal dashboard for the true charge).

    PYTHONPATH=src:. python scripts/run_spend_probe.py --minutes 60 --total-budget-usd 4 --runner-kind modal
"""

from __future__ import annotations

import argparse
import sys
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
    ap.add_argument("--minutes", type=float, default=60.0, help="wall-clock budget")
    ap.add_argument("--total-budget-usd", type=float, default=4.0, help="HARD kill-switch across all passes")
    ap.add_argument("--interval-min", type=float, default=8.0, help="gap between passes")
    ap.add_argument("--per-candidate", type=float, default=0.50)
    ap.add_argument("--campaign-budget-usd", type=float, default=2.0, help="cap for a single pass")
    ap.add_argument("--max-rounds", type=int, default=2)
    ap.add_argument("--runner-kind", default="modal")
    ap.add_argument("--lane", action="append", default=["docking", "omics"])
    args = ap.parse_args()

    db_url = _database_url()

    def _fresh_service() -> HSAResearchService:
        # Rebuild per pass — Neon (serverless) drops idle connections across the inter-pass sleep,
        # so a long-lived connection goes stale. A fresh repo each pass is cheap + robust.
        service = HSAResearchService(PostgresResearchRepository(db_url, seed=False))
        if args.runner_kind != "mock":
            service.input_resolvers = NetworkInputResolvers()
        return service

    _log(f"SPEND PROBE start · runner={args.runner_kind} · {args.minutes:.0f} min · "
         f"HARD total cap ${args.total_budget_usd:.2f} · per-candidate ${args.per_candidate:.2f} · "
         f"per-pass ${args.campaign_budget_usd:.2f} · lanes={args.lane}")

    start = time.monotonic()
    deadline = start + args.minutes * 60
    cumulative = 0.0
    pass_n = 0

    while time.monotonic() < deadline:
        if cumulative >= args.total_budget_usd:
            _log(f"⛔ HARD total budget ${args.total_budget_usd:.2f} reached (cumulative ${cumulative:.4f}) — stopping.")
            break
        remaining = args.total_budget_usd - cumulative
        pass_budget = min(args.campaign_budget_usd, remaining)  # never let one pass exceed the total cap
        pass_n += 1
        _log(f"— pass {pass_n} starting (pass cap ${pass_budget:.2f}, ${remaining:.2f} of total left) …")
        try:
            service = _fresh_service()
            result = service.run_falsification_campaign(
                max_rounds=args.max_rounds,
                budget_usd_per_candidate=args.per_candidate,
                campaign_budget_usd=pass_budget,
                lane_allowlist=args.lane or None,
                runner_kind=args.runner_kind,
                submitted_by="spend_probe",
            )
        except Exception as exc:  # never let one failed pass kill the probe silently
            _log(f"  pass {pass_n} ERROR: {type(exc).__name__}: {exc}")
            time.sleep(min(args.interval_min * 60, max(0, deadline - time.monotonic())))
            continue

        rollup = result.output_refs.get("rollup", {})
        cost = float(rollup.get("total_est_cost_usd") or 0.0)
        cumulative += cost
        _log(f"  pass {pass_n} done · manifest={result.manifest_id} · "
             f"processed {rollup.get('candidates_processed')}/{rollup.get('candidates_selected')} · "
             f"pass ${cost:.4f} · CUMULATIVE ${cumulative:.4f} · "
             f"terminal={rollup.get('terminal_reasons')} · status={rollup.get('leading_hypothesis_status')}")

        if time.monotonic() + args.interval_min * 60 >= deadline:
            break
        time.sleep(args.interval_min * 60)

    elapsed_min = (time.monotonic() - start) / 60
    _log(f"SPEND PROBE done · {pass_n} pass(es) · {elapsed_min:.1f} min · "
         f"TOTAL est spend ${cumulative:.4f} · ~${(cumulative / elapsed_min * 60) if elapsed_min else 0:.2f}/hr rate")


if __name__ == "__main__":
    main()
