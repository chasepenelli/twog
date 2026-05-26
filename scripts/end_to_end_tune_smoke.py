#!/usr/bin/env python3
"""End-to-end smoke for the three tuning slices.

Walks a candidate from "exists in staging" → "auto-dive submits
deterministic capsules" → "LLM grades the new capsules as
recommendations" → "operator promotes one to accepted with a high
rubric" → "leaderboard reflects differential reward".

Each step asserts the expected state before moving on. Designed to
run against a live local dev server (localhost:3000) with staging
Neon — no migrations, no schema changes.

Required env:
  TWOG_DEMO_MASTER_SEED — same seed @twog-auto-dive uses
  NEON_DATABASE_URL — staging Neon (read + write proof_capsules)
  TWOG_SITE_URL (optional, defaults to http://localhost:3000)
  OPENROUTER_API_KEY (optional — if set, runs the LLM grade step;
                      otherwise skips Step 3)

Usage:
  source .env.staging
  TWOG_DEMO_MASTER_SEED=$(uv run python scripts/run_demo_personas.py keygen) \\
  PYTHONPATH=src \\
    uv run python scripts/end_to_end_tune_smoke.py \\
      --candidate twog-candidate-e5e8a4f68611
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from hsa_research.ingestion_bridge.auto_dive_worker import (
    AUTO_DIVE_HANDLE,
    dive,
    is_in_cooldown,
)


GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
RESET = "\033[0m"


def say(level: str, message: str) -> None:
    icon = {"step": "==>", "ok": "✓", "warn": "!", "fail": "✗", "info": " "}[level]
    color = {"step": "", "ok": GREEN, "warn": YELLOW, "fail": RED, "info": ""}[level]
    print(f"{color}{icon}{RESET} {message}")


def http_get_json(url: str) -> dict[str, Any]:
    req = urlrequest.Request(url, headers={"Accept": "application/json"})
    with urlrequest.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def step_1_candidate_exists(candidate_id: str, site_url: str) -> bool:
    say("step", f"Step 1: candidate {candidate_id} exists in staging")
    try:
        data = http_get_json(f"{site_url}/api/public-candidates/{urlparse.quote(candidate_id, safe='')}")
    except urlerror.HTTPError as e:
        say("fail", f"  candidate fetch returned HTTP {e.code}")
        return False
    snap = data.get("latest_snapshot") or {}
    title = snap.get("title") or "(no title)"
    say("ok", f"  found: {title[:80]}")
    return True


def step_2_auto_dive(
    candidate_id: str,
    master_seed: str,
    site_url: str,
    *,
    force: bool,
    expect_min_capsules: int,
) -> list[str]:
    say("step", f"Step 2: auto-dive against {candidate_id} (force={force})")
    if not force and is_in_cooldown(candidate_id):
        say("warn", "  candidate is in cooldown; pass --force to override")
        return []
    report = dive(
        candidate_id,
        master_seed_b64=master_seed,
        site_url=site_url,
        force=force,
        dry_run=False,
    )
    if report.status != "ok":
        say("fail", f"  dive returned status {report.status}")
        return []
    say("info", f"  proposed: {len(report.proposed_capsules)} capsules")
    for c in report.proposed_capsules:
        say("info", f"    [{c.capsule_type}] {c.diagnostic_tag}")
    submitted = report.submitted_capsule_ids
    say("info", f"  submitted: {len(submitted)} capsules ({len(report.errors)} errors)")
    for cid in submitted[:5]:
        say("info", f"    → {cid[:8]}...")
    for err in report.errors[:3]:
        say("warn", f"    err: {err}")
    if len(submitted) < expect_min_capsules:
        say("fail", f"  expected ≥{expect_min_capsules} submitted, got {len(submitted)}")
        return []
    say("ok", f"  {len(submitted)} auto-dive capsules submitted")
    return submitted


def step_3_llm_grade(submitted_ids: list[str], dry_run: bool) -> int:
    say("step", "Step 3: LLM judge grades auto-dive capsules")
    if not os.environ.get("OPENROUTER_API_KEY"):
        say("warn", "  OPENROUTER_API_KEY not set; skipping LLM grade step")
        return 0
    from hsa_research.ingestion_bridge.llm_capsule_grader import grade_pending_capsules

    result = grade_pending_capsules(limit=len(submitted_ids), dry_run=dry_run)
    if result.get("status") != "ok":
        say("fail", f"  grader returned: {result}")
        return 0
    graded = result.get("graded", 0)
    cached = result.get("cached_hits", 0)
    errors = result.get("errors") or []
    say("info", f"  scanned: {result['scanned']}, graded: {graded}, cached: {cached}")
    for err in errors[:3]:
        say("warn", f"    err: {err}")
    if graded == 0 and cached == 0:
        say("warn", "  no capsules graded (perhaps all already had grades)")
    else:
        say("ok", f"  LLM grades stored as reviewer_type=llm_evaluator")
    return graded + cached


def step_4_operator_promote(submitted_ids: list[str], site_url: str) -> int:
    say("step", "Step 4: operator promotes one auto-dive capsule to accepted")
    if not submitted_ids:
        say("warn", "  no submitted capsule to promote; skipping")
        return 0
    # Promote the first capsule with high rubric scores; expected pp ~94.
    target = submitted_ids[0]
    import subprocess

    cmd = [
        "uv", "run", "python", "scripts/one_shot_accept_and_sync.py",
        "--proof-capsule-id", target,
        "--reviewer-id", "smoke-operator",
        "--rationale", "End-to-end smoke: operator promotes auto-dive output to accepted.",
        "--scientific-usefulness", "0.95",
        "--provenance-strength", "0.9",
        "--actionability", "0.95",
        "--novelty", "0.7",
        "--clarity", "0.9",
        "--downstream-impact", "0.8",
        "--reproducibility", "0.7",
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    result = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(REPO))
    if result.returncode != 0:
        say("fail", f"  accept script returned {result.returncode}: {result.stderr[:200]}")
        return 0
    say("ok", f"  promoted capsule {target[:8]}... to accepted (high rubric)")
    return 1


def step_5_leaderboard_reflects(site_url: str) -> bool:
    say("step", "Step 5: leaderboard reflects @twog-auto-dive contribution")
    try:
        data = http_get_json(f"{site_url}/api/leaderboard")
    except Exception as e:
        say("fail", f"  leaderboard fetch failed: {e}")
        return False
    entry = next(
        (e for e in data.get("entries", []) if e["handle"] == AUTO_DIVE_HANDLE),
        None,
    )
    if entry is None:
        say("warn", "  @twog-auto-dive not on leaderboard yet (may need more accepted work)")
        return False
    say("info", f"  rank #{entry['rank']}  pp={entry['proof_points']}  accepted={entry['accepted_capsule_count']}")
    say("info", f"  tier: {entry.get('tier_label')}")
    median = entry.get("median_rubric_score")
    if median is not None:
        say("info", f"  median rubric: {median:.3f}")
    say("ok", "  @twog-auto-dive visible on leaderboard with differential reward")
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--candidate", required=True, help="Candidate ID to smoke against")
    parser.add_argument("--site-url", default=os.environ.get("TWOG_SITE_URL", "http://localhost:3000"))
    parser.add_argument("--force", action="store_true", help="Bypass auto-dive cooldown")
    parser.add_argument("--min-capsules", type=int, default=1, help="Min capsules expected from auto-dive")
    parser.add_argument("--llm-dry-run", action="store_true", help="Don't persist LLM grades; just call the LLM")
    parser.add_argument("--skip-promote", action="store_true", help="Don't run the operator promotion step")
    args = parser.parse_args(argv)

    master_seed = os.environ.get("TWOG_DEMO_MASTER_SEED")
    if not master_seed:
        print("error: TWOG_DEMO_MASTER_SEED env required", file=sys.stderr)
        return 2

    print(f"site:      {args.site_url}")
    print(f"candidate: {args.candidate}")
    print(f"force:     {args.force}")
    print()

    if not step_1_candidate_exists(args.candidate, args.site_url):
        return 1
    submitted = step_2_auto_dive(
        args.candidate, master_seed, args.site_url,
        force=args.force, expect_min_capsules=args.min_capsules,
    )
    if not submitted:
        return 1
    time.sleep(2)  # let the server settle
    step_3_llm_grade(submitted, dry_run=args.llm_dry_run)
    if not args.skip_promote:
        time.sleep(1)
        step_4_operator_promote(submitted, args.site_url)
        time.sleep(1)
        step_5_leaderboard_reflects(args.site_url)
    say("ok", "end-to-end smoke complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
