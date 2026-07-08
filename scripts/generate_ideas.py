"""Run autonomous candidate generation against the store (Neon by default).

Proposes NEW grounded candidate ideas from data/candidate_generation_seed.json, gates each on real
docking-input resolvability (verified target library + PubChem ligand), dedups against existing
candidates, and seeds the dockable survivors validation-ready. Generation spends NO GPU money (only
PubChem lookups); the falsification loop docks the new candidates afterward.

    PYTHONPATH=src:. python scripts/generate_ideas.py --max-new 5
    PYTHONPATH=src:. python scripts/generate_ideas.py --dry-run    # preview, seed nothing
"""

from __future__ import annotations

import argparse

from hsa_research.ingestion_bridge.input_resolvers import NetworkInputResolvers
from hsa_research.ingestion_bridge.postgres_store import PostgresResearchRepository
from hsa_research.ingestion_bridge.service import HSAResearchService

from scripts.run_real_demo import _database_url


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="curated_seed", choices=["curated_seed", "claims"])
    ap.add_argument("--seed-path", default=None, help="path to a seed JSON (e.g. data/repurposing_seed.json)")
    ap.add_argument("--max-new", type=int, default=5)
    ap.add_argument("--check-novelty", action="store_true", help="gate ideas on literature prior-art (keep only novel)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--submitted-by", default="chasepenelli")
    args = ap.parse_args()

    repo = PostgresResearchRepository(_database_url(), seed=False)
    service = HSAResearchService(repo)
    service.input_resolvers = NetworkInputResolvers()  # real PubChem for the resolvability gate

    manifest = service.generate_candidate_ideas(
        source=args.source, seed_path=args.seed_path, max_new=args.max_new,
        check_novelty=args.check_novelty, dry_run=args.dry_run, submitted_by=args.submitted_by,
    )
    refs = manifest.output_refs
    roll = refs.get("rollup", {})
    print(f"\n=== CANDIDATE GENERATION {manifest.manifest_id} (source={args.source}, novelty={args.check_novelty}, dry_run={args.dry_run}) ===")
    print(f"  seeded={roll.get('seeded')}  skipped_exists={roll.get('skipped_exists')}  rejected={roll.get('rejected')}")
    for s in refs.get("seeded", []):
        nov = f"  novelty={s['novelty_score']}" if "novelty_score" in s else ""
        print(f"   + {s['candidate_id']:<28} {s.get('therapy')} → {s.get('target')}{nov}")
    for s in refs.get("skipped", []):
        print(f"   = {s['candidate_id']:<28} ({s.get('reason')})")
    for r in refs.get("rejected", []):
        extra = f" (n_ctd={r.get('n_ctd')}, novelty={r.get('novelty_score')})" if r.get("reason") == "not_novel" else f" {r.get('missing', '')}"
        print(f"   - {r.get('candidate_id', r.get('row'))}  REJECTED: {r.get('reason')}{extra}")


if __name__ == "__main__":
    main()
