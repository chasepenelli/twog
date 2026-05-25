#!/usr/bin/env python3
"""One-shot operator path: accept a proof capsule and emit its reward event.

Usage:
    HSA_DATABASE_URL='<neon-url>' PYTHONPATH=src uv run python \\
        scripts/one_shot_accept_and_sync.py \\
        --proof-capsule-id <uuid> \\
        --reviewer-id chase \\
        [--verdict accepted]

This is the operator side of the loop the agent CLI drives. The Python
service is the same one a future operator review console will call:

    1. ``record_proof_capsule_review`` writes a review event and
       transitions the capsule's status.
    2. ``sync_reward_events_from_proof_capsule_reviews`` reads the
       review row back, idempotently emits a reward event through the
       research repository (which writes to the ``reward_events`` table).

After this script runs, ``twog-agent contributor whoami --handle <h>``
should show the contributor on the tier ladder.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from uuid import UUID, uuid4

# Make src/ importable without installing the project.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from hsa_research.ingestion_bridge.contracts import ProofCapsuleReviewRecord
from hsa_research.ingestion_bridge.postgres_store import PostgresResearchRepository
from hsa_research.ingestion_bridge.proof_capsules import (
    PROOF_CAPSULE_VERDICTS,
    record_proof_capsule_review,
    sync_reward_events_from_proof_capsule_reviews,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proof-capsule-id", required=True, help="UUID of the capsule to review")
    parser.add_argument(
        "--verdict",
        default="accepted",
        choices=list(PROOF_CAPSULE_VERDICTS),
        help="Operator verdict (default: accepted)",
    )
    parser.add_argument("--reviewer-id", default="chase", help="Operator identity")
    parser.add_argument("--reviewer-type", default="operator", help="operator|llm_evaluator|system|external_expert")
    parser.add_argument("--rationale", default="One-shot end-to-end smoke. Operator accepts.")
    parser.add_argument("--confidence", type=float, default=0.85)
    parser.add_argument("--scientific-usefulness", type=float, default=0.9)
    parser.add_argument("--provenance-strength", type=float, default=0.8)
    parser.add_argument("--reproducibility", type=float, default=0.7)
    parser.add_argument("--actionability", type=float, default=0.85)
    parser.add_argument("--novelty", type=float, default=0.5)
    parser.add_argument("--clarity", type=float, default=0.9)
    parser.add_argument("--downstream-impact", type=float, default=0.6)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_url = os.environ.get("HSA_DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
    if not db_url:
        print("Set HSA_DATABASE_URL (or NEON_DATABASE_URL).", file=sys.stderr)
        return 2

    review = ProofCapsuleReviewRecord(
        review_id=uuid4(),
        proof_capsule_id=UUID(args.proof_capsule_id),
        reviewer_type=args.reviewer_type,
        reviewer_id=args.reviewer_id,
        verdict=args.verdict,
        confidence=args.confidence,
        scientific_usefulness=args.scientific_usefulness,
        provenance_strength=args.provenance_strength,
        reproducibility=args.reproducibility,
        actionability=args.actionability,
        novelty=args.novelty,
        clarity=args.clarity,
        downstream_impact=args.downstream_impact,
        rationale=args.rationale,
    )

    print(f"step 1 — recording review {review.review_id} on capsule {review.proof_capsule_id}")
    review_result = record_proof_capsule_review(review, database_url=db_url)
    print(json.dumps(review_result, indent=2, default=str))
    if not review_result.get("stored"):
        print("review did not store; bailing out.", file=sys.stderr)
        return 1

    print("\nstep 2 — instantiating PostgresResearchRepository (creates reward_events table if needed)")
    repository = PostgresResearchRepository(db_url)
    print(f"repository ready: {type(repository).__name__}")

    print("\nstep 3 — syncing reward events from proof_capsule_reviews")
    sync_result = sync_reward_events_from_proof_capsule_reviews(
        repository, database_url=db_url, limit=500
    )
    print(json.dumps(sync_result, indent=2, default=str))
    if sync_result.get("errors"):
        print("sync reported errors; see above.", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
