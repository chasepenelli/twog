"""Seed the real campaign roster (data/real_roster.json) into the store (Neon by default).

Each candidate is seeded validation-ready via the real readiness gate. Docking candidates resolve
their receptor from the curated verified target_library at run time (no inputs stored here); the omics
crux carries inline Megquier expression+strata so the REAL omics engine runs.

    PYTHONPATH=src:. python scripts/seed_real_roster.py
"""

from __future__ import annotations

from hsa_research.ingestion_bridge import real_roster
from hsa_research.ingestion_bridge.postgres_store import PostgresResearchRepository
from hsa_research.ingestion_bridge.service import HSAResearchService

from scripts.run_real_demo import _database_url


def main() -> None:
    repo = PostgresResearchRepository(_database_url(), seed=False)
    service = HSAResearchService(repo)
    candidates = real_roster.load_roster()
    if not candidates:
        raise SystemExit("No candidates in data/real_roster.json")

    omics_inputs = None
    for c in candidates:
        lane_inputs = None
        if c.get("omics_source") == "megquier":
            omics_inputs = omics_inputs or real_roster.build_megquier_omics_inputs()  # fetch once, reuse
            lane_inputs = {"omics": omics_inputs}
        readiness = service.seed_validation_ready_candidate(
            c["candidate_id"],
            title=c["title"],
            evidence_refs=c.get("evidence_refs", []),
            targets=c.get("targets"),
            candidate_therapies=c.get("candidate_therapies"),
            biomarkers=c.get("biomarkers"),
            lane_inputs=lane_inputs,
            rationale=c.get("rationale", ""),
        )
        ready = getattr(readiness, "ready", None)
        omics = " +omics(inline)" if lane_inputs else ""
        print(f"  seeded {c['candidate_id']:<32} validation_ready={ready}  lanes={c.get('lanes')}{omics}")
    print(f"\nSeeded {len(candidates)} candidates. Verify the docking targets first: scripts/verify_target_library.py")


if __name__ == "__main__":
    main()
