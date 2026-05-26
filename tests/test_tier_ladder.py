"""Unit tests for compute_tier with the quality-weighted thresholds.

Tier thresholds are framed in proof-point terms (not raw accepted
count) so that quality-weighted rewards translate into tier movement.
These tests pin the threshold behavior so future changes are
intentional.
"""

from __future__ import annotations

from hsa_research.ingestion_bridge.contributor_profiles import compute_tier


# ---------- Floors ---------------------------------------------------


def test_observer_when_no_proof_points() -> None:
    assert compute_tier(
        accepted_count=0, routed_count=0, distinct_candidates=0,
        capsule_type_counts={}, proof_points=0,
    ) == "observer"


def test_observer_when_pp_below_scout_floor() -> None:
    """A single near-zero-rubric capsule (40 pp) shouldn't promote to
    Scout; the pp floor is the gate."""
    assert compute_tier(
        accepted_count=1, routed_count=0, distinct_candidates=1,
        capsule_type_counts={"citation_repair": 1}, proof_points=40,
    ) == "observer"


def test_scout_at_fifty_proof_points() -> None:
    """First meaningful contribution: any capsule that clears 50 pp."""
    assert compute_tier(
        accepted_count=1, routed_count=0, distinct_candidates=1,
        capsule_type_counts={"citation_repair": 1}, proof_points=50,
    ) == "scout"


# ---------- Citation Repairer ---------------------------------------


def test_citation_repairer_requires_pp_and_specialty() -> None:
    """200 pp + an accepted citation_repair → Citation Repairer."""
    assert compute_tier(
        accepted_count=2, routed_count=0, distinct_candidates=1,
        capsule_type_counts={"citation_repair": 2}, proof_points=200,
    ) == "citation_repairer"


def test_citation_repairer_does_not_apply_without_citation_specialty() -> None:
    """200 pp but no citation_repair → falls through to scout."""
    assert compute_tier(
        accepted_count=2, routed_count=0, distinct_candidates=1,
        capsule_type_counts={"claim_critique": 2}, proof_points=200,
    ) == "scout"


# ---------- Record Builder ------------------------------------------


def test_record_builder_at_five_hundred_pp_two_candidates() -> None:
    """The dive run brought @demo-akira-claims to ~500 pp across 2 candidates;
    this test pins the threshold for that exact scenario."""
    assert compute_tier(
        accepted_count=5, routed_count=0, distinct_candidates=2,
        capsule_type_counts={"claim_critique": 5}, proof_points=500,
    ) == "record_builder"


def test_record_builder_falls_back_to_scout_with_only_one_candidate() -> None:
    """500 pp on one candidate doesn't earn Record Builder — breadth matters."""
    assert compute_tier(
        accepted_count=5, routed_count=0, distinct_candidates=1,
        capsule_type_counts={"claim_critique": 5}, proof_points=500,
    ) == "scout"


# ---------- Specialty tiers -----------------------------------------


def test_validation_contributor_requires_seven_hundred_pp() -> None:
    """700 pp + accepted validation_proposal → Validation Contributor."""
    assert compute_tier(
        accepted_count=4, routed_count=0, distinct_candidates=2,
        capsule_type_counts={"validation_proposal": 1, "claim_critique": 3},
        proof_points=700,
    ) == "validation_contributor"


def test_validation_contributor_below_pp_floor_drops_to_record_builder() -> None:
    """At 500 pp with a validation_proposal, falls back to Record Builder
    (still meets the 500 pp + 2 candidates floor)."""
    assert compute_tier(
        accepted_count=4, routed_count=0, distinct_candidates=2,
        capsule_type_counts={"validation_proposal": 1, "claim_critique": 3},
        proof_points=500,
    ) == "record_builder"


def test_replication_contributor_with_docking_replication() -> None:
    assert compute_tier(
        accepted_count=3, routed_count=1, distinct_candidates=2,
        capsule_type_counts={"docking_replication": 2, "citation_repair": 2},
        proof_points=700,
    ) == "replication_contributor"


def test_replication_contributor_with_md_review() -> None:
    assert compute_tier(
        accepted_count=4, routed_count=0, distinct_candidates=2,
        capsule_type_counts={"md_review": 2, "claim_critique": 2},
        proof_points=750,
    ) == "replication_contributor"


# ---------- Top of ladder -------------------------------------------


def test_trusted_reviewer_at_one_thousand_pp() -> None:
    assert compute_tier(
        accepted_count=10, routed_count=0, distinct_candidates=3,
        capsule_type_counts={"citation_repair": 10}, proof_points=1000,
    ) == "trusted_reviewer"


def test_proof_partner_requires_volume_pp_and_breadth() -> None:
    """The top tier needs all three: 20+ positive, 2000+ pp, 5+ candidates."""
    assert compute_tier(
        accepted_count=20, routed_count=0, distinct_candidates=5,
        capsule_type_counts={"citation_repair": 12, "claim_critique": 8},
        proof_points=2000,
    ) == "proof_partner"


def test_proof_partner_without_breadth_drops_to_trusted_reviewer() -> None:
    assert compute_tier(
        accepted_count=20, routed_count=0, distinct_candidates=3,
        capsule_type_counts={"citation_repair": 20}, proof_points=2200,
    ) == "trusted_reviewer"


def test_proof_partner_without_volume_drops_to_trusted_reviewer() -> None:
    """High pp + breadth but low total count → still Trusted Reviewer.
    Proof Partner needs you to have shown up many times."""
    assert compute_tier(
        accepted_count=10, routed_count=0, distinct_candidates=5,
        capsule_type_counts={"citation_repair": 10}, proof_points=2200,
    ) == "trusted_reviewer"


# ---------- Monotonicity --------------------------------------------


def test_compute_tier_monotonic_in_proof_points() -> None:
    """As pp increases, the contributor's tier can only move up the ladder."""
    base = dict(
        accepted_count=10, routed_count=0, distinct_candidates=3,
        capsule_type_counts={"citation_repair": 10},
    )
    tier_low = compute_tier(**base, proof_points=200)
    tier_mid = compute_tier(**base, proof_points=600)
    tier_high = compute_tier(**base, proof_points=1500)
    ladder = [
        "observer", "scout", "citation_repairer", "record_builder",
        "replication_contributor", "validation_contributor",
        "trusted_reviewer", "proof_partner",
    ]
    assert ladder.index(tier_low) <= ladder.index(tier_mid) <= ladder.index(tier_high)
