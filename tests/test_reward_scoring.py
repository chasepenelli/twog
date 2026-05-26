"""Unit tests for the per-rubric weighted reward function.

These tests pin the math of compute_capsule_reward_from_rubric so that
a change in weights or verdict-multiplier is intentional and visible
in diffs. They also pin the fallback behavior for legacy reviews that
predate the rubric-weighted path.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from hsa_research.ingestion_bridge.contracts import ProofCapsuleReviewRecord
from hsa_research.ingestion_bridge.proof_capsules import (
    PROOF_CAPSULE_VERDICT_REWARD_SCORES,
    compute_capsule_reward_from_rubric,
    reward_event_from_proof_capsule_review,
)


def _review(verdict: str = "accepted", **dims) -> ProofCapsuleReviewRecord:
    """Build a review with all 7 rubric dims defaulting to None unless overridden."""
    defaults: dict = {
        "proof_capsule_id": uuid4(),
        "reviewer_type": "operator",
        "reviewer_id": "chase",
        "verdict": verdict,
        "confidence": 0.85,
        "scientific_usefulness": None,
        "provenance_strength": None,
        "reproducibility": None,
        "actionability": None,
        "novelty": None,
        "clarity": None,
        "downstream_impact": None,
        "rationale": "test",
    }
    defaults.update(dims)
    return ProofCapsuleReviewRecord(**defaults)


# ---------- Pure math ------------------------------------------------


def test_all_ones_accepted_returns_full_score() -> None:
    """An accepted capsule with every dim at 1.0 should score 1.0 → 100 pp."""
    review = _review(
        verdict="accepted",
        scientific_usefulness=1.0, provenance_strength=1.0, reproducibility=1.0,
        actionability=1.0, novelty=1.0, clarity=1.0, downstream_impact=1.0,
    )
    assert compute_capsule_reward_from_rubric(review, "accepted") == pytest.approx(1.0)


def test_all_halves_accepted_returns_half_score() -> None:
    """An accepted capsule with every dim at 0.5 should score 0.5 → 50 pp."""
    review = _review(
        verdict="accepted",
        scientific_usefulness=0.5, provenance_strength=0.5, reproducibility=0.5,
        actionability=0.5, novelty=0.5, clarity=0.5, downstream_impact=0.5,
    )
    assert compute_capsule_reward_from_rubric(review, "accepted") == pytest.approx(0.5)


def test_rejected_verdict_returns_zero_regardless_of_rubric() -> None:
    """A rejected capsule never awards points, even with perfect rubric scores."""
    review = _review(
        verdict="rejected",
        scientific_usefulness=1.0, provenance_strength=1.0, reproducibility=1.0,
        actionability=1.0, novelty=1.0, clarity=1.0, downstream_impact=1.0,
    )
    assert compute_capsule_reward_from_rubric(review, "rejected") == 0.0


def test_needs_changes_returns_zero() -> None:
    """needs_changes = 'come back when you've revised'. No points for the
    pending version; the contributor resubmits as a new capsule."""
    review = _review(
        verdict="needs_changes",
        scientific_usefulness=0.9, provenance_strength=0.9, reproducibility=0.9,
        actionability=0.9, novelty=0.9, clarity=0.9, downstream_impact=0.9,
    )
    assert compute_capsule_reward_from_rubric(review, "needs_changes") == 0.0


def test_routed_verdict_applies_ten_percent_haircut() -> None:
    """routed_to_validation and routed_to_compute_review keep 90% of the
    rubric score. The reasoning: the work was real but the verdict says
    'pass this downstream' rather than 'accept as a public receipt'."""
    review = _review(
        verdict="routed_to_validation",
        scientific_usefulness=1.0, provenance_strength=1.0, reproducibility=1.0,
        actionability=1.0, novelty=1.0, clarity=1.0, downstream_impact=1.0,
    )
    assert compute_capsule_reward_from_rubric(review, "routed_to_validation") == pytest.approx(0.9)
    assert compute_capsule_reward_from_rubric(review, "routed_to_compute_review") == pytest.approx(0.9)


# ---------- Realistic capsule profiles -------------------------------


def test_load_bearing_finding_earns_high_reward() -> None:
    """The HCC/HSA misattribution finding shape: strong on sci_use,
    provenance, actionability; moderate on novelty/clarity/downstream;
    lower on reproducibility (it's a citation critique, not a reproduction)."""
    review = _review(
        verdict="accepted",
        scientific_usefulness=1.0,
        provenance_strength=1.0,
        actionability=1.0,
        novelty=0.9,
        clarity=0.95,
        downstream_impact=0.85,
        reproducibility=0.7,
    )
    score = compute_capsule_reward_from_rubric(review, "accepted")
    # Weighted sum: 0.25*1.0 + 0.20*1.0 + 0.20*1.0 + 0.10*0.9 + 0.05*0.95 +
    #               0.10*0.85 + 0.10*0.7 = 0.9425
    assert score == pytest.approx(0.9425, abs=0.001)


def test_thin_finding_earns_low_reward() -> None:
    """A weak capsule (low confidence guess, no provenance, no actionable
    next step) should land well below the historical flat 100 pp.
    Quality-weighted scoring is the whole point: not every accept is equal."""
    review = _review(
        verdict="accepted",
        scientific_usefulness=0.4,
        provenance_strength=0.3,
        actionability=0.4,
        novelty=0.5,
        clarity=0.5,
        downstream_impact=0.4,
        reproducibility=0.3,
    )
    score = compute_capsule_reward_from_rubric(review, "accepted")
    # Should land between 35 and 45 pp.
    assert 0.35 <= score <= 0.45


def test_rubric_values_out_of_range_rejected_at_record_construction() -> None:
    """ProofCapsuleReviewRecord pydantic-validates each rubric dim to [0, 1]
    so the reward function never has to defend against out-of-range input."""
    import pydantic

    with pytest.raises(pydantic.ValidationError):
        _review(verdict="accepted", scientific_usefulness=2.5)
    with pytest.raises(pydantic.ValidationError):
        _review(verdict="accepted", provenance_strength=-0.5)


# ---------- Fallback for legacy reviews -----------------------------


def test_no_rubric_dims_falls_back_to_flat_verdict_lookup() -> None:
    """Legacy reviews without rubric dims should still award the historical
    flat per-verdict reward, so this change is backwards-compatible."""
    review = _review(verdict="accepted")  # all dims are None
    score = compute_capsule_reward_from_rubric(review, "accepted")
    assert score == PROOF_CAPSULE_VERDICT_REWARD_SCORES["accepted"]
    assert score == 1.0


def test_partial_rubric_renormalizes_over_populated_weights() -> None:
    """When some dims are populated and others aren't, the weight base
    should be the populated weights' sum, not 1.0. This means a review
    with only sci_use=1.0 and provenance=1.0 (and no others) scores 1.0,
    not 0.45."""
    review = _review(
        verdict="accepted",
        scientific_usefulness=1.0,
        provenance_strength=1.0,
        # actionability, reproducibility, novelty, clarity, downstream_impact = None
    )
    score = compute_capsule_reward_from_rubric(review, "accepted")
    # Populated weights: 0.25 + 0.20 = 0.45. Weighted sum: 0.45. Renormalized:
    # 0.45 / 0.45 = 1.0. × accepted multiplier 1.0 = 1.0.
    assert score == pytest.approx(1.0)


# ---------- Integration with reward_event_from_proof_capsule_review ---


def test_reward_event_uses_weighted_rubric_score() -> None:
    """The full reward_event_from_proof_capsule_review path must call
    through to compute_capsule_reward_from_rubric, not the flat lookup."""
    review = _review(
        verdict="accepted",
        scientific_usefulness=0.6, provenance_strength=0.6,
        reproducibility=0.6, actionability=0.6, novelty=0.6,
        clarity=0.6, downstream_impact=0.6,
    )
    event = reward_event_from_proof_capsule_review(
        review,
        candidate_id="twog-candidate-test",
        work_packet_id=None,
        capsule_type="citation_repair",
        contributor_handle="@test",
        contributor_kind="human",
    )
    # Weighted sum across all dims at 0.6 = 0.6.
    assert event.score == pytest.approx(0.6)
    assert event.dimension_scores["overall"] == pytest.approx(0.6)


def test_reward_event_rejected_capsule_score_is_zero() -> None:
    """A rejected capsule produces a reward event with score=0, even if
    the rubric was completed."""
    review = _review(
        verdict="rejected",
        scientific_usefulness=0.9, provenance_strength=0.9,
        reproducibility=0.9, actionability=0.9, novelty=0.9,
        clarity=0.9, downstream_impact=0.9,
    )
    event = reward_event_from_proof_capsule_review(
        review,
        candidate_id="twog-candidate-test",
        work_packet_id=None,
        capsule_type="citation_repair",
        contributor_handle="@test",
        contributor_kind="human",
    )
    assert event.score == 0.0
