"""Unit tests for the LLM-as-judge proof capsule grader.

The grader is intentionally split into:
- _parse_grade_response: pure JSON parser + validator
- _weighted_score: pure preview math
- _openrouter_completion: the I/O boundary (mocked in tests)
- grade_capsule: composes the above; covered via mocked _openrouter

We don't make real OpenRouter calls in tests. We assert:
- The parser accepts a well-formed JSON response from the LLM
- Code-fenced responses are handled
- Out-of-range scores are clamped, not crashed
- Missing dims raise (so we don't silently write zeros)
- Unknown verdicts default to needs_changes (safer than misclassifying as accepted)
- grade_capsule returns the expected LLMGradeResult shape
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from hsa_research.ingestion_bridge.llm_capsule_grader import (
    LLMGradeResult,
    _parse_grade_response,
    _weighted_score,
    grade_capsule,
)


# ---------- Parser ----------------------------------------------------


def _valid_grade_json() -> str:
    return json.dumps({
        "scientific_usefulness": 0.85,
        "provenance_strength": 0.9,
        "actionability": 0.8,
        "reproducibility": 0.6,
        "novelty": 0.7,
        "clarity": 0.85,
        "downstream_impact": 0.75,
        "verdict": "accepted",
        "rationale": "Strong primary-source-backed citation repair.",
    })


def test_parser_accepts_well_formed_grade() -> None:
    parsed = _parse_grade_response(_valid_grade_json())
    assert parsed["scientific_usefulness"] == 0.85
    assert parsed["verdict"] == "accepted"
    assert "primary-source" in parsed["rationale"]


def test_parser_strips_markdown_code_fences() -> None:
    """LLMs often wrap JSON in ```json ... ``` fences. Parser must handle it."""
    raw = "```json\n" + _valid_grade_json() + "\n```"
    parsed = _parse_grade_response(raw)
    assert parsed["scientific_usefulness"] == 0.85


def test_parser_handles_leading_trailing_text() -> None:
    """Some models prepend text before the JSON. Parser falls back to
    extracting between the first { and last }."""
    raw = "Here is my grade:\n" + _valid_grade_json() + "\n\nLet me know if you have questions."
    parsed = _parse_grade_response(raw)
    assert parsed["scientific_usefulness"] == 0.85


def test_parser_clamps_out_of_range_scores() -> None:
    """A model that returns 1.2 or -0.1 shouldn't blow up the math."""
    raw = json.dumps({
        "scientific_usefulness": 1.5,   # clamped to 1.0
        "provenance_strength": -0.2,    # clamped to 0.0
        "actionability": 0.8,
        "reproducibility": 0.6,
        "novelty": 0.7,
        "clarity": 0.85,
        "downstream_impact": 0.75,
        "verdict": "accepted",
        "rationale": "test",
    })
    parsed = _parse_grade_response(raw)
    assert parsed["scientific_usefulness"] == 1.0
    assert parsed["provenance_strength"] == 0.0


def test_parser_raises_on_missing_dimension() -> None:
    """A response missing a dim is malformed; better to raise than write zero."""
    incomplete = json.dumps({
        "scientific_usefulness": 0.85,
        "provenance_strength": 0.9,
        # actionability missing
        "reproducibility": 0.6,
        "novelty": 0.7,
        "clarity": 0.85,
        "downstream_impact": 0.75,
        "verdict": "accepted",
        "rationale": "test",
    })
    with pytest.raises(RuntimeError, match="actionability"):
        _parse_grade_response(incomplete)


def test_parser_defaults_unknown_verdict_to_needs_changes() -> None:
    """An LLM emitting 'great work!' as the verdict shouldn't auto-accept.
    Default to needs_changes (which awards 0 pp) so a malformed grade
    can't inflate the leaderboard."""
    raw = json.dumps({
        "scientific_usefulness": 0.8, "provenance_strength": 0.8,
        "actionability": 0.8, "reproducibility": 0.8, "novelty": 0.8,
        "clarity": 0.8, "downstream_impact": 0.8,
        "verdict": "great_work",
        "rationale": "ok",
    })
    parsed = _parse_grade_response(raw)
    assert parsed["verdict"] == "needs_changes"


def test_parser_defaults_missing_rationale() -> None:
    raw = json.dumps({
        "scientific_usefulness": 0.8, "provenance_strength": 0.8,
        "actionability": 0.8, "reproducibility": 0.8, "novelty": 0.8,
        "clarity": 0.8, "downstream_impact": 0.8,
        "verdict": "accepted",
        # rationale missing
    })
    parsed = _parse_grade_response(raw)
    assert parsed["rationale"]  # not empty
    assert "no rationale" in parsed["rationale"].lower()


# ---------- Weighted score preview -----------------------------------


def test_weighted_score_matches_python_reward_weights() -> None:
    """The preview score in the grade summary should match
    compute_capsule_reward_from_rubric. Same weights, same arithmetic."""
    result = LLMGradeResult(
        proof_capsule_id="x",
        scientific_usefulness=1.0,
        provenance_strength=1.0,
        actionability=1.0,
        reproducibility=1.0,
        novelty=1.0,
        clarity=1.0,
        downstream_impact=1.0,
        verdict="accepted",
        rationale="r",
        model_name="m",
        content_hash="sha256:x",
    )
    # All weights sum to 1.0 by construction
    assert _weighted_score(result) == pytest.approx(1.0)

    half = LLMGradeResult(
        proof_capsule_id="x",
        scientific_usefulness=0.5,
        provenance_strength=0.5,
        actionability=0.5,
        reproducibility=0.5,
        novelty=0.5,
        clarity=0.5,
        downstream_impact=0.5,
        verdict="accepted",
        rationale="r",
        model_name="m",
        content_hash="sha256:x",
    )
    assert _weighted_score(half) == pytest.approx(0.5)


# ---------- grade_capsule (mocked LLM) --------------------------------


def test_grade_capsule_returns_structured_result() -> None:
    """grade_capsule should mock cleanly without touching the network."""
    fake_completion = {
        "text": _valid_grade_json(),
        "metadata": {"provider": "openrouter", "model_name": "anthropic/claude-haiku-4-5-20251001"},
    }
    capsule_body = {
        "title": "Test capsule",
        "capsule_type": "citation_repair",
        "analysis_summary": "Some analysis.",
        "content_hash": "sha256:test-hash",
    }
    with patch(
        "hsa_research.ingestion_bridge.llm_capsule_grader._openrouter_completion",
        return_value=fake_completion,
    ):
        result = grade_capsule(
            proof_capsule_id="abc-123",
            capsule_body=capsule_body,
            candidate_excerpt={"candidate_id": "twog-c-1"},
        )
    assert result.proof_capsule_id == "abc-123"
    assert result.scientific_usefulness == 0.85
    assert result.verdict == "accepted"
    assert result.model_name == "anthropic/claude-haiku-4-5-20251001"
    assert result.content_hash == "sha256:test-hash"


def test_grade_capsule_propagates_parser_failures() -> None:
    """If the LLM returns garbage, the error should surface clearly,
    not silently produce a default grade."""
    fake_completion = {
        "text": "i refuse to grade this capsule",
        "metadata": {"provider": "openrouter", "model_name": "test"},
    }
    with patch(
        "hsa_research.ingestion_bridge.llm_capsule_grader._openrouter_completion",
        return_value=fake_completion,
    ):
        with pytest.raises((RuntimeError, ValueError, Exception)):
            grade_capsule(
                proof_capsule_id="abc",
                capsule_body={"title": "x", "content_hash": "sha256:x"},
            )


# ---------- Reward-sync filter (cross-module integration check) ------


def test_sync_reward_events_filters_llm_evaluator_reviews() -> None:
    """Slice 1 added the llm_evaluator filter to the sync; Slice 2 relies
    on it. This test pins the filter so a future refactor doesn't drop
    the safety check that prevents LLM grades from awarding proof points.
    """
    from hsa_research.ingestion_bridge import proof_capsules as pc

    captured_filter_terms: list[str] = []

    def fake_list_contexts(**kwargs):
        return [
            {
                "review_id": "00000000-0000-0000-0000-000000000001",
                "proof_capsule_id": "00000000-0000-0000-0000-000000000002",
                "reviewer_type": "llm_evaluator",  # should be skipped
                "reviewer_id": "llm:test",
                "verdict": "accepted",
                "confidence": 0.8,
                "scientific_usefulness": 0.9,
                "provenance_strength": 0.9,
                "reproducibility": 0.7,
                "actionability": 0.8,
                "novelty": 0.6,
                "clarity": 0.9,
                "downstream_impact": 0.7,
                "rationale": "llm test",
                "required_changes": None,
                "linked_agent_run_id": None,
                "review_payload": {},
                "created_at": None,
                "work_packet_id": None,
                "candidate_id": "twog-candidate-test",
                "capsule_type": "citation_repair",
                "contributor": {"handle": "@test", "kind": "human"},
            },
        ]

    class _FakeRepo:
        def list_reward_events(self, **kwargs):
            return []
        def create_reward_event(self, event):
            captured_filter_terms.append("CREATED")  # should NOT happen
            return event

    with patch.object(pc, "list_proof_capsule_review_contexts", side_effect=fake_list_contexts):
        result = pc.sync_reward_events_from_proof_capsule_reviews(
            _FakeRepo(), database_url="postgresql://fake/url",
        )
    assert result["scanned_review_count"] == 1
    assert result["eligible_review_count"] == 0
    assert result["created_count"] == 0
    assert captured_filter_terms == []  # no reward event was created
