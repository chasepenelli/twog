"""Unit tests for the auto-dive worker.

The worker's I/O (DB, HTTP, signing) is at the boundaries; the
interesting behavior is in the pure diagnostic functions. These tests
pin the deterministic rules so future changes to candidate snapshot
shape stay backwards-compatible.
"""

from __future__ import annotations

import base64

import pytest

from hsa_research.ingestion_bridge.auto_dive_worker import (
    AUTO_DIVE_HANDLE,
    AutoDiveCapsule,
    _persona_keypair,
    _short_candidate_id,
    _target_token_in,
    collect_proposed_capsules,
    diagnose_compute_alignment,
    diagnose_unresolved_citations,
)


# ---------- Unresolved citation diagnostic ---------------------------


def test_diagnose_unresolved_emits_one_capsule_per_unresolved_ref() -> None:
    payload = {
        "literature": [
            {"ref": "C11", "resolved": False, "supports": "Real signal in HSA"},
            {"ref": "C12", "resolved": False, "supports": "Adjacent evidence"},
            {"ref": "C13", "resolved": True, "supports": "Already resolved"},
        ],
    }
    capsules = diagnose_unresolved_citations(payload, candidate_short_id="abcd1234")
    assert len(capsules) == 2
    refs_found = {c.diagnostic_tag for c in capsules}
    assert refs_found == {"unresolved_citation:C11", "unresolved_citation:C12"}


def test_diagnose_unresolved_includes_quoted_supporting_context() -> None:
    payload = {
        "literature": [
            {
                "ref": "C11",
                "resolved": False,
                "supports": "Canine direct evidence: PIK3CA/TP53 + rapamycin",
            },
        ],
    }
    capsules = diagnose_unresolved_citations(payload, candidate_short_id="x")
    assert len(capsules) == 1
    # The supporting context must be quoted so a follow-up contributor
    # has the search terms in hand.
    assert "Canine direct evidence" in capsules[0].analysis_summary


def test_diagnose_unresolved_skips_when_no_unresolved_refs() -> None:
    payload = {
        "literature": [
            {"ref": "C1", "resolved": True, "supports": "All good"},
        ],
    }
    assert diagnose_unresolved_citations(payload, candidate_short_id="x") == []


def test_diagnose_unresolved_handles_missing_literature() -> None:
    assert diagnose_unresolved_citations({}, candidate_short_id="x") == []
    assert diagnose_unresolved_citations({"literature": None}, candidate_short_id="x") == []
    assert diagnose_unresolved_citations({"literature": "not a list"}, candidate_short_id="x") == []


def test_diagnose_unresolved_capsule_passes_quality_thresholds() -> None:
    """Each emitted capsule must meet the server's analysis_summary
    quality gates (≥ 80 chars, ≥ 12 words) so the auto-dive
    submission isn't rejected at validation."""
    payload = {
        "literature": [
            {"ref": "C11", "resolved": False, "supports": "x"},  # very thin context
        ],
    }
    capsules = diagnose_unresolved_citations(payload, candidate_short_id="x")
    assert len(capsules) == 1
    body = capsules[0].analysis_summary
    assert len(body) >= 80
    assert len(body.split()) >= 12


# ---------- Compute alignment diagnostic -----------------------------


def test_diagnose_compute_alignment_flags_misaligned_jobs() -> None:
    """The TWOG-B41B9F failure mode: rationale says mTOR/PIK3CA;
    compute jobs target VEGFR2/KDR. Worker should flag this."""
    payload = {
        "computational_evidence": {
            "md_jobs": [
                {"target": "VEGFR2", "label": "MD smoke pazopanib vs VEGFR2"},
                {"target": "KDR", "label": "MD smoke 2"},
            ],
        },
        "biology": {
            "targets": [
                {"name": "MTOR", "uniprot": "P42345"},
                {"name": "PIK3CA"},
            ],
        },
        "rationale": {
            "mechanism": "Rapamycin inhibits mTORC1 in PIK3CA-mutant HSA",
        },
    }
    capsules = diagnose_compute_alignment(payload, candidate_short_id="abc")
    assert len(capsules) == 1
    assert capsules[0].capsule_type == "claim_critique"
    assert "VEGFR2" in capsules[0].analysis_summary


def test_diagnose_compute_alignment_collapses_many_misaligned_jobs_into_one_capsule() -> None:
    """Five misaligned jobs should produce one capsule, not five —
    the operator gets one item to triage, not a flood."""
    payload = {
        "computational_evidence": {
            "jobs": [{"target": f"WRONGTARGET_{i}"} for i in range(5)],
        },
        "biology": {"targets": [{"name": "MTOR"}]},
        "rationale": {"mechanism": "mTOR pathway"},
    }
    capsules = diagnose_compute_alignment(payload, candidate_short_id="x")
    assert len(capsules) == 1


def test_diagnose_compute_alignment_passes_when_targets_match() -> None:
    payload = {
        "computational_evidence": {
            "md_jobs": [{"target": "MTOR"}, {"target": "PIK3CA"}],
        },
        "biology": {"targets": [{"name": "MTOR"}, {"name": "PIK3CA"}]},
        "rationale": {"mechanism": "mTOR inhibition"},
    }
    assert diagnose_compute_alignment(payload, candidate_short_id="x") == []


def test_diagnose_compute_alignment_matches_via_mechanism_text() -> None:
    """A job target that appears in the rationale.mechanism text (not
    necessarily in biology.targets) should count as aligned."""
    payload = {
        "computational_evidence": {"jobs": [{"target": "MTOR"}]},
        "biology": {"targets": [{"name": "PIK3CA"}]},  # MTOR not in targets list
        "rationale": {"mechanism": "Rapamycin inhibits mTORC1 via MTOR FRB binding"},
    }
    # MTOR appears in the rationale mechanism text → should match
    assert diagnose_compute_alignment(payload, candidate_short_id="x") == []


def test_diagnose_compute_alignment_handles_compound_target_strings() -> None:
    """A job target like 'VEGFR2/KDR' should match if EITHER token is
    in the candidate's targets list."""
    payload = {
        "computational_evidence": {"jobs": [{"target": "VEGFR2/KDR"}]},
        "biology": {"targets": [{"name": "KDR"}]},
        "rationale": {"mechanism": "anti-angiogenic"},
    }
    assert diagnose_compute_alignment(payload, candidate_short_id="x") == []


def test_diagnose_compute_alignment_handles_missing_compute_block() -> None:
    assert diagnose_compute_alignment({}, candidate_short_id="x") == []
    assert diagnose_compute_alignment(
        {"computational_evidence": {}}, candidate_short_id="x"
    ) == []


# ---------- _target_token_in --------------------------------------------


def test_target_token_in_matches_substring_and_token_split() -> None:
    assert _target_token_in("vegfr2", "vegfr2") is True
    assert _target_token_in("vegfr2/kdr", "kdr") is True
    assert _target_token_in("vegfr-2", "vegfr") is True  # hyphen split
    assert _target_token_in("vegfr2", "mtor") is False
    assert _target_token_in("", "mtor") is False


# ---------- collect_proposed_capsules + cap ------------------------------


def test_collect_proposed_runs_all_diagnostics() -> None:
    payload = {
        "literature": [{"ref": "C1", "resolved": False, "supports": "missing"}],
        "computational_evidence": {"jobs": [{"target": "WRONG"}]},
        "biology": {"targets": [{"name": "MTOR"}]},
        "rationale": {"mechanism": "mTOR pathway"},
    }
    capsules = collect_proposed_capsules(payload, candidate_short_id="x")
    types = {c.capsule_type for c in capsules}
    assert "citation_repair" in types
    assert "claim_critique" in types
    assert len(capsules) == 2


def test_collect_proposed_respects_cap() -> None:
    payload = {
        "literature": [
            {"ref": f"C{i}", "resolved": False, "supports": "missing"} for i in range(20)
        ],
    }
    capsules = collect_proposed_capsules(payload, candidate_short_id="x", cap=3)
    assert len(capsules) == 3


def test_short_candidate_id_strips_twog_prefix() -> None:
    assert _short_candidate_id("twog-candidate-447eb8089965") == "447eb8089965"
    assert _short_candidate_id("twog-candidate-abc") == "abc"
    assert _short_candidate_id("some-other-id-12345") == "some-other-id-1234"


# ---------- Persona signing -----------------------------------------


def test_persona_keypair_deterministic_for_same_seed() -> None:
    """The @twog-auto-dive ed25519 key derives from TWOG_DEMO_MASTER_SEED
    + HKDF salted by handle. Same seed always yields same key, so
    re-running the auto-dive on the same content produces the same
    content_hash and the server's dedup kicks in (no double-submission)."""
    seed_b64 = base64.b64encode(b"\x42" * 32).decode("ascii")
    a_seed, a_pub = _persona_keypair(seed_b64, AUTO_DIVE_HANDLE)
    b_seed, b_pub = _persona_keypair(seed_b64, AUTO_DIVE_HANDLE)
    assert a_seed == b_seed
    assert a_pub == b_pub


def test_persona_keypair_differs_by_handle() -> None:
    seed_b64 = base64.b64encode(b"\x42" * 32).decode("ascii")
    a, _ = _persona_keypair(seed_b64, AUTO_DIVE_HANDLE)
    b, _ = _persona_keypair(seed_b64, "@some-other-handle")
    assert a != b


def test_persona_keypair_rejects_short_seed() -> None:
    with pytest.raises(ValueError):
        _persona_keypair(base64.b64encode(b"short").decode(), AUTO_DIVE_HANDLE)
