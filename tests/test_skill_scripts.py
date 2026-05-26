"""Tests for the per-skill executable scripts.

These scripts (wrap_as_capsule.py, validate_capsule.py) live under
``skills/twog-agent/scripts/`` so they ship with the skill bundle and
work when an agent invokes them from within Claude Code. The tests
treat them as ordinary Python modules and pin their pure behavior:
- wrap_as_capsule.build_capsule() produces a valid capsule from CLI args
- validate_capsule.validate_capsule() catches every server-side gate
- The example_capsule.json in assets/ passes validation
- A round-trip of build → validate produces no errors
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "skills" / "twog-agent" / "scripts"
ASSETS_DIR = REPO_ROOT / "skills" / "twog-agent" / "assets"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import validate_capsule as vc  # noqa: E402
import wrap_as_capsule as wc  # noqa: E402


# ---------- validate_capsule ------------------------------------------


def _minimal_valid() -> dict:
    return {
        "candidate_id": "twog-candidate-x",
        "capsule_type": "citation_repair",
        "title": "Citation repair on candidate X",
        "analysis_summary": (
            "Walked the cited reference back to its primary source and confirmed "
            "the original paper supports the sentence it is attached to in the candidate "
            "rationale section."
        ),
        "contributor": {
            "kind": "human",
            "handle": "@test-handle",
            "contact": "test@example.com",
        },
    }


def test_validator_accepts_minimal_capsule() -> None:
    assert vc.validate_capsule(_minimal_valid()) == []


def test_validator_flags_short_title() -> None:
    body = _minimal_valid()
    body["title"] = "Hi"
    problems = vc.validate_capsule(body)
    assert any("title" in p for p in problems)


def test_validator_flags_thin_analysis() -> None:
    body = _minimal_valid()
    body["analysis_summary"] = "Way too short."
    problems = vc.validate_capsule(body)
    assert problems  # short string fails the hard floor


def test_validator_flags_word_count_below_min() -> None:
    body = _minimal_valid()
    # 80+ chars but fewer than 12 words
    body["analysis_summary"] = "a" * 90
    problems = vc.validate_capsule(body)
    assert any("12 words" in p or "thin" in p for p in problems)


def test_validator_flags_unknown_capsule_type() -> None:
    body = _minimal_valid()
    body["capsule_type"] = "not_a_real_type"
    problems = vc.validate_capsule(body)
    assert any("capsule_type" in p for p in problems)


def test_validator_flags_missing_contributor_fields() -> None:
    body = _minimal_valid()
    body["contributor"] = {"kind": "human"}  # missing handle + contact
    problems = vc.validate_capsule(body)
    assert any("handle" in p for p in problems)
    assert any("contact" in p for p in problems)


def test_validator_flags_handle_without_at_sign() -> None:
    body = _minimal_valid()
    body["contributor"]["handle"] = "no-at-prefix"
    problems = vc.validate_capsule(body)
    assert any("must start with @" in p for p in problems)


def test_validator_flags_bad_email() -> None:
    body = _minimal_valid()
    body["contributor"]["contact"] = "not-an-email"
    problems = vc.validate_capsule(body)
    assert any("contact" in p and "email" in p for p in problems)


def test_validator_flags_artifact_missing_content_hash() -> None:
    body = _minimal_valid()
    body["artifact_manifest"] = [{"label": "missing-hash"}]
    problems = vc.validate_capsule(body)
    assert any("content_hash" in p for p in problems)


def test_validator_flags_bad_review_route() -> None:
    body = _minimal_valid()
    body["requested_review_route"] = "self_review"
    problems = vc.validate_capsule(body)
    assert any("requested_review_route" in p for p in problems)


def test_validator_flags_repetitive_analysis() -> None:
    body = _minimal_valid()
    body["analysis_summary"] = (
        "candidate candidate candidate candidate candidate "
        "candidate candidate candidate candidate candidate "
        "candidate candidate"
    )
    problems = vc.validate_capsule(body)
    assert any("repeat" in p.lower() for p in problems)


# ---------- wrap_as_capsule -------------------------------------------


def _build_args(**overrides) -> SimpleNamespace:
    """Construct an argparse.Namespace-like object with all fields wrap expects."""
    defaults = dict(
        packet="00000000-0000-0000-0000-000000000001",
        candidate="twog-candidate-y",
        type="claim_critique",
        title="A claim critique with enough text",
        analysis=(
            "Pressure-tested the strongest claim against the cited evidence and "
            "adjacent literature; the effect size is plausible but the CI is wide."
        ),
        findings=None,
        limitations=None,
        method_refs=None,
        output_refs=None,
        snapshot_hash=None,
        bundle_hash=None,
        notebook=None,
        artifact=None,
        review_route=None,
        handle="@wrap-test",
        contact="wrap@example.com",
        kind="human",
        name=None,
        affiliation=None,
        out=None,
        validate=False,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_wrap_builds_minimal_capsule() -> None:
    args = _build_args()
    capsule = wc.build_capsule(args)
    assert capsule["candidate_id"] == "twog-candidate-y"
    assert capsule["capsule_type"] == "claim_critique"
    assert capsule["work_packet_id"] == "00000000-0000-0000-0000-000000000001"
    assert capsule["contributor"]["handle"] == "@wrap-test"


def test_wrap_reads_at_file_references(tmp_path: Path) -> None:
    analysis_file = tmp_path / "analysis.md"
    analysis_file.write_text(
        "Did the work and recorded each step. Pressure-tested the claim from "
        "multiple angles and reached a stable conclusion.",
        encoding="utf-8",
    )
    args = _build_args(analysis=f"@{analysis_file}")
    capsule = wc.build_capsule(args)
    assert "Pressure-tested" in capsule["analysis_summary"]


def test_wrap_parses_method_refs_csv() -> None:
    args = _build_args(method_refs="literature-review v1, doi.org, europepmc")
    capsule = wc.build_capsule(args)
    assert capsule["method_refs"] == ["literature-review v1", "doi.org", "europepmc"]


def test_wrap_parses_artifact_flags() -> None:
    args = _build_args(artifact=[
        "primary_pdf|https://example.org/p.pdf|sha256:aaa",
        "secondary_pdf||sha256:bbb",  # no URL
    ])
    capsule = wc.build_capsule(args)
    assert len(capsule["artifact_manifest"]) == 2
    assert capsule["artifact_manifest"][0]["url"] == "https://example.org/p.pdf"
    assert capsule["artifact_manifest"][0]["content_hash"] == "sha256:aaa"
    assert capsule["artifact_manifest"][1]["url"] is None
    assert capsule["artifact_manifest"][1]["content_hash"] == "sha256:bbb"


def test_wrap_rejects_bad_type() -> None:
    args = _build_args(type="not_a_real_type")
    with pytest.raises(SystemExit):
        wc.build_capsule(args)


def test_wrap_rejects_short_title() -> None:
    args = _build_args(title="abc")
    # short title goes through wrap but fails validate
    # (wrap intentionally does NOT pre-validate so callers can chain)
    capsule = wc.build_capsule(args)
    problems = vc.validate_capsule(capsule)
    assert any("title" in p for p in problems)


def test_wrap_then_validate_round_trip() -> None:
    """A capsule built from sane inputs must pass validation."""
    args = _build_args(
        method_refs="literature-review v1, doi.org",
        findings="The replacement DOI verifies as a primary source.",
        limitations="Did not retrieve the full PDF.",
    )
    capsule = wc.build_capsule(args)
    assert vc.validate_capsule(capsule) == []


def test_wrap_identity_precedence_cli_beats_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TWOG_AGENT_HANDLE", "@env-handle")
    monkeypatch.setenv("TWOG_AGENT_CONTACT", "env@example.com")
    args = _build_args(handle="@cli-handle", contact="cli@example.com")
    capsule = wc.build_capsule(args)
    assert capsule["contributor"]["handle"] == "@cli-handle"
    assert capsule["contributor"]["contact"] == "cli@example.com"


def test_wrap_falls_back_to_env_then_credentials_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    creds = tmp_path / "credentials.json"
    creds.write_text(
        json.dumps({"handle": "@from-file", "contact": "file@example.com", "kind": "agent"}),
        encoding="utf-8",
    )
    monkeypatch.delenv("TWOG_AGENT_HANDLE", raising=False)
    monkeypatch.delenv("TWOG_AGENT_CONTACT", raising=False)
    monkeypatch.delenv("TWOG_AGENT_KIND", raising=False)
    monkeypatch.setenv("TWOG_AGENT_CREDENTIALS_FILE", str(creds))
    args = _build_args(handle=None, contact=None, kind=None)
    capsule = wc.build_capsule(args)
    assert capsule["contributor"]["handle"] == "@from-file"
    assert capsule["contributor"]["contact"] == "file@example.com"
    assert capsule["contributor"]["kind"] == "agent"


# ---------- assets/example_capsule.json --------------------------------


def test_example_capsule_in_assets_passes_validation() -> None:
    """The example shipped under skills/twog-agent/assets/ must pass."""
    example = json.loads((ASSETS_DIR / "example_capsule.json").read_text())
    assert vc.validate_capsule(example) == []


@pytest.mark.parametrize("soul", [
    "twog-citation-repairer",
    "twog-claim-critic",
    "twog-evidence-finder",
    "twog-validation-proposer",
])
def test_every_soul_example_capsule_passes_validation(soul: str) -> None:
    """Each specialty soul ships its own example capsule. They must all
    pass the same gates as a real submission, so a contributor copying
    the example as a starting point doesn't ship something the server
    rejects."""
    path = REPO_ROOT / "skills" / soul / "assets" / "example_capsule.json"
    assert path.exists(), f"missing example for {soul}"
    example = json.loads(path.read_text())
    problems = vc.validate_capsule(example)
    assert problems == [], f"{soul} example fails validation: {problems}"


@pytest.mark.parametrize("soul", [
    "twog-agent",
    "twog-citation-repairer",
    "twog-claim-critic",
    "twog-evidence-finder",
    "twog-validation-proposer",
])
def test_every_soul_has_required_subdirs(soul: str) -> None:
    """K-Dense-style layout: every soul ships SKILL.md + references/ + assets/.
    scripts/ is optional but the directory exists for future use."""
    root = REPO_ROOT / "skills" / soul
    assert (root / "SKILL.md").is_file(), f"{soul}/SKILL.md missing"
    assert (root / "references").is_dir(), f"{soul}/references/ missing"
    assert (root / "assets").is_dir(), f"{soul}/assets/ missing"
    assert (root / "scripts").is_dir(), f"{soul}/scripts/ missing"


def test_twog_agent_skill_ships_shared_references() -> None:
    """The generic twog-agent skill is the source of truth for shared
    references that specialty souls cite by relative path. If any of
    these go missing the cross-skill composition is broken."""
    refs = REPO_ROOT / "skills" / "twog-agent" / "references"
    for name in ("capsule_schema_v1.md", "rubric_dimensions.md", "exit_codes.md", "specialty_map.md"):
        assert (refs / name).is_file(), f"missing shared reference: {name}"


def test_twog_agent_skill_ships_executable_scripts() -> None:
    """The generic twog-agent skill ships the helper scripts every soul
    references. Specialty souls should not duplicate these."""
    scripts = REPO_ROOT / "skills" / "twog-agent" / "scripts"
    for name in ("wrap_as_capsule.py", "validate_capsule.py"):
        assert (scripts / name).is_file(), f"missing helper script: {name}"
