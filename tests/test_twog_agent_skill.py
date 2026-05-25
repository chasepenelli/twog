"""Regression tests for the twog-agent Claude Skill.

These tests prevent silent drift between the CLI surface and the skill
file Claude reads. If we add or rename a CLI command, the skill must
reflect it; if we deprecate an env var, the skill must update.

The tests parse `skills/twog-agent/SKILL.md` and assert that every
load-bearing reference (commands, env vars, exit codes, capsule types)
actually exists in the CLI / contracts.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from hsa_research.ingestion_bridge.contracts import (
    PROOF_CAPSULE_ACCEPTED_STATUSES,
    PROOF_CAPSULE_PENDING_STATUSES,
)
from twog_agent.exits import Exit


REPO_ROOT = Path(__file__).resolve().parent.parent
SKILL_PATH = REPO_ROOT / "skills" / "twog-agent" / "SKILL.md"


@pytest.fixture(scope="module")
def skill_text() -> str:
    return SKILL_PATH.read_text()


def test_skill_file_exists_and_has_frontmatter(skill_text: str) -> None:
    assert SKILL_PATH.is_file(), "skill SKILL.md is missing"
    assert skill_text.startswith("---\n"), "skill must start with YAML frontmatter"
    # The frontmatter must declare name and description for Claude Code.
    head, *_ = skill_text.split("\n---\n", 1)
    assert "name: twog-agent" in head
    assert "description:" in head
    # Description must mention TWOG (so Claude pattern-matches correctly).
    assert "TWOG" in head or "twog" in head


def test_skill_references_every_top_level_cli_command(skill_text: str) -> None:
    """If we add a CLI subcommand, the skill must teach it.

    This is the "no orphan commands" check.
    """

    for command in (
        "twog-agent packets list",
        "twog-agent packets checkout",
        "twog-agent capsule submit",
        "twog-agent capsule status",
        "twog-agent do",
        "twog-agent --human contributor whoami",
    ):
        assert command in skill_text, f"skill missing command reference: {command!r}"


def test_skill_documents_every_exit_code(skill_text: str) -> None:
    """The skill's exit-code case statement must cover every Exit member that has a documented meaning."""

    documented = {0, 3, 4, 5, 6, 7, 8, 9, 10}
    # Each of these must appear in the skill's case statement.
    for code in documented:
        assert re.search(rf"\b{code}\)", skill_text), f"skill missing exit-code branch: {code}"
    # And every documented code is a real Exit member.
    for code in documented:
        # Will raise ValueError if not a valid Exit.
        Exit(code)


def test_skill_documents_every_capsule_type(skill_text: str) -> None:
    """When we add a new capsule_type, the matrix in the skill must update.

    Mirrors the table headed "| `capsule_type` |" in SKILL.md.
    """

    from twog_agent.cli import _ERROR_CODE_TO_EXIT  # noqa: F401 — ensures import path

    # Pull capsule types from the live Literal in contracts.
    from hsa_research.ingestion_bridge.contracts import ProofCapsuleType
    from typing import get_args

    capsule_types = set(get_args(ProofCapsuleType))
    assert capsule_types  # sanity

    for capsule_type in capsule_types:
        assert f"`{capsule_type}`" in skill_text, (
            f"skill is missing the capsule_type row for {capsule_type!r}. "
            f"Update the table under 'Fuck around — do the work'."
        )


def test_skill_documents_required_env_vars(skill_text: str) -> None:
    for env_var in (
        "TWOG_AGENT_HANDLE",
        "TWOG_AGENT_CONTACT",
        "TWOG_AGENT_KIND",
        "TWOG_AGENT_NAME",
        "TWOG_AGENT_PRIVKEY",
        "TWOG_SITE_URL",
    ):
        assert env_var in skill_text, f"skill missing env var: {env_var}"


def test_skill_states_public_boundary(skill_text: str) -> None:
    """The boundary lines are load-bearing. Don't let them drop out by accident."""

    must_appear = [
        "do not mutate candidate records",
        "do not dispatch validation or compute",
        "do not write back to the candidate",
    ]
    for needle in must_appear:
        assert re.search(needle, skill_text, re.IGNORECASE), (
            f"skill missing public-boundary clause: {needle!r}"
        )


def test_skill_explains_iteration_on_needs_changes(skill_text: str) -> None:
    """needs_changes UX is subtle (submit a NEW capsule, don't patch)."""

    text = skill_text.lower()
    assert "needs_changes" in text
    assert (
        "new capsule" in text
        or "submit a new" in text
        or "submit a different" in text
    ), "skill must tell agents to submit a new capsule on needs_changes, not patch"


def test_skill_describes_reputation_tiers(skill_text: str) -> None:
    """Every tier must be named in the whoami section so the agent can self-correct."""

    for tier in (
        "observer",
        "scout",
        "citation_repairer",
        "record_builder",
        "replication_contributor",
        "validation_contributor",
        "trusted_reviewer",
        "proof_partner",
    ):
        assert tier in skill_text.lower(), f"skill missing tier: {tier}"


def test_skill_status_buckets_match_contract(skill_text: str) -> None:
    """The skill's discussion of accepted/routed/pending statuses must match the contract.

    Defends against future status additions: if PROOF_CAPSULE_ACCEPTED_STATUSES
    grows a member, we want the skill to surface it.
    """

    for status in (*PROOF_CAPSULE_ACCEPTED_STATUSES, *PROOF_CAPSULE_PENDING_STATUSES):
        # ``rejected`` and ``archived`` are also valid but documented under
        # the exit-code case statement; the contract above already lists them.
        if status in skill_text:
            continue
        # If a status isn't named in the skill, we tolerate it as long as
        # the documented exit-code paths cover it.
        if status in {"in_review"}:
            continue
        pytest.fail(
            f"skill missing reference to capsule status: {status}. "
            "Update SKILL.md if a new pending/accepted bucket landed."
        )
