"""Deterministic exit codes for twog-agent.

A wrapping agent harness keys its branching logic off these. The intent is
that the **bash exit code alone** lets the agent decide what to do next
without re-parsing the JSON payload.

Reserve the range 0–9 for control-flow signals. Specific error categories
that the agent might handle differently get their own slot.
"""

from __future__ import annotations

import enum


class Exit(enum.IntEnum):
    """Exit codes the CLI returns. Stable contract for agent wrappers."""

    SUCCESS = 0
    """Command succeeded. For verdicts: ``accepted`` or ``routed_to_*``."""

    GENERIC_ERROR = 1
    """Unexpected error. Stack trace on stderr; not actionable by code."""

    INVALID_ARGS = 2
    """Bad CLI args (argparse default)."""

    STORAGE_NOT_CONFIGURED = 3
    """Server reports no Neon/Postgres backend. Agent should retry later."""

    NOT_FOUND = 4
    """Packet or capsule id not found at the resolved site."""

    INVALID_PACKET = 5
    """The submitted capsule failed validation. Inspect ``details``."""

    REJECTED = 6
    """Reviewer rejected the capsule. Terminal negative; do not retry."""

    NEEDS_CHANGES = 7
    """Reviewer requested changes. Submit a fresh capsule with edits."""

    NETWORK_ERROR = 8
    """HTTP-level failure (DNS, TLS, 5xx). Retryable."""

    WAIT_TIMEOUT = 9
    """``--wait`` reached its deadline before a terminal verdict."""

    RATE_LIMITED = 10
    """Server reports the handle exceeded its per-hour submission limit.

    Retry after the trailing-hour window has headroom (typically a few
    minutes). The agent should back off; spamming will keep the counter
    pinned. The 429 response carries ``details`` describing the current
    and limit values.
    """


# Mapping from server-side proof_capsule status → exit code for terminal states.
# Non-terminal statuses are not in this table; callers handle them as "keep waiting."
TERMINAL_STATUS_EXIT: dict[str, Exit] = {
    "accepted": Exit.SUCCESS,
    "routed_to_validation": Exit.SUCCESS,
    "routed_to_compute_review": Exit.SUCCESS,
    "rejected": Exit.REJECTED,
    "needs_changes": Exit.NEEDS_CHANGES,
    "archived": Exit.REJECTED,
}

NON_TERMINAL_STATUSES: frozenset[str] = frozenset({"submitted", "in_review"})
