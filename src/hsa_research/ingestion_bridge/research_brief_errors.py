"""Error classification helpers for research brief records."""

from __future__ import annotations

from collections.abc import Sequence


_HARD_RESEARCH_BRIEF_ERROR_TERMS = {
    "chunk search failed",
    "claim search failed",
    "lookup failed",
    "openrouter",
    "json",
    "parse",
    "could not validate",
    "exception",
    "traceback",
    "unknown citation",
    "invalid citation",
}


def is_hard_research_brief_error(error: str) -> bool:
    """Return true for tool, parser, contract, and citation integrity failures."""

    lowered = error.lower()
    return any(term in lowered for term in _HARD_RESEARCH_BRIEF_ERROR_TERMS)


def split_research_brief_errors(errors: Sequence[str] | str) -> tuple[list[str], list[str]]:
    """Split legacy/model error strings into hard errors and evidence limitations."""

    hard: list[str] = []
    limitations: list[str] = []
    iterable = [errors] if isinstance(errors, str) else errors
    for error in iterable:
        error = str(error).strip()
        if not error:
            continue
        if is_hard_research_brief_error(error):
            hard.append(error)
        else:
            limitations.append(error)
    return hard, limitations
