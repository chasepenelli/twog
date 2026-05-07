"""Central OpenRouter model defaults for agent lanes."""

from __future__ import annotations

import os


DEFAULT_OPENROUTER_MODEL = "~anthropic/claude-sonnet-latest"
BIG_IDEA_OPENROUTER_MODEL = "~anthropic/claude-opus-latest"


def default_openrouter_model() -> str:
    """Return the default model for routine agent work."""

    return os.getenv("HSA_DEFAULT_OPENROUTER_MODEL", DEFAULT_OPENROUTER_MODEL)


def big_idea_openrouter_model() -> str:
    """Return the default model for big-idea thesis/program work."""

    return os.getenv("HSA_BIG_IDEA_OPENROUTER_MODEL", BIG_IDEA_OPENROUTER_MODEL)
