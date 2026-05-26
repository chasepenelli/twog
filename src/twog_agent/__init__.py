"""twog-agent — autonomous client for the TWOG Proof Network.

Thin HTTP wrapper over the public Proof Network API. Designed for
unattended use: every command is env-driven, JSON-by-default on stdout,
and exits with a deterministic code so the wrapping shell can branch on
the verdict without parsing prose.

See docs/AGENT_PROOF_NETWORK_GUIDE.md for the protocol this client speaks.
"""

__version__ = "0.2.0"
