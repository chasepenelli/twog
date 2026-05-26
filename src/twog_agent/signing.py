"""Optional ed25519 signing for ProofCapsule submissions.

If ``TWOG_AGENT_PRIVKEY`` is set, the CLI signs the capsule's
``content_hash`` and writes the signature into the capsule's ``signature``
field. Verification is out-of-band; the public Proof Network does not
enforce signatures yet but stores them so reviewers can verify authorship.

Keys are expected as base64-encoded 32-byte ed25519 seeds (the same format
``openssl rand -base64 32`` produces, or what ``twog-agent contributor
keygen`` will emit in a future iteration).
"""

from __future__ import annotations

import base64
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Signature:
    algorithm: str
    public_key_b64: str
    signature_b64: str

    def as_packet_string(self) -> str:
        """Serialize for the capsule's ``signature`` text field."""

        return f"{self.algorithm}:{self.public_key_b64}:{self.signature_b64}"


def sign_content_hash(content_hash: str) -> Signature | None:
    """Sign ``content_hash`` if a private key is configured.

    Returns ``None`` when no key is set so the caller can leave the
    capsule's ``signature`` field unset.
    """

    raw_key = os.environ.get("TWOG_AGENT_PRIVKEY")
    if not raw_key:
        return None
    try:
        # Lazy import — cryptography is heavyweight; only pay if a key is set.
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    except ImportError as exc:  # noqa: PERF203
        raise RuntimeError(
            "TWOG_AGENT_PRIVKEY is set but `cryptography` is not installed. "
            "Install it with `uv add cryptography` (or remove the env var to skip signing)."
        ) from exc

    seed = base64.b64decode(raw_key.strip())
    if len(seed) != 32:
        raise RuntimeError(
            f"TWOG_AGENT_PRIVKEY must decode to a 32-byte ed25519 seed (got {len(seed)} bytes)."
        )
    key = Ed25519PrivateKey.from_private_bytes(seed)
    signature_bytes = key.sign(content_hash.encode("utf-8"))
    pub_bytes = key.public_key().public_bytes_raw()
    return Signature(
        algorithm="ed25519",
        public_key_b64=base64.b64encode(pub_bytes).decode("ascii"),
        signature_b64=base64.b64encode(signature_bytes).decode("ascii"),
    )
