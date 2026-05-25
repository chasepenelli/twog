"""Local identity store for ``twog-agent``.

The promise: ``twog-agent login`` creates a stable identity once — handle,
contact, kind, name, and an ed25519 keypair — and writes it to
``~/.twog-agent/credentials.json`` with 0600 perms. Every subsequent
command picks it up automatically. No more typing your handle into a
prompt every time.

This is *local* identity. It doesn't verify against GitHub or any
external authority; that's a follow-on. What it does give you:

* A durable handle bound to a private key, so signed capsules are
  attributable to a contributor who actually has the secret.
* A single source of truth across ``install``, ``do``, ``submit``,
  ``whoami``, ``mcp``, etc.
* Easy revoke: ``twog-agent logout`` shreds the file.

GitHub OAuth (and the eventual JWT-binding-server-side) plugs into the
same place — the ``Credentials`` record gains a ``github_identity``
field and the server begins to honor it. Until then, this is the right
local primitive.
"""

from __future__ import annotations

import base64
import json
import os
import stat
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "1"


def credentials_dir() -> Path:
    """Default location for the credentials file (``~/.twog-agent``)."""

    override = os.environ.get("TWOG_AGENT_CREDENTIALS_DIR")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".twog-agent"


def credentials_path() -> Path:
    override = os.environ.get("TWOG_AGENT_CREDENTIALS_FILE")
    if override:
        return Path(override).expanduser()
    return credentials_dir() / "credentials.json"


@dataclass
class Credentials:
    handle: str
    contact: str
    kind: str = "agent"
    name: str | None = None
    affiliation: str | None = None
    agent_id: str | None = None
    website: str | None = None
    ed25519_private_key_b64: str | None = None
    """Base64-encoded 32-byte ed25519 seed. Optional but recommended."""

    ed25519_public_key_b64: str | None = None
    """Base64-encoded 32-byte ed25519 public key (derived)."""

    github_identity: dict[str, Any] | None = None
    """Reserved for the GitHub OAuth follow-on; ``None`` for v1."""

    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    schema_version: str = SCHEMA_VERSION

    def as_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


def generate_ed25519_keypair() -> tuple[str, str]:
    """Generate a fresh ed25519 keypair. Returns ``(private_b64, public_b64)``.

    Lazy-imports cryptography because the CLI's hot path doesn't need it.
    """

    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    seed = os.urandom(32)
    key = Ed25519PrivateKey.from_private_bytes(seed)
    pub = key.public_key().public_bytes_raw()
    return (
        base64.b64encode(seed).decode("ascii"),
        base64.b64encode(pub).decode("ascii"),
    )


def public_key_from_private(private_b64: str) -> str:
    """Derive the matching ed25519 public key from a private seed."""

    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    seed = base64.b64decode(private_b64)
    key = Ed25519PrivateKey.from_private_bytes(seed)
    pub = key.public_key().public_bytes_raw()
    return base64.b64encode(pub).decode("ascii")


def save_credentials(creds: Credentials, *, path: Path | None = None) -> Path:
    """Persist credentials to disk with restrictive perms."""

    target = path or credentials_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    # Restrict parent dir perms too: 0700 owner-only.
    try:
        os.chmod(target.parent, 0o700)
    except OSError:
        pass
    raw = json.dumps(creds.as_dict(), indent=2, sort_keys=True) + "\n"
    target.write_text(raw)
    try:
        os.chmod(target, 0o600)
    except OSError:
        pass
    return target


def load_credentials(*, path: Path | None = None) -> Credentials | None:
    """Read credentials from disk; ``None`` if missing."""

    target = path or credentials_path()
    if not target.exists():
        return None
    try:
        payload = json.loads(target.read_text())
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"credentials at {target} are not valid JSON: {exc}. "
            f"Run 'twog-agent logout' and 'twog-agent login' to regenerate."
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"credentials at {target} are not a JSON object")
    return Credentials(
        handle=str(payload.get("handle") or ""),
        contact=str(payload.get("contact") or ""),
        kind=str(payload.get("kind") or "agent"),
        name=payload.get("name"),
        affiliation=payload.get("affiliation"),
        agent_id=payload.get("agent_id"),
        website=payload.get("website"),
        ed25519_private_key_b64=payload.get("ed25519_private_key_b64"),
        ed25519_public_key_b64=payload.get("ed25519_public_key_b64"),
        github_identity=payload.get("github_identity"),
        created_at=str(payload.get("created_at") or datetime.now(timezone.utc).isoformat()),
        schema_version=str(payload.get("schema_version") or SCHEMA_VERSION),
    )


def delete_credentials(*, path: Path | None = None) -> bool:
    """Remove the credentials file. Returns True if anything was removed."""

    target = path or credentials_path()
    if not target.exists():
        return False
    target.unlink()
    return True


def permissions_ok(*, path: Path | None = None) -> bool:
    """Return True iff the credentials file has 0600 perms (group/other = 0)."""

    target = path or credentials_path()
    if not target.exists():
        return False
    mode = target.stat().st_mode
    # Anyone else readable/writable/executable is bad.
    forbidden = stat.S_IRWXG | stat.S_IRWXO
    return (mode & forbidden) == 0


def public_view(creds: Credentials) -> dict[str, Any]:
    """A view of credentials that's safe to print to stdout.

    The private key is never echoed; we surface its presence and the
    public key (which is fine to share) only.
    """

    return {
        "handle": creds.handle,
        "contact_set": bool(creds.contact),
        "kind": creds.kind,
        "name": creds.name,
        "affiliation": creds.affiliation,
        "website": creds.website,
        "ed25519_public_key_b64": creds.ed25519_public_key_b64,
        "has_ed25519_private_key": bool(creds.ed25519_private_key_b64),
        "github_identity": creds.github_identity,
        "created_at": creds.created_at,
        "schema_version": creds.schema_version,
    }


def merged_environment(creds: Credentials | None, base_env: dict[str, str] | None = None) -> dict[str, str]:
    """Return an env dict where credentials fill in any unset identity vars.

    Precedence: explicit env > credentials > unset. So a user can still
    override their stored handle via ``TWOG_AGENT_HANDLE=@other`` for a
    one-off call without re-running ``login``.
    """

    env: dict[str, str] = dict(base_env if base_env is not None else os.environ)
    if creds is None:
        return env
    pairs: list[tuple[str, str | None]] = [
        ("TWOG_AGENT_HANDLE", creds.handle or None),
        ("TWOG_AGENT_CONTACT", creds.contact or None),
        ("TWOG_AGENT_KIND", creds.kind or None),
        ("TWOG_AGENT_NAME", creds.name),
        ("TWOG_AGENT_AFFILIATION", creds.affiliation),
        ("TWOG_AGENT_ID", creds.agent_id),
        ("TWOG_AGENT_WEBSITE", creds.website),
        ("TWOG_AGENT_PRIVKEY", creds.ed25519_private_key_b64),
    ]
    for key, value in pairs:
        if value and not env.get(key):
            env[key] = value
    return env
