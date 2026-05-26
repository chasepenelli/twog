"""Tests for the local credentials store (twog-agent login).

The credentials file is the single source of truth for "who am I right
now" across every CLI subcommand. These tests lock the round-trip
shape, the keypair generation, file perms, and the env-merge precedence
that lets users still override a stored handle for a one-off call.
"""

from __future__ import annotations

import base64
import json
import os
import stat
from pathlib import Path

import pytest

from twog_agent import credentials as credentials_module
from twog_agent.credentials import (
    Credentials,
    SCHEMA_VERSION,
    delete_credentials,
    generate_ed25519_keypair,
    load_credentials,
    merged_environment,
    permissions_ok,
    public_key_from_private,
    public_view,
    save_credentials,
)


@pytest.fixture
def cred_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect the credentials path to a tmp file for the test."""

    path = tmp_path / "creds" / "credentials.json"
    monkeypatch.setenv("TWOG_AGENT_CREDENTIALS_FILE", str(path))
    return path


def _sample_creds(**overrides) -> Credentials:
    base = dict(
        handle="@me",
        contact="me@example.com",
        kind="agent",
    )
    base.update(overrides)
    return Credentials(**base)


# ---------- keypair helpers --------------------------------------------


def test_generate_ed25519_keypair_returns_32_byte_seed_and_pubkey() -> None:
    priv_b64, pub_b64 = generate_ed25519_keypair()
    assert len(base64.b64decode(priv_b64)) == 32
    assert len(base64.b64decode(pub_b64)) == 32
    # Pubkey derives deterministically from the private key.
    assert public_key_from_private(priv_b64) == pub_b64


def test_generate_keypairs_are_unique() -> None:
    a = generate_ed25519_keypair()
    b = generate_ed25519_keypair()
    assert a != b


# ---------- save / load round-trip -------------------------------------


def test_save_and_load_round_trip(cred_path: Path) -> None:
    creds = _sample_creds(
        name="Test Display",
        ed25519_private_key_b64="dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdA==",
        ed25519_public_key_b64="cHViY2tleXB1YmtleXB1YmtleXB1YmtleQ==",
    )
    written = save_credentials(creds)
    assert written == cred_path

    loaded = load_credentials()
    assert loaded is not None
    assert loaded.handle == "@me"
    assert loaded.contact == "me@example.com"
    assert loaded.name == "Test Display"
    assert loaded.ed25519_private_key_b64 == creds.ed25519_private_key_b64
    assert loaded.ed25519_public_key_b64 == creds.ed25519_public_key_b64
    assert loaded.schema_version == SCHEMA_VERSION


def test_load_returns_none_when_missing(cred_path: Path) -> None:
    assert load_credentials() is None


def test_load_raises_on_malformed_json(cred_path: Path) -> None:
    cred_path.parent.mkdir(parents=True, exist_ok=True)
    cred_path.write_text("{not json")
    with pytest.raises(RuntimeError, match="not valid JSON"):
        load_credentials()


def test_save_sets_owner_only_permissions(cred_path: Path) -> None:
    save_credentials(_sample_creds())
    mode = cred_path.stat().st_mode
    assert (mode & stat.S_IRWXG) == 0, "group bits leaked on credentials file"
    assert (mode & stat.S_IRWXO) == 0, "other bits leaked on credentials file"
    assert permissions_ok()


def test_save_sets_owner_only_directory_permissions(cred_path: Path) -> None:
    save_credentials(_sample_creds())
    parent_mode = cred_path.parent.stat().st_mode
    assert (parent_mode & stat.S_IRWXG) == 0, "group bits on credentials dir"
    assert (parent_mode & stat.S_IRWXO) == 0, "other bits on credentials dir"


def test_delete_credentials_removes_the_file(cred_path: Path) -> None:
    save_credentials(_sample_creds())
    assert cred_path.exists()
    assert delete_credentials() is True
    assert not cred_path.exists()
    # Second delete is a no-op, returns False.
    assert delete_credentials() is False


# ---------- public_view masks the private key --------------------------


def test_public_view_omits_private_key() -> None:
    creds = _sample_creds(
        ed25519_private_key_b64="cHJpdmF0ZS1zZWVk",
        ed25519_public_key_b64="cHViLWtleQ==",
    )
    view = public_view(creds)
    assert "ed25519_private_key_b64" not in view
    assert "ed25519_private_key" not in view
    assert view["has_ed25519_private_key"] is True
    assert view["ed25519_public_key_b64"] == "cHViLWtleQ=="
    assert view["contact_set"] is True


def test_public_view_when_no_private_key() -> None:
    view = public_view(_sample_creds())
    assert view["has_ed25519_private_key"] is False
    assert view["ed25519_public_key_b64"] is None


# ---------- merged_environment: env wins, creds backfill --------------


def test_merged_environment_uses_credentials_when_env_unset() -> None:
    creds = _sample_creds(
        name="Display",
        ed25519_private_key_b64="cHJpdg==",
    )
    env = merged_environment(creds, base_env={})
    assert env["TWOG_AGENT_HANDLE"] == "@me"
    assert env["TWOG_AGENT_CONTACT"] == "me@example.com"
    assert env["TWOG_AGENT_KIND"] == "agent"
    assert env["TWOG_AGENT_NAME"] == "Display"
    assert env["TWOG_AGENT_PRIVKEY"] == "cHJpdg=="


def test_merged_environment_explicit_env_wins() -> None:
    """One-off override: env var beats stored credential."""

    creds = _sample_creds(name="Stored Name")
    env = merged_environment(
        creds, base_env={"TWOG_AGENT_HANDLE": "@override", "TWOG_AGENT_NAME": "Override Name"}
    )
    assert env["TWOG_AGENT_HANDLE"] == "@override"
    assert env["TWOG_AGENT_NAME"] == "Override Name"
    # Untouched fields still backfill.
    assert env["TWOG_AGENT_CONTACT"] == "me@example.com"


def test_merged_environment_with_no_credentials_is_identity() -> None:
    base = {"FOO": "bar"}
    env = merged_environment(None, base_env=base)
    assert env == base


# ---------- credentials_dir / credentials_path env overrides ----------


def test_credentials_dir_honours_env_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TWOG_AGENT_CREDENTIALS_DIR", str(tmp_path / "elsewhere"))
    assert credentials_module.credentials_dir() == tmp_path / "elsewhere"


def test_credentials_path_honours_env_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TWOG_AGENT_CREDENTIALS_FILE", str(tmp_path / "custom.json"))
    assert credentials_module.credentials_path() == tmp_path / "custom.json"


# ---------- The contributor identity sink reads credentials -----------


def test_cli_contributor_from_env_picks_up_credentials(
    cred_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`_contributor_from_env()` must fall back to credentials when env unset."""

    # Strip env so credentials are the only source.
    for var in (
        "TWOG_AGENT_HANDLE",
        "TWOG_AGENT_CONTACT",
        "TWOG_AGENT_KIND",
        "TWOG_AGENT_NAME",
    ):
        monkeypatch.delenv(var, raising=False)
    save_credentials(_sample_creds(name="From Creds"))

    # Late import so the patched env is seen.
    from twog_agent.cli import _contributor_from_env

    contributor = _contributor_from_env()
    assert contributor["handle"] == "@me"
    assert contributor["contact"] == "me@example.com"
    assert contributor["name"] == "From Creds"


def test_cli_contributor_env_overrides_credentials(
    cred_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    save_credentials(_sample_creds(name="From Creds"))
    monkeypatch.setenv("TWOG_AGENT_HANDLE", "@override")
    monkeypatch.setenv("TWOG_AGENT_CONTACT", "override@example.com")

    from twog_agent.cli import _contributor_from_env

    contributor = _contributor_from_env()
    assert contributor["handle"] == "@override"
    assert contributor["contact"] == "override@example.com"
    # Unrelated stored fields still backfill.
    assert contributor["name"] == "From Creds"
