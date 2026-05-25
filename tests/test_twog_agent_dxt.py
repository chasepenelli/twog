"""Tests for the .dxt extension builder.

The DXT format is Claude Desktop's drag-and-drop install package. These
tests lock the manifest shape, the env-var wiring (so user_config flows
to the server process correctly), and the round-trip read/write of the
zip archive.
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from twog_agent.dxt import (
    DECLARED_TOOLS,
    DxtBuildOptions,
    MANIFEST_VERSION,
    USER_CONFIG_SCHEMA,
    build_manifest,
    validate_dxt,
    write_dxt,
)


# ---------- Manifest shape contracts -----------------------------------


def test_manifest_has_required_top_level_fields() -> None:
    manifest = build_manifest(DxtBuildOptions(output=Path("/tmp/never.dxt")))
    for key in (
        "dxt_version",
        "name",
        "display_name",
        "version",
        "description",
        "author",
        "server",
        "tools",
        "user_config",
    ):
        assert key in manifest, f"manifest missing {key!r}"
    assert manifest["dxt_version"] == MANIFEST_VERSION
    assert manifest["name"] == "twog"


def test_manifest_server_block_wires_to_twog_agent_mcp() -> None:
    manifest = build_manifest(DxtBuildOptions(output=Path("/tmp/x.dxt")))
    server = manifest["server"]
    assert server["type"] == "binary"
    assert server["mcp_config"]["command"] == "twog-agent"
    assert server["mcp_config"]["args"] == ["mcp"]


def test_manifest_user_config_passes_through_as_env_vars() -> None:
    """Every user_config field declared must appear in mcp_config.env."""

    manifest = build_manifest(DxtBuildOptions(output=Path("/tmp/x.dxt")))
    env = manifest["server"]["mcp_config"]["env"]
    for field_name in USER_CONFIG_SCHEMA.keys():
        assert field_name in env, f"user_config field {field_name} not wired into env"
        # The value should be a placeholder Claude Desktop will substitute.
        assert env[field_name] == "${user_config." + field_name + "}"


def test_user_config_has_handle_contact_required() -> None:
    """Identity prompts (handle + contact) are non-negotiable."""

    assert USER_CONFIG_SCHEMA["TWOG_AGENT_HANDLE"]["required"] is True
    assert USER_CONFIG_SCHEMA["TWOG_AGENT_CONTACT"]["required"] is True


def test_declared_tools_match_mcp_server() -> None:
    """If we add an MCP tool, declare it in the DXT so install-time UI sees it."""

    from twog_agent.mcp_server import TOOL_NAMES

    declared_names = {t["name"] for t in DECLARED_TOOLS}
    mcp_names = set(TOOL_NAMES)
    assert declared_names == mcp_names, (
        f"DXT declared tools out of sync with MCP server. "
        f"Missing from DXT: {mcp_names - declared_names}. "
        f"Extra in DXT: {declared_names - mcp_names}."
    )


def test_manifest_compatibility_supports_three_platforms() -> None:
    manifest = build_manifest(DxtBuildOptions(output=Path("/tmp/x.dxt")))
    platforms = set(manifest["compatibility"]["platforms"])
    assert {"darwin", "linux", "win32"}.issubset(platforms)


def test_manifest_custom_binary_path_is_respected() -> None:
    """`twog-agent dxt-build --binary /abs/path` should bake that path in."""

    manifest = build_manifest(
        DxtBuildOptions(output=Path("/tmp/x.dxt"), binary="/usr/local/bin/twog-agent")
    )
    assert manifest["server"]["entry_point"] == "/usr/local/bin/twog-agent"
    assert manifest["server"]["mcp_config"]["command"] == "/usr/local/bin/twog-agent"


# ---------- Archive round-trip -----------------------------------------


def test_write_dxt_produces_valid_archive(tmp_path: Path) -> None:
    out = tmp_path / "twog.dxt"
    options = DxtBuildOptions(output=out)
    written = write_dxt(options)
    assert written == out.resolve()
    assert written.exists()
    assert written.suffix == ".dxt"

    with zipfile.ZipFile(written, "r") as zf:
        assert "manifest.json" in zf.namelist()
        raw = zf.read("manifest.json")
    manifest = json.loads(raw)
    assert manifest["name"] == "twog"


def test_validate_dxt_round_trips(tmp_path: Path) -> None:
    out = tmp_path / "twog.dxt"
    written = write_dxt(DxtBuildOptions(output=out))
    parsed = validate_dxt(written)
    assert parsed["dxt_version"] == MANIFEST_VERSION
    assert parsed["server"]["type"] == "binary"


def test_validate_dxt_rejects_missing_manifest(tmp_path: Path) -> None:
    bad = tmp_path / "bad.dxt"
    with zipfile.ZipFile(bad, "w") as zf:
        zf.writestr("not_a_manifest.txt", "hello")
    with pytest.raises(ValueError, match="missing manifest.json"):
        validate_dxt(bad)


def test_write_dxt_creates_parent_dir(tmp_path: Path) -> None:
    out = tmp_path / "nested" / "deep" / "twog.dxt"
    write_dxt(DxtBuildOptions(output=out))
    assert out.exists()


def test_dxt_archive_is_small(tmp_path: Path) -> None:
    """A thin DXT (manifest only) should easily fit in a few KB."""

    out = tmp_path / "x.dxt"
    write_dxt(DxtBuildOptions(output=out))
    assert out.stat().st_size < 5_000, "DXT manifest unexpectedly large"


# ---------- CLI dxt-build portability ----------


def test_cli_dxt_build_warns_when_resolved_path_is_local(
    monkeypatch, tmp_path
):
    """If shutil.which finds twog-agent under a venv or repo path, the CLI
    must fall back to the bare 'twog-agent' string and print a warning.
    A baked-in venv path silently fails on any other machine."""
    import io
    import shutil as _sh
    import sys

    from twog_agent import cli as cli_module
    from twog_agent import dxt as dxt_module
    from twog_agent.exits import Exit

    fake_venv_path = "/Users/somebody/Documents/myrepo/.venv/bin/twog-agent"
    monkeypatch.setattr(_sh, "which", lambda name: fake_venv_path)
    out_path = tmp_path / "out.dxt"
    monkeypatch.delenv("TWOG_AGENT_DXT_BINARY", raising=False)

    buffer = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buffer)

    rc = cli_module.main(["--human", "dxt-build", "--output", str(out_path)])
    assert rc == Exit.SUCCESS

    payload = buffer.getvalue()
    assert "note:" in payload
    assert ".venv" in payload  # warning mentions the offending path
    assert out_path.exists()

    # Confirm the manifest does NOT have the local path baked in
    manifest = dxt_module.validate_dxt(out_path)
    assert manifest["server"]["entry_point"] == "twog-agent"
    assert manifest["server"]["mcp_config"]["command"] == "twog-agent"


def test_cli_dxt_build_uses_resolved_path_when_not_local(
    monkeypatch, tmp_path
):
    """When twog-agent resolves to a system-style path, the absolute path
    is the right choice — Claude Desktop's PATH isn't always the user's
    shell PATH."""
    import io
    import shutil as _sh
    import sys

    from twog_agent import cli as cli_module
    from twog_agent import dxt as dxt_module
    from twog_agent.exits import Exit

    monkeypatch.setattr(_sh, "which", lambda name: "/opt/homebrew/bin/twog-agent")
    out_path = tmp_path / "out.dxt"
    monkeypatch.delenv("TWOG_AGENT_DXT_BINARY", raising=False)

    buffer = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buffer)

    rc = cli_module.main(["--human", "dxt-build", "--output", str(out_path)])
    assert rc == Exit.SUCCESS

    manifest = dxt_module.validate_dxt(out_path)
    assert manifest["server"]["entry_point"] == "/opt/homebrew/bin/twog-agent"
