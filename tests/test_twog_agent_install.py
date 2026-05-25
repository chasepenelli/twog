"""Unit tests for ``twog-agent install``.

These tests work entirely off the filesystem (tmp_path) so we never
modify a real user's Claude Desktop / Cursor config. They lock the
contracts the install command is meant to keep:

1. Existing ``mcpServers`` entries from other tools are preserved.
2. Re-running install is idempotent (no duplicate writes).
3. Uninstall is symmetric — removes the entry, leaves siblings.
4. Backups are written whenever a config file already existed.
5. The mcpServers.twog entry uses the absolute binary path.
6. Identity normalization works (``foo`` and ``@foo`` both yield ``@foo``).
"""

from __future__ import annotations

import io
import json
import os
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from twog_agent import install as install_module
from twog_agent.install import (
    ClientTarget,
    InstallPlan,
    SOUL_BUNDLES,
    build_mcp_entry,
    looks_like_email,
    normalize_handle,
    read_json_config,
    remove_mcp_entry,
    run_install,
    run_uninstall,
    upsert_mcp_entry,
)


# ---------- Pure helpers ------------------------------------------------


def test_normalize_handle_adds_at_prefix() -> None:
    assert normalize_handle("foo") == "@foo"
    assert normalize_handle("@foo") == "@foo"
    assert normalize_handle("  @foo  ") == "@foo"
    assert normalize_handle("  bar  ") == "@bar"
    assert normalize_handle("") == ""
    assert normalize_handle("   ") == ""


def test_looks_like_email_accepts_obvious_cases() -> None:
    assert looks_like_email("you@example.com")
    assert looks_like_email("first.last@sub.domain.co")
    assert not looks_like_email("not an email")
    assert not looks_like_email("@example.com")
    assert not looks_like_email("missing-at.example.com")
    assert not looks_like_email("a b @ c")


def test_build_mcp_entry_shape() -> None:
    entry = build_mcp_entry(
        handle="@x",
        contact="x@example.com",
        kind="agent",
        name=None,
        site_url="https://twog.bio",
    )
    assert entry["args"] == ["mcp"]
    assert isinstance(entry["command"], str)
    assert entry["env"]["TWOG_AGENT_HANDLE"] == "@x"
    assert entry["env"]["TWOG_AGENT_CONTACT"] == "x@example.com"
    assert entry["env"]["TWOG_AGENT_KIND"] == "agent"
    assert entry["env"]["TWOG_SITE_URL"] == "https://twog.bio"
    assert "TWOG_AGENT_NAME" not in entry["env"]
    assert "TWOG_AGENT_PRIVKEY" not in entry["env"]


def test_build_mcp_entry_includes_optionals_when_present() -> None:
    entry = build_mcp_entry(
        handle="@x",
        contact="x@example.com",
        kind="human",
        name="Display Name",
        site_url="https://twog.bio",
        privkey="seed",
    )
    assert entry["env"]["TWOG_AGENT_NAME"] == "Display Name"
    assert entry["env"]["TWOG_AGENT_PRIVKEY"] == "seed"


# ---------- Config upsert ------------------------------------------------


def test_upsert_into_empty_config_adds_entry() -> None:
    entry = build_mcp_entry(handle="@x", contact="x@example.com", kind="agent", name=None, site_url="s")
    new_config, change = upsert_mcp_entry({}, server_name="twog", entry=entry)
    assert change == "added"
    assert new_config["mcpServers"]["twog"] == entry


def test_upsert_preserves_existing_unrelated_servers() -> None:
    existing = {
        "mcpServers": {
            "other-tool": {"command": "other", "args": ["mcp"], "env": {}},
        },
    }
    entry = build_mcp_entry(handle="@x", contact="x@example.com", kind="agent", name=None, site_url="s")
    new_config, change = upsert_mcp_entry(existing, server_name="twog", entry=entry)
    assert change == "added"
    assert "other-tool" in new_config["mcpServers"]
    assert "twog" in new_config["mcpServers"]


def test_upsert_unchanged_when_entry_identical() -> None:
    entry = build_mcp_entry(handle="@x", contact="x@example.com", kind="agent", name=None, site_url="s")
    base = {"mcpServers": {"twog": entry}}
    _, change = upsert_mcp_entry(base, server_name="twog", entry=entry)
    assert change == "unchanged"


def test_upsert_updates_when_entry_differs() -> None:
    old = build_mcp_entry(handle="@a", contact="a@example.com", kind="agent", name=None, site_url="s")
    new = build_mcp_entry(handle="@b", contact="b@example.com", kind="agent", name=None, site_url="s")
    base = {"mcpServers": {"twog": old}}
    _, change = upsert_mcp_entry(base, server_name="twog", entry=new)
    assert change == "updated"


def test_remove_mcp_entry_leaves_siblings_untouched() -> None:
    base = {
        "mcpServers": {
            "twog": {"command": "x", "args": [], "env": {}},
            "keep-me": {"command": "y", "args": [], "env": {}},
        },
    }
    new_config, removed = remove_mcp_entry(base, server_name="twog")
    assert removed
    assert "twog" not in new_config["mcpServers"]
    assert "keep-me" in new_config["mcpServers"]


def test_remove_mcp_entry_drops_empty_section() -> None:
    base = {"mcpServers": {"twog": {}}, "other_key": "preserved"}
    new_config, removed = remove_mcp_entry(base, server_name="twog")
    assert removed
    assert "mcpServers" not in new_config
    assert new_config["other_key"] == "preserved"


def test_remove_mcp_entry_noop_when_absent() -> None:
    base = {"mcpServers": {"keep-me": {}}}
    new_config, removed = remove_mcp_entry(base, server_name="twog")
    assert not removed
    assert "keep-me" in new_config["mcpServers"]


# ---------- File-level read/write --------------------------------------


def test_read_missing_config_returns_empty_dict(tmp_path: Path) -> None:
    config, existed = read_json_config(tmp_path / "missing.json")
    assert config == {}
    assert not existed


def test_read_malformed_config_raises(tmp_path: Path) -> None:
    p = tmp_path / "bad.json"
    p.write_text("{not json")
    with pytest.raises(RuntimeError, match="not valid JSON"):
        read_json_config(p)


def test_run_install_writes_config_with_backup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Apply the install plan to a fake Claude Desktop config and verify
    the file was written + the old version backed up + an existing
    unrelated mcpServers entry is preserved."""

    config_path = tmp_path / "claude_desktop_config.json"
    config_path.write_text(json.dumps({
        "mcpServers": {
            "other-tool": {"command": "other", "args": ["serve"], "env": {}}
        }
    }))

    # Don't actually install skills (we'd need the repo source).
    target = ClientTarget(name="Claude Desktop", config_path=config_path, skills_dir=None)
    plan = InstallPlan(
        handle="@me",
        contact="me@example.com",
        kind="agent",
        name=None,
        site_url="https://twog.bio",
        privkey=None,
        targets=[target],
        skill_bundles=(),
        use_symlink=True,
    )
    report = run_install(plan)

    # Config rewritten
    written = json.loads(config_path.read_text())
    assert "twog" in written["mcpServers"]
    assert "other-tool" in written["mcpServers"]
    # Identity flowed in
    assert written["mcpServers"]["twog"]["env"]["TWOG_AGENT_HANDLE"] == "@me"
    # Backup made
    backups = list(tmp_path.glob("claude_desktop_config.json.bak.*"))
    assert len(backups) == 1
    assert "other-tool" in json.loads(backups[0].read_text())["mcpServers"]
    # Report reflects the change
    assert report.overall_change is True
    assert report.targets[0].config_change == "added"


def test_run_install_idempotent(tmp_path: Path) -> None:
    """A second install with the same plan must not write again or backup."""

    config_path = tmp_path / "config.json"
    target = ClientTarget(name="Claude Desktop", config_path=config_path)
    plan = InstallPlan(
        handle="@me",
        contact="me@example.com",
        kind="agent",
        name=None,
        site_url="https://twog.bio",
        privkey=None,
        targets=[target],
        skill_bundles=(),
        use_symlink=True,
    )
    run_install(plan)
    first_backups = list(tmp_path.glob("config.json.bak.*"))
    report = run_install(plan)
    assert report.targets[0].config_change == "unchanged"
    second_backups = list(tmp_path.glob("config.json.bak.*"))
    assert first_backups == second_backups  # no new backup on no-op


def test_run_install_dry_run_does_not_write(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"mcpServers": {}}))
    original = config_path.read_text()
    target = ClientTarget(name="Claude Desktop", config_path=config_path)
    plan = InstallPlan(
        handle="@me",
        contact="me@example.com",
        kind="agent",
        name=None,
        site_url="https://twog.bio",
        privkey=None,
        targets=[target],
        skill_bundles=(),
        use_symlink=True,
    )
    report = run_install(plan, dry_run=True)
    assert config_path.read_text() == original
    assert report.targets[0].config_change == "added"  # what *would* happen
    assert report.targets[0].backup is None


def test_run_uninstall_removes_only_twog_entry(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({
        "mcpServers": {
            "twog": {"command": "x", "args": [], "env": {}},
            "keep-me": {"command": "y", "args": [], "env": {}},
        }
    }))
    target = ClientTarget(name="Claude Desktop", config_path=config_path)
    report = run_uninstall(targets=[target], remove_skills=False)
    written = json.loads(config_path.read_text())
    assert "twog" not in written["mcpServers"]
    assert "keep-me" in written["mcpServers"]
    assert report.targets[0].config_change == "removed"


def test_run_uninstall_noop_when_twog_absent(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"mcpServers": {"keep-me": {}}}))
    target = ClientTarget(name="Claude Desktop", config_path=config_path)
    report = run_uninstall(targets=[target], remove_skills=False)
    assert report.targets[0].config_change == "skipped:not_present"


def test_run_uninstall_skipped_when_config_missing(tmp_path: Path) -> None:
    target = ClientTarget(name="Claude Desktop", config_path=tmp_path / "missing.json")
    report = run_uninstall(targets=[target], remove_skills=False)
    assert report.targets[0].config_change == "skipped:missing"


# ---------- Skill bundle install ---------------------------------------


def test_install_skill_bundles_symlinks_each(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Skill installs are pure filesystem ops; verify symlink behaviour."""

    # Stand up a fake skill source so we don't depend on repo layout.
    src_root = tmp_path / "src_skills"
    src_root.mkdir()
    for bundle in ("twog-agent", "twog-citation-repairer"):
        (src_root / bundle).mkdir()
        (src_root / bundle / "SKILL.md").write_text(f"# {bundle}\n")

    monkeypatch.setattr(install_module, "repo_skills_dir", lambda: src_root)

    dest = tmp_path / "skills"
    result = install_module.install_skill_bundles(
        skills_dest=dest,
        bundles=("twog-agent", "twog-citation-repairer"),
        use_symlink=True,
    )
    assert result.installed == ["twog-agent", "twog-citation-repairer"]
    assert (dest / "twog-agent").is_symlink()
    assert (dest / "twog-agent" / "SKILL.md").exists()


def test_install_skill_bundles_idempotent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src_root = tmp_path / "src_skills"
    src_root.mkdir()
    (src_root / "twog-agent").mkdir()
    (src_root / "twog-agent" / "SKILL.md").write_text("# x\n")
    monkeypatch.setattr(install_module, "repo_skills_dir", lambda: src_root)

    dest = tmp_path / "skills"
    install_module.install_skill_bundles(
        skills_dest=dest, bundles=("twog-agent",), use_symlink=True
    )
    second = install_module.install_skill_bundles(
        skills_dest=dest, bundles=("twog-agent",), use_symlink=True
    )
    assert second.installed == []
    assert second.skipped == ["twog-agent"]


def test_install_skill_bundles_missing_source_reports_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(install_module, "repo_skills_dir", lambda: None)
    result = install_module.install_skill_bundles(
        skills_dest=tmp_path / "skills",
        bundles=("twog-agent",),
        use_symlink=True,
    )
    assert result.installed == []
    assert result.errors and "not found" in result.errors[0]


def test_uninstall_skill_bundles_removes_symlinks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src_root = tmp_path / "src_skills"
    src_root.mkdir()
    (src_root / "twog-agent").mkdir()
    (src_root / "twog-agent" / "SKILL.md").write_text("# x\n")
    monkeypatch.setattr(install_module, "repo_skills_dir", lambda: src_root)
    dest = tmp_path / "skills"
    install_module.install_skill_bundles(
        skills_dest=dest, bundles=("twog-agent",), use_symlink=True
    )
    assert (dest / "twog-agent").exists()
    result = install_module.uninstall_skill_bundles(
        skills_dest=dest, bundles=("twog-agent",)
    )
    assert not (dest / "twog-agent").exists()
    assert result.installed == ["twog-agent"]


# ---------- The contract that matters most: SOUL_BUNDLES list ----------


def test_soul_bundles_match_skills_directory(tmp_path: Path) -> None:
    """If we add a new specialized skill, SOUL_BUNDLES must list it."""

    skills_dir = install_module.repo_skills_dir()
    if skills_dir is None:
        pytest.skip("running outside the repo checkout")
    on_disk = {p.name for p in skills_dir.iterdir() if p.is_dir() and (p / "SKILL.md").is_file()}
    declared = set(SOUL_BUNDLES)
    missing = on_disk - declared
    assert not missing, (
        f"new skill bundles exist on disk but aren't in SOUL_BUNDLES: {missing}"
    )


# ---------- CLI install behavior: dry-run with no clients --------------


def test_cli_install_dry_run_no_clients_exits_zero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`twog-agent install --dry-run` with no detected MCP clients should
    exit 0 and print a friendly "nothing to preview" message, not crash with
    GENERIC_ERROR. The user is trying to see what *would* happen — we
    should tell them, not block them."""

    from twog_agent import cli as cli_module
    from twog_agent.exits import Exit

    monkeypatch.setattr(install_module, "detect_clients", lambda: [])
    monkeypatch.setenv("TWOG_AGENT_HANDLE", "@dryrun-noclients")
    monkeypatch.setenv("TWOG_AGENT_CONTACT", "preview@example.com")
    monkeypatch.setenv("TWOG_AGENT_KIND", "agent")
    buffer = io.StringIO()
    monkeypatch.setattr(cli_module.sys, "stdout", buffer)

    exit_code = cli_module.main(["--human", "install", "--dry-run"])

    assert exit_code == Exit.SUCCESS
    out = buffer.getvalue()
    assert "no MCP clients detected" in out or "no MCP-capable clients" in out
    assert "https://claude.ai/download" in out


def test_cli_install_no_clients_without_dry_run_still_errors(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without --dry-run, no detected clients should still surface as an
    error — we can't actually install anything."""

    from twog_agent import cli as cli_module
    from twog_agent.exits import Exit

    monkeypatch.setattr(install_module, "detect_clients", lambda: [])
    monkeypatch.setenv("TWOG_AGENT_HANDLE", "@noclients")
    monkeypatch.setenv("TWOG_AGENT_CONTACT", "preview@example.com")
    monkeypatch.setenv("TWOG_AGENT_KIND", "agent")
    buffer = io.StringIO()
    monkeypatch.setattr(cli_module.sys, "stderr", buffer)

    exit_code = cli_module.main(["--human", "install"])

    assert exit_code == Exit.GENERIC_ERROR
