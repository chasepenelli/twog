"""One-command installer for the twog-agent MCP server.

The user-visible promise: after ``pipx install twog-agent`` they run a
single command:

    twog-agent install

…and we detect their MCP-capable clients, write the right config, and
symlink the agent skills so Claude (or Codex, or any MCP host) can use
the TWOG tools without anyone hand-editing JSON.

Design rules:

* Never clobber an existing ``mcpServers`` entry for another tool.
* Back up every config file before editing (``<name>.bak.<unix-ts>``).
* Be cross-platform on path discovery. Default to OS-appropriate
  locations; honour ``XDG_CONFIG_HOME`` on Linux.
* Use the absolute path of the ``twog-agent`` binary (resolved via
  ``shutil.which``) so launched-from-GUI clients with stripped PATH
  still find it.
* Identity (handle, contact, kind) comes from env vars first, then
  flags, then interactive prompts. No silent defaults for these.
* Be honest about idempotency: re-running ``install`` updates the entry
  in place; ``uninstall`` removes it without touching siblings.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from twog_agent.exits import Exit


# ---------- Identity helpers --------------------------------------------


def _env(name: str) -> str | None:
    value = os.environ.get(name)
    return value.strip() if value else None


def normalize_handle(value: str) -> str:
    """``foo`` and ``@foo`` both become ``@foo``; whitespace stripped."""

    cleaned = value.strip()
    if not cleaned:
        return ""
    return cleaned if cleaned.startswith("@") else f"@{cleaned}"


def looks_like_email(value: str) -> bool:
    cleaned = value.strip()
    if " " in cleaned or "@" not in cleaned:
        return False
    local, _, domain = cleaned.partition("@")
    return bool(local) and "." in domain and not domain.startswith(".")


VALID_KINDS = ("human", "agent", "team", "lab", "company")


# ---------- Client detection --------------------------------------------


@dataclass
class ClientTarget:
    """A single MCP-capable client config we know how to write."""

    name: str
    config_path: Path
    skills_dir: Path | None = None
    """Optional skills directory to symlink skill bundles into."""


def claude_desktop_config_path() -> Path:
    """Resolve Claude Desktop's config path per OS."""

    home = Path.home()
    if sys.platform == "darwin":
        return home / "Library/Application Support/Claude/claude_desktop_config.json"
    if sys.platform.startswith("win"):
        appdata = os.environ.get("APPDATA")
        base = Path(appdata) if appdata else home / "AppData/Roaming"
        return base / "Claude/claude_desktop_config.json"
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg else home / ".config"
    return base / "Claude/claude_desktop_config.json"


def claude_code_skills_dir() -> Path:
    return Path.home() / ".claude/skills"


def codex_config_path() -> Path | None:
    """Codex's MCP config is in flux; return the best-known location.

    We only WRITE here if the parent directory exists, so a fresh machine
    without Codex installed won't get a config dropped on it.
    """

    home = Path.home()
    candidates = [
        home / ".codex/config.json",
        home / ".config/codex/config.json",
    ]
    for path in candidates:
        if path.parent.exists():
            return path
    return None


def cursor_config_path() -> Path | None:
    """Cursor's mcp config lives at ~/.cursor/mcp.json on most installs."""

    candidate = Path.home() / ".cursor/mcp.json"
    if candidate.parent.exists():
        return candidate
    return None


def detect_clients() -> list[ClientTarget]:
    """Return the clients we recognize on this machine, in install order."""

    targets: list[ClientTarget] = []
    cd_path = claude_desktop_config_path()
    if cd_path.parent.exists():
        targets.append(
            ClientTarget(
                name="Claude Desktop",
                config_path=cd_path,
                skills_dir=claude_code_skills_dir(),
            )
        )
    cc_skills = claude_code_skills_dir()
    if cc_skills.parent.exists() and not any(t.skills_dir == cc_skills for t in targets):
        targets.append(
            ClientTarget(
                name="Claude Code skills",
                config_path=cc_skills,  # not actually a json file; we only symlink
                skills_dir=cc_skills,
            )
        )
    codex = codex_config_path()
    if codex is not None:
        targets.append(ClientTarget(name="Codex", config_path=codex))
    cursor = cursor_config_path()
    if cursor is not None:
        targets.append(ClientTarget(name="Cursor", config_path=cursor))
    return targets


# ---------- twog-agent binary discovery ---------------------------------


def resolve_twog_agent_binary() -> str:
    """Return the absolute path of the ``twog-agent`` binary if findable.

    Claude Desktop and similar GUI launchers don't inherit a user shell's
    PATH, so writing just ``twog-agent`` into the config often breaks.
    Prefer the absolute path.
    """

    path = shutil.which("twog-agent")
    return path if path else "twog-agent"


# ---------- Config builders ---------------------------------------------


def build_mcp_entry(
    *,
    handle: str,
    contact: str,
    kind: str,
    name: str | None,
    site_url: str,
    privkey: str | None = None,
    extra_env: dict[str, str] | None = None,
) -> dict[str, Any]:
    """The ``mcpServers.twog`` entry every MCP client gets."""

    env: dict[str, str] = {
        "TWOG_AGENT_HANDLE": handle,
        "TWOG_AGENT_CONTACT": contact,
        "TWOG_AGENT_KIND": kind,
        "TWOG_SITE_URL": site_url,
    }
    if name:
        env["TWOG_AGENT_NAME"] = name
    if privkey:
        env["TWOG_AGENT_PRIVKEY"] = privkey
    if extra_env:
        env.update(extra_env)
    return {
        "command": resolve_twog_agent_binary(),
        "args": ["mcp"],
        "env": env,
    }


# ---------- File I/O ----------------------------------------------------


def backup_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + f".bak.{int(time.time())}")


def read_json_config(path: Path) -> tuple[dict[str, Any], bool]:
    """Return (parsed_config, exists). Returns ``({}, False)`` if missing."""

    if not path.exists():
        return {}, False
    raw = path.read_text()
    if not raw.strip():
        return {}, True
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Existing config at {path} is not valid JSON: {exc}. "
            f"Fix or move it; we will not overwrite a corrupt file."
        ) from exc
    if not isinstance(data, dict):
        raise RuntimeError(
            f"Existing config at {path} is JSON but not an object."
        )
    return data, True


def write_json_config(path: Path, data: dict[str, Any], *, make_backup: bool) -> Path | None:
    """Write the config, returning the backup path if one was made."""

    backup: Path | None = None
    if make_backup and path.exists():
        backup = backup_path(path)
        backup.write_text(path.read_text())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")
    return backup


def upsert_mcp_entry(
    config: dict[str, Any],
    *,
    server_name: str,
    entry: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    """Return (new_config, change) where change is added|updated|unchanged."""

    new_config = dict(config)
    servers = dict(new_config.get("mcpServers") or {})
    existing = servers.get(server_name)
    change = "added" if existing is None else "updated"
    if existing == entry:
        change = "unchanged"
    servers[server_name] = entry
    new_config["mcpServers"] = servers
    return new_config, change


def remove_mcp_entry(
    config: dict[str, Any],
    *,
    server_name: str,
) -> tuple[dict[str, Any], bool]:
    """Return (new_config, removed_bool)."""

    new_config = dict(config)
    servers = dict(new_config.get("mcpServers") or {})
    if server_name not in servers:
        return new_config, False
    servers.pop(server_name)
    if servers:
        new_config["mcpServers"] = servers
    else:
        new_config.pop("mcpServers", None)
    return new_config, True


# ---------- Skill bundle installation -----------------------------------


def repo_skills_dir() -> Path | None:
    """Locate the in-repo ``skills/`` directory if we're running from a checkout.

    When installed via pipx the skills aren't shipped (they're docs, not
    code). In that case we return ``None`` and the caller falls back to
    pointing users at the GitHub source.
    """

    # twog_agent is at <repo>/src/twog_agent/install.py; skills live at
    # <repo>/skills.
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "skills"
        if candidate.is_dir() and (candidate / "twog-agent" / "SKILL.md").is_file():
            return candidate
    return None


SOUL_BUNDLES = (
    "twog-agent",
    "twog-citation-repairer",
    "twog-claim-critic",
    "twog-evidence-finder",
    "twog-validation-proposer",
)


@dataclass
class SkillInstallResult:
    installed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def install_skill_bundles(
    *,
    skills_dest: Path,
    bundles: Iterable[str],
    use_symlink: bool = True,
) -> SkillInstallResult:
    """Install the requested skill bundles into ``skills_dest``.

    Each bundle becomes ``<skills_dest>/<name>``. By default we symlink
    so edits to the repo show up live; ``use_symlink=False`` does a copy
    instead (better for pipx-installed setups that don't have the repo).
    """

    result = SkillInstallResult()
    src_root = repo_skills_dir()
    if src_root is None:
        result.errors.append(
            "Skill source directory not found. If you installed via pipx, "
            "the skills aren't shipped with the wheel — clone the repo or "
            "download skills/ from GitHub and re-run with --skills-source."
        )
        return result
    skills_dest.mkdir(parents=True, exist_ok=True)
    for bundle in bundles:
        src = src_root / bundle
        if not src.is_dir():
            result.errors.append(f"skill bundle missing in source: {bundle}")
            continue
        dest = skills_dest / bundle
        if dest.exists() or dest.is_symlink():
            # Already installed; consider it good.
            result.skipped.append(bundle)
            continue
        try:
            if use_symlink:
                dest.symlink_to(src, target_is_directory=True)
            else:
                shutil.copytree(src, dest)
            result.installed.append(bundle)
        except OSError as exc:
            result.errors.append(f"{bundle}: {exc}")
    return result


def uninstall_skill_bundles(
    *,
    skills_dest: Path,
    bundles: Iterable[str],
) -> SkillInstallResult:
    result = SkillInstallResult()
    for bundle in bundles:
        dest = skills_dest / bundle
        if not (dest.exists() or dest.is_symlink()):
            result.skipped.append(bundle)
            continue
        try:
            if dest.is_symlink() or dest.is_file():
                dest.unlink()
            else:
                shutil.rmtree(dest)
            result.installed.append(bundle)  # used as "actions" for the report
        except OSError as exc:
            result.errors.append(f"{bundle}: {exc}")
    return result


# ---------- Installer orchestration -------------------------------------


@dataclass
class InstallPlan:
    handle: str
    contact: str
    kind: str
    name: str | None
    site_url: str
    privkey: str | None
    targets: list[ClientTarget]
    skill_bundles: tuple[str, ...]
    use_symlink: bool


@dataclass
class TargetResult:
    target: ClientTarget
    config_change: str | None = None  # added | updated | unchanged | skipped:<reason>
    backup: Path | None = None
    skills: SkillInstallResult | None = None
    error: str | None = None


@dataclass
class InstallReport:
    plan: InstallPlan
    targets: list[TargetResult]
    overall_change: bool

    def as_payload(self) -> dict[str, Any]:
        return {
            "handle": self.plan.handle,
            "contact_set": bool(self.plan.contact),
            "kind": self.plan.kind,
            "site_url": self.plan.site_url,
            "binary": resolve_twog_agent_binary(),
            "skill_bundles": list(self.plan.skill_bundles),
            "use_symlink": self.plan.use_symlink,
            "overall_change": self.overall_change,
            "targets": [
                {
                    "name": t.target.name,
                    "config_path": str(t.target.config_path),
                    "config_change": t.config_change,
                    "backup": str(t.backup) if t.backup else None,
                    "skills_installed": t.skills.installed if t.skills else None,
                    "skills_skipped": t.skills.skipped if t.skills else None,
                    "skills_errors": t.skills.errors if t.skills else None,
                    "error": t.error,
                }
                for t in self.targets
            ],
        }


def run_install(plan: InstallPlan, *, dry_run: bool = False) -> InstallReport:
    entry = build_mcp_entry(
        handle=plan.handle,
        contact=plan.contact,
        kind=plan.kind,
        name=plan.name,
        site_url=plan.site_url,
        privkey=plan.privkey,
    )
    results: list[TargetResult] = []
    overall = False
    for target in plan.targets:
        tr = TargetResult(target=target)
        try:
            if target.name == "Claude Code skills":
                # Skills-only target; no JSON config to write.
                if not dry_run:
                    sk = install_skill_bundles(
                        skills_dest=target.skills_dir or claude_code_skills_dir(),
                        bundles=plan.skill_bundles,
                        use_symlink=plan.use_symlink,
                    )
                    tr.skills = sk
                    if sk.installed:
                        overall = True
                else:
                    tr.skills = SkillInstallResult()
                tr.config_change = "skills_only"
            else:
                config, _existed = read_json_config(target.config_path)
                new_config, change = upsert_mcp_entry(
                    config, server_name="twog", entry=entry
                )
                tr.config_change = change
                if not dry_run and change != "unchanged":
                    tr.backup = write_json_config(
                        target.config_path, new_config, make_backup=True
                    )
                    overall = True
                # Also install skills alongside the Claude Desktop install,
                # because the Claude Code skills directory is shared.
                if target.skills_dir and not dry_run:
                    sk = install_skill_bundles(
                        skills_dest=target.skills_dir,
                        bundles=plan.skill_bundles,
                        use_symlink=plan.use_symlink,
                    )
                    tr.skills = sk
                    if sk.installed:
                        overall = True
        except Exception as exc:  # noqa: BLE001 — capture per-target, continue
            tr.error = str(exc)
        results.append(tr)
    return InstallReport(plan=plan, targets=results, overall_change=overall)


def run_uninstall(*, targets: list[ClientTarget], remove_skills: bool) -> InstallReport:
    results: list[TargetResult] = []
    overall = False
    for target in targets:
        tr = TargetResult(target=target)
        try:
            if target.name == "Claude Code skills":
                if remove_skills:
                    sk = uninstall_skill_bundles(
                        skills_dest=target.skills_dir or claude_code_skills_dir(),
                        bundles=SOUL_BUNDLES,
                    )
                    tr.skills = sk
                    if sk.installed:
                        overall = True
                tr.config_change = "skills_removed" if remove_skills else "skills_skipped"
            else:
                config, existed = read_json_config(target.config_path)
                if not existed:
                    tr.config_change = "skipped:missing"
                    results.append(tr)
                    continue
                new_config, removed = remove_mcp_entry(config, server_name="twog")
                if removed:
                    tr.backup = write_json_config(
                        target.config_path, new_config, make_backup=True
                    )
                    tr.config_change = "removed"
                    overall = True
                else:
                    tr.config_change = "skipped:not_present"
                if remove_skills and target.skills_dir is not None:
                    sk = uninstall_skill_bundles(
                        skills_dest=target.skills_dir, bundles=SOUL_BUNDLES
                    )
                    tr.skills = sk
                    if sk.installed:
                        overall = True
        except Exception as exc:  # noqa: BLE001
            tr.error = str(exc)
        results.append(tr)
    return InstallReport(
        plan=InstallPlan(
            handle="",
            contact="",
            kind="",
            name=None,
            site_url="",
            privkey=None,
            targets=targets,
            skill_bundles=SOUL_BUNDLES,
            use_symlink=False,
        ),
        targets=results,
        overall_change=overall,
    )


# ---------- Doctor ------------------------------------------------------


def run_doctor() -> dict[str, Any]:
    """Report what we'd write and which clients we can see."""

    targets = detect_clients()
    report: dict[str, Any] = {
        "binary": resolve_twog_agent_binary(),
        "binary_on_path": shutil.which("twog-agent") is not None,
        "skills_source": str(repo_skills_dir()) if repo_skills_dir() else None,
        "detected": [],
    }
    for t in targets:
        record: dict[str, Any] = {
            "name": t.name,
            "config_path": str(t.config_path),
            "config_exists": t.config_path.exists(),
            "skills_dir": str(t.skills_dir) if t.skills_dir else None,
        }
        if t.name != "Claude Code skills" and t.config_path.exists():
            try:
                config, _ = read_json_config(t.config_path)
                servers = config.get("mcpServers") or {}
                record["twog_entry"] = "present" if "twog" in servers else "absent"
                record["other_servers"] = sorted(name for name in servers if name != "twog")
            except RuntimeError as exc:
                record["parse_error"] = str(exc)
        report["detected"].append(record)
    return report
