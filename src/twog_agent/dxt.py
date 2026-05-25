"""Generate a Claude Desktop Extension (.dxt) for the TWOG MCP server.

A ``.dxt`` is a ZIP archive containing a ``manifest.json`` that tells
Claude Desktop how to launch the server and what config prompts to show
the user on first install. Drag-and-drop the resulting file onto Claude
Desktop and the agent appears in the chat UI without anyone editing
JSON.

This builder produces a *thin* DXT: it assumes ``twog-agent`` is already
installed on the user's PATH (via ``pipx install twog-agent`` or the
``install.sh`` one-liner). That keeps the DXT small (~1 KB manifest, no
bundled runtime) and lets users upgrade the server independently of the
extension.

For a self-contained DXT that bundles Python + the package, see
``dxt-build --bundle`` (deferred; that flow needs PyInstaller or
similar).

References:
- https://github.com/anthropics/dxt (manifest format reference)
"""

from __future__ import annotations

import io
import json
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from twog_agent import __version__

# Match Anthropic's published manifest version. Bumping this in
# Claude Desktop releases doesn't usually break old manifests, but it's
# the right field to update if Anthropic ships a v2.
MANIFEST_VERSION = "0.1"

DEFAULT_DISPLAY_NAME = "TWOG Proof Network"
DEFAULT_DESCRIPTION = (
    "Pick up bounded research tasks on TWOG candidates, submit proof capsules, "
    "earn reputation. Works in any MCP-speaking chat (this extension auto-wires "
    "the TWOG MCP server)."
)
HOMEPAGE = "https://twog.bio"
DOCS = "https://twog.bio/connect"
REPOSITORY = "https://github.com/chasepenelli/twog"


@dataclass
class DxtBuildOptions:
    output: Path
    binary: str = "twog-agent"
    display_name: str = DEFAULT_DISPLAY_NAME
    version: str = __version__
    site_url_default: str = "https://twog.bio"


# The user_config block Claude Desktop renders as a form on install.
# Values flow through to the server process as environment variables of
# the same names. Keep this aligned with ``twog_agent.cli`` /
# ``twog_agent.install`` so identity stays consistent across surfaces.
USER_CONFIG_SCHEMA: dict[str, dict[str, Any]] = {
    "TWOG_AGENT_HANDLE": {
        "type": "string",
        "title": "Your handle",
        "description": (
            "Durable handle your reputation accrues to (e.g. @my-agent). "
            "Add the @ if you didn't."
        ),
        "required": True,
        "default": "@your-handle",
    },
    "TWOG_AGENT_CONTACT": {
        "type": "string",
        "title": "Contact email",
        "description": "How TWOG operators can reach you for follow-up on submitted capsules.",
        "required": True,
        "default": "you@example.com",
    },
    "TWOG_AGENT_KIND": {
        "type": "string",
        "title": "Contributor kind",
        "description": "human, agent, team, lab, or company. Defaults to 'agent' for MCP installs.",
        "required": False,
        "default": "agent",
        "enum": ["human", "agent", "team", "lab", "company"],
    },
    "TWOG_AGENT_NAME": {
        "type": "string",
        "title": "Display name (optional)",
        "description": "Shown on your public contributor profile alongside the handle.",
        "required": False,
        "default": "",
    },
    "TWOG_SITE_URL": {
        "type": "string",
        "title": "TWOG site URL",
        "description": "Override for staging or local dev. Most users leave this as-is.",
        "required": False,
        "default": "https://twog.bio",
    },
}


# The tool list mirrors what ``twog_agent.mcp_server`` exposes.
# Declared up-front so Claude Desktop can show users what the extension
# can do *before* they install it.
DECLARED_TOOLS: list[dict[str, str]] = [
    {
        "name": "list_work_packets",
        "description": "List open TWOG work packets, optionally filtered by candidate or capsule type.",
    },
    {
        "name": "get_work_packet",
        "description": "Fetch one work packet by id.",
    },
    {
        "name": "checkout_work_packet",
        "description": "Pull the checkout payload (candidate snapshot, evidence bundle, capsule template) for a packet.",
    },
    {
        "name": "submit_proof_capsule",
        "description": "Submit a proof capsule. Identity, content hash, and signature are added automatically.",
    },
    {
        "name": "get_proof_capsule",
        "description": "Look up a proof capsule's current status (submitted, in_review, accepted, etc.).",
    },
    {
        "name": "get_contributor_profile",
        "description": "Read a contributor's proof portfolio: tier, points, accepted capsules.",
    },
    {
        "name": "get_leaderboard",
        "description": "Fetch the proof-network leaderboard for all-time, last 30 days, or last 7 days.",
    },
]


def build_manifest(options: DxtBuildOptions) -> dict[str, Any]:
    """Return the manifest.json contents for the DXT."""

    return {
        "dxt_version": MANIFEST_VERSION,
        "name": "twog",
        "display_name": options.display_name,
        "version": options.version,
        "description": DEFAULT_DESCRIPTION,
        "long_description": (
            "TWOG is a public Proof Network for scientific research. Humans and "
            "agents pick up bounded research tasks against candidate records "
            "(citation repairs, claim critiques, evidence additions, validation "
            "proposals, replications), submit proof capsules, and earn proof "
            "points when their work survives operator review.\n\n"
            "This extension wires Claude Desktop's MCP transport to the TWOG "
            "MCP server (the `twog-agent mcp` binary). After install, ask the "
            "chat 'List open work packets on TWOG' to start the loop."
        ),
        "author": {"name": "TWOG Proof Network", "url": HOMEPAGE},
        "repository": {"type": "git", "url": REPOSITORY},
        "homepage": HOMEPAGE,
        "documentation": DOCS,
        "support": HOMEPAGE + "/connect",
        "icon": None,
        "screenshots": [],
        "keywords": [
            "twog",
            "proof-network",
            "research",
            "agent",
            "mcp",
            "biotech",
            "science",
        ],
        "license": "Same as parent repo",
        "compatibility": {
            "client": ">=0.10.0",
            "platforms": ["darwin", "linux", "win32"],
            "runtimes": {"python": ">=3.11"},
        },
        "server": {
            "type": "binary",
            "entry_point": options.binary,
            "mcp_config": {
                "command": options.binary,
                "args": ["mcp"],
                "env": {
                    "TWOG_AGENT_HANDLE": "${user_config.TWOG_AGENT_HANDLE}",
                    "TWOG_AGENT_CONTACT": "${user_config.TWOG_AGENT_CONTACT}",
                    "TWOG_AGENT_KIND": "${user_config.TWOG_AGENT_KIND}",
                    "TWOG_AGENT_NAME": "${user_config.TWOG_AGENT_NAME}",
                    "TWOG_SITE_URL": "${user_config.TWOG_SITE_URL}",
                },
            },
        },
        "tools": DECLARED_TOOLS,
        "user_config": USER_CONFIG_SCHEMA,
    }


def write_dxt(options: DxtBuildOptions) -> Path:
    """Write the .dxt archive to ``options.output`` and return its path."""

    manifest = build_manifest(options)
    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
    out_path = options.output.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # ZIP_DEFLATED keeps the .dxt small; manifest.json compresses well.
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as archive:
        zi = zipfile.ZipInfo("manifest.json")
        zi.external_attr = 0o644 << 16
        archive.writestr(zi, manifest_bytes)
    return out_path


def validate_dxt(path: Path) -> dict[str, Any]:
    """Read a .dxt and return parsed manifest. Raises on invalid archives."""

    with zipfile.ZipFile(path, "r") as archive:
        names = archive.namelist()
        if "manifest.json" not in names:
            raise ValueError(f"{path}: missing manifest.json")
        return json.loads(archive.read("manifest.json"))
