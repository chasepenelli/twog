"""Model Context Protocol (stdio) server for the TWOG Proof Network.

Exposes the same primitives as the ``twog-agent`` CLI as MCP tools so that
Claude Desktop, Codex, and any other MCP-speaking client can drive the
Proof Network the same way a human operator would.

Design notes
------------
- Every tool returns a single :class:`mcp.types.TextContent` whose ``text``
  is a JSON document. MCP clients can parse the payload directly, mirroring
  the CLI's "JSON-by-default" contract.
- Errors raised by :class:`twog_agent.client.ProofNetworkClient` are caught
  and surfaced as a structured ``{"error": ..., "details": ...}`` payload
  (still a single ``TextContent``) so the LLM can reason over them without
  the MCP transport raising an exception.
- Identity, endpoint, and signing config are read from the environment on
  every call — the server is stateless. This matches the CLI's contract so
  the same env vars (``TWOG_AGENT_HANDLE``, ``TWOG_SITE_URL``,
  ``TWOG_AGENT_PRIVKEY``, ...) work identically.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from twog_agent.client import (
    NetworkUnavailable,
    ProofNetworkClient,
    ProofNetworkError,
    resolve_site_url,
)
from twog_agent.content_hash import compute_proof_capsule_content_hash
from twog_agent.signing import sign_content_hash


# ---------- Identity helpers (mirrors cli._contributor_from_env) --------


def _env(name: str) -> str | None:
    value = os.environ.get(name)
    return value.strip() if value else None


def _contributor_from_env() -> dict[str, Any]:
    """Build a contributor block from the same env vars the CLI reads."""

    contributor: dict[str, Any] = {
        "kind": _env("TWOG_AGENT_KIND") or "agent",
        "handle": _env("TWOG_AGENT_HANDLE"),
        "contact": _env("TWOG_AGENT_CONTACT"),
    }
    for key, env_key in (
        ("name", "TWOG_AGENT_NAME"),
        ("affiliation", "TWOG_AGENT_AFFILIATION"),
        ("agent_id", "TWOG_AGENT_ID"),
        ("website", "TWOG_AGENT_WEBSITE"),
    ):
        value = _env(env_key)
        if value:
            contributor[key] = value
    return contributor


def _merge_contributor(
    explicit: dict[str, Any] | None, env: dict[str, Any]
) -> dict[str, Any]:
    merged: dict[str, Any] = dict(env)
    if explicit:
        for key, value in explicit.items():
            if value:
                merged[key] = value
    return merged


# ---------- Tool schemas ------------------------------------------------


TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "list_work_packets",
        "description": (
            "List open TWOG Proof Network work packets. Returns a JSON payload "
            "with ``work_packet_count`` and ``work_packets``. All filter args "
            "are optional."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "statuses": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Status filters (e.g. 'open').",
                },
                "packet_types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "packet_type filters (e.g. 'citation_repair').",
                },
                "candidate_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Restrict to specific candidate_ids.",
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 200,
                    "description": "Max packets to return (default 25).",
                },
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "get_work_packet",
        "description": "Fetch a single work packet by id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "work_packet_id": {"type": "string", "description": "work_packet_id (UUID)."},
            },
            "required": ["work_packet_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "checkout_work_packet",
        "description": (
            "Check out a work packet. Returns the work packet + candidate + "
            "evidence_bundle_summary needed to build a proof capsule."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "work_packet_id": {"type": "string", "description": "work_packet_id (UUID)."},
            },
            "required": ["work_packet_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "submit_proof_capsule",
        "description": (
            "Submit a proof capsule. Contributor identity is merged from env vars "
            "(TWOG_AGENT_HANDLE, TWOG_AGENT_CONTACT, ...). content_hash is "
            "computed client-side and the body is signed when TWOG_AGENT_PRIVKEY "
            "is set."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "candidate_id": {"type": "string"},
                "capsule_type": {"type": "string"},
                "title": {"type": "string"},
                "analysis_summary": {"type": "string"},
                "work_packet_id": {"type": "string"},
                "findings": {"type": "string"},
                "method_refs": {"type": "array", "items": {"type": "string"}},
                "output_refs": {"type": "array", "items": {"type": "string"}},
                "artifact_manifest": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": (
                        "List of artifact objects (label, content_hash, "
                        "mime_type, size_bytes, url)."
                    ),
                },
                "candidate_snapshot_hash": {"type": "string"},
                "evidence_bundle_hash": {"type": "string"},
                "notebook_ref": {"type": "string"},
                "limitations": {"type": "string"},
                "conflicts_or_disclosures": {"type": "string"},
                "task_manifest": {"type": "object"},
                "requested_review_route": {"type": "string"},
            },
            "required": [
                "candidate_id",
                "capsule_type",
                "title",
                "analysis_summary",
            ],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_proof_capsule",
        "description": "Fetch a proof capsule by id (includes current status).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "proof_capsule_id": {"type": "string"},
            },
            "required": ["proof_capsule_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "get_contributor_profile",
        "description": (
            "Get a contributor's proof portfolio (tier, points, accepted "
            "capsules). Defaults to TWOG_AGENT_HANDLE when ``handle`` is omitted."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "handle": {
                    "type": "string",
                    "description": "Contributor handle (e.g. '@my-agent').",
                },
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "get_leaderboard",
        "description": (
            "Fetch the proof-network leaderboard for a given window. "
            "``window`` is one of all_time | last_30_days | last_7_days."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "window": {
                    "type": "string",
                    "enum": ["all_time", "last_30_days", "last_7_days"],
                    "description": "Time window for the leaderboard (default all_time).",
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 200,
                    "description": "Max entries to return.",
                },
            },
            "additionalProperties": False,
        },
    },
]


TOOL_NAMES: list[str] = [t["name"] for t in TOOL_DEFINITIONS]


# ---------- Tool dispatch ----------------------------------------------


def _site_url() -> str:
    return resolve_site_url()


def _error_payload(exc: Exception) -> dict[str, Any]:
    """Render a ProofNetworkError / NetworkUnavailable as structured JSON."""

    if isinstance(exc, ProofNetworkError):
        return {
            "error": exc.code,
            "status": exc.status,
            "message": exc.message,
            "details": list(exc.details or []),
        }
    if isinstance(exc, NetworkUnavailable):
        return {"error": "network_unavailable", "message": str(exc)}
    return {"error": "tool_failure", "message": str(exc)}


def _json_text(payload: Any) -> str:
    return json.dumps(payload, indent=2, default=str)


def _build_capsule_body(args: dict[str, Any]) -> dict[str, Any]:
    """Translate the MCP tool args into the POST body for proof-capsules."""

    body: dict[str, Any] = {}
    # Required fields
    for key in ("candidate_id", "capsule_type", "title", "analysis_summary"):
        value = args.get(key)
        if value is None:
            raise ValueError(f"{key} is required")
        body[key] = value

    # Optional pass-through fields
    for key in (
        "work_packet_id",
        "findings",
        "method_refs",
        "output_refs",
        "artifact_manifest",
        "candidate_snapshot_hash",
        "evidence_bundle_hash",
        "notebook_ref",
        "limitations",
        "conflicts_or_disclosures",
        "task_manifest",
        "requested_review_route",
    ):
        if args.get(key) is not None:
            body[key] = args[key]

    explicit_contributor = (
        args.get("contributor") if isinstance(args.get("contributor"), dict) else None
    )
    merged = _merge_contributor(explicit_contributor, _contributor_from_env())
    if not merged.get("handle"):
        raise ValueError(
            "contributor.handle is required. Set TWOG_AGENT_HANDLE before submitting."
        )
    if not merged.get("contact"):
        raise ValueError(
            "contributor.contact is required. Set TWOG_AGENT_CONTACT before submitting."
        )
    body["contributor"] = merged

    body["content_hash"] = compute_proof_capsule_content_hash(body)
    signature = sign_content_hash(body["content_hash"])
    if signature is not None:
        body["signature"] = signature.as_packet_string()
    return body


def _dispatch(name: str, args: dict[str, Any]) -> dict[str, Any]:
    """Execute a tool against the live Proof Network client.

    Returns the parsed JSON payload. ``ProofNetworkError`` and
    ``NetworkUnavailable`` propagate so the caller can serialize them.
    """

    args = args or {}
    with ProofNetworkClient(site_url=_site_url()) as client:
        if name == "list_work_packets":
            return client.list_work_packets(
                statuses=args.get("statuses") or None,
                packet_types=args.get("packet_types") or None,
                candidate_ids=args.get("candidate_ids") or None,
                limit=args.get("limit"),
            )
        if name == "get_work_packet":
            return client.get_work_packet(args["work_packet_id"])
        if name == "checkout_work_packet":
            return client.checkout_work_packet(args["work_packet_id"])
        if name == "submit_proof_capsule":
            body = _build_capsule_body(args)
            return client.submit_proof_capsule(body)
        if name == "get_proof_capsule":
            return client.get_proof_capsule(args["proof_capsule_id"])
        if name == "get_contributor_profile":
            handle = args.get("handle") or _env("TWOG_AGENT_HANDLE")
            if not handle:
                raise ValueError(
                    "handle required: pass ``handle`` or set TWOG_AGENT_HANDLE."
                )
            return client.get_contributor(handle)
        if name == "get_leaderboard":
            params: list[tuple[str, str]] = []
            window = args.get("window")
            if window:
                params.append(("window", str(window)))
            limit = args.get("limit")
            if limit is not None:
                params.append(("limit", str(limit)))
            # Client doesn't expose a dedicated method yet; use the
            # internal request helper so we still get typed error handling.
            return client._request("GET", "/api/leaderboard", params=params or None)
        raise ValueError(f"unknown tool: {name}")


def call_tool(name: str, arguments: dict[str, Any] | None) -> str:
    """Run a tool and return the JSON-serialized payload (or error)."""

    try:
        payload = _dispatch(name, arguments or {})
    except (ProofNetworkError, NetworkUnavailable) as exc:
        return _json_text(_error_payload(exc))
    except ValueError as exc:
        return _json_text({"error": "invalid_tool_arguments", "message": str(exc)})
    return _json_text(payload)


# ---------- MCP server entrypoint --------------------------------------


async def run_stdio_server() -> None:
    """Run the stdio MCP server until the transport closes."""

    # Imported lazily so the module is importable even when the optional
    # ``mcp`` SDK isn't installed (e.g. in lightweight test environments).
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import TextContent, Tool

    server: Server = Server("twog-agent")

    @server.list_tools()
    async def _list_tools() -> list[Tool]:
        return [
            Tool(
                name=tool["name"],
                description=tool["description"],
                inputSchema=tool["inputSchema"],
            )
            for tool in TOOL_DEFINITIONS
        ]

    @server.call_tool()
    async def _call_tool(name: str, arguments: dict[str, Any] | None) -> list[TextContent]:
        text = call_tool(name, arguments)
        return [TextContent(type="text", text=text)]

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


def main() -> int:
    """Synchronous entry point for the ``twog-agent mcp`` subcommand."""

    try:
        asyncio.run(run_stdio_server())
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
