"""Unit tests for the twog-agent MCP server tool layer.

These tests exercise :func:`twog_agent.mcp_server.call_tool` (the sync
helper the async MCP server delegates to) by mocking the underlying
``ProofNetworkClient``. We never spin up a stdio transport — that's an
integration concern handled by the official ``mcp`` SDK. What we *do*
guarantee here:

- Every advertised tool is dispatched and returns a JSON payload.
- Identity env vars (``TWOG_AGENT_HANDLE`` etc.) flow into the capsule
  body the client sees.
- ``ProofNetworkError`` / ``NetworkUnavailable`` are surfaced as
  structured error JSON rather than raised across the MCP boundary.
- The advertised tool list contains exactly the contract Claude Desktop
  and Codex consume.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from twog_agent import mcp_server
from twog_agent.client import NetworkUnavailable, ProofNetworkError


# ---------- Fixtures ----------------------------------------------------


@pytest.fixture
def mocked_client(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Patch :class:`ProofNetworkClient` inside the MCP server module."""

    mock = MagicMock()
    mock.__enter__.return_value = mock
    mock.__exit__.return_value = False
    monkeypatch.setattr(mcp_server, "ProofNetworkClient", lambda **_: mock)
    return mock


@pytest.fixture
def env_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TWOG_AGENT_HANDLE", "@mcp-agent")
    monkeypatch.setenv("TWOG_AGENT_CONTACT", "mcp@example.com")
    monkeypatch.setenv("TWOG_AGENT_KIND", "agent")
    monkeypatch.setenv("TWOG_AGENT_NAME", "MCP Agent")
    monkeypatch.delenv("TWOG_AGENT_PRIVKEY", raising=False)


def _parse(text: str) -> dict[str, Any]:
    return json.loads(text)


# ---------- Tool surface contract ---------------------------------------


def test_tool_definitions_cover_full_surface() -> None:
    """The seven documented tools must all be advertised."""

    assert set(mcp_server.TOOL_NAMES) == {
        "list_work_packets",
        "get_work_packet",
        "checkout_work_packet",
        "submit_proof_capsule",
        "get_proof_capsule",
        "get_contributor_profile",
        "get_leaderboard",
    }


def test_tool_definitions_have_schemas() -> None:
    for tool in mcp_server.TOOL_DEFINITIONS:
        assert tool["name"]
        assert tool["description"]
        schema = tool["inputSchema"]
        assert schema["type"] == "object"
        assert "properties" in schema


# ---------- list_work_packets ------------------------------------------


def test_list_work_packets_returns_json(mocked_client: MagicMock) -> None:
    mocked_client.list_work_packets.return_value = {
        "work_packet_count": 1,
        "work_packets": [{"work_packet_id": "abc"}],
    }

    result = mcp_server.call_tool("list_work_packets", {"limit": 3})

    payload = _parse(result)
    assert payload["work_packet_count"] == 1
    mocked_client.list_work_packets.assert_called_once_with(
        statuses=None, packet_types=None, candidate_ids=None, limit=3
    )


def test_list_work_packets_passes_filters(mocked_client: MagicMock) -> None:
    mocked_client.list_work_packets.return_value = {"work_packet_count": 0, "work_packets": []}

    mcp_server.call_tool(
        "list_work_packets",
        {
            "statuses": ["open"],
            "packet_types": ["citation_repair"],
            "candidate_ids": ["twog-candidate-a"],
            "limit": 7,
        },
    )

    call = mocked_client.list_work_packets.call_args
    assert call.kwargs["statuses"] == ["open"]
    assert call.kwargs["packet_types"] == ["citation_repair"]
    assert call.kwargs["candidate_ids"] == ["twog-candidate-a"]
    assert call.kwargs["limit"] == 7


# ---------- get_work_packet / checkout ---------------------------------


def test_get_work_packet_returns_payload(mocked_client: MagicMock) -> None:
    mocked_client.get_work_packet.return_value = {"work_packet": {"work_packet_id": "abc"}}

    result = mcp_server.call_tool("get_work_packet", {"work_packet_id": "abc"})

    assert _parse(result)["work_packet"]["work_packet_id"] == "abc"
    mocked_client.get_work_packet.assert_called_once_with("abc")


def test_checkout_work_packet_returns_payload(mocked_client: MagicMock) -> None:
    mocked_client.checkout_work_packet.return_value = {
        "work_packet": {"work_packet_id": "abc"},
        "candidate": {"candidate_id": "twog-candidate-x"},
    }

    result = mcp_server.call_tool("checkout_work_packet", {"work_packet_id": "abc"})

    payload = _parse(result)
    assert payload["candidate"]["candidate_id"] == "twog-candidate-x"
    mocked_client.checkout_work_packet.assert_called_once_with("abc")


# ---------- submit_proof_capsule ---------------------------------------


def test_submit_proof_capsule_merges_env_identity(
    mocked_client: MagicMock, env_identity: None
) -> None:
    mocked_client.submit_proof_capsule.return_value = {
        "proof_capsule": {"proof_capsule_id": "cap-1", "status": "submitted"}
    }

    result = mcp_server.call_tool(
        "submit_proof_capsule",
        {
            "candidate_id": "twog-candidate-x",
            "capsule_type": "citation_repair",
            "title": "MCP smoke capsule",
            "analysis_summary": "Body of work, long enough to satisfy the validator.",
            "method_refs": ["pubmed"],
            "artifact_manifest": [{"label": "smoke", "content_hash": "sha256:smoke"}],
        },
    )

    payload = _parse(result)
    assert payload["proof_capsule"]["proof_capsule_id"] == "cap-1"
    body = mocked_client.submit_proof_capsule.call_args.args[0]
    assert body["contributor"]["handle"] == "@mcp-agent"
    assert body["contributor"]["contact"] == "mcp@example.com"
    assert body["contributor"]["name"] == "MCP Agent"
    assert body["candidate_id"] == "twog-candidate-x"
    assert body["content_hash"].startswith("sha256:")


def test_submit_proof_capsule_missing_handle_returns_invalid_args(
    mocked_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TWOG_AGENT_HANDLE", raising=False)
    monkeypatch.setenv("TWOG_AGENT_CONTACT", "x@example.com")

    result = mcp_server.call_tool(
        "submit_proof_capsule",
        {
            "candidate_id": "twog-candidate-x",
            "capsule_type": "citation_repair",
            "title": "no handle",
            "analysis_summary": "no handle should error before HTTP.",
        },
    )

    payload = _parse(result)
    assert payload["error"] == "invalid_tool_arguments"
    mocked_client.submit_proof_capsule.assert_not_called()


def test_submit_proof_capsule_proof_network_error_is_structured(
    mocked_client: MagicMock, env_identity: None
) -> None:
    mocked_client.submit_proof_capsule.side_effect = ProofNetworkError(
        code="invalid_proof_capsule_submission",
        status=400,
        message="capsule failed validation",
        details=["title too short"],
    )

    result = mcp_server.call_tool(
        "submit_proof_capsule",
        {
            "candidate_id": "twog-candidate-x",
            "capsule_type": "citation_repair",
            "title": "tiny",
            "analysis_summary": "Body of work; long enough to satisfy contract.",
        },
    )

    payload = _parse(result)
    assert payload == {
        "error": "invalid_proof_capsule_submission",
        "status": 400,
        "message": "capsule failed validation",
        "details": ["title too short"],
    }


# ---------- get_proof_capsule ------------------------------------------


def test_get_proof_capsule_returns_payload(mocked_client: MagicMock) -> None:
    mocked_client.get_proof_capsule.return_value = {
        "proof_capsule": {"proof_capsule_id": "cap-1", "status": "accepted"}
    }

    result = mcp_server.call_tool("get_proof_capsule", {"proof_capsule_id": "cap-1"})

    assert _parse(result)["proof_capsule"]["status"] == "accepted"
    mocked_client.get_proof_capsule.assert_called_once_with("cap-1")


def test_get_proof_capsule_network_unavailable_is_structured(
    mocked_client: MagicMock,
) -> None:
    mocked_client.get_proof_capsule.side_effect = NetworkUnavailable("connection refused")

    result = mcp_server.call_tool("get_proof_capsule", {"proof_capsule_id": "cap-1"})

    payload = _parse(result)
    assert payload["error"] == "network_unavailable"
    assert "connection refused" in payload["message"]


# ---------- get_contributor_profile ------------------------------------


def test_get_contributor_profile_uses_env_handle_by_default(
    mocked_client: MagicMock, env_identity: None
) -> None:
    mocked_client.get_contributor.return_value = {
        "handle": "@mcp-agent",
        "tier": "scout",
        "proof_points": 12,
    }

    result = mcp_server.call_tool("get_contributor_profile", {})

    payload = _parse(result)
    assert payload["handle"] == "@mcp-agent"
    mocked_client.get_contributor.assert_called_once_with("@mcp-agent")


def test_get_contributor_profile_explicit_handle_overrides(
    mocked_client: MagicMock, env_identity: None
) -> None:
    mocked_client.get_contributor.return_value = {"handle": "@other", "tier": "scout"}

    mcp_server.call_tool("get_contributor_profile", {"handle": "@other"})

    mocked_client.get_contributor.assert_called_once_with("@other")


def test_get_contributor_profile_missing_handle_returns_invalid_args(
    mocked_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TWOG_AGENT_HANDLE", raising=False)

    result = mcp_server.call_tool("get_contributor_profile", {})

    payload = _parse(result)
    assert payload["error"] == "invalid_tool_arguments"
    mocked_client.get_contributor.assert_not_called()


# ---------- get_leaderboard --------------------------------------------


def test_get_leaderboard_passes_window_and_limit(mocked_client: MagicMock) -> None:
    mocked_client._request.return_value = {
        "window": "last_7_days",
        "entries": [{"handle": "@top", "proof_points": 99}],
    }

    result = mcp_server.call_tool(
        "get_leaderboard", {"window": "last_7_days", "limit": 25}
    )

    payload = _parse(result)
    assert payload["window"] == "last_7_days"
    call = mocked_client._request.call_args
    assert call.args == ("GET", "/api/leaderboard")
    assert call.kwargs["params"] == [("window", "last_7_days"), ("limit", "25")]


def test_get_leaderboard_default_no_params(mocked_client: MagicMock) -> None:
    mocked_client._request.return_value = {"entries": []}

    mcp_server.call_tool("get_leaderboard", {})

    call = mocked_client._request.call_args
    assert call.kwargs["params"] is None


def test_get_leaderboard_error_is_structured(mocked_client: MagicMock) -> None:
    mocked_client._request.side_effect = ProofNetworkError(
        code="leaderboard_not_configured", status=503
    )

    result = mcp_server.call_tool("get_leaderboard", {})

    payload = _parse(result)
    assert payload["error"] == "leaderboard_not_configured"
    assert payload["status"] == 503


# ---------- unknown tool -----------------------------------------------


def test_unknown_tool_returns_structured_error(mocked_client: MagicMock) -> None:
    result = mcp_server.call_tool("not_a_tool", {})

    payload = _parse(result)
    assert payload["error"] == "invalid_tool_arguments"
    assert "unknown tool" in payload["message"]


# ---------- identity helper --------------------------------------------


def test_contributor_from_env_strips_whitespace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TWOG_AGENT_HANDLE", "  @padded  ")
    monkeypatch.setenv("TWOG_AGENT_CONTACT", "  c@example.com  ")
    monkeypatch.setenv("TWOG_AGENT_NAME", "  Padded Name  ")

    contributor = mcp_server._contributor_from_env()

    assert contributor["handle"] == "@padded"
    assert contributor["contact"] == "c@example.com"
    assert contributor["name"] == "Padded Name"
    assert contributor["kind"] == "agent"
