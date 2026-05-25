"""Unit tests for the twog-agent CLI.

These tests mock at the ``ProofNetworkClient`` boundary so we never rely
on a live HTTP server. They verify the CLI's four load-bearing
contracts: exit codes, JSON output shape, identity-merge behavior, and
the client-side content hash matching the server.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from hsa_research.ingestion_bridge.contracts import (
    ProofCapsuleArtifact,
    ProofCapsuleContributor,
    ProofCapsuleRecord,
)
from twog_agent import cli as cli_module
from twog_agent.client import NetworkUnavailable, ProofNetworkError
from twog_agent.content_hash import compute_proof_capsule_content_hash
from twog_agent.exits import Exit


# ---------- Fixtures ----------------------------------------------------


@pytest.fixture
def mocked_client(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Patch ``ProofNetworkClient`` so the CLI never opens a real socket."""

    mock = MagicMock()
    # Context manager support: with ProofNetworkClient(...) as client -> mock
    mock.__enter__.return_value = mock
    mock.__exit__.return_value = False
    monkeypatch.setattr(cli_module, "ProofNetworkClient", lambda **kwargs: mock)
    return mock


@pytest.fixture
def env_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TWOG_AGENT_HANDLE", "@test-agent")
    monkeypatch.setenv("TWOG_AGENT_CONTACT", "test@example.com")
    monkeypatch.setenv("TWOG_AGENT_KIND", "agent")
    monkeypatch.setenv("TWOG_AGENT_NAME", "Test Agent")
    monkeypatch.delenv("TWOG_AGENT_PRIVKEY", raising=False)


def _capture_stdout(monkeypatch: pytest.MonkeyPatch) -> io.StringIO:
    buffer = io.StringIO()
    monkeypatch.setattr(cli_module.sys, "stdout", buffer)
    return buffer


# ---------- packets list ------------------------------------------------


def test_packets_list_returns_json_and_success(
    mocked_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    mocked_client.list_work_packets.return_value = {
        "work_packet_count": 1,
        "work_packets": [
            {
                "work_packet_id": "11111111-1111-1111-1111-111111111111",
                "candidate_id": "twog-candidate-x",
                "packet_type": "citation_repair",
                "title": "Test packet",
                "difficulty": "light",
            }
        ],
    }
    stdout = _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["packets", "list", "--limit", "5"])

    assert exit_code == Exit.SUCCESS
    payload = json.loads(stdout.getvalue())
    assert payload["work_packet_count"] == 1
    mocked_client.list_work_packets.assert_called_once()


def test_packets_list_passes_filters_through(
    mocked_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    mocked_client.list_work_packets.return_value = {"work_packet_count": 0, "work_packets": []}
    _capture_stdout(monkeypatch)

    cli_module.main(
        [
            "packets",
            "list",
            "--status",
            "open",
            "--type",
            "citation_repair",
            "--type",
            "claim_critique",
            "--candidate-id",
            "twog-candidate-a",
            "--limit",
            "7",
        ]
    )

    call = mocked_client.list_work_packets.call_args
    assert call.kwargs["statuses"] == ["open"]
    assert call.kwargs["packet_types"] == ["citation_repair", "claim_critique"]
    assert call.kwargs["candidate_ids"] == ["twog-candidate-a"]
    assert call.kwargs["limit"] == 7


def test_packets_list_storage_unconfigured_exits_three(
    mocked_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    mocked_client.list_work_packets.side_effect = ProofNetworkError(
        code="work_packet_storage_not_configured",
        status=503,
        message="db not set",
    )
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["packets", "list"])

    assert exit_code == Exit.STORAGE_NOT_CONFIGURED


def test_packets_list_network_failure_exits_eight(
    mocked_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    mocked_client.list_work_packets.side_effect = NetworkUnavailable("connection refused")
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["packets", "list"])

    assert exit_code == Exit.NETWORK_ERROR


# ---------- packets checkout --------------------------------------------


def test_packets_checkout_not_found_exits_four(
    mocked_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    mocked_client.checkout_work_packet.side_effect = ProofNetworkError(
        code="work_packet_not_found", status=404
    )
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["packets", "checkout", "00000000-0000-0000-0000-000000000000"])

    assert exit_code == Exit.NOT_FOUND


def test_packets_checkout_writes_to_out_file(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    mocked_client.checkout_work_packet.return_value = {"work_packet": {"work_packet_id": "abc"}}
    _capture_stdout(monkeypatch)
    out = tmp_path / "checkout.json"

    exit_code = cli_module.main(
        ["packets", "checkout", "abc", "--out", str(out)],
    )

    assert exit_code == Exit.SUCCESS
    assert json.loads(out.read_text())["work_packet"]["work_packet_id"] == "abc"


# ---------- capsule submit ----------------------------------------------


def _write_capsule(tmp_path: Path, **overrides: Any) -> Path:
    body = {
        "capsule_type": "citation_repair",
        "title": "Test capsule for CLI",
        "analysis_summary": "This is a smoke test capsule body covering required field lengths.",
        "method_refs": ["pubmed"],
        "output_refs": [],
        "artifact_manifest": [
            {"label": "smoke", "content_hash": "sha256:smoke12345"}
        ],
    }
    body.update(overrides)
    path = tmp_path / "capsule.json"
    path.write_text(json.dumps(body))
    return path


def test_capsule_submit_succeeds_and_returns_capsule(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_identity: None,
) -> None:
    capsule = _write_capsule(tmp_path, candidate_id="twog-candidate-x")
    mocked_client.submit_proof_capsule.return_value = {
        "proof_capsule": {
            "proof_capsule_id": "22222222-2222-2222-2222-222222222222",
            "content_hash": "sha256:abc",
            "status": "submitted",
            "status_url": "/api/proof-capsules/22222222-2222-2222-2222-222222222222",
        }
    }
    stdout = _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["capsule", "submit", "--file", str(capsule)])

    assert exit_code == Exit.SUCCESS
    payload = json.loads(stdout.getvalue())
    assert payload["proof_capsule"]["proof_capsule_id"] == "22222222-2222-2222-2222-222222222222"
    # Env-driven contributor identity was merged into the submitted body.
    submitted_body = mocked_client.submit_proof_capsule.call_args.args[0]
    assert submitted_body["contributor"]["handle"] == "@test-agent"
    assert submitted_body["contributor"]["contact"] == "test@example.com"
    assert submitted_body["candidate_id"] == "twog-candidate-x"


def test_capsule_submit_invalid_packet_exits_five(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_identity: None,
) -> None:
    capsule = _write_capsule(tmp_path, candidate_id="twog-candidate-x")
    mocked_client.submit_proof_capsule.side_effect = ProofNetworkError(
        code="invalid_proof_capsule_submission",
        status=400,
        message="capsule is invalid",
        details=["title too short"],
    )
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["capsule", "submit", "--file", str(capsule)])

    assert exit_code == Exit.INVALID_PACKET


def test_capsule_submit_invalid_signature_exits_five(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_identity: None,
) -> None:
    """A bad signature is the same shape as any other invalid packet."""

    capsule = _write_capsule(tmp_path, candidate_id="twog-candidate-x")
    mocked_client.submit_proof_capsule.side_effect = ProofNetworkError(
        code="invalid_proof_capsule_signature",
        status=400,
        message="signature did not verify",
    )
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["capsule", "submit", "--file", str(capsule)])

    assert exit_code == Exit.INVALID_PACKET


def test_capsule_submit_too_large_exits_five(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_identity: None,
) -> None:
    capsule = _write_capsule(tmp_path, candidate_id="twog-candidate-x")
    mocked_client.submit_proof_capsule.side_effect = ProofNetworkError(
        code="proof_capsule_submission_too_large",
        status=413,
    )
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["capsule", "submit", "--file", str(capsule)])

    assert exit_code == Exit.INVALID_PACKET


def test_capsule_submit_rate_limited_exits_ten(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_identity: None,
) -> None:
    """429 rate-limit is its own exit code so the wrapper can back off."""

    capsule = _write_capsule(tmp_path, candidate_id="twog-candidate-x")
    mocked_client.submit_proof_capsule.side_effect = ProofNetworkError(
        code="proof_capsule_rate_limit_exceeded",
        status=429,
        message="too many submissions",
        details=["handle=@x", "current=61", "limit=60"],
    )
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["capsule", "submit", "--file", str(capsule)])

    assert exit_code == Exit.RATE_LIMITED


def test_capsule_submit_missing_handle_exits_five(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Missing handle should fail fast before any HTTP call."""

    monkeypatch.delenv("TWOG_AGENT_HANDLE", raising=False)
    monkeypatch.setenv("TWOG_AGENT_CONTACT", "test@example.com")
    capsule = _write_capsule(tmp_path, candidate_id="twog-candidate-x")
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["capsule", "submit", "--file", str(capsule)])

    assert exit_code == Exit.INVALID_PACKET
    mocked_client.submit_proof_capsule.assert_not_called()


def test_capsule_submit_missing_candidate_id_exits_five(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_identity: None,
) -> None:
    capsule = _write_capsule(tmp_path)  # no candidate_id, no checkout
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["capsule", "submit", "--file", str(capsule)])

    assert exit_code == Exit.INVALID_PACKET
    mocked_client.submit_proof_capsule.assert_not_called()


def test_capsule_submit_resolves_candidate_from_checkout_file(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_identity: None,
) -> None:
    capsule = _write_capsule(tmp_path)  # no candidate_id in capsule
    checkout = tmp_path / "checkout.json"
    checkout.write_text(
        json.dumps(
            {
                "candidate": {
                    "candidate_id": "twog-candidate-resolved",
                    "snapshot_content_hash": "sha256:snap",
                },
                "evidence_bundle_summary": {"snapshot": {"content_hash": "sha256:bundle"}},
            }
        )
    )
    mocked_client.submit_proof_capsule.return_value = {
        "proof_capsule": {"proof_capsule_id": "x", "status": "submitted"}
    }
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(
        ["capsule", "submit", "--file", str(capsule), "--checkout", str(checkout)]
    )

    assert exit_code == Exit.SUCCESS
    body = mocked_client.submit_proof_capsule.call_args.args[0]
    assert body["candidate_id"] == "twog-candidate-resolved"
    assert body["candidate_snapshot_hash"] == "sha256:snap"
    assert body["evidence_bundle_hash"] == "sha256:bundle"


# ---------- capsule status ----------------------------------------------


def test_capsule_status_accepted_exits_zero(
    mocked_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    mocked_client.get_proof_capsule.return_value = {
        "proof_capsule": {"proof_capsule_id": "x", "status": "accepted"}
    }
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["capsule", "status", "x"])

    assert exit_code == Exit.SUCCESS


def test_capsule_status_rejected_exits_six(
    mocked_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    mocked_client.get_proof_capsule.return_value = {
        "proof_capsule": {"proof_capsule_id": "x", "status": "rejected"}
    }
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["capsule", "status", "x"])

    assert exit_code == Exit.REJECTED


def test_capsule_status_needs_changes_exits_seven(
    mocked_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    mocked_client.get_proof_capsule.return_value = {
        "proof_capsule": {"proof_capsule_id": "x", "status": "needs_changes"}
    }
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["capsule", "status", "x"])

    assert exit_code == Exit.NEEDS_CHANGES


def test_capsule_status_wait_polls_until_terminal(
    mocked_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    sequence = iter(
        [
            {"proof_capsule": {"proof_capsule_id": "x", "status": "submitted"}},
            {"proof_capsule": {"proof_capsule_id": "x", "status": "in_review"}},
            {"proof_capsule": {"proof_capsule_id": "x", "status": "accepted"}},
        ]
    )
    mocked_client.get_proof_capsule.side_effect = lambda _id: next(sequence)
    # Make sleep a no-op so the test is instant.
    monkeypatch.setattr(cli_module.time, "sleep", lambda _seconds: None)
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(
        ["capsule", "status", "x", "--wait", "--timeout", "30", "--poll-interval", "0.1"]
    )

    assert exit_code == Exit.SUCCESS
    assert mocked_client.get_proof_capsule.call_count == 3


def test_capsule_status_wait_timeout_exits_nine(
    mocked_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    mocked_client.get_proof_capsule.return_value = {
        "proof_capsule": {"proof_capsule_id": "x", "status": "submitted"}
    }
    # Force time.time to advance past the deadline on the first check.
    times = iter([0.0, 100.0, 200.0, 300.0])
    monkeypatch.setattr(cli_module.time, "time", lambda: next(times))
    monkeypatch.setattr(cli_module.time, "sleep", lambda _seconds: None)
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(
        ["capsule", "status", "x", "--wait", "--timeout", "1", "--poll-interval", "0.1"]
    )

    assert exit_code == Exit.WAIT_TIMEOUT


# ---------- do pipeline -------------------------------------------------


def test_do_pipeline_submits_after_checkout(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_identity: None,
) -> None:
    mocked_client.checkout_work_packet.return_value = {
        "candidate": {
            "candidate_id": "twog-candidate-z",
            "snapshot_content_hash": "sha256:snap",
        },
        "evidence_bundle_summary": {"snapshot": {"content_hash": "sha256:bundle"}},
    }
    mocked_client.submit_proof_capsule.return_value = {
        "proof_capsule": {
            "proof_capsule_id": "33333333-3333-3333-3333-333333333333",
            "status": "submitted",
            "content_hash": "sha256:capsule",
        }
    }
    capsule = _write_capsule(tmp_path)  # no candidate_id; do resolves from checkout
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(
        ["do", "--packet", "packet-id", "--capsule", str(capsule)]
    )

    assert exit_code == Exit.SUCCESS
    mocked_client.checkout_work_packet.assert_called_once_with("packet-id")
    body = mocked_client.submit_proof_capsule.call_args.args[0]
    assert body["candidate_id"] == "twog-candidate-z"
    assert body["work_packet_id"] == "packet-id"
    assert body["candidate_snapshot_hash"] == "sha256:snap"


def test_do_wait_returns_accepted_verdict(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_identity: None,
) -> None:
    mocked_client.checkout_work_packet.return_value = {
        "candidate": {"candidate_id": "twog-candidate-z", "snapshot_content_hash": "sha256:s"},
    }
    mocked_client.submit_proof_capsule.return_value = {
        "proof_capsule": {"proof_capsule_id": "abc", "status": "submitted"}
    }
    sequence = iter(
        [
            {"proof_capsule": {"proof_capsule_id": "abc", "status": "in_review"}},
            {"proof_capsule": {"proof_capsule_id": "abc", "status": "accepted"}},
        ]
    )
    mocked_client.get_proof_capsule.side_effect = lambda _id: next(sequence)
    monkeypatch.setattr(cli_module.time, "sleep", lambda _seconds: None)
    capsule = _write_capsule(tmp_path)
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(
        ["do", "--packet", "p", "--capsule", str(capsule), "--wait", "--poll-interval", "0.1"]
    )

    assert exit_code == Exit.SUCCESS


# ---------- contributor whoami ------------------------------------------


def test_whoami_uses_env_handle_when_not_overridden(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    env_identity: None,
) -> None:
    mocked_client.get_contributor.return_value = {
        "handle": "@test-agent",
        "tier": "scout",
        "proof_points": 100,
        "summary": {"accepted_capsule_count": 1, "routed_capsule_count": 0, "candidate_count": 1, "reward_event_count": 1},
    }
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["contributor", "whoami"])

    assert exit_code == Exit.SUCCESS
    mocked_client.get_contributor.assert_called_once_with("@test-agent")


def test_whoami_missing_handle_exits_invalid_args(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TWOG_AGENT_HANDLE", raising=False)
    _capture_stdout(monkeypatch)

    exit_code = cli_module.main(["contributor", "whoami"])

    assert exit_code == Exit.INVALID_ARGS
    mocked_client.get_contributor.assert_not_called()


# ---------- Client / server hash parity ---------------------------------


def test_twog_agent_client_hash_matches_server() -> None:
    """The CLI's content_hash must equal the server's for the same body.

    This is the cross-language contract that makes signed submissions
    meaningful: an agent signs *their* computed hash, and the server's
    canonical hash on the same body must be byte-identical.
    """

    body = {
        "candidate_id": "  twog-candidate-447eb8089965  ",  # extra whitespace
        "work_packet_id": "11111111-1111-1111-1111-111111111111",
        "capsule_type": "citation_repair",
        "title": "  Repair citation C4 with primary source  ",
        "analysis_summary": (
            "  Verified the cited paper supports the strongest claim "
            "in the rationale and proposed a stronger replacement citation.  "
        ),
        "findings": "  Found a stronger source.  ",
        "limitations": "",
        "candidate_snapshot_hash": "  sha256:candidate-snap  ",
        "evidence_bundle_hash": "sha256:evidence-bundle",
        "method_refs": ["pubmed_search", "PubMed_Search", "doi_resolution"],  # casefold dedup
        "output_refs": [],
        "notebook_ref": None,
        "artifact_manifest": [
            {
                "label": "replacement_citation_doc",
                "content_hash": "sha256:artifact-1",
                "url": "https://example.com/paper",
                "mime_type": "application/pdf",
                "size_bytes": 12345,
            }
        ],
        "contributor": {
            "kind": "human",
            "name": "Test Reviewer",
            "handle": "@reviewer",
            "contact": "reviewer@example.com",
        },
    }
    client_hash = compute_proof_capsule_content_hash(body)

    record = ProofCapsuleRecord(
        work_packet_id=body["work_packet_id"],
        candidate_id=body["candidate_id"],
        capsule_type=body["capsule_type"],
        title=body["title"],
        analysis_summary=body["analysis_summary"],
        findings=body["findings"],
        limitations=body["limitations"],
        candidate_snapshot_hash=body["candidate_snapshot_hash"],
        evidence_bundle_hash=body["evidence_bundle_hash"],
        method_refs=list(body["method_refs"]),
        output_refs=list(body["output_refs"]),
        notebook_ref=body["notebook_ref"],
        artifact_manifest=[
            ProofCapsuleArtifact(
                label=body["artifact_manifest"][0]["label"],
                content_hash=body["artifact_manifest"][0]["content_hash"],
                url=body["artifact_manifest"][0]["url"],
                mime_type=body["artifact_manifest"][0]["mime_type"],
                size_bytes=body["artifact_manifest"][0]["size_bytes"],
            )
        ],
        contributor=ProofCapsuleContributor(**body["contributor"]),
    )

    assert client_hash == record.content_hash, (
        "CLI content_hash drifted from server. Re-port "
        "_proof_capsule_content_hash to twog_agent.content_hash."
    )


def test_twog_agent_client_hash_ignores_non_load_bearing_fields() -> None:
    """Two bodies that differ only in unhashed fields produce the same hash."""

    base = {
        "candidate_id": "twog-candidate-x",
        "capsule_type": "citation_repair",
        "title": "A capsule title",
        "analysis_summary": "Body of work over the rationale's strongest claim.",
        "method_refs": [],
        "output_refs": [],
        "artifact_manifest": [],
        "contributor": {"handle": "@a", "contact": "a@example.com"},
    }
    variant = dict(base)
    variant["task_manifest"] = {"step": "should not affect hash"}
    variant["conflicts_or_disclosures"] = "should not affect hash either"
    variant["requested_review_route"] = "accepted"
    variant["signature"] = "noise"
    assert compute_proof_capsule_content_hash(base) == compute_proof_capsule_content_hash(variant)


def test_twog_agent_client_hash_detects_artifact_swap() -> None:
    """Swapping an artifact's content_hash must change the capsule hash."""

    base = {
        "candidate_id": "twog-candidate-x",
        "capsule_type": "citation_repair",
        "title": "A capsule",
        "analysis_summary": "Body of work; long enough to satisfy contract.",
        "method_refs": [],
        "output_refs": [],
        "artifact_manifest": [{"label": "one", "content_hash": "sha256:a"}],
        "contributor": {"handle": "@a", "contact": "a@example.com"},
    }
    swapped = dict(base)
    swapped["artifact_manifest"] = [{"label": "one", "content_hash": "sha256:b"}]
    assert compute_proof_capsule_content_hash(base) != compute_proof_capsule_content_hash(swapped)


def test_capsule_submit_attaches_content_hash_when_missing(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_identity: None,
) -> None:
    """CLI must compute and attach content_hash before submit."""

    capsule = _write_capsule(tmp_path, candidate_id="twog-candidate-x")
    mocked_client.submit_proof_capsule.return_value = {
        "proof_capsule": {"proof_capsule_id": "abc", "status": "submitted"}
    }
    _capture_stdout(monkeypatch)

    cli_module.main(["capsule", "submit", "--file", str(capsule)])

    body = mocked_client.submit_proof_capsule.call_args.args[0]
    assert body.get("content_hash", "").startswith("sha256:")
    # Recomputing it must reproduce the same value.
    assert body["content_hash"] == compute_proof_capsule_content_hash(body)


def test_capsule_submit_does_not_overwrite_caller_content_hash(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_identity: None,
) -> None:
    """If the capsule file already declares a content_hash, keep it.

    This lets advanced callers (e.g. an agent that already hashed and
    signed at an earlier stage) ship the canonical hash through unchanged.
    """

    capsule = _write_capsule(
        tmp_path,
        candidate_id="twog-candidate-x",
        content_hash="sha256:caller-supplied-hash",
    )
    mocked_client.submit_proof_capsule.return_value = {
        "proof_capsule": {"proof_capsule_id": "abc", "status": "submitted"}
    }
    _capture_stdout(monkeypatch)

    cli_module.main(["capsule", "submit", "--file", str(capsule)])

    body = mocked_client.submit_proof_capsule.call_args.args[0]
    assert body["content_hash"] == "sha256:caller-supplied-hash"


def test_capsule_submit_signs_when_privkey_set(
    mocked_client: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    env_identity: None,
) -> None:
    """With TWOG_AGENT_PRIVKEY set, the capsule submission carries a signature."""

    pytest.importorskip("cryptography")
    import base64
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    seed = b"\x42" * 32
    monkeypatch.setenv("TWOG_AGENT_PRIVKEY", base64.b64encode(seed).decode("ascii"))

    capsule = _write_capsule(tmp_path, candidate_id="twog-candidate-x")
    mocked_client.submit_proof_capsule.return_value = {
        "proof_capsule": {"proof_capsule_id": "abc", "status": "submitted"}
    }
    _capture_stdout(monkeypatch)

    cli_module.main(["capsule", "submit", "--file", str(capsule)])

    body = mocked_client.submit_proof_capsule.call_args.args[0]
    sig = body.get("signature")
    assert isinstance(sig, str) and sig.startswith("ed25519:")
    # Verify the signature against the body's content_hash.
    parts = sig.split(":")
    assert len(parts) == 3
    pub_bytes = base64.b64decode(parts[1])
    sig_bytes = base64.b64decode(parts[2])
    private_key = Ed25519PrivateKey.from_private_bytes(seed)
    # The public key in the signature packet should match the one derived
    # from our seed (deterministic for ed25519).
    assert pub_bytes == private_key.public_key().public_bytes_raw()
    # The signature must verify against the content_hash.
    private_key.public_key().verify(sig_bytes, body["content_hash"].encode("utf-8"))
