"""Tests for the demo persona orchestrator.

The orchestrator's I/O (HTTP, signing) is intentionally thin; the
interesting behavior is in the pure functions (overdue selection,
packet matching, template rendering, key derivation). We pin those
here and confirm idempotency by signing the same persona+packet
twice and checking the content_hash matches.
"""

from __future__ import annotations

import base64
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import run_demo_personas as orch  # noqa: E402


# ---------- Personas catalog ------------------------------------------


def test_personas_json_matches_dataclass_schema() -> None:
    """Every committed persona must round-trip through load_personas."""
    personas = orch.load_personas()
    assert len(personas) >= 5  # we ship 8
    handles = [p.handle for p in personas]
    assert len(handles) == len(set(handles)), "duplicate handles in personas.json"
    specialties = {p.specialty for p in personas}
    # Each persona's specialty must have a capsule template
    for p in personas:
        assert p.specialty in orch.CAPSULE_TEMPLATES, (
            f"persona {p.handle} has specialty {p.specialty!r} with no template"
        )
    # Spread: at least 3 distinct specialties
    assert len(specialties) >= 3
    # Spread: at least 2 kinds (human/agent/team/lab)
    kinds = {p.kind for p in personas}
    assert len(kinds) >= 2


# ---------- Key derivation --------------------------------------------


def test_persona_keys_are_deterministic() -> None:
    seed = base64.b64encode(b"\x01" * 32).decode("ascii")
    a1 = orch.persona_privkey_b64(seed, "@demo-saanvi-citations")
    a2 = orch.persona_privkey_b64(seed, "@demo-saanvi-citations")
    b1 = orch.persona_privkey_b64(seed, "@demo-akira-claims")
    assert a1 == a2, "same (seed, handle) must always produce same key"
    assert a1 != b1, "different handles must produce different keys"


def test_persona_keys_change_when_master_seed_rotates() -> None:
    seed_a = base64.b64encode(b"\x01" * 32).decode("ascii")
    seed_b = base64.b64encode(b"\x02" * 32).decode("ascii")
    handle = "@demo-saanvi-citations"
    assert orch.persona_privkey_b64(seed_a, handle) != orch.persona_privkey_b64(seed_b, handle)


def test_persona_pubkey_matches_privkey() -> None:
    """The derived public key must correspond to the derived private key."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    seed = base64.b64encode(b"\xab" * 32).decode("ascii")
    handle = "@demo-test"
    priv_b64 = orch.persona_privkey_b64(seed, handle)
    pub_b64 = orch.persona_pubkey_b64(seed, handle)

    sk = Ed25519PrivateKey.from_private_bytes(base64.b64decode(priv_b64))
    expected_pub = sk.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    assert base64.b64decode(pub_b64) == expected_pub


def test_persona_seed_rejects_tiny_master() -> None:
    with pytest.raises(ValueError):
        orch.derive_persona_seed(base64.b64encode(b"short").decode(), "@x")


# ---------- Overdue selection -----------------------------------------


def _make_persona(handle: str, cadence: int = 4) -> orch.Persona:
    return orch.Persona(
        handle=handle,
        name=handle,
        kind="agent",
        affiliation="test",
        specialty="citation_repair",
        cadence_hours=cadence,
        blurb="",
    )


def test_pick_overdue_persona_returns_none_when_no_one_overdue() -> None:
    now = datetime(2026, 5, 22, 12, 0, tzinfo=timezone.utc)
    personas = [_make_persona("@a", cadence=4)]
    last = {"@a": now - timedelta(hours=1)}  # only 1h ago, cadence is 4h
    assert orch.pick_overdue_persona(personas, last, now) is None


def test_pick_overdue_persona_returns_never_submitted_as_infinitely_overdue() -> None:
    now = datetime(2026, 5, 22, 12, 0, tzinfo=timezone.utc)
    p_a = _make_persona("@a", cadence=4)
    p_b = _make_persona("@b", cadence=4)
    last = {"@a": now - timedelta(hours=10), "@b": None}
    chosen = orch.pick_overdue_persona([p_a, p_b], last, now)
    assert chosen is not None
    assert chosen.handle == "@b"  # never submitted beats 10h ago


def test_pick_overdue_persona_picks_most_stale_when_multiple_overdue() -> None:
    now = datetime(2026, 5, 22, 12, 0, tzinfo=timezone.utc)
    p_a = _make_persona("@a", cadence=4)
    p_b = _make_persona("@b", cadence=4)
    last = {"@a": now - timedelta(hours=5), "@b": now - timedelta(hours=20)}
    chosen = orch.pick_overdue_persona([p_a, p_b], last, now)
    assert chosen is not None
    assert chosen.handle == "@b"


def test_pick_overdue_persona_respects_per_persona_cadence() -> None:
    now = datetime(2026, 5, 22, 12, 0, tzinfo=timezone.utc)
    fast = _make_persona("@fast", cadence=3)
    slow = _make_persona("@slow", cadence=24)
    last = {"@fast": now - timedelta(hours=4), "@slow": now - timedelta(hours=4)}
    chosen = orch.pick_overdue_persona([fast, slow], last, now)
    assert chosen is not None
    assert chosen.handle == "@fast"  # slow is not overdue at 4h with 24h cadence


# ---------- Packet selection ------------------------------------------


def test_pick_packet_matches_persona_specialty() -> None:
    persona = orch.Persona(
        handle="@a", name="a", kind="agent", affiliation="x",
        specialty="claim_critique", cadence_hours=4, blurb="",
    )
    packets = [
        {"work_packet_id": "p1", "packet_type": "citation_repair", "candidate_id": "twog-c-1"},
        {"work_packet_id": "p2", "packet_type": "claim_critique", "candidate_id": "twog-c-2"},
    ]
    chosen = orch.pick_packet_for_persona(persona, packets, set())
    assert chosen is not None
    assert chosen["work_packet_id"] == "p2"


def test_pick_packet_skips_already_submitted() -> None:
    persona = _make_persona("@a")
    packets = [
        {"work_packet_id": "p1", "packet_type": "citation_repair", "candidate_id": "twog-c-1"},
        {"work_packet_id": "p2", "packet_type": "citation_repair", "candidate_id": "twog-c-2"},
    ]
    chosen = orch.pick_packet_for_persona(persona, packets, {"p1"})
    assert chosen is not None
    assert chosen["work_packet_id"] == "p2"


def test_pick_packet_returns_none_when_nothing_matches() -> None:
    persona = _make_persona("@a")
    packets = [
        {"work_packet_id": "p1", "packet_type": "claim_critique", "candidate_id": "x"},
    ]
    assert orch.pick_packet_for_persona(persona, packets, set()) is None


def test_overdue_personas_returns_most_stale_first() -> None:
    now = datetime(2026, 5, 22, 12, 0, tzinfo=timezone.utc)
    p_mid = _make_persona("@mid", cadence=4)
    p_old = _make_persona("@old", cadence=4)
    p_fresh = _make_persona("@fresh", cadence=4)
    last = {
        "@mid": now - timedelta(hours=6),
        "@old": now - timedelta(hours=30),
        "@fresh": now - timedelta(hours=1),
    }
    queue = orch.overdue_personas([p_mid, p_old, p_fresh], last, now)
    assert [p.handle for p in queue] == ["@old", "@mid"]


def test_overdue_personas_handles_never_submitted_as_infinite_staleness() -> None:
    now = datetime(2026, 5, 22, 12, 0, tzinfo=timezone.utc)
    p_never = _make_persona("@never", cadence=4)
    p_old = _make_persona("@old", cadence=4)
    last = {"@never": None, "@old": now - timedelta(hours=30)}
    queue = orch.overdue_personas([p_never, p_old], last, now)
    assert [p.handle for p in queue] == ["@never", "@old"]


# ---------- Capsule template rendering --------------------------------


def test_build_capsule_packet_passes_quality_thresholds() -> None:
    """The server rejects analysis_summary < 80 chars or < 12 words. Each
    rendered template must pass these gates."""
    for specialty in orch.CAPSULE_TEMPLATES:
        persona = orch.Persona(
            handle=f"@demo-{specialty}", name="Test", kind="agent",
            affiliation="test", specialty=specialty, cadence_hours=4, blurb="",
        )
        packet = {
            "work_packet_id": "p1",
            "candidate_id": "twog-candidate-447eb8089965",
            "packet_type": specialty,
        }
        body = orch.build_capsule_packet(persona, packet)
        assert len(body["title"]) >= 6, specialty
        assert len(body["analysis_summary"]) >= 80, specialty
        assert len(body["analysis_summary"].split()) >= 12, specialty
        assert body["contributor"]["handle"] == persona.handle
        assert body["capsule_type"] == specialty
        assert body["work_packet_id"] == "p1"
        assert body["candidate_id"] == "twog-candidate-447eb8089965"


def test_build_capsule_packet_unknown_specialty_raises() -> None:
    persona = orch.Persona(
        handle="@a", name="a", kind="agent", affiliation="x",
        specialty="never_heard_of_this", cadence_hours=4, blurb="",
    )
    packet = {"work_packet_id": "p", "candidate_id": "twog-c", "packet_type": "x"}
    with pytest.raises(ValueError):
        orch.build_capsule_packet(persona, packet)


# ---------- Idempotency ----------------------------------------------


def test_content_hash_handles_non_ascii_to_match_ts_server() -> None:
    """Regression: Python json.dumps defaults to ensure_ascii=True and
    escapes non-ASCII characters (`—` → `\\u2014`), while the TS server's
    canonicalJsonStringify keeps them literal. Without ensure_ascii=False
    on the Python side, a capsule containing an em-dash hashes
    differently in Python vs TS — and the ed25519 signature, which is
    over the Python hash, fails to verify on submission.

    This test pins a body containing several non-ASCII characters to a
    known TS-reference hash. If this test ever breaks, the Python and
    TS canonicalization functions have drifted again.
    """
    from twog_agent.content_hash import compute_proof_capsule_content_hash

    body = {
        "candidate_id": "twog-candidate-abc",
        "work_packet_id": "11111111-1111-1111-1111-111111111111",
        "capsule_type": "citation_repair",
        "title": "Citação reparo — caso é",  # Portuguese w/ em-dash
        "analysis_summary": "Smãll body — con accents éá, em-dash, plus a curly quote “yes”.",
        "findings": "",
        "limitations": "",
        "candidate_snapshot_hash": None,
        "evidence_bundle_hash": None,
        "method_refs": [],
        "output_refs": [],
        "notebook_ref": None,
        "artifact_manifest": [],
        "contributor": {"handle": "@nó-ascii", "kind": "agent", "contact": "x@y.z"},
    }
    h = compute_proof_capsule_content_hash(body)
    # Reference value computed against the TS canonicalJsonStringify path
    # in twog/lib/proof-capsules.ts. If we ever change the digest_payload
    # shape, this value updates only after also updating the TS path.
    assert h == "sha256:8d2903ea09afe62e60635f3ac7d77a4be4f44bcf472738d397c09958204867ea", (
        f"non-ASCII content_hash drifted from TS reference: {h}"
    )


def test_sign_capsule_packet_is_deterministic_for_same_input() -> None:
    """Re-signing the same persona+packet must yield the same content_hash,
    so the server's content_hash-based dedup makes the orchestrator
    idempotent across reruns."""
    seed = base64.b64encode(b"\xc1" * 32).decode("ascii")
    persona = orch.Persona(
        handle="@demo-idem", name="Idem", kind="agent", affiliation="x",
        specialty="citation_repair", cadence_hours=4, blurb="",
    )
    packet = {
        "work_packet_id": "00000000-0000-0000-0000-000000000001",
        "candidate_id": "twog-candidate-deadbeef0000",
        "packet_type": "citation_repair",
    }
    body = orch.build_capsule_packet(persona, packet)
    privkey = orch.persona_privkey_b64(seed, persona.handle)
    a = orch.sign_capsule_packet(body, privkey)
    b = orch.sign_capsule_packet(body, privkey)
    assert a["content_hash"] == b["content_hash"]
    # Note: ed25519 signatures CAN be deterministic (RFC 8032), so
    # signatures should also match. If a future crypto lib swaps to
    # randomized ed25519ph or similar, this assertion is the canary.
    assert a["signature"] == b["signature"]
