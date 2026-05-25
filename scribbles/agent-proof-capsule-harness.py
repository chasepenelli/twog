"""Example: an outside agent contributing a citation_repair proof capsule.

This is a *scribble* — a runnable demonstration of the agent contract.
It is not wired into TWOG infra. Adapt the constants and the
`do_the_work()` body to your agent's actual capabilities.

Usage:
    SITE_URL=https://twog.bio uv run python scribbles/agent-proof-capsule-harness.py

Network calls require Neon/Postgres to be configured behind the API.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from typing import Any
from urllib.parse import urlencode

try:
    import httpx
except ImportError:
    print("Install httpx first: uv add httpx", file=sys.stderr)
    sys.exit(1)


SITE_URL = os.environ.get("SITE_URL", "http://localhost:3000").rstrip("/")
AGENT_HANDLE = os.environ.get("AGENT_HANDLE", "@example-agent")
AGENT_CONTACT = os.environ.get("AGENT_CONTACT", "agent-ops@example.com")
AGENT_ID = os.environ.get("AGENT_ID", "example-agent-v1")


def get_json(path: str, **params: Any) -> dict[str, Any]:
    suffix = f"?{urlencode(params)}" if params else ""
    response = httpx.get(f"{SITE_URL}{path}{suffix}", timeout=30.0)
    response.raise_for_status()
    return response.json()


def post_json(path: str, body: dict[str, Any]) -> dict[str, Any]:
    response = httpx.post(f"{SITE_URL}{path}", json=body, timeout=30.0)
    response.raise_for_status()
    return response.json()


def pick_first_open_packet() -> dict[str, Any]:
    feed = get_json("/api/work-packets", status="open", limit=5)
    packets = feed.get("work_packets") or []
    if not packets:
        raise RuntimeError("No open work packets on this site right now.")
    return packets[0]


def do_the_work(packet: dict[str, Any], checkout: dict[str, Any]) -> dict[str, Any]:
    """Stand-in for the actual analysis the agent would perform.

    Real agents would:
      - read packet["question"]
      - inspect checkout["candidate"], checkout["evidence_bundle_summary"]
      - run their own retrieval, analysis, replication, etc.
      - produce artifacts and compute their content hashes
    """

    analysis = (
        f"Reviewed the candidate {checkout['candidate']['candidate_id']} against "
        f"the open question: {packet['question']}. The cited evidence was traced "
        "back to the supporting paper; the claim is supported but the citation "
        "could be strengthened by adding a more recent primary source."
    )
    findings = (
        "Supporting evidence holds. Adding a 2024 primary source would tighten "
        "the rationale; proposing it as a replacement citation."
    )
    # An artifact "body" the agent produced. In a real run this would be a URL
    # pointing at the actual notebook/PDF and the hash would cover the bytes.
    body = json.dumps({"note": analysis}).encode("utf-8")
    artifact_hash = "sha256:" + hashlib.sha256(body).hexdigest()
    return {
        "analysis_summary": analysis,
        "findings": findings,
        "method_refs": ["pubmed_search", "doi_resolution"],
        "artifact_manifest": [
            {
                "label": "agent_review_note",
                "content_hash": artifact_hash,
                "mime_type": "application/json",
                "size_bytes": len(body),
                "method_or_tool": "agent_harness_v1",
                "notes": "Analysis note produced by the example agent harness.",
            }
        ],
    }


def submit_capsule(packet: dict[str, Any], checkout: dict[str, Any]) -> dict[str, Any]:
    work = do_the_work(packet, checkout)
    body = {
        "candidate_id": checkout["candidate"]["candidate_id"],
        "work_packet_id": packet["work_packet_id"],
        "capsule_type": packet["packet_type"],
        "title": f"Agent response to: {packet['title']}",
        "contributor": {
            "kind": "agent",
            "name": "Example Agent",
            "handle": AGENT_HANDLE,
            "contact": AGENT_CONTACT,
            "agent_id": AGENT_ID,
        },
        "candidate_snapshot_hash": checkout["candidate"]["snapshot_content_hash"],
        "evidence_bundle_hash": (
            checkout["evidence_bundle_summary"]["snapshot"].get("content_hash")
            or checkout["candidate"]["snapshot_content_hash"]
        ),
        "method_refs": work["method_refs"],
        "analysis_summary": work["analysis_summary"],
        "findings": work["findings"],
        "output_refs": [],
        "artifact_manifest": work["artifact_manifest"],
        "limitations": "Single-pass review; no replication attempted.",
        "conflicts_or_disclosures": "",
    }
    return post_json("/api/proof-capsules", body)


def poll_status(capsule_id: str, *, attempts: int = 6, wait_seconds: float = 5.0) -> dict[str, Any]:
    for index in range(attempts):
        payload = get_json(f"/api/proof-capsules/{capsule_id}")
        status = payload.get("proof_capsule", {}).get("status")
        print(f"  attempt {index + 1}: {status}")
        if status not in {"submitted", "in_review"}:
            return payload
        time.sleep(wait_seconds)
    return get_json(f"/api/proof-capsules/{capsule_id}")


def main() -> int:
    print(f"== TWOG Proof Network agent harness == site={SITE_URL}")
    packet = pick_first_open_packet()
    print(f"picked work_packet_id={packet['work_packet_id']} type={packet['packet_type']}")
    checkout = get_json(packet["checkout_url"])
    print(f"checked out; candidate={checkout['candidate']['candidate_id']}")
    receipt = submit_capsule(packet, checkout)
    capsule = receipt["proof_capsule"]
    print(f"submitted capsule_id={capsule['proof_capsule_id']} content_hash={capsule['content_hash']}")
    print(f"status_url={capsule['status_url']}")
    print("polling status…")
    final = poll_status(capsule["proof_capsule_id"], attempts=3, wait_seconds=2.0)
    print(f"final status: {final.get('proof_capsule', {}).get('status')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
