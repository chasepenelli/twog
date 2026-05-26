#!/usr/bin/env python3
"""Multi-agent onboarding demo.

Spins up several distinct synthetic Proof Network agent identities and
runs each through the full loop: install check → soul setup → packet
list → checkout → submit (signed) → status. Then the operator accepts
each capsule and the leaderboard reflects the new state.

Designed to be safe against the staging branch — every agent has its
own ephemeral ed25519 keypair and a clearly fake handle prefixed with
``@demo-``. The script never touches prod state.

Usage:
    HSA_DATABASE_URL='<staging-url>' TWOG_SITE_URL='http://localhost:3000' \\
      PYTHONPATH=src uv run python scripts/multi_agent_onboarding_demo.py
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SITE_URL = os.environ.get("TWOG_SITE_URL", "http://localhost:3000")
HSA_DATABASE_URL = os.environ.get("HSA_DATABASE_URL")


@dataclass
class DemoAgent:
    handle: str
    capsule_type: str
    display_name: str
    kind: str  # human | agent | team | lab | company
    affiliation: str | None
    privkey_b64: str


DEMO_AGENTS = [
    DemoAgent(
        handle="@demo-citation-agent",
        capsule_type="citation_repair",
        display_name="Citation Agent Demo",
        kind="agent",
        affiliation="demo-lab",
        privkey_b64="",  # filled per-run
    ),
    DemoAgent(
        handle="@demo-evidence-scout",
        capsule_type="evidence_addition",
        display_name="Evidence Scout Demo",
        kind="agent",
        affiliation="demo-lab",
        privkey_b64="",
    ),
    DemoAgent(
        handle="@demo-claim-critic",
        capsule_type="claim_critique",
        display_name="Claim Critic Demo",
        kind="human",
        affiliation="demo-university",
        privkey_b64="",
    ),
]


def fresh_keypair() -> str:
    return base64.b64encode(os.urandom(32)).decode("ascii")


def agent_env(agent: DemoAgent) -> dict[str, str]:
    env = os.environ.copy()
    env["TWOG_SITE_URL"] = SITE_URL
    env["TWOG_AGENT_HANDLE"] = agent.handle
    env["TWOG_AGENT_CONTACT"] = f"{agent.handle.lstrip('@')}@demo.example.com"
    env["TWOG_AGENT_KIND"] = agent.kind
    env["TWOG_AGENT_NAME"] = agent.display_name
    if agent.affiliation:
        env["TWOG_AGENT_AFFILIATION"] = agent.affiliation
    env["TWOG_AGENT_ID"] = f"{agent.handle.lstrip('@')}-v1"
    env["TWOG_AGENT_PRIVKEY"] = agent.privkey_b64
    return env


def run_cli(env: dict[str, str], *args: str, expect_json: bool = True) -> dict[str, Any]:
    cmd = ["uv", "run", "twog-agent", *args]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        return {"_exit": proc.returncode, "_stderr": proc.stderr.strip()}
    if not expect_json:
        return {"_exit": 0, "_stdout": proc.stdout}
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {"_exit": proc.returncode, "_raw": proc.stdout}


def make_capsule_for_agent(agent: DemoAgent, workdir: Path) -> Path:
    """Write a capsule JSON tailored to this agent's capsule_type."""

    bodies: dict[str, dict[str, Any]] = {
        "citation_repair": {
            "title": f"{agent.display_name}: repaired strongest citation",
            "analysis_summary": (
                "Traced the cited paper back to the strongest claim in the candidate "
                "rationale and verified it stands. Located a tighter 2024 primary source "
                "(doi:10.1038/s41586-demo-citation) that improves provenance and tightens "
                "the argument's anchor."
            ),
            "findings": (
                "Replacement DOI proposed: 10.1038/s41586-demo-citation. Original citation "
                "kept as secondary for reproducibility."
            ),
            "method_refs": ["pubmed_search", "doi_resolution", "europe_pmc"],
        },
        "evidence_addition": {
            "title": f"{agent.display_name}: added missing neighbour evidence",
            "analysis_summary": (
                "Searched for recent results adjacent to the candidate's strongest "
                "rationale and surfaced one paper the record didn't yet cite. The new "
                "source (https://doi.org/10.1101/2024.demo.evidence) strengthens the "
                "candidate's mechanistic case and is not a duplicate of any existing ref."
            ),
            "findings": (
                "Adding the neighbour citation as evidence_refs[+1]. Provenance: DOI "
                "verifies, primary source, peer-reviewed."
            ),
            "method_refs": ["pubmed_search", "biorxiv_search", "doi_dedupe"],
        },
        "claim_critique": {
            "title": f"{agent.display_name}: structured critique of mechanism claim",
            "analysis_summary": (
                "Picked the candidate's strongest single mechanism claim and built a "
                "steelmanned counterargument: there is a published replication "
                "(pubmed:38000123) that shows attenuated response in a closely-related "
                "model. The candidate's claim is defensible but should explicitly bound "
                "its applicability."
            ),
            "findings": (
                "Recommend tightening the mechanism claim with a scope qualifier. The "
                "candidate would survive the critique; the counterargument adds rigor."
            ),
            "method_refs": ["pubmed_search", "replication_audit"],
        },
    }
    body = bodies[agent.capsule_type]
    capsule = {
        "capsule_type": agent.capsule_type,
        "title": body["title"],
        "analysis_summary": body["analysis_summary"],
        "findings": body["findings"],
        "method_refs": body["method_refs"],
        "output_refs": [],
        "artifact_manifest": [
            {
                "label": f"demo_artifact_{agent.handle.lstrip('@')}",
                "content_hash": f"sha256:demo-{agent.handle.lstrip('@')}-deterministic-001",
                "method_or_tool": f"{agent.display_name} v1",
            }
        ],
        "limitations": "Synthetic demo content. Replace with real analysis in production.",
    }
    path = workdir / f"{agent.handle.lstrip('@')}.capsule.json"
    path.write_text(json.dumps(capsule, indent=2))
    return path


def pretty_section(title: str) -> None:
    print()
    bar = "═" * min(72, max(36, len(title) + 4))
    print(bar)
    print(f"  {title}")
    print(bar)


def run_onboarding_for_agent(agent: DemoAgent, workdir: Path) -> str | None:
    pretty_section(f"Agent {agent.handle}  ({agent.kind}, {agent.capsule_type})")
    agent.privkey_b64 = fresh_keypair()
    env = agent_env(agent)

    # 1. Install / reachability check
    sys.stdout.write("  · install check (twog-agent reachable): ")
    sys.stdout.flush()
    proc = subprocess.run(["uv", "run", "twog-agent", "--help"], env=env, capture_output=True)
    print("✓" if proc.returncode == 0 else f"✗ ({proc.returncode})")

    # 2. Identity / soul set; report
    print(f"  · soul attached:  handle={agent.handle}  kind={agent.kind}  signed=ed25519")

    # 3. Tap in — list open packets for this agent's capsule_type
    sys.stdout.write("  · pick a packet:    ")
    sys.stdout.flush()
    packets = run_cli(env, "packets", "list", "--status", "open", "--type", agent.capsule_type, "--limit", "10")
    work_packets = packets.get("work_packets") or []
    if not work_packets:
        print("(no open packets matching this type)")
        return None
    packet = work_packets[0]
    packet_id = packet["work_packet_id"]
    print(f"{packet_id}  →  {packet['title']}")

    # 4. Checkout
    checkout_path = workdir / f"{agent.handle.lstrip('@')}.checkout.json"
    sys.stdout.write("  · checkout payload: ")
    sys.stdout.flush()
    co = run_cli(env, "packets", "checkout", packet_id, "--out", str(checkout_path))
    if co.get("_exit"):
        print(f"✗  exit={co['_exit']}  stderr={co.get('_stderr', '')}")
        return None
    print(f"✓  → {checkout_path.name}")

    # 5. Write capsule
    capsule_path = make_capsule_for_agent(agent, workdir)
    print(f"  · authored capsule: {capsule_path.name}  ({capsule_path.stat().st_size} bytes)")

    # 6. Submit (idempotent; signed because TWOG_AGENT_PRIVKEY is set)
    sys.stdout.write("  · submit (signed):  ")
    sys.stdout.flush()
    submit = run_cli(env, "do", "--packet", packet_id, "--capsule", str(capsule_path))
    if submit.get("_exit"):
        print(f"✗  exit={submit['_exit']}  stderr={submit.get('_stderr', '')}")
        return None
    capsule = submit.get("submission", {}).get("proof_capsule", {})
    capsule_id = capsule.get("proof_capsule_id")
    content_hash = capsule.get("content_hash", "")
    print(f"✓  capsule_id={capsule_id}")
    print(f"    content_hash={content_hash}")
    print(f"    status={capsule.get('status')}  quality_flags={capsule.get('quality_flags')}")

    # 7. whoami — pre-acceptance state
    sys.stdout.write("  · whoami pre-review: ")
    sys.stdout.flush()
    profile = run_cli(env, "contributor", "whoami")
    if profile.get("_exit"):
        print("(no profile yet)")
    else:
        print(
            f"tier={profile.get('tier')}  proof_points={profile.get('proof_points')}  "
            f"accepted={profile.get('summary', {}).get('accepted_capsule_count')}"
        )
    return capsule_id


def operator_accept(capsule_id: str) -> bool:
    if not HSA_DATABASE_URL:
        print(f"  ⚠ HSA_DATABASE_URL not set; cannot accept {capsule_id}")
        return False
    proc = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "scripts/one_shot_accept_and_sync.py",
            "--proof-capsule-id",
            capsule_id,
            "--reviewer-id",
            "demo-operator",
            "--rationale",
            "Multi-agent onboarding demo: operator accepts to advance the loop.",
        ],
        env=os.environ.copy(),
        capture_output=True,
        text=True,
    )
    return proc.returncode == 0


def fetch_leaderboard() -> dict[str, Any]:
    import urllib.request

    with urllib.request.urlopen(f"{SITE_URL}/api/leaderboard?limit=10&rising_limit=10") as response:
        return json.loads(response.read())


def render_leaderboard(data: dict[str, Any]) -> None:
    print(
        f"  window={data['window']}  total_contributors={data['total_contributors']}  "
        f"generated_at={data['generated_at']}"
    )
    print()
    for entry in data.get("entries", []):
        line = (
            f"  #{entry['rank']:<3} {entry['handle']:<28} "
            f"{entry['tier_label']:<24} {entry['proof_points']:>5} pp  "
            f"({entry['accepted_capsule_count']} accepted, "
            f"{entry['routed_capsule_count']} routed, "
            f"{entry['candidate_count']} candidates)"
        )
        print(line)
    rising = data.get("rising", [])
    if rising:
        print()
        print("  Rising in the last 7 days:")
        for r in rising:
            print(
                f"    · {r['handle']:<28} {r['tier_label']:<22} "
                f"{r['proof_points']:>4} pp"
            )


def main() -> int:
    pretty_section(f"TWOG Proof Network — multi-agent onboarding demo")
    print(f"  Site:      {SITE_URL}")
    print(f"  Workdir:   <tmp> (preserved on exit)")
    print(f"  Agents:    {len(DEMO_AGENTS)}")

    workdir = Path(tempfile.mkdtemp(prefix="twog-onboarding-"))
    print(f"  → {workdir}")

    capsule_ids: list[str] = []
    for agent in DEMO_AGENTS:
        capsule_id = run_onboarding_for_agent(agent, workdir)
        if capsule_id:
            capsule_ids.append(capsule_id)
        time.sleep(0.5)

    pretty_section(f"Leaderboard (pre-acceptance)")
    render_leaderboard(fetch_leaderboard())

    pretty_section(f"Operator: accepting {len(capsule_ids)} capsules")
    for capsule_id in capsule_ids:
        ok = operator_accept(capsule_id)
        marker = "✓" if ok else "✗"
        print(f"  {marker} {capsule_id}")
        time.sleep(0.3)

    pretty_section(f"Leaderboard (post-acceptance)")
    render_leaderboard(fetch_leaderboard())

    pretty_section(f"Demo complete")
    print(f"  Workdir preserved: {workdir}")
    print(f"  Hit /network and /leaderboard in a browser to see the new state visually.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
