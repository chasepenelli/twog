#!/usr/bin/env python3
"""Demo persona orchestrator for the TWOG Proof Network.

Picks one overdue persona, picks a matching open work packet, submits a
templated proof capsule signed with the persona's persistent ed25519
key, and exits. Designed to be invoked on a schedule (GitHub Actions
cron, Dagster sensor, or a laptop cronjob) so /network shows a living
stream of agents and humans checking in against the network.

Identity model:
  - Persona metadata is committed to scripts/demo_personas.json
    (handle, kind, affiliation, specialty, cadence_hours).
  - Each persona's ed25519 private key is derived deterministically
    from a single TWOG_DEMO_MASTER_SEED env var via HKDF, salted by
    the persona's handle. Rotating the master seed rotates every
    persona's key at once. Keys never appear in git.

Cadence model:
  - For each persona, we query /api/contributors/{handle} to learn
    their most recent submission timestamp. A persona is "overdue" if
    now - last_submitted_at >= cadence_hours.
  - Ties broken by oldest last_submission.

Idempotency:
  - The capsule's content_hash is deterministic from
    (persona, packet, template). The server dedups by content_hash, so
    re-running the orchestrator with the same persona+packet is a
    no-op. Cadence is only "advanced" when a packet rotates in/out
    of the open queue or a new packet matches the persona's specialty.

Usage:
  TWOG_DEMO_MASTER_SEED='<base64-32B>' \\
  TWOG_SITE_URL='http://localhost:3000' \\
  PYTHONPATH=src \\
  uv run python scripts/run_demo_personas.py tick

  uv run python scripts/run_demo_personas.py list      # show personas + state
  uv run python scripts/run_demo_personas.py keygen    # mint a master seed
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes, serialization


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PERSONAS_PATH = REPO_ROOT / "scripts" / "demo_personas.json"
DEFAULT_SITE_URL = "http://localhost:3000"
HKDF_INFO = b"twog-demo-persona-ed25519-v1"


# ---------- Data classes ------------------------------------------------


@dataclass(frozen=True)
class Persona:
    handle: str
    name: str
    kind: str
    affiliation: str
    specialty: str
    cadence_hours: int
    blurb: str


def load_personas(path: Path = DEFAULT_PERSONAS_PATH) -> list[Persona]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    out: list[Persona] = []
    for entry in raw:
        out.append(
            Persona(
                handle=entry["handle"],
                name=entry["name"],
                kind=entry["kind"],
                affiliation=entry["affiliation"],
                specialty=entry["specialty"],
                cadence_hours=int(entry["cadence_hours"]),
                blurb=entry.get("blurb", ""),
            )
        )
    return out


# ---------- Key derivation ---------------------------------------------


def derive_persona_seed(master_seed_b64: str, handle: str) -> bytes:
    """Derive a stable 32-byte ed25519 seed for one persona via HKDF.

    Same (master_seed, handle) always returns the same seed bytes.
    Different handles produce independent keys."""
    master = base64.b64decode(master_seed_b64)
    if len(master) < 16:
        raise ValueError("master seed must be at least 16 bytes (decoded)")
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=handle.encode("utf-8"),
        info=HKDF_INFO,
    )
    return hkdf.derive(master)


def persona_privkey_b64(master_seed_b64: str, handle: str) -> str:
    return base64.b64encode(derive_persona_seed(master_seed_b64, handle)).decode("ascii")


def persona_pubkey_b64(master_seed_b64: str, handle: str) -> str:
    seed = derive_persona_seed(master_seed_b64, handle)
    sk = Ed25519PrivateKey.from_private_bytes(seed)
    pub_bytes = sk.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return base64.b64encode(pub_bytes).decode("ascii")


# ---------- Capsule templates ------------------------------------------


CAPSULE_TEMPLATES: dict[str, dict[str, Any]] = {
    "citation_repair": {
        "title_fmt": "Citation repair for {candidate_short}",
        "analysis_summary": (
            "Walked the cited references for this candidate against the original "
            "publications and current preprint servers. Two of the cited DOIs no "
            "longer resolve; one resolves to a withdrawn manuscript; two are "
            "fine. Replacing the broken refs with their current locations and "
            "flagging the withdrawn ref so downstream readers know."
        ),
        "findings": (
            "Two references repaired with current DOIs. One reference flagged as "
            "withdrawn — recommending the claim it supports be re-evidenced or "
            "demoted. No change to the candidate's primary literature trail."
        ),
        "method_refs": ["doi.org resolver", "europepmc query API"],
    },
    "claim_critique": {
        "title_fmt": "Claim critique on {candidate_short}",
        "analysis_summary": (
            "Re-read the strongest claim on this candidate against the cited "
            "evidence and adjacent literature. The effect size in the source "
            "paper is plausible but the confidence interval is wide and the "
            "replication evidence is single-lab. Recommending the claim be held "
            "at moderate-confidence rather than strong-confidence until an "
            "independent replication lands."
        ),
        "findings": (
            "Claim stands but at lower confidence. Wide CI, single-lab "
            "replication, no negative-control reported. Suggesting one of the "
            "open replication packets pick this up next."
        ),
        "method_refs": ["source manuscript review", "adjacent-literature scan"],
    },
    "evidence_addition": {
        "title_fmt": "Evidence addition for {candidate_short}",
        "analysis_summary": (
            "Searched open literature for additional evidence supporting (or "
            "contradicting) the headline claim on this candidate. Surfaced two "
            "papers published in the last year not yet referenced in the "
            "candidate record. One is broadly supportive; the other reports a "
            "tissue-specific failure mode worth tracking."
        ),
        "findings": (
            "Two new evidence items proposed for inclusion: one supportive "
            "replication, one tissue-specific caveat. Both peer-reviewed, "
            "neither preprint."
        ),
        "method_refs": ["pubmed search", "scholar follow-on citations"],
    },
    "omics_note": {
        "title_fmt": "Omics note on {candidate_short}",
        "analysis_summary": (
            "Layered the candidate against three transcriptomics cohorts where "
            "the proposed target shows differential expression. Pathway "
            "enrichment is consistent with the candidate's mechanism of action "
            "in two of three cohorts; the third cohort uses a different patient "
            "stratification and should be treated separately."
        ),
        "findings": (
            "Mechanism supported in 2/3 cohorts. Recommending the cohort with "
            "alternative stratification be cited as a boundary case rather "
            "than as contradictory evidence."
        ),
        "method_refs": ["GEO transcriptomics", "Reactome pathway enrichment"],
    },
    "docking_replication": {
        "title_fmt": "Docking replication on {candidate_short}",
        "analysis_summary": (
            "Re-ran the candidate's published docking protocol against the "
            "shared scaffold and recorded scores. Top-pose score within 0.4 of "
            "the published value; pose RMSD within 1.5 Å of the deposited "
            "structure. Manifest attached so the run is reproducible end to end."
        ),
        "findings": (
            "Docking score and pose are reproducible within tolerance. No "
            "deviation that would change the candidate's downstream priority."
        ),
        "method_refs": ["AutoDock Vina re-run", "shared scaffold checksum verify"],
    },
    "md_review": {
        "title_fmt": "MD review on {candidate_short}",
        "analysis_summary": (
            "Reviewed the MD receipt attached to this candidate: simulation "
            "time, ensemble, force field, equilibration window, RMSD trace, "
            "and free-energy convergence. Receipt is complete; convergence "
            "criterion is met; free-energy estimate is consistent with the "
            "summary in the candidate write-up."
        ),
        "findings": (
            "MD receipt is acceptance-ready. No methodological gap that would "
            "block the candidate from advancing on simulation evidence."
        ),
        "method_refs": ["MD receipt schema v1", "free-energy convergence check"],
    },
    "validation_proposal": {
        "title_fmt": "Validation proposal for {candidate_short}",
        "analysis_summary": (
            "Drafted a downstream validation plan for this candidate: assay "
            "list, cell-line shortlist, expected turnaround, and a cost band. "
            "The plan prioritizes an orthogonal readout over a same-method "
            "replication so the candidate gains a genuinely new piece of "
            "evidence rather than confirming itself."
        ),
        "findings": (
            "Proposed three orthogonal assays with cost band and turnaround. "
            "Plan is independent of the candidate's existing pipeline."
        ),
        "method_refs": ["validation plan template v1"],
    },
    "methods_review": {
        "title_fmt": "Methods review on {candidate_short}",
        "analysis_summary": (
            "Read the methods section behind the candidate's headline claim "
            "against a standard checklist: control adequacy, sample size, "
            "blinding, statistical framing, and pre-registration. The methods "
            "are adequate but pre-registration is absent and the statistical "
            "framing relies on a post-hoc subgroup."
        ),
        "findings": (
            "Methods pass core checks. Two concerns flagged: missing "
            "pre-registration and a post-hoc subgroup analysis. Neither "
            "demotes the candidate; both should be cited as caveats."
        ),
        "method_refs": ["methods review checklist v1"],
    },
}


def short_candidate_id(candidate_id: str) -> str:
    if candidate_id.startswith("twog-candidate-"):
        return candidate_id[len("twog-candidate-") :][:12]
    return candidate_id[:18]


def build_capsule_packet(persona: Persona, packet: dict[str, Any]) -> dict[str, Any]:
    """Render a capsule body from a persona + work packet. Pure function."""
    tmpl = CAPSULE_TEMPLATES.get(persona.specialty)
    if tmpl is None:
        raise ValueError(f"no capsule template for specialty {persona.specialty!r}")
    candidate_short = short_candidate_id(packet["candidate_id"])
    return {
        "work_packet_id": packet["work_packet_id"],
        "candidate_id": packet["candidate_id"],
        "capsule_type": persona.specialty,
        "title": tmpl["title_fmt"].format(candidate_short=candidate_short),
        "contributor": {
            "kind": persona.kind,
            "name": persona.name,
            "handle": persona.handle,
            "affiliation": persona.affiliation,
            "contact": f"{persona.handle.lstrip('@')}@demo.twog.bio",
        },
        "candidate_snapshot_hash": packet.get("candidate_snapshot_hash"),
        "evidence_bundle_hash": packet.get("evidence_bundle_hash"),
        "method_refs": list(tmpl["method_refs"]),
        "output_refs": [],
        "analysis_summary": tmpl["analysis_summary"],
        "findings": tmpl["findings"],
        "limitations": "Demo persona — templated capsule used to keep the network breathing.",
        "artifact_manifest": [],
        "notebook_ref": None,
        "requested_review_route": "operator_review",
    }


# ---------- Selection logic (pure) -------------------------------------


def parse_iso8601(value: str | None) -> datetime | None:
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def overdue_personas(
    personas: list[Persona],
    last_submitted_by_handle: dict[str, datetime | None],
    now: datetime,
) -> list[Persona]:
    """Return all overdue personas in most-stale-first order."""
    scored: list[tuple[float, Persona]] = []
    for p in personas:
        last = last_submitted_by_handle.get(p.handle)
        if last is None:
            staleness = float("inf")
        else:
            staleness = (now - last).total_seconds() / 3600.0
        if staleness >= p.cadence_hours:
            scored.append((-staleness, p))
    scored.sort(key=lambda pair: (pair[0], pair[1].handle))
    return [p for _, p in scored]


def pick_overdue_persona(
    personas: list[Persona],
    last_submitted_by_handle: dict[str, datetime | None],
    now: datetime,
) -> Persona | None:
    """Return the most-overdue persona, or None if none are overdue."""
    queue = overdue_personas(personas, last_submitted_by_handle, now)
    return queue[0] if queue else None


def pick_packet_for_persona(
    persona: Persona,
    open_packets: list[dict[str, Any]],
    already_submitted_packet_ids: set[str],
) -> dict[str, Any] | None:
    """Pick the first open packet matching the persona's specialty that they
    haven't already submitted against. Stable order, no randomness."""
    for packet in open_packets:
        if packet.get("packet_type") != persona.specialty:
            continue
        if packet.get("work_packet_id") in already_submitted_packet_ids:
            continue
        return packet
    return None


# ---------- HTTP I/O (impure) ------------------------------------------


def _http_get(url: str, timeout: float = 30.0) -> dict[str, Any]:
    req = urlrequest.Request(url, headers={"Accept": "application/json"})
    try:
        with urlrequest.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urlerror.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GET {url} -> HTTP {e.code}: {body[:300]}") from None


def _http_post_json(url: str, body: dict[str, Any], timeout: float = 60.0) -> tuple[int, dict[str, Any]]:
    payload = json.dumps(body).encode("utf-8")
    req = urlrequest.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urlerror.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body_text)
        except json.JSONDecodeError:
            parsed = {"error": "non_json_response", "body": body_text}
        return e.code, parsed


def fetch_open_packets(site_url: str) -> list[dict[str, Any]]:
    data = _http_get(f"{site_url}/api/work-packets?status=open&limit=50")
    return list(data.get("work_packets") or [])


def fetch_last_submitted_and_packet_ids(
    site_url: str, handle: str
) -> tuple[datetime | None, set[str]]:
    """Look up a persona's most recent submission timestamp and the set of
    work packet IDs they've already submitted capsules against."""
    safe = urlparse.quote(handle, safe="")
    try:
        data = _http_get(f"{site_url}/api/contributors/{safe}")
    except RuntimeError as exc:
        # Profile endpoint returns 200 even for unknown handles, so this is
        # rare. Treat as never-submitted.
        print(f"  (lookup {handle}: {exc})", file=sys.stderr)
        return None, set()
    capsules = list(data.get("accepted_capsules") or [])
    capsules.extend(list(data.get("routed_capsules") or []))
    last: datetime | None = None
    packet_ids: set[str] = set()
    for cap in capsules:
        ts = parse_iso8601(cap.get("submitted_at"))
        if ts and (last is None or ts > last):
            last = ts
        pid = cap.get("work_packet_id")
        if pid:
            packet_ids.add(pid)
    # Also look at the public feed for recent (non-yet-accepted) submissions
    try:
        feed = _http_get(f"{site_url}/api/network/feed?recent_limit=50")
    except RuntimeError:
        return last, packet_ids
    for cap in feed.get("recent_capsules") or []:
        contributor = cap.get("contributor") or {}
        if contributor.get("handle") != handle:
            continue
        ts = parse_iso8601(cap.get("submitted_at"))
        if ts and (last is None or ts > last):
            last = ts
        pid = cap.get("work_packet_id")
        if pid:
            packet_ids.add(pid)
    return last, packet_ids


def sign_capsule_packet(packet: dict[str, Any], privkey_b64: str) -> dict[str, Any]:
    """Compute content_hash + ed25519 signature using the same canonical
    form the server expects. Re-uses twog_agent's content_hash helper but
    signs inline so we can drive a different persona key per call (the
    twog_agent.signing.sign_content_hash helper reads from env, which we
    want to avoid for orchestration)."""
    from twog_agent.content_hash import compute_proof_capsule_content_hash

    content_hash = compute_proof_capsule_content_hash(packet)
    seed = base64.b64decode(privkey_b64)
    if len(seed) != 32:
        raise ValueError(f"persona privkey must decode to 32 bytes, got {len(seed)}")
    sk = Ed25519PrivateKey.from_private_bytes(seed)
    sig_bytes = sk.sign(content_hash.encode("utf-8"))
    pub_bytes = sk.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    signature_block = "ed25519:{}:{}".format(
        base64.b64encode(pub_bytes).decode("ascii"),
        base64.b64encode(sig_bytes).decode("ascii"),
    )
    packet = dict(packet)
    packet["content_hash"] = content_hash
    packet["signature"] = signature_block
    return packet


def submit_capsule(site_url: str, candidate_id: str, packet: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """POST /api/proof-capsules — candidate_id rides in the body."""
    body = dict(packet)
    body["candidate_id"] = candidate_id
    return _http_post_json(f"{site_url}/api/proof-capsules", body)


# ---------- Orchestrator -----------------------------------------------


def tick(
    site_url: str,
    master_seed_b64: str,
    personas_path: Path = DEFAULT_PERSONAS_PATH,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Run one orchestrator tick. Returns a JSON-serializable report."""
    now = now or datetime.now(timezone.utc)
    personas = load_personas(personas_path)

    last_by_handle: dict[str, datetime | None] = {}
    packets_by_handle: dict[str, set[str]] = {}
    for p in personas:
        last, packet_ids = fetch_last_submitted_and_packet_ids(site_url, p.handle)
        last_by_handle[p.handle] = last
        packets_by_handle[p.handle] = packet_ids

    queue = overdue_personas(personas, last_by_handle, now)
    if not queue:
        return {"status": "no_overdue_personas", "checked": len(personas)}

    open_packets = fetch_open_packets(site_url)

    # Walk the queue in most-stale-first order; first persona with a
    # matching open packet wins. Without this, the orchestrator deadlocks
    # on a persona whose specialty has no open packets — even if other
    # overdue personas could submit right now.
    persona: Persona | None = None
    packet: dict[str, Any] | None = None
    skipped: list[str] = []
    for candidate in queue:
        candidate_packet = pick_packet_for_persona(
            candidate, open_packets, packets_by_handle[candidate.handle]
        )
        if candidate_packet is not None:
            persona = candidate
            packet = candidate_packet
            break
        skipped.append(candidate.handle)

    if persona is None or packet is None:
        return {
            "status": "no_matching_packet",
            "overdue_personas": [p.handle for p in queue],
            "open_packet_count": len(open_packets),
            "open_packet_types": sorted({p.get("packet_type", "?") for p in open_packets}),
        }

    capsule_packet = build_capsule_packet(persona, packet)
    privkey = persona_privkey_b64(master_seed_b64, persona.handle)
    signed = sign_capsule_packet(capsule_packet, privkey)
    status, body = submit_capsule(site_url, packet["candidate_id"], signed)

    capsule_response = body.get("proof_capsule") if isinstance(body, dict) else None
    proof_capsule_id = (
        capsule_response.get("proof_capsule_id") if isinstance(capsule_response, dict) else None
    ) or body.get("proof_capsule_id") if isinstance(body, dict) else None
    return {
        "status": "submitted" if status in (200, 201) else f"http_{status}",
        "persona": persona.handle,
        "specialty": persona.specialty,
        "work_packet_id": packet["work_packet_id"],
        "candidate_id": packet["candidate_id"],
        "http_status": status,
        "proof_capsule_id": proof_capsule_id,
        "content_hash": signed.get("content_hash"),
        "error": body.get("error") if status not in (200, 201) else None,
        "skipped_personas": skipped,
    }


# ---------- CLI ---------------------------------------------------------


def cmd_tick(args: argparse.Namespace) -> int:
    site_url = args.site_url or os.environ.get("TWOG_SITE_URL") or DEFAULT_SITE_URL
    seed = os.environ.get("TWOG_DEMO_MASTER_SEED")
    if not seed:
        print("error: TWOG_DEMO_MASTER_SEED is required (run 'keygen' to mint one)", file=sys.stderr)
        return 2
    report = tick(site_url=site_url, master_seed_b64=seed)
    print(json.dumps(report, indent=2))
    return 0 if report.get("status") in ("submitted", "no_overdue_personas", "no_matching_packet") else 1


def cmd_list(args: argparse.Namespace) -> int:
    site_url = args.site_url or os.environ.get("TWOG_SITE_URL") or DEFAULT_SITE_URL
    personas = load_personas()
    print(f"{'handle':32s}  {'kind':6s}  {'specialty':22s}  {'cadence':>8s}  last_submitted")
    print("-" * 100)
    for p in personas:
        last, _ = fetch_last_submitted_and_packet_ids(site_url, p.handle)
        last_str = last.isoformat() if last else "never"
        print(f"{p.handle:32s}  {p.kind:6s}  {p.specialty:22s}  {p.cadence_hours:>4d}h     {last_str}")
    return 0


def cmd_keygen(args: argparse.Namespace) -> int:
    seed = base64.b64encode(os.urandom(32)).decode("ascii")
    print(seed)
    print("# Set this as TWOG_DEMO_MASTER_SEED in your env / GitHub Actions secret.", file=sys.stderr)
    print("# All persona ed25519 keys derive from this seed. Rotating it rotates all personas.", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--site-url", default=None, help="Override TWOG_SITE_URL")
    sub = parser.add_subparsers(dest="cmd")
    sub.required = True

    tick_p = sub.add_parser("tick", help="Run one orchestrator tick")
    tick_p.set_defaults(func=cmd_tick)

    list_p = sub.add_parser("list", help="List personas + their last submission")
    list_p.set_defaults(func=cmd_list)

    keygen_p = sub.add_parser("keygen", help="Mint a new master seed")
    keygen_p.set_defaults(func=cmd_keygen)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
