"""Auto-orchestrated dive worker for new public candidate snapshots.

When a candidate lands (or its snapshot revs), this worker runs a
deterministic pass over the candidate's structured payload and
produces 1-N proof capsules. The capsules submit via the public
/api/proof-capsules endpoint signed with the @twog-auto-dive
persona's deterministic ed25519 key.

What it checks (deterministic, no agent calls):

1. **Unresolved citations.** Any citation_ref with `resolved: false` is
   flagged. Each unresolved cite becomes a citation_repair capsule
   that quotes the supporting context and invites a human / K-Dense
   `citation-management` contributor to fully resolve via Crossref /
   Europe PMC.

2. **Compute-vs-mechanism alignment.** Compares the target gene/
   protein names listed in `computational_evidence` job manifests
   against the candidate's `biology.targets` and the names that
   appear in `rationale.mechanism`. If the compute jobs target a
   different protein than the rationale (the exact failure mode the
   K-Dense dive found on TWOG-B41B9F), emit a claim_critique capsule
   flagging the misalignment.

What it does NOT do:
- Resolve citations via Crossref / Europe PMC (that's deeper work
  left for K-Dense citation-management + human review).
- Critique mechanistic claims (that's K-Dense scientific-critical-thinking).
- Search adjacent literature (that's K-Dense literature-review).

The auto-dive is the floor — every candidate gets at least these
deterministic flags. Human and agent contributors using K-Dense
skills then build on top with substantive critiques and resolutions.

Per-candidate cooldown:
- Default 24h between auto-dives on the same candidate.
- A re-dive can be forced by passing dive(force=True) — useful when
  the candidate's content_hash changes.
- The cooldown is enforced by querying proof_capsules for the most
  recent @twog-auto-dive submission against the candidate; no extra
  table is required.
"""

from __future__ import annotations

import base64
import json
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

import psycopg2
from psycopg2.extras import RealDictCursor

from .candidate_contribution_intake import candidate_contribution_database_url


AUTO_DIVE_HANDLE = "@twog-auto-dive"
AUTO_DIVE_CADENCE_HOURS_DEFAULT = 24
DEFAULT_PER_CANDIDATE_CAPSULE_CAP = 8


@dataclass
class AutoDiveCapsule:
    """One proposed capsule from the auto-dive worker. Not yet submitted."""
    capsule_type: str
    title: str
    analysis_summary: str
    findings: str
    method_refs: list[str]
    limitations: str = ""
    # Tag identifying the rule that emitted this capsule.
    diagnostic_tag: str = ""


@dataclass
class AutoDiveReport:
    """Per-candidate result of one dive pass."""
    candidate_id: str
    status: str  # "ok" | "cooldown" | "no_candidate" | "no_persona_seed"
    candidate_snapshot_hash: str | None = None
    proposed_capsules: list[AutoDiveCapsule] = None  # type: ignore[assignment]
    submitted_capsule_ids: list[str] = None  # type: ignore[assignment]
    errors: list[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.proposed_capsules is None:
            self.proposed_capsules = []
        if self.submitted_capsule_ids is None:
            self.submitted_capsule_ids = []
        if self.errors is None:
            self.errors = []


# ---------- Pure: rules that emit AutoDiveCapsules from a candidate -----


def diagnose_unresolved_citations(
    payload: Mapping[str, Any],
    *,
    candidate_short_id: str,
) -> list[AutoDiveCapsule]:
    """Emit one citation_repair capsule per `resolved: false` citation
    in the candidate's `literature` block. Quotes the supporting context
    so a human or K-Dense contributor can pick it up and resolve."""

    literature = payload.get("literature") or []
    if not isinstance(literature, list):
        return []
    out: list[AutoDiveCapsule] = []
    for entry in literature:
        if not isinstance(entry, dict):
            continue
        if entry.get("resolved") is not False:
            continue
        ref = str(entry.get("ref") or "").strip()
        supports = str(entry.get("supports") or "").strip()
        if not ref:
            continue
        snippet = supports[:300] + ("…" if len(supports) > 300 else "")
        out.append(
            AutoDiveCapsule(
                capsule_type="citation_repair",
                title=f"Unresolved citation {ref} on {candidate_short_id} flagged for repair",
                analysis_summary=(
                    f"Citation {ref} on this candidate is marked resolved=false in "
                    f"the structured literature block; it currently has no real DOI / "
                    f"PMID / verified title attached. The supporting context attached "
                    f"to the reference reads: \"{snippet}\". "
                    f"This is the entry point for a human or agent contributor running "
                    f"K-Dense's citation-management or literature-review skill to find "
                    f"the actual peer-reviewed source that matches the supporting context "
                    f"and submit a higher-confidence citation_repair capsule that fully "
                    f"resolves the reference."
                ),
                findings=(
                    f"Auto-dive flag only. Recommended next step: a contributor with "
                    f"PubMed / Crossref / Europe PMC access resolves {ref} to a concrete "
                    f"DOI and submits a follow-up citation_repair capsule attaching the "
                    f"primary source. Reviewer should treat this auto-dive capsule as "
                    f"a placeholder and prefer the human/agent follow-up for the reward."
                ),
                method_refs=["auto-dive: structured-literature scan", "citation-management v1 (recommended)"],
                limitations=(
                    "Deterministic flag from candidate payload only. No external "
                    "literature search performed. Confidence on the proposed "
                    "resolution is zero — that work is for the follow-up contributor."
                ),
                diagnostic_tag=f"unresolved_citation:{ref}",
            )
        )
    return out


def diagnose_compute_alignment(
    payload: Mapping[str, Any],
    *,
    candidate_short_id: str,
) -> list[AutoDiveCapsule]:
    """Detect compute_evidence jobs targeting proteins that aren't in the
    candidate's stated biology.targets / rationale.mechanism.

    Returns a single claim_critique capsule when at least one job is
    misaligned. (We collapse multiple misaligned jobs into one capsule
    so the operator gets one item to act on, not eight.)
    """

    compute = payload.get("computational_evidence")
    jobs: list[dict[str, Any]] = []
    if isinstance(compute, list):
        # Newer schema: top-level list of jobs.
        jobs = [j for j in compute if isinstance(j, dict)]
    elif isinstance(compute, dict):
        # Older schema variants stored jobs under various keys.
        for key in ("md_jobs", "jobs", "docking_jobs", "items", "runs"):
            raw_jobs = compute.get(key)
            if isinstance(raw_jobs, list):
                jobs = [j for j in raw_jobs if isinstance(j, dict)]
                break
    if not jobs:
        return []

    biology = payload.get("biology") or {}
    targets_block = biology.get("targets") or []
    target_names: set[str] = set()
    if isinstance(targets_block, list):
        for t in targets_block:
            if isinstance(t, dict):
                for k in ("name", "gene", "symbol", "uniprot", "id"):
                    value = str(t.get(k) or "").strip().casefold()
                    if value:
                        target_names.add(value)

    rationale = payload.get("rationale") or {}
    mechanism_text = str(rationale.get("mechanism") or "").casefold()

    misaligned: list[str] = []
    aligned_count = 0
    for job in jobs:
        job_target = ""
        for k in ("target", "target_name", "protein", "gene", "symbol"):
            value = str(job.get(k) or "").strip()
            if value:
                job_target = value
                break
        if not job_target:
            # Extract target hints from title/objective when no explicit
            # target field exists. Looks for "vs X", "against X", or
            # ALL-CAPS gene-name-shaped tokens.
            for text_field in ("title", "objective", "summary"):
                text_value = str(job.get(text_field) or "")
                if not text_value:
                    continue
                m = re.search(r"\b(?:against|vs\.?|targeting)\s+([A-Z][A-Z0-9/\-]{2,})", text_value)
                if m:
                    job_target = m.group(1)
                    break
                # Fallback: look for capitalized gene-name-shaped tokens
                # that aren't obvious English (skip the first capitalized
                # word, which is usually a sentence start).
                tokens = re.findall(r"\b([A-Z][A-Z0-9/\-]{3,})\b", text_value)
                gene_like = [t for t in tokens if t not in ("MD", "RNA", "DNA", "PCR", "FDA", "USA")]
                if gene_like:
                    job_target = gene_like[0]
                    break
        if not job_target:
            continue
        job_norm = job_target.casefold()
        if any(_target_token_in(job_norm, name) for name in target_names):
            aligned_count += 1
            continue
        # Match if any subtoken of the job target (split on /, -, _) appears
        # in the rationale mechanism text. Filters out tiny tokens that
        # would cause false-positive matches.
        if any(
            token and len(token) >= 3 and token in mechanism_text
            for token in re.split(r"[/_\-]+", job_norm)
        ):
            aligned_count += 1
            continue
        misaligned.append(job_target)

    if not misaligned:
        return []

    unique_misaligned = sorted(set(misaligned))
    return [
        AutoDiveCapsule(
            capsule_type="claim_critique",
            title=f"Attached compute jobs may not align with stated mechanism on {candidate_short_id}",
            analysis_summary=(
                f"The candidate's computational_evidence block contains "
                f"{len(jobs)} job(s). At least {len(misaligned)} of them target "
                f"proteins ({', '.join(unique_misaligned)}) that do not appear in "
                f"the candidate's stated biology.targets list and are not "
                f"mentioned in rationale.mechanism. A reviewer relying on the "
                f"structured payload would conclude computational evidence backs "
                f"the headline mechanism; this auto-dive check suggests the "
                f"compute jobs may be testing a different protein than the "
                f"rationale claims to depend on."
            ),
            findings=(
                "Recommended next step: either (a) re-run the compute jobs "
                "against the proteins in biology.targets, or (b) de-link the "
                "current jobs from this candidate's computational_evidence "
                "block so the snapshot accurately reflects what has been "
                "computed. A human reviewer or K-Dense critical-thinking "
                "contributor should confirm whether the compute mismatch is "
                "intentional (e.g. negative-control work) before action."
            ),
            method_refs=[
                "auto-dive: computational_evidence vs biology.targets string-match",
                "scientific-critical-thinking v1 (recommended for follow-up)",
            ],
            limitations=(
                f"Deterministic string-match only. May produce false positives when "
                f"job targets use aliases the candidate doesn't list (e.g. PDGFRB "
                f"vs PDGFR-β). {aligned_count} of {len(jobs)} jobs DID match the "
                f"declared targets, so the misalignment is partial."
            ),
            diagnostic_tag=f"compute_misalign:{','.join(unique_misaligned[:3])}",
        )
    ]


def _target_token_in(haystack_lower: str, needle_lower: str) -> bool:
    """Match a target name against a (possibly hyphenated/aliased) gene
    string. Splits on common separators so 'KDR' matches 'VEGFR2/KDR'."""
    if not needle_lower:
        return False
    if needle_lower in haystack_lower:
        return True
    for token in re.split(r"[\s\-/_,]+", haystack_lower):
        if token and token == needle_lower:
            return True
    return False


def collect_proposed_capsules(
    payload: Mapping[str, Any],
    *,
    candidate_short_id: str,
    cap: int = DEFAULT_PER_CANDIDATE_CAPSULE_CAP,
) -> list[AutoDiveCapsule]:
    """Run every diagnostic rule and concatenate, capped."""
    out: list[AutoDiveCapsule] = []
    out.extend(diagnose_unresolved_citations(payload, candidate_short_id=candidate_short_id))
    out.extend(diagnose_compute_alignment(payload, candidate_short_id=candidate_short_id))
    return out[:cap]


# ---------- I/O: cooldown check, persona signing, HTTP submission ------


def is_in_cooldown(
    candidate_id: str,
    *,
    database_url: str | None = None,
    cooldown_hours: int = AUTO_DIVE_CADENCE_HOURS_DEFAULT,
    now: datetime | None = None,
) -> bool:
    """True if @twog-auto-dive has submitted any capsule against this
    candidate within the last `cooldown_hours`. Cooldown prevents
    flooding the leaderboard when the candidate snapshot revs."""

    connection_string = database_url or candidate_contribution_database_url()
    if not connection_string:
        return False
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=cooldown_hours)
    try:
        with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select submitted_at
                    from proof_capsules
                    where candidate_id = %s
                      and contributor ->> 'handle' = %s
                      and submitted_at >= %s
                    order by submitted_at desc
                    limit 1
                    """,
                    [candidate_id, AUTO_DIVE_HANDLE, cutoff],
                )
                row = cur.fetchone()
                return row is not None
    except Exception:
        # If the cooldown check itself fails (DB unavailable, etc),
        # err on the side of NOT diving — better to under-dive than to
        # flood. The operator can force a dive with --force.
        return True


def fetch_candidate_payload(
    candidate_id: str,
    *,
    database_url: str | None = None,
    site_url: str | None = None,
) -> Mapping[str, Any] | None:
    """Read the candidate's structured payload directly from Neon.

    Auto-dive is a server-side worker; it should not HTTP-roundtrip its
    own public API to read its own database. Reading from Postgres
    directly removes the dependency on a deployed/reachable public site
    so this worker runs cleanly inside Dagster+ Serverless.

    The `site_url` parameter is kept for backwards compatibility but is
    no longer required. If `database_url` is omitted, we resolve via the
    standard `candidate_contribution_database_url()` env helper.

    Returns None if the candidate has no published snapshot yet.
    """

    connection_string = database_url or candidate_contribution_database_url()
    if not connection_string:
        if site_url:
            # Fallback for the local-dev script path where the worker
            # may be invoked without DB access.
            return _fetch_candidate_payload_via_http(candidate_id, site_url=site_url)
        return None
    try:
        with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select content_hash, payload
                    from public_candidate_snapshots
                    where candidate_id = %s
                    order by snapshot_version desc
                    limit 1
                    """,
                    [candidate_id],
                )
                row = cur.fetchone()
    except psycopg2.errors.UndefinedTable:
        if site_url:
            return _fetch_candidate_payload_via_http(candidate_id, site_url=site_url)
        return None
    if row is None:
        return None
    raw_payload = row["payload"] if isinstance(row, dict) else row[1]
    # The snapshot row's `payload` jsonb wraps the candidate envelope
    # plus a nested `payload` dict carrying biology/literature/rationale.
    # That nested dict is what every diagnostic rule reads from. Match
    # the HTTP API's shape (data.latest_snapshot.payload) exactly so
    # the diagnostic functions work identically against either source.
    if isinstance(raw_payload, dict) and isinstance(raw_payload.get("payload"), dict):
        inner_payload = raw_payload["payload"]
        title = raw_payload.get("title")
    else:
        inner_payload = raw_payload if isinstance(raw_payload, dict) else {}
        title = None
    return {
        "candidate_id": candidate_id,
        "candidate_short_id": _short_candidate_id(candidate_id),
        "snapshot_content_hash": row["content_hash"] if isinstance(row, dict) else row[0],
        "title": title,
        "payload": inner_payload,
    }


def _fetch_candidate_payload_via_http(
    candidate_id: str,
    *,
    site_url: str,
) -> Mapping[str, Any] | None:
    """Local-dev fallback when no database URL is configured."""
    safe = urlparse.quote(candidate_id, safe="")
    req = urlrequest.Request(
        f"{site_url}/api/public-candidates/{safe}",
        headers={"Accept": "application/json"},
    )
    try:
        with urlrequest.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urlerror.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    snap = data.get("latest_snapshot") or {}
    return {
        "candidate_id": candidate_id,
        "candidate_short_id": _short_candidate_id(candidate_id),
        "snapshot_content_hash": snap.get("content_hash"),
        "title": snap.get("title"),
        "payload": snap.get("payload") or {},
    }


def _short_candidate_id(candidate_id: str) -> str:
    if candidate_id.startswith("twog-candidate-"):
        return candidate_id[len("twog-candidate-") :][:12]
    return candidate_id[:18]


def _persona_keypair(master_seed_b64: str, handle: str) -> tuple[bytes, bytes]:
    """HKDF-derive the persona's ed25519 seed + return (seed, pubkey)."""
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives.kdf.hkdf import HKDF

    master = base64.b64decode(master_seed_b64)
    if len(master) < 16:
        raise ValueError("master seed must be at least 16 bytes (decoded)")
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=handle.encode("utf-8"),
        info=b"twog-demo-persona-ed25519-v1",
    )
    seed = hkdf.derive(master)
    sk = Ed25519PrivateKey.from_private_bytes(seed)
    pub = sk.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return seed, pub


def build_and_sign(
    capsule: AutoDiveCapsule,
    *,
    candidate_id: str,
    snapshot_content_hash: str | None,
    master_seed_b64: str,
) -> dict[str, Any]:
    """Render the AutoDiveCapsule into a fully-signed submission body."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    from twog_agent.content_hash import compute_proof_capsule_content_hash

    contact_local = AUTO_DIVE_HANDLE.lstrip("@")
    body = {
        "candidate_id": candidate_id,
        "capsule_type": capsule.capsule_type,
        "title": capsule.title,
        "analysis_summary": capsule.analysis_summary,
        "findings": capsule.findings,
        "limitations": capsule.limitations,
        "method_refs": list(capsule.method_refs),
        "output_refs": [],
        "artifact_manifest": [],
        "notebook_ref": None,
        "candidate_snapshot_hash": snapshot_content_hash,
        "evidence_bundle_hash": None,
        "requested_review_route": "operator_review",
        "contributor": {
            "kind": "agent",
            "name": "TWOG Auto-Dive",
            "handle": AUTO_DIVE_HANDLE,
            "affiliation": "TWOG auto-orchestrator",
            "contact": f"{contact_local}@demo.twog.bio",
        },
    }
    content_hash = compute_proof_capsule_content_hash(body)
    seed, pub_bytes = _persona_keypair(master_seed_b64, AUTO_DIVE_HANDLE)
    sk = Ed25519PrivateKey.from_private_bytes(seed)
    sig_bytes = sk.sign(content_hash.encode("utf-8"))
    body["content_hash"] = content_hash
    body["signature"] = (
        "ed25519:"
        + base64.b64encode(pub_bytes).decode("ascii")
        + ":"
        + base64.b64encode(sig_bytes).decode("ascii")
    )
    return body


def submit_capsule(body: dict[str, Any], *, site_url: str) -> tuple[int, dict[str, Any]]:
    payload = json.dumps(body).encode("utf-8")
    req = urlrequest.Request(
        f"{site_url}/api/proof-capsules",
        data=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urlerror.HTTPError as e:
        text = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(text)
        except json.JSONDecodeError:
            return e.code, {"error": "non_json", "body": text[:300]}


def dive(
    candidate_id: str,
    *,
    master_seed_b64: str,
    site_url: str | None = None,
    database_url: str | None = None,
    cooldown_hours: int = AUTO_DIVE_CADENCE_HOURS_DEFAULT,
    cap: int = DEFAULT_PER_CANDIDATE_CAPSULE_CAP,
    force: bool = False,
    dry_run: bool = False,
) -> AutoDiveReport:
    """Run one auto-dive against one candidate.

    Sandbox-friendly: dry_run=True returns the proposed capsules
    without HTTP submission.
    """
    site = site_url or os.environ.get("TWOG_SITE_URL") or "http://localhost:3000"

    if not force and is_in_cooldown(
        candidate_id, database_url=database_url, cooldown_hours=cooldown_hours
    ):
        return AutoDiveReport(candidate_id=candidate_id, status="cooldown")

    snapshot = fetch_candidate_payload(
        candidate_id, database_url=database_url, site_url=site
    )
    if snapshot is None:
        return AutoDiveReport(candidate_id=candidate_id, status="no_candidate")

    proposed = collect_proposed_capsules(
        snapshot["payload"],
        candidate_short_id=snapshot["candidate_short_id"],
        cap=cap,
    )
    report = AutoDiveReport(
        candidate_id=candidate_id,
        status="ok",
        candidate_snapshot_hash=snapshot.get("snapshot_content_hash"),
        proposed_capsules=proposed,
    )
    if dry_run:
        return report

    for capsule in proposed:
        try:
            body = build_and_sign(
                capsule,
                candidate_id=candidate_id,
                snapshot_content_hash=snapshot.get("snapshot_content_hash"),
                master_seed_b64=master_seed_b64,
            )
            status, response = submit_capsule(body, site_url=site)
        except Exception as exc:  # noqa: BLE001
            report.errors.append(f"{capsule.diagnostic_tag}: {exc}")
            continue
        if status not in (200, 201):
            report.errors.append(
                f"{capsule.diagnostic_tag}: HTTP {status} {response.get('error')}"
            )
            continue
        capsule_response = response.get("proof_capsule") if isinstance(response, dict) else None
        if isinstance(capsule_response, dict):
            cap_id = capsule_response.get("proof_capsule_id")
            if cap_id:
                report.submitted_capsule_ids.append(str(cap_id))
    return report


def dive_all_eligible(
    *,
    master_seed_b64: str,
    site_url: str | None = None,
    database_url: str | None = None,
    cooldown_hours: int = AUTO_DIVE_CADENCE_HOURS_DEFAULT,
    candidate_limit: int = 50,
    dry_run: bool = False,
) -> list[AutoDiveReport]:
    """Find candidates not in cooldown and dive each one. Used by the
    scheduled Dagster job to scan all public candidates."""

    connection_string = database_url or candidate_contribution_database_url()
    if not connection_string:
        return []
    candidates: list[str] = []
    try:
        with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select distinct candidate_id
                    from proof_capsules
                    union
                    select candidate_id
                    from public_candidates
                    order by candidate_id
                    limit %s
                    """,
                    [candidate_limit],
                )
                candidates = [str(row["candidate_id"]) for row in cur.fetchall() if row["candidate_id"]]
    except Exception:
        # public_candidates table may not exist in some deployments;
        # fall back to whatever we can see from proof_capsules alone.
        try:
            with psycopg2.connect(connection_string, cursor_factory=RealDictCursor) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        select distinct candidate_id
                        from proof_capsules
                        order by candidate_id
                        limit %s
                        """,
                        [candidate_limit],
                    )
                    candidates = [str(row["candidate_id"]) for row in cur.fetchall() if row["candidate_id"]]
        except Exception:
            return []

    out: list[AutoDiveReport] = []
    for cid in candidates:
        report = dive(
            cid,
            master_seed_b64=master_seed_b64,
            site_url=site_url,
            database_url=database_url,
            cooldown_hours=cooldown_hours,
            dry_run=dry_run,
        )
        out.append(report)
    return out
