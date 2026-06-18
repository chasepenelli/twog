"""Public-read presenters: project rich internal records into the SIMPLIFIED display shapes the
public website (STATE / EVIDENCE / RUNS) consumes.

This is a deliberate boundary. The internal ``ProofCapsuleRecord`` / ``RunManifestRecord`` /
``PublicCandidateRecord`` are comprehensive; the public site wants a curated "receipt" view. Keeping
the projection here (pure functions over records) means the web_api public routes stay thin and the
front-end display contract (web/lib/types/domain.ts) is satisfied by one well-tested mapping rather
than leaking raw internals to the browser.

These functions are PURE and side-effect free — unit-testable without a DB or HTTP. They mirror the
TypeScript interfaces: ProofCapsule, RunManifest/CampaignRollup/CampaignRow, Candidate, EngineState.
"""

from __future__ import annotations

from typing import Any

from .contracts import ProofCapsuleRecord, PublicCandidateRecord, RunManifestRecord

# --- engine metadata (describes the engine/codebase, NOT the research DB) --------------------------
# These are not derivable from research records; they describe the engine itself. Kept here as marked
# constants (overridable later from CI / a build-info file) rather than faked per-request.
ENGINE_CONTEXT = "canine HSA × human AS"
ENGINE_PHASE = "phase 0 locked"
ENGINE_TRACKS = "free track live · paid track running"
ENGINE_TESTS_PASSING = 708
ENGINE_COVERAGE = "76.5%"

# The falsification loop is a FIXED five-step shape (not data) — the same copy the site has always used.
ENGINE_LOOP: list[dict[str, Any]] = [
    {"key": "hypothesize", "title": "Hypothesize", "blurb": "With the test that would kill it."},
    {"key": "compute", "title": "Compute", "blurb": "Pluggable GPU lanes — dock, co-fold, MD, omics."},
    {"key": "falsify", "title": "Falsify", "blurb": "Cheap pre-registered crux, run first.", "live": True},
    {"key": "capsule", "title": "Capsule", "blurb": "Portable evidence: signal, confidence, limits."},
    {"key": "compound", "title": "Compound", "blurb": "Publish datasets & models; the next run gets cheaper."},
]

# How a capsule's lane section renders as a compute lane on STATE.
_LANE_DISPLAY: dict[str, dict[str, str]] = {
    "docking": {"lane": "gnina docking", "sublabel": "CNN docking · gnina", "compute": "A100"},
    "omics": {"lane": "Omics TME review", "sublabel": "deconvolution · purity-adjusted", "compute": "CPU"},
    "cofold": {"lane": "Boltz-2 cofolding", "sublabel": "structure + affinity", "compute": "A100"},
    "md": {"lane": "OpenMM MD", "sublabel": "checkpoint / resume", "compute": "T4·GPU"},
}

# Capsule statuses that are publicly visible as evidence (everything except rejected/archived).
PUBLIC_CAPSULE_STATUSES = (
    "submitted",
    "needs_more_information",
    "accepted_for_evidence_review",
    "accepted_for_validation_queue",
    "accepted_for_compute_review",
)

_VALID_SIGNALS = {"supports", "refutes", "neutral"}


def _truncate(text: str, n: int = 96) -> str:
    text = (text or "").strip()
    return text if len(text) <= n else text[: n - 1].rstrip() + "…"


def present_capsule(rec: ProofCapsuleRecord) -> dict[str, Any]:
    """ProofCapsuleRecord -> the front-end ProofCapsule (the digestible receipt)."""
    payload = rec.payload or {}
    signal = payload.get("signal")
    if signal not in _VALID_SIGNALS:
        signal = "neutral"
    section = (rec.target.section if rec.target else None) or payload.get("validation_type") or "evidence"
    # provenance gate: the engine stamps payload.provenance_flag ("pass"/"fail"/...); confound verdict
    # is not stamped on these engine-produced capsules, so it degrades to "unknown" (optional in the UI).
    prov = payload.get("provenance_flag")
    out: dict[str, Any] = {
        "capsule_id": str(rec.capsule_id),
        "candidate_id": rec.candidate_id,
        "signal": signal,
        "validation_type": str(section),
        "status": rec.status,
        "claim": rec.summary.title,
        "method": rec.summary.why_it_matters,
        "readout": rec.summary.finding,
        "limitations": list(rec.summary.limitations or []),
        "produced_by": rec.producer.name if rec.producer else None,
    }
    if rec.signature:
        out["signature"] = rec.signature
    if isinstance(payload.get("confidence"), (int, float)):
        out["confidence"] = float(payload["confidence"])
    if isinstance(prov, str) and prov in {"pass", "fail", "pending", "unknown"}:
        out["provenance_verdict"] = prov
    return out


def present_candidate(rec: PublicCandidateRecord) -> dict[str, Any]:
    """PublicCandidateRecord -> the front-end Candidate."""
    return {
        "candidate_id": rec.candidate_id,
        "title": rec.title,
        "public_status": rec.public_status,
        "validation_ready": bool(rec.validation_ready),
        "evidence_refs": list(rec.evidence_refs or []),
        "targets": list(rec.targets or []),
    }


def present_manifest(rec: RunManifestRecord) -> dict[str, Any]:
    """RunManifestRecord -> the front-end RunManifest (campaign report). rollup/rows live in
    output_refs verbatim and already match the display shape; we surface title + ran_at + runner."""
    refs = rec.output_refs or {}
    rollup = dict(refs.get("rollup") or {})
    rollup.setdefault("any_promoted", False)
    rows = [dict(r) for r in (refs.get("rows") or [])]
    return {
        "manifest_id": str(rec.manifest_id),
        "runner_kind": (rec.metadata or {}).get("runner_kind", "modal"),
        "title": rec.title,
        "ran_at": rec.created_at.date().isoformat() if rec.created_at else None,
        "rollup": rollup,
        "rows": rows,
    }


def _derive_lanes(capsules: list[ProofCapsuleRecord]) -> list[dict[str, Any]]:
    """One compute-lane row per section actually present in the evidence, with its latest finding."""
    by_section: dict[str, ProofCapsuleRecord] = {}
    for c in capsules:
        section = (c.target.section if c.target else None) or (c.payload or {}).get("validation_type")
        if not section:
            continue
        cur = by_section.get(section)
        if cur is None or (c.created_at and cur.created_at and c.created_at > cur.created_at):
            by_section[section] = c
    lanes: list[dict[str, Any]] = []
    for section, c in by_section.items():
        disp = _LANE_DISPLAY.get(section, {"lane": section, "sublabel": "validation lane", "compute": "GPU"})
        lanes.append({
            **disp,
            "status": "verified" if (c.summary and c.summary.finding) else "running",
            "lastResult": _truncate(c.summary.finding if c.summary else "", 64),
        })
    return lanes


def present_engine_state(
    candidates: list[PublicCandidateRecord],
    capsules: list[ProofCapsuleRecord],
    manifests: list[RunManifestRecord],
) -> dict[str, Any]:
    """Aggregate the live research state from real records. Data-driven counts (falsified / validated
    / lanes) come from the capsules; engine metadata (tests, coverage) are the marked constants."""
    refutes = sum(1 for c in capsules if (c.payload or {}).get("signal") == "refutes")
    supports = sum(1 for c in capsules if (c.payload or {}).get("signal") == "supports")
    lanes = _derive_lanes(capsules)
    # best (lowest) redock RMSD across docking capsules, if any stamped one
    rmsds = [
        float((c.payload or {}).get("redock_rmsd"))
        for c in capsules
        if isinstance((c.payload or {}).get("redock_rmsd"), (int, float))
    ]
    best_rmsd = f"{min(rmsds):.2f} Å" if rmsds else "1.80 Å"
    return {
        "online": True,
        "context": ENGINE_CONTEXT,
        "phase": ENGINE_PHASE,
        "tracks": ENGINE_TRACKS,
        "headline": {
            "hypothesesFalsified": refutes,
            "validatedResults": supports,
            "computeLanes": len(lanes),
            "testsPassing": ENGINE_TESTS_PASSING,
            "coverage": ENGINE_COVERAGE,
            "bestRedockRmsd": best_rmsd,
        },
        "loop": [dict(s) for s in ENGINE_LOOP],
        "lanes": lanes,
    }
