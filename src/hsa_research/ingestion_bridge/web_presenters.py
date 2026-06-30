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
    # Provenance-forward: the verifiable anchors (recomputable content_hash, the hash-linked edit chain,
    # who produced it, what pinned inputs it's bound to) — so the receipt is re-derivable, not just stated.
    out["content_hash"] = rec.content_hash
    if rec.parent_content_hash:
        out["parent_content_hash"] = rec.parent_content_hash
    out["lineage_index"] = rec.lineage_index
    if rec.submitted_by:
        out["submitted_by"] = rec.submitted_by
    if rec.candidate_snapshot_hash:
        out["candidate_snapshot_hash"] = rec.candidate_snapshot_hash
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


def _present_kill_criterion(kc: dict[str, Any] | None) -> dict[str, Any] | None:
    """Flatten a KillCriterion dict to the display shape (the anti-p-hacking pre-registered threshold)."""
    if not isinstance(kc, dict):
        return None
    return {
        "metric": kc.get("metric"),
        "comparator": kc.get("comparator"),
        "threshold": kc.get("threshold"),
        "kills_on": kc.get("observed_signal_kills"),
        "rationale": _truncate(str(kc.get("rationale") or ""), 200),
    }


def present_rubric(rubric: dict[str, Any]) -> dict[str, Any]:
    """The published MoonshotRubric (snapshot payload['moonshot_rubric']) -> the front-end MoonshotRubric:
    the pre-registered "whole shabang" — thesis + moonshot-gate verdict, targets with 3-state docking
    verification, compounds with SMILES-resolution status, the ordered per-lane test plan (each with its
    pre-registered kill criterion, standing, maturity, and the MD schedule for the md lane), the honest
    inputs readiness rollup, confounds to audit, the cross-species replication test, and the never-auto
    promotion bar. Pure projection — answers "this capsule is not just a question; here is exactly what
    the moonshot must show and how it will be tested." Defensive over a plain dict (degrades gracefully)."""
    r = rubric or {}
    gate = r.get("moonshot_gate") or {}

    targets = [
        {
            "target": t.get("target"),
            "role": t.get("role"),
            "verification": t.get("verification"),  # verified | unverified | absent (the docking spend gate)
            "uniprot": t.get("uniprot"),
            "pdb_id": t.get("pdb_id"),
            "chain": t.get("chain"),
            "redock_rmsd": t.get("redock_rmsd"),
            "cocrystal_ligand_code": t.get("cocrystal_ligand_code"),
        }
        for t in (r.get("targets_needed") or [])
    ]
    compounds = [
        {
            "name": c.get("name"),
            "role": c.get("role"),
            "smiles": c.get("smiles"),
            "readiness": c.get("readiness"),  # resolved | needs_verification | missing (NEVER fabricated)
            "resolution_source": c.get("resolution_source"),
            "intended_targets": list(c.get("intended_targets") or []),
        }
        for c in (r.get("compounds_needed") or [])
    ]

    def _lane_test(t: dict[str, Any]) -> dict[str, Any]:
        ins = t.get("inputs") or {}
        md = ins.get("md_schedule")
        interp = t.get("interpretation") or {}
        return {
            "order": t.get("order"),
            "lane": t.get("lane"),
            "objective": _truncate(str(t.get("test_objective") or ""), 220),
            "standing": t.get("standing"),  # untested | queued | supports_unaudited | refuted | controlled
            "maturity": t.get("maturity"),  # smoke (<=1000 MD steps) | production
            "inputs_ready": bool(t.get("inputs_ready")),
            "is_proposed": bool(t.get("is_proposed")),  # the genuine NEXT test to run
            "autonomously_runnable": bool(t.get("autonomously_runnable")),
            "est_cost_usd": t.get("est_cost_usd"),
            "value_of_information": t.get("value_of_information"),
            "kill_criterion": _present_kill_criterion(t.get("kill_criterion")),
            "expected_signal_if_alive": t.get("expected_signal_if_alive"),
            "addresses_confound": (t.get("addresses_confound") or {}).get("kind") if t.get("addresses_confound") else None,
            # reasoning spine: what this lane PROBES + why it bears on the thesis + the pre-committed reading
            "probes": list(t.get("probes") or []),
            "why_it_bears": _truncate(str(t.get("why_it_bears") or ""), 360),
            "interpretation": {
                "supports": _truncate(str(interp.get("supports") or ""), 360),
                "refutes": _truncate(str(interp.get("refutes") or ""), 360),
                "neutral": _truncate(str(interp.get("neutral") or ""), 360),
            },
            "inputs": {
                "readiness": ins.get("readiness"),
                "resolution_source": ins.get("resolution_source"),
                "required_keys": list(ins.get("required_keys") or []),
                "missing": list(ins.get("missing") or []),
                "md_schedule": (
                    {
                        "simulation_steps": md.get("simulation_steps"),
                        "temperature": md.get("temperature"),
                        "ph": md.get("ph"),
                        "force_field": md.get("force_field"),
                        "solvent_model": md.get("solvent_model"),
                        "equilibration": _truncate(str(md.get("equilibration") or ""), 400),
                        "preparation_method": _truncate(str(md.get("preparation_method") or ""), 400),
                    }
                    if isinstance(md, dict)
                    else None
                ),
            },
        }

    rollup = r.get("inputs_rollup") or {}
    confounds = r.get("confounds") or {}
    cross = r.get("cross_species") or {}
    promo = r.get("promotion") or {}

    def _flags(items: Any) -> list[dict[str, Any]]:
        return [{"kind": f.get("kind"), "status": f.get("status"), "control_lane": f.get("control_lane")}
                for f in (items or [])]

    premises = [
        {
            "claim": _truncate(str(p.get("claim") or ""), 300),
            "basis": _truncate(str(p.get("basis") or ""), 400),
            "supports_quality": _truncate(str(p.get("supports_quality") or ""), 220),
            "strength": p.get("strength", "unknown"),
            "is_specified": bool(p.get("is_specified")),
        }
        for p in (r.get("premises") or [])
    ]
    inference_chain = [
        {
            "step": link.get("step"),
            "from_lanes": list(link.get("from_lanes") or []),
            "infers": _truncate(str(link.get("infers") or ""), 360),
            "if_broken": _truncate(str(link.get("if_broken") or ""), 360),
        }
        for link in (r.get("inference_chain") or [])
    ]
    payoff = r.get("expected_payoff") or {}

    return {
        "rubric_version": r.get("rubric_version", "moonshot-rubric-v1"),
        "candidate_id": r.get("candidate_id"),
        "title": r.get("title"),
        "thesis": _truncate(str(r.get("thesis") or ""), 600),
        # reasoning spine: premise ("because of A") → inference chain → expected payoff
        "mechanistic_premise": _truncate(str(r.get("mechanistic_premise") or ""), 600),
        "premises": premises,
        "inference_chain": inference_chain,
        "expected_payoff": {
            "if_survives": _truncate(str(payoff.get("if_survives") or ""), 400),
            "translational_claim": _truncate(str(payoff.get("translational_claim") or ""), 300),
            "next_step": _truncate(str(payoff.get("next_step") or ""), 240),
            "value_of_information": payoff.get("value_of_information"),
            "is_specified": bool(payoff.get("is_specified")),
            "caveat": _truncate(str(payoff.get("caveat") or ""), 240),
        },
        "moonshot_grade": bool(r.get("moonshot_grade")),
        "moonshot_score": r.get("moonshot_score"),
        "moonshot_gate": {
            "passed": bool(gate.get("passed")),
            "weighted_score": gate.get("weighted_score"),
            "reasons": list(gate.get("reasons") or [])[:8],
            "blockers": list(gate.get("blockers") or []),
        },
        "net_signal": r.get("net_signal", "none"),
        "net_confidence": r.get("net_confidence", 0.0),
        "signalful_capsule_count": r.get("signalful_capsule_count", 0),
        "has_falsifiable_plan": bool(r.get("has_falsifiable_plan")),
        "ready_to_run": bool(r.get("ready_to_run")),
        "runnable_lanes": list(r.get("runnable_lanes") or []),
        "targets_needed": targets,
        "compounds_needed": compounds,
        "test_plan": [_lane_test(t) for t in (r.get("test_plan") or [])],
        "inputs_rollup": {
            "resolved_lanes": list(rollup.get("resolved_lanes") or []),
            "needs_verification_lanes": list(rollup.get("needs_verification_lanes") or []),
            "missing_lanes": list(rollup.get("missing_lanes") or []),
            "ready_to_run_lanes": list(rollup.get("ready_to_run_lanes") or []),
            "blockers": list(rollup.get("blockers") or [])[:20],
        },
        "confounds": {
            "open": _flags(confounds.get("open_confounds")),
            "controlled": _flags(confounds.get("controlled_confounds")),
            "audit_policy": _truncate(str(confounds.get("audit_policy") or ""), 400),
        },
        "cross_species": {
            "species": list(cross.get("species") or []),
            "disease_context": cross.get("disease_context"),
            "replication_axis": _truncate(str(cross.get("replication_axis") or ""), 400),
            "replication_lane": cross.get("replication_lane"),
            "orthogonal_cohort_required": bool(cross.get("orthogonal_cohort_required")),
            "kill_criterion": _present_kill_criterion(cross.get("kill_criterion")),
            "evidence_to_date": list(cross.get("evidence_to_date") or []),
        },
        "promotion": {
            "auto_promotable": False,  # typed invariant — never auto-promoted
            "required_surviving_lanes": list(promo.get("required_surviving_lanes") or []),
            "required_confounds_controlled": list(promo.get("required_confounds_controlled") or []),
            "cross_species_replication_required": bool(promo.get("cross_species_replication_required", True)),
            "min_signalful_capsules": promo.get("min_signalful_capsules", 2),
            "statement": _truncate(str(promo.get("statement") or ""), 600),
        },
        "evidence_anchors": list(r.get("evidence_anchors") or []),
        "risks": list(r.get("risks") or []),
        "assembly_notes": list(r.get("assembly_notes") or []),
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
    best_rmsd = f"{min(rmsds):.2f} Å" if rmsds else "—"  # honest: no fabricated "1.80 Å" when no real redock
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


def _iso(ts: Any) -> str | None:
    try:
        return ts.isoformat() if ts is not None else None
    except Exception:
        return None


def _status_str(s: Any) -> str:
    v = getattr(s, "value", s)
    return str(v) if v is not None else "unknown"


def present_activity_feed(
    *,
    agent_runs: list[Any] | None = None,
    compute_jobs: list[Any] | None = None,
    capsules: list[ProofCapsuleRecord] | None = None,
    manifests: list[RunManifestRecord] | None = None,
    limit: int = 40,
) -> dict[str, Any]:
    """Merge the engine's time-ordered ledgers into ONE reverse-chronological activity stream — agents
    reacting, GPU lanes dispatching, evidence capsules landing, campaigns running — with HONEST status and
    an idle/online signal derived from REAL job state (the engine genuinely idles $0 when out of runnable
    work, so 'idle' is the common, honest state — never faked as busy). Pure projection over fetched
    records; mock-runner jobs are labelled 'mock (CI)' so simulated activity is never passed off as GPU."""
    events: list[dict[str, Any]] = []

    for j in (compute_jobs or []):
        runner = getattr(j, "runner_kind", None)
        lane = getattr(j, "validation_type", None)
        profile = getattr(j, "compute_profile", None)
        runner_label = "mock (CI)" if runner == "mock" else (runner or "—")
        events.append({
            "type": "compute",
            "occurred_at": _iso(getattr(j, "updated_at", None) or getattr(j, "created_at", None)),
            "status": _status_str(getattr(j, "status", None)),
            "title": f"{lane or 'compute'} lane · {runner_label}" + (f"/{profile}" if profile else ""),
            "candidate_id": getattr(j, "candidate_id", None),
            "lane": lane,
        })

    for a in (agent_runs or []):
        events.append({
            "type": "agent",
            "occurred_at": _iso(getattr(a, "completed_at", None) or getattr(a, "started_at", None) or getattr(a, "created_at", None)),
            "status": _status_str(getattr(a, "status", None)),
            "title": f"{getattr(a, 'agent_name', 'agent')} · {_status_str(getattr(a, 'status', None))}",
        })

    for c in (capsules or []):
        payload = getattr(c, "payload", None) or {}
        sig = payload.get("signal")
        section = (c.target.section if getattr(c, "target", None) else None) or payload.get("validation_type") or "evidence"
        events.append({
            "type": "capsule",
            "occurred_at": _iso(getattr(c, "updated_at", None) or getattr(c, "created_at", None)),
            "status": _status_str(getattr(c, "status", None)),
            "title": f"capsule · {sig if sig in _VALID_SIGNALS else 'evidence'} · {section}",
            "signal": sig if sig in _VALID_SIGNALS else None,
            "candidate_id": getattr(c, "candidate_id", None),
            "capsule_id": str(getattr(c, "capsule_id", "")) or None,
        })

    for m in (manifests or []):
        events.append({
            "type": "campaign",
            "occurred_at": _iso(getattr(m, "created_at", None)),
            "status": _status_str(getattr(m, "status", None)),
            "title": getattr(m, "title", None) or "falsification campaign",
        })

    events = [e for e in events if e["occurred_at"]]
    events.sort(key=lambda e: e["occurred_at"], reverse=True)  # ISO-UTC sorts lexically == chronologically

    # in-flight = a job committed to run but not yet terminal. 'approved' is post-gate / pre-dispatch —
    # genuinely work-in-progress (NOT idle). 'needs_approval' is excluded (awaiting the human gate).
    _ACTIVE = ("approved", "queued", "submitted", "running")
    running = [j for j in (compute_jobs or []) if _status_str(getattr(j, "status", None)) in _ACTIVE]
    idle = len(running) == 0
    return {
        "events": events[:limit],
        "running_jobs": len(running),
        "idle": idle,
        "idle_reason": "idle — out of runnable work ($0)" if idle else None,
        "last_event_at": events[0]["occurred_at"] if events else None,
    }
