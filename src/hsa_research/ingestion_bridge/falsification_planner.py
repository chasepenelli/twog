"""Active Falsification Planner — twog's first autonomous discovery move (increment 1).

Pure and deterministic: given a candidate and its signed proof-capsule ledger, distill the state of
belief and propose the next cheapest test that could KILL the leading hypothesis, pre-registering an
explicit kill-criterion. NO I/O here — no repository, no service, no compute. The service layer wraps
``propose`` in AgentRunner for durable provenance and supplies the runnable-lane set + cost function.

Design discipline (autonomy and rigor in the same act):
- Falsification-first: every generated test is framed to REFUTE, with a pre-registered kill-criterion.
- Grounded: a plan that addresses a confound must name the specific ledger capsule it would refute.
- Honest confidence: signal is read ONLY from compute_artifact capsules that actually carry it; a thin
  signal base (< 2 signalful capsules) caps confidence low rather than fabricating certainty.
"""

from __future__ import annotations

from typing import Any, Callable

from .contracts import (
    BeliefState,
    ConfoundFlag,
    FalsificationLane,
    FalsificationPlan,
    FalsificationPlannerResult,
    KillCriterion,
)

# Signal is read only from these packet types (a literature/citation capsule carries no run signal).
_SIGNAL_PACKET_TYPES = {"compute_artifact"}
_SIGNAL_VALUES = {"supports", "refutes", "neutral"}
# The science lanes this increment knows how to template a falsification test for.
_LANE_MEMBERS = set(FalsificationLane.__args__)  # type: ignore[attr-defined]
_THIN_SIGNAL_CONFIDENCE_CAP = 0.3


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def _capsule_confidence(payload: dict[str, Any]) -> float:
    raw = payload.get("confidence")
    try:
        return _clamp01(float(raw))
    except (TypeError, ValueError):
        return 0.5  # a signalful capsule with no stated confidence still carries some weight


def distill_belief_state(candidate: Any, capsules: list[Any], decisions: list[Any]) -> BeliefState:
    """Read the signed ledger into a BeliefState. ``decisions`` is accepted for signature stability
    (the multi-round controller will use it) but is not consumed in increment 1."""
    leading = (getattr(candidate, "summary", "") or getattr(candidate, "title", "") or "").strip()
    candidate_id = getattr(candidate, "candidate_id")

    supporting: list = []
    refuting: list = []
    contributions: list[float] = []  # signed: + supports, - refutes, 0 neutral
    tested: list[str] = []
    has_purity_control = False

    for cap in capsules:
        payload = getattr(cap, "payload", {}) or {}
        # tested-lane axis: any capsule that names a real lane counts that lane as tested.
        vt = payload.get("validation_type")
        if isinstance(vt, str) and vt in _LANE_MEMBERS and vt not in tested:
            tested.append(vt)
        section = getattr(getattr(cap, "target", None), "section", None)
        if isinstance(section, str) and section in _LANE_MEMBERS and section not in tested:
            tested.append(section)
        # an explicit control marker controls the tumor-purity confound (de-dup substrate).
        if payload.get("controls_confound") == "tumor_purity":
            has_purity_control = True
        # signal axis: only compute_artifact capsules carry a run signal.
        if getattr(cap, "packet_type", None) not in _SIGNAL_PACKET_TYPES:
            continue
        signal = payload.get("signal")
        if signal not in _SIGNAL_VALUES:
            continue
        weight = _capsule_confidence(payload)
        cap_id = getattr(cap, "capsule_id")
        if signal == "supports":
            supporting.append(cap_id)
            contributions.append(weight)
        elif signal == "refutes":
            refuting.append(cap_id)
            contributions.append(-weight)
        else:  # neutral
            contributions.append(0.0)

    signalful = len(contributions)
    net_raw = sum(contributions)
    if signalful == 0:
        net_signal = "none"
        net_confidence = 0.0
    else:
        if net_raw > 1e-9:
            net_signal = "supports"
        elif net_raw < -1e-9:
            net_signal = "refutes"
        else:
            net_signal = "neutral"
        magnitude = sum(abs(c) for c in contributions) / signalful
        net_confidence = magnitude if signalful >= 2 else min(magnitude, _THIN_SIGNAL_CONFIDENCE_CAP)

    open_confounds: list[ConfoundFlag] = []
    # An unaudited `supports` signal carries the tumor-purity confound until a control capsule exists.
    if supporting and not has_purity_control:
        open_confounds.append(
            ConfoundFlag(
                kind="tumor_purity",
                status="open",
                control_lane="omics",
                refutes_capsule_id=supporting[0],
            )
        )

    return BeliefState(
        candidate_id=candidate_id,
        leading_hypothesis=leading,
        net_signal=net_signal,
        net_confidence=_clamp01(net_confidence),
        supporting_capsule_ids=supporting,
        refuting_capsule_ids=refuting,
        open_confounds=open_confounds,
        tested_lanes=tested,
        ledger_capsule_count=len(capsules),
        signalful_capsule_count=signalful,
    )


def _generic_kill_criterion(lane: str) -> tuple[KillCriterion, str, str]:
    """Return (kill_criterion, test_objective, expected_signal_if_alive) for an orthogonal lane test."""
    if lane == "docking":
        return (
            KillCriterion(
                metric="gnina_cnn_affinity",
                comparator="<",
                threshold=4.0,
                observed_signal_kills="refutes",
                rationale="No measurable engagement (CNN affinity below threshold) refutes target-mediated action.",
            ),
            "Dock the compound against the target to test whether it engages the proposed site.",
            "supports",
        )
    if lane == "md":
        return (
            KillCriterion(
                metric="ligand_pocket_rmsd_nm",
                comparator=">",
                threshold=0.5,
                observed_signal_kills="refutes",
                rationale="If the ligand drifts out of the pocket under MD, it is not a stable binder.",
            ),
            "Run MD to test binding-pose stability of the complex.",
            "supports",
        )
    if lane == "omics":
        return (
            KillCriterion(
                metric="cross_species_axis_direction",
                comparator="signal_is",
                threshold="refutes",
                observed_signal_kills="refutes",
                rationale="A refuting cross-species expression axis kills the translational claim (neutral is inconclusive).",
            ),
            "Re-test the expression axis against the orthogonal cohort.",
            "supports",
        )
    # Fallback for lanes with no dedicated template (not runnable today, but keep it total).
    return (
        KillCriterion(
            metric=f"{lane}_readout",
            comparator="signal_is",
            threshold="refutes",
            observed_signal_kills="refutes",
            rationale=f"A refuting {lane} readout kills the leading hypothesis.",
        ),
        f"Run the {lane} lane as an orthogonal falsification test.",
        "supports",
    )


def _confound_plan(belief: BeliefState, flag: ConfoundFlag, cost_fn: Callable[[str], float]) -> FalsificationPlan:
    lane = flag.control_lane or "omics"
    voi = _clamp01(belief.net_confidence * 0.8)  # belief mass riding on the unaudited signal
    cost = cost_fn(lane)
    targets = [flag.refutes_capsule_id] if flag.refutes_capsule_id else []
    return FalsificationPlan(
        candidate_id=belief.candidate_id,
        hypothesis=belief.leading_hypothesis,
        test_objective=(
            "Re-run the supporting signal with tumor-purity (TME-composition) adjustment to rule out a "
            "purity confound before the signal can be trusted."
        ),
        lane=lane,
        validation_type=lane,
        kill_criterion=KillCriterion(
            metric="purity_adjusted_effect",
            comparator="signal_is",
            threshold="neutral",
            observed_signal_kills="neutral",
            rationale=(
                "If the supporting signal disappears after tumor-purity / cell-composition adjustment, "
                "it was a composition artifact, not biology."
            ),
        ),
        expected_signal_if_alive="supports",
        addresses_confound=flag,
        targets_capsule_ids=targets,
        est_cost_usd=cost,
        value_of_information=voi,
        rank_rationale=(
            f"Audits an open {flag.kind} confound on the leading signal — VOI {voi:.2f} at est ${cost:.2f}."
        ),
        novelty_note="Confound audit of an unaudited supporting capsule.",
    )


def _generic_plan(belief: BeliefState, lane: str, cost_fn: Callable[[str], float]) -> FalsificationPlan:
    kc, objective, expected = _generic_kill_criterion(lane)
    voi = _clamp01(max(belief.net_confidence, 0.2) * 0.4)
    cost = cost_fn(lane)
    return FalsificationPlan(
        candidate_id=belief.candidate_id,
        hypothesis=belief.leading_hypothesis,
        test_objective=objective,
        lane=lane,
        validation_type=lane,
        kill_criterion=kc,
        expected_signal_if_alive=expected,
        est_cost_usd=cost,
        value_of_information=voi,
        rank_rationale=f"Orthogonal {lane} falsification — VOI {voi:.2f} at est ${cost:.2f} ({voi / max(cost, 1e-6):.3f}/$).",
        novelty_note=f"Lane {lane} not yet tested for this candidate.",
    )


def rank_falsification_tests(
    belief: BeliefState,
    runnable_lanes: set[str],
    cost_fn: Callable[[str], float],
    ruled_out: frozenset[str] = frozenset(),
    inputs_unresolved: frozenset[str] = frozenset(),
) -> list[FalsificationPlan]:
    """Generate candidate tests and rank them by value-of-information per dollar (falsification-first).
    Confound-audit tests are keyed on the confound (not the lane), so they survive lane de-dup; generic
    orthogonal tests are skipped for already-tested lanes. ``ruled_out`` lanes (settled in the Failure
    Corpus) take a novelty penalty. ``inputs_unresolved`` lanes (the candidate has no real inputs for
    them) are flagged inputs_ready=False and rank BELOW all input-ready lanes — the planner prefers what
    it can actually run with real data."""
    plans: list[FalsificationPlan] = []

    for flag in belief.open_confounds:
        if flag.status != "open":
            continue
        lane = flag.control_lane or "omics"
        if lane in runnable_lanes and flag.refutes_capsule_id is not None:
            plans.append(_confound_plan(belief, flag, cost_fn))

    tested = set(belief.tested_lanes)
    for lane in sorted(runnable_lanes):
        if lane in tested:
            continue
        plans.append(_generic_plan(belief, lane, cost_fn))

    if ruled_out:  # novelty penalty: deprioritize approaches already settled (refuted) for this candidate
        plans = [
            p.model_copy(
                update={
                    "value_of_information": round(p.value_of_information * 0.5, 6),
                    "novelty_note": "Lane previously refuted for this candidate — deprioritized (novelty penalty).",
                }
            )
            if p.lane in ruled_out
            else p
            for p in plans
        ]

    if inputs_unresolved:  # flag tests the candidate has no real inputs for (still proposed, ranked last)
        plans = [
            p.model_copy(
                update={
                    "inputs_ready": False,
                    "novelty_note": (
                        f"{p.novelty_note} | inputs unresolved — attach "
                        f"candidate.metadata['lane_inputs']['{p.lane}'] to run real compute."
                    ).strip(" |"),
                }
            )
            if p.lane in inputs_unresolved
            else p
            for p in plans
        ]

    # Rank input-ready lanes FIRST (prefer what we can really run), then by VOI per dollar (cheaper
    # wins on equal VOI), then by raw cost, then lane for stability.
    plans.sort(
        key=lambda p: (not p.inputs_ready, -(p.value_of_information / max(p.est_cost_usd, 1e-6)), p.est_cost_usd, p.lane)
    )
    return plans


def propose(
    candidate: Any,
    capsules: list[Any],
    decisions: list[Any],
    *,
    runnable_lanes: set[str],
    cost_fn: Callable[[str], float],
    ruled_out: frozenset[str] = frozenset(),
    inputs_unresolved: frozenset[str] = frozenset(),
) -> FalsificationPlannerResult:
    """Compose belief distillation + ranking into a read-only proposal."""
    belief = distill_belief_state(candidate, capsules, decisions)
    ranked = rank_falsification_tests(
        belief, set(runnable_lanes), cost_fn, ruled_out=ruled_out, inputs_unresolved=inputs_unresolved
    )

    blockers: list[str] = []
    if belief.signalful_capsule_count < 2:
        blockers.append("thin_signal_base")

    proposed = ranked[0] if ranked else None
    if proposed is None:
        blockers.append("no_runnable_lane")

    return FalsificationPlannerResult(
        candidate_id=belief.candidate_id,
        belief_state=belief,
        proposed=proposed,
        alternatives=ranked[1:6],
        blockers=blockers,
        runnable_lanes=sorted(runnable_lanes),
    )
