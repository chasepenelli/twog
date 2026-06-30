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
    RubricExpectedPayoff,
    RubricInferenceLink,
    RubricPremise,
    TestInterpretation,
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


# ---------------------------------------------------------------------------------------------------
# Reasoning-spine composers (pure, deterministic — used by service.build_moonshot_rubric)
#
# Turn the rubric from a checklist ("dock / MD / omics") into a falsification-first ARGUMENT
# ("because of A, we test X/Y/Z to interrogate quality Q; if each survives its kill criterion we are
# entitled to expect P; any single refutation kills it"). Every string traces to a REAL input — the
# therapy idea's mechanism/rationale/biomarkers/translational_path, the candidate's named compound/
# target, or the lane's OWN pre-registered KillCriterion. Where the thesis is silent, these emit honest
# "unstated" markers, NEVER fabricated biology. No LLM, no I/O, no RNG/datetime/uuid — so the composed
# prose folds into the deterministic rubric_hash. The verdict ceiling is structurally survived_known_
# confounds; the words "true"/"proven"/"cure" never appear; vocabulary is supports|refutes|neutral only.
# ---------------------------------------------------------------------------------------------------

def mech_short(mechanism: str | None) -> str:
    """Hash-stable short form of a mechanism: its first CLAUSE — stop at the earliest sentence/clause
    boundary (period, semicolon, colon, em/en-dash) — capped at 200. Empty/None -> ''. Deterministic
    (no RNG/locale), so identical mechanism text always yields the identical substring."""
    s = (mechanism or "").strip()
    if not s:
        return ""
    cut = len(s)
    for delim in (". ", "; ", ": ", " — ", " – ", " - "):
        i = s.find(delim)
        if i != -1:
            cut = min(cut, i)
    return s[:cut].strip()[:200]


_DECLARATIVE_MARKERS = (" is ", " are ", " was ", " were ", " engages ", "hypothesiz", " should ", " acts ")


def _mech_label(mechanism_short: str) -> str:
    """A short phrase safe to embed mid-sentence ('the site that <label> requires', 'treat <label> as
    live'). Only a genuinely short NOUN PHRASE (<=60 chars, no finite-verb/declarative marker) is embedded
    verbatim; a declarative or long mechanism is referenced as 'the proposed mechanism' (its full text
    lives in the premise block), so the prose never reads clunky. '' when no mechanism. Deterministic."""
    s = (mechanism_short or "").strip()
    if not s:
        return ""
    declarative = any(m in s.lower() for m in _DECLARATIVE_MARKERS)
    if len(s) <= 60 and not declarative:
        return s  # a clean noun phrase ("mutation-selective PI3Kα inhibition") — safe to embed
    return "the proposed mechanism"


def compose_premise(
    *, mechanism: str, rationale: str, claim_fallback: str, evidence_strength: str, is_specified: bool
) -> RubricPremise:
    """The grounded 'because of A'. `is_specified` (computed by the caller from whether the SOURCE stated
    a mechanism/rationale, not a fallback) gates honest 'premise_unstated'. `strength` is the idea's
    stated tier VERBATIM — never upgraded (the verdict ceiling). `basis`/`supports_quality` only carry
    text that ADDS to the claim — when the only grounding is the bare hypothesis (the roster case), they
    stay empty rather than echoing the claim sentence three times."""
    ms = mech_short(mechanism)
    claim = (ms or claim_fallback or "").strip()
    if not is_specified:
        basis = "premise_unstated — no upstream mechanism/rationale; treat as hypothesis-only"
    else:
        # the rationale is the 'why' ONLY when it's real and distinct from the claim/mechanism — else
        # it would just repeat the claim, so leave basis empty (the UI then shows the claim alone).
        r = (rationale or "").strip()
        basis = r if (r and r != claim and r != (mechanism or "").strip()) else ""
    supports_quality = ms.strip() if ms.strip() and ms.strip() != claim else ""  # never echo the claim
    strength = evidence_strength if evidence_strength in ("high", "medium", "low", "unknown") else "unknown"
    return RubricPremise(
        claim=claim[:1000],
        basis=basis[:1500],
        supports_quality=supports_quality[:600],
        strength=strength,
        is_specified=is_specified,
    )


def compose_lane_reasoning(
    *,
    lane: str,
    compound: str,
    target: str,
    pdb_id: str | None,
    biomarker: str | None,
    kill_criterion: KillCriterion,
    mechanism_short: str,
    maturity: str,
) -> tuple[list[str], str, TestInterpretation]:
    """(probes, why_it_bears, interpretation) for one lane — entity-grounded in the compound/target/
    biomarker the candidate NAMES + the lane's REAL kill criterion. Empty mechanism still yields
    entity-grounded probes (it never invents biology). interpretation pre-commits every outcome; for a
    smoke MD run `supports` is structurally replaced so a sanity run is never sold as stability proof."""
    kc = kill_criterion
    thr = str(kc.threshold)
    pdb = f" (PDB {pdb_id})" if pdb_id else ""
    label = _mech_label(mechanism_short)  # safe to embed mid-sentence ('' when no mechanism)
    if lane == "docking":
        probes = [
            f"Can the modeled pose let {compound} occupy the site at {target}{pdb}"
            + (f" that {label} requires" if label else "")
            + "?"
        ]
    elif lane == "md":
        probes = [
            f"Does the docked {compound}-{target} pose stay seated under dynamics "
            f"(ligand_pocket_rmsd_nm <= {thr}) — stable engagement vs a single-pose artifact?"
        ]
    elif lane == "cofolding":
        probes = [f"Does an independent co-fold predict the {compound}-{target} complex (iptm >= {thr})?"]
    elif lane == "omics":
        probes = [f"Does the {biomarker or target} axis move in the SAME direction across canine HSA x human AS?"]
    else:
        probes = [f"Does the {lane} readout support engagement of {compound} at {target}?"]

    if label:
        why = f"This lane interrogates {label} — the {lane} precondition the thesis depends on. {kc.rationale}"
    else:
        why = f"generic lane test; mechanism unstated upstream. {kc.rationale}"

    refutes = (
        f"Observed {kc.metric} {kc.comparator} {thr} -> {kc.rationale} "
        f"The target-mediated story for {compound} collapses here."
    )
    if maturity == "smoke":
        supports = (
            "Pose did not drift within the <=1000-step smoke window; this is NOT binding-pose-stability "
            "evidence and cannot be read as engagement confirmation."
        )
    else:
        supports = (
            f"Consistent with {label or ('engagement at ' + target)}; it stays live but remains "
            "unaudited (open confounds not yet controlled) — never accepted as proof."
        )
    neutral = "Inconclusive; belief unmoved — the lane does not license the next inference link."
    interp = TestInterpretation(supports=supports[:800], refutes=refutes[:800], neutral=neutral[:800])
    return [p[:600] for p in probes][:8], why[:1200], interp


def compose_inference_chain(
    steps: list[tuple[str, str, KillCriterion]],
    *,
    target: str,
    mechanism_short: str,
    translational_path: str,
    payoff_specified: bool,
) -> list[RubricInferenceLink]:
    """One link per test_plan entry in the planner's existing VOI order. Conjunctive-survival /
    single-refutation-kills: each `if_broken` restates that lane's REAL kill criterion as the refutation
    that breaks the chain HERE. The terminal link caps at survived_known_confounds (+ the payoff if a
    real translational path exists). `steps` = parallel (lane, expected_signal_if_alive, kill_criterion)."""
    links: list[RubricInferenceLink] = []
    label = _mech_label(mechanism_short)
    n = len(steps)
    for i, (lane, expected_signal, kc) in enumerate(steps):
        thr = str(kc.threshold)
        if i + 1 < n:
            next_lane = steps[i + 1][0]
            infers = (
                f"If {lane} survives ({expected_signal} at {target}), treat "
                f"{label or 'engagement'} as live and proceed to {next_lane}."
            )
        else:
            tail = f" and we are entitled to expect: {translational_path}" if (payoff_specified and translational_path) else ""
            infers = (
                f"If {lane} survives, every pre-registered necessary condition has survived its kill "
                f"criterion -> the claim has survived_known_confounds{tail}."
            )
        if_broken = (
            f"A refuting {lane} readout ({kc.metric} {kc.comparator} {thr}) breaks the chain at step {i + 1}; "
            "the leading claim dies — survival of earlier lanes does not rescue it; any single refutation is "
            "sufficient."
        )
        links.append(RubricInferenceLink(step=i + 1, from_lanes=[lane], infers=infers[:1000], if_broken=if_broken[:800]))
    return links


def compose_expected_payoff(
    *, translational_path: str, next_experiment: str, mechanism_short: str, vois: list[float]
) -> RubricExpectedPayoff:
    """The conclusion the program is trying to EARN. Conditional + capped at survived_known_confounds
    (the fixed caveat). Honest fallback when no upstream translational path exists."""
    tp = (translational_path or "").strip()
    is_specified = bool(tp)
    if_survives = "If all lanes survive their pre-registered kill criteria and the open confounds are controlled, " + (
        tp if tp else "the candidate has survived its known confounds (no upstream translational path stated)"
    )
    voi = round(sum(vois) / len(vois), 4) if vois else None
    return RubricExpectedPayoff(
        if_survives=if_survives[:1500],
        translational_claim=(tp or mechanism_short or "")[:1200],
        next_step=(next_experiment or "Confirm the cross-species axis in an orthogonal cohort.")[:800],
        value_of_information=voi,
        is_specified=is_specified,
    )


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
    if lane == "cofolding":
        return (
            KillCriterion(
                metric="iptm",
                comparator="<",
                threshold=0.5,
                observed_signal_kills="refutes",
                rationale="A low interface predicted-TM (no confident co-folded complex) refutes binding.",
            ),
            "Co-fold the target with the ligand to test complex formation + affinity.",
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
    include_tested: bool = False,
) -> list[FalsificationPlan]:
    """Generate candidate tests and rank them by value-of-information per dollar (falsification-first).
    Confound-audit tests are keyed on the confound (not the lane), so they survive lane de-dup; generic
    orthogonal tests are skipped for already-tested lanes UNLESS ``include_tested`` (the rubric wants the
    COMPLETE intended plan, incl. already-run lanes). ``ruled_out`` lanes (settled in the Failure Corpus)
    take a novelty penalty. ``inputs_unresolved`` lanes (the candidate has no real inputs for them) are
    flagged inputs_ready=False and rank BELOW all input-ready lanes — the planner prefers what it can
    actually run with real data."""
    plans: list[FalsificationPlan] = []

    for flag in belief.open_confounds:
        if flag.status != "open":
            continue
        lane = flag.control_lane or "omics"
        if lane in runnable_lanes and flag.refutes_capsule_id is not None:
            plans.append(_confound_plan(belief, flag, cost_fn))

    tested = set(belief.tested_lanes)
    for lane in sorted(runnable_lanes):
        if lane in tested and not include_tested:
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
    include_tested: bool = False,
) -> FalsificationPlannerResult:
    """Compose belief distillation + ranking into a read-only proposal."""
    belief = distill_belief_state(candidate, capsules, decisions)
    ranked = rank_falsification_tests(
        belief, set(runnable_lanes), cost_fn, ruled_out=ruled_out, inputs_unresolved=inputs_unresolved,
        include_tested=include_tested,
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
