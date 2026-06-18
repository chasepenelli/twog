"""MoonshotRubric — the pre-registered "whole shabang" per moonshot (Increment 8).

Verifies the assembler (build_moonshot_rubric) + the publication gate. Hermetic: a fake SMILES
resolver (no network) + the real data/target_library.json (PIK3CA/KDR verified, MTOR unverified).
"""

from __future__ import annotations

from uuid import uuid4

import pytest

import json
from datetime import UTC, datetime, timedelta

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import (
    HSAResearchService, ProofCapsuleRecord, ProofCapsuleSummary, ProofCapsuleTarget,
    SQLiteResearchRepository, uuid4,
)

from hsa_research.ingestion_bridge.contracts import (
    MoonshotRubric, PublicCandidateGenerateRequest, PublicCandidateRecord, TherapyIdea, TherapyIdeaRecord,
)
from hsa_research.ingestion_bridge.service import _public_candidate_moonshot_gate


def _seed_capsule(repo, candidate_id, *, signal=None, validation_type=None, confidence=None,
                  packet_type="compute_artifact", controls_confound=None, updated_at=None):
    """Seed a signal-bearing proof capsule directly (mirrors test_falsification_planner._seed_capsule)."""
    payload = {}
    if signal is not None:
        payload["signal"] = signal
    if validation_type is not None:
        payload["validation_type"] = validation_type
    if confidence is not None:
        payload["confidence"] = confidence
    if controls_confound is not None:
        payload["controls_confound"] = controls_confound
    kw = {}
    if updated_at is not None:
        kw["updated_at"] = updated_at
        kw["created_at"] = updated_at
    capsule = ProofCapsuleRecord(
        workspace_id=uuid4(), checkout_manifest_hash="sha256:" + "b" * 24, candidate_id=candidate_id,
        packet_type=packet_type, requested_action="no_action",
        target=ProofCapsuleTarget(section=validation_type or "abstract"),
        summary=ProofCapsuleSummary(title="seed", finding="seeded signal", why_it_matters="rubric standing test",
                                    limitations=["synthetic fixture"]),
        payload=payload, content_hash="c" * 40, **kw,
    )
    return repo.upsert_proof_capsule(capsule)


class _FakeResolver:
    """No-network SMILES resolver — returns a SMILES for any therapy (so docking against a verified
    target resolves), None to simulate an unresolvable compound."""

    def __init__(self, smiles="C1=CC=CC=C1"):
        self._s = smiles

    def compound_smiles(self, name): return self._s
    def target_structure(self, t): return None
    def protein_sequence(self, t): return None


def _svc(tmp_path, name="ms"):
    svc = HSAResearchService(SQLiteResearchRepository(tmp_path / f"{name}.sqlite3", seed=False))
    svc.input_resolvers = _FakeResolver()
    return svc


# ---- assembler -------------------------------------------------------------------------------
def test_md_schedule_always_present_even_when_md_inputs_missing(tmp_path):
    svc = _svc(tmp_path)
    svc.seed_validation_ready_candidate("alpelisib-pik3ca", title="alpelisib × PIK3CA",
                                        evidence_refs=["curate:x"], targets=["PIK3CA"], candidate_therapies=["alpelisib"])
    r = svc.build_moonshot_rubric("alpelisib-pik3ca")
    md = next((t for t in r.test_plan if t.lane == "md"), None)
    assert md is not None, "the MD lane must be in the full pre-registered plan"
    assert md.inputs.md_schedule is not None, "the MD SCHEDULE must be present even when md inputs are unresolved"
    assert md.inputs.md_schedule.simulation_steps == 1000 and md.inputs.md_schedule.force_field
    assert md.inputs.readiness == "missing"  # md inputs aren't curated -> honestly missing
    assert md.maturity == "smoke"  # <=1000 steps => never sold as binding-pose-stability evidence


def test_target_verification_three_state(tmp_path):
    svc = _svc(tmp_path)
    svc.seed_validation_ready_candidate("c-pik3ca", title="verified target", evidence_refs=["curate:x"],
                                        targets=["PIK3CA"], candidate_therapies=["alpelisib"])
    svc.seed_validation_ready_candidate("c-mtor", title="unverified target", evidence_refs=["curate:x"],
                                        targets=["MTOR"], candidate_therapies=["everolimus"])
    r_ok = svc.build_moonshot_rubric("c-pik3ca")
    r_unv = svc.build_moonshot_rubric("c-mtor")
    assert r_ok.targets_needed[0].verification == "verified"
    assert r_unv.targets_needed[0].verification == "unverified"  # MTOR entry exists but verified=False
    # docking on the unverified target reports needs_verification, NOT missing (the spend-gate truth)
    dock = next((t for t in r_unv.test_plan if t.lane == "docking"), None)
    if dock is not None:
        assert dock.inputs.readiness == "needs_verification"


def test_rubric_hash_deterministic(tmp_path):
    svc = _svc(tmp_path)
    svc.seed_validation_ready_candidate("c-det", title="det", evidence_refs=["curate:x"],
                                        targets=["PIK3CA"], candidate_therapies=["alpelisib"])
    a = svc.build_moonshot_rubric("c-det")
    b = svc.build_moonshot_rubric("c-det")
    assert a.rubric_hash and a.rubric_hash == b.rubric_hash


def test_promotion_never_auto_and_falsifiable(tmp_path):
    svc = _svc(tmp_path)
    svc.seed_validation_ready_candidate("c-promo", title="promo", evidence_refs=["curate:x"],
                                        targets=["KDR"], candidate_therapies=["toceranib"])
    r = svc.build_moonshot_rubric("c-promo")
    assert r.promotion.auto_promotable is False  # typed invariant — never auto-promotes
    assert r.has_falsifiable_plan is True  # a real target+therapy yields pre-registered kill criteria
    assert all(t.kill_criterion for t in r.test_plan)


def test_no_therapy_idea_is_graceful(tmp_path):
    svc = _svc(tmp_path)
    svc.seed_validation_ready_candidate("c-noidea", title="no idea", evidence_refs=["curate:x"],
                                        targets=["PIK3CA"], candidate_therapies=["alpelisib"])
    r = svc.build_moonshot_rubric("c-noidea")
    assert r.moonshot_grade is False  # un-gradeable without a linked thesis record — honest, not a fake pass
    assert any("no_therapy_idea_linked" in n for n in r.assembly_notes)


def test_unknown_candidate_returns_none(tmp_path):
    assert _svc(tmp_path).build_moonshot_rubric("does-not-exist") is None


def test_gate_stored_verbatim(tmp_path):
    svc = _svc(tmp_path)
    idea = TherapyIdea(
        title="Vimentin peptide blockade", hypothesis="A VIM-directed peptide may disrupt HSA invasion.",
        rationale="VIM sits at a plausible tumor-ecology interface.", candidate_therapies=["vimentin-targeting peptide"],
        targets=["VIM"], biomarkers=["VIM expression"], evidence_refs=["PMID:1", "PMID:2"], evidence_strength="medium",
        risks=["sparse canine evidence"], next_experiments=["omics VIM readout"], priority_score=0.82,
    )
    svc.repository.upsert_therapy_idea(TherapyIdeaRecord(idea=idea, topic="VIM", status="ready_for_promotion", score=0.82))
    svc.repository.upsert_public_candidate(PublicCandidateRecord(
        candidate_id="vim-cand", title="Vimentin peptide", therapy_idea_id=idea.idea_id,
        targets=["VIM"], candidate_therapies=["vimentin-targeting peptide"], evidence_refs=["PMID:1"]))
    r = svc.build_moonshot_rubric("vim-cand")
    assert r.moonshot_gate == _public_candidate_moonshot_gate(
        svc.get_therapy_idea(idea.idea_id), min_score=0.8)  # RAW dict, zero drift
    assert r.moonshot_grade == bool(r.moonshot_gate["passed"])


# ---- publication gate ------------------------------------------------------------------------
def _moonshot_idea():
    return TherapyIdea(
        title="Cross-species PI3Ka strategy", hypothesis="Alpelisib engages PI3Ka across the canine-HSA × human-AS axis.",
        rationale="Mutation-selective PI3Ka inhibition; cross-species precision strategy.",
        candidate_therapies=["alpelisib"], targets=["PIK3CA"], biomarkers=["PIK3CA mutation"],
        evidence_refs=["PMID:1", "PMID:2", "PMID:3"], evidence_strength="high",
        risks=["alpelisib hyperglycemia in dogs"], next_experiments=["dock against verified PIK3CA pocket"],
        priority_score=0.9,
    )


def test_publication_injects_rubric_for_moonshot(tmp_path):
    svc = _svc(tmp_path)
    idea = _moonshot_idea()
    svc.repository.upsert_therapy_idea(TherapyIdeaRecord(idea=idea, source_brief_id=uuid4(), topic="PI3Ka", status="ready_for_promotion", score=0.9))
    res = svc.generate_public_candidate_snapshot(
        PublicCandidateGenerateRequest(therapy_idea_id=idea.idea_id, require_moonshot_grade=True, persist=False))
    assert res.snapshot is not None, f"a moonshot-grade idea with a falsifiable plan must publish; errors={res.errors}"
    assert "moonshot_rubric" in res.snapshot.payload  # the rubric is folded into the snapshot
    assert any(m.startswith("moonshot-rubric:") for m in res.snapshot.method_refs)


def test_publication_hard_fails_without_falsifiable_plan(tmp_path, monkeypatch):
    """A candidate that passes the score gate but has no falsifiable plan must NOT publish as moonshot."""
    svc = _svc(tmp_path)
    idea = _moonshot_idea()
    svc.repository.upsert_therapy_idea(TherapyIdeaRecord(idea=idea, topic="PI3Ka", status="ready_for_promotion", score=0.9))
    # force a rubric with no falsifiable plan (the internal generator builds its own service instance,
    # so patch at the class level)
    def _empty_rubric(self, candidate_id, **kw):
        return MoonshotRubric(candidate_id=candidate_id, has_falsifiable_plan=False, test_plan=[])
    monkeypatch.setattr(HSAResearchService, "build_moonshot_rubric", _empty_rubric)
    res = svc.generate_public_candidate_snapshot(
        PublicCandidateGenerateRequest(therapy_idea_id=idea.idea_id, require_moonshot_grade=True, persist=False))
    assert res.snapshot is None
    assert "missing_falsification_rubric" in res.errors


# ---- adversarial-review regression tests (Increment 8 fixes) ---------------------------------
def test_full_plan_reincludes_already_tested_lane(tmp_path):
    """Fix #1: full_plan=True must re-include an ALREADY-TESTED lane in the rubric (the planner's
    unconditional tested-lane skip previously dropped the strongest evidence lane from the snapshot)."""
    svc = _svc(tmp_path)
    svc.seed_validation_ready_candidate("c-tested", title="tested docking", evidence_refs=["curate:x"],
                                        targets=["PIK3CA"], candidate_therapies=["alpelisib"])
    _seed_capsule(svc.repository, "c-tested", signal="supports", validation_type="docking", confidence=0.8)
    r = svc.build_moonshot_rubric("c-tested")
    dock = next((t for t in r.test_plan if t.lane == "docking"), None)
    assert dock is not None, "an already-tested docking lane must still appear in the full pre-registered plan"
    assert dock.standing == "supports_unaudited"  # tested + supports, not auto-audited


def test_lane_signal_is_latest_wins_not_oldest(tmp_path):
    """Fix #2: a later 'supports' must supersede an earlier 'refutes' for the same lane — the standing
    baked into the content-hashed snapshot is the NEWEST signal, never an artifact of ledger order."""
    svc = _svc(tmp_path)
    svc.seed_validation_ready_candidate("c-latest", title="latest wins", evidence_refs=["curate:x"],
                                        targets=["PIK3CA"], candidate_therapies=["alpelisib"])
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    _seed_capsule(svc.repository, "c-latest", signal="refutes", validation_type="docking", confidence=0.8, updated_at=t0)
    _seed_capsule(svc.repository, "c-latest", signal="supports", validation_type="docking", confidence=0.8,
                  updated_at=t0 + timedelta(days=7))
    r = svc.build_moonshot_rubric("c-latest")
    dock = next((t for t in r.test_plan if t.lane == "docking"), None)
    assert dock is not None and dock.standing == "supports_unaudited"  # newest signal wins, NOT 'refuted'


def test_confound_audit_standing_is_its_own_state_not_audited_lane_signal(tmp_path):
    """Fix #4: a confound-audit test derives standing from its OWN audit state — an untested purity
    control must NOT inherit the audited (supporting) omics lane's 'supports' signal."""
    svc = _svc(tmp_path)
    svc.seed_validation_ready_candidate("c-audit", title="open confound", evidence_refs=["curate:x"],
                                        targets=["PIK3CA"], candidate_therapies=["alpelisib"])
    _seed_capsule(svc.repository, "c-audit", signal="supports", validation_type="omics", confidence=0.8)  # no control
    r = svc.build_moonshot_rubric("c-audit")
    audit = next((t for t in r.test_plan if t.addresses_confound is not None
                  and t.addresses_confound.kind == "tumor_purity"), None)
    assert audit is not None, "an open tumor_purity confound must yield a confound-audit test in the plan"
    assert audit.standing == "untested"  # the control hasn't run; never 'supports_unaudited'/'refuted'
    assert audit.standing not in ("supports_unaudited", "refuted")


def test_controlled_confound_surfaces_from_failure_corpus(tmp_path):
    """Fix #5: a successfully-controlled confound must appear in controlled_confounds (distill_belief_state
    drops it from open_confounds the moment it's controlled, so it would otherwise vanish from both)."""
    svc = _svc(tmp_path)
    svc.seed_validation_ready_candidate("c-ctrl", title="controlled confound", evidence_refs=["curate:x"],
                                        targets=["PIK3CA"], candidate_therapies=["alpelisib"])
    _seed_capsule(svc.repository, "c-ctrl", signal="supports", validation_type="omics", confidence=0.8)
    _seed_capsule(svc.repository, "c-ctrl", validation_type="omics", controls_confound="tumor_purity")  # the control
    r = svc.build_moonshot_rubric("c-ctrl")
    controlled = [c for c in r.confounds.controlled_confounds if c.kind == "tumor_purity"]
    assert controlled and controlled[0].status == "controlled"
    assert all(c.kind != "tumor_purity" for c in r.confounds.open_confounds)  # not double-counted as open


def test_publication_hard_fails_for_hollow_thesis_with_absent_target(tmp_path):
    """Fix #6: the publication HARD-FAIL gate is substantive (not a tautology). A score-passing thesis
    whose target does NOT exist in the verified-or-unverified library must NOT publish as moonshot —
    exercised through the REAL generate_public_candidate_snapshot, not a monkeypatched rubric."""
    svc = _svc(tmp_path)
    # Reuse the moonshot-grade PROSE (so the frontier score gate PASSES) but name a target absent from the
    # verified-or-unverified library — isolating the substantive falsifiable-plan gate as the thing that bites.
    base = _moonshot_idea()
    hollow = base.model_copy(update={"targets": ["MADEUPGENE"], "biomarkers": ["MADEUPGENE expression"]})
    svc.repository.upsert_therapy_idea(TherapyIdeaRecord(idea=hollow, topic="HOLLOW", status="ready_for_promotion", score=0.9))
    gate = _public_candidate_moonshot_gate(svc.get_therapy_idea(hollow.idea_id), min_score=0.8)
    assert gate["passed"] is True, "fixture must PASS the score gate so the falsifiable-plan gate is what bites"
    res = svc.generate_public_candidate_snapshot(
        PublicCandidateGenerateRequest(therapy_idea_id=hollow.idea_id, require_moonshot_grade=True, persist=False))
    assert res.snapshot is None
    assert "missing_falsification_rubric" in res.errors


# ---- reasoning spine (Increment 8b): the grounded scientific argument -------------------------
def _grounded_candidate(candidate_id="c-spine"):
    """A candidate carrying grounded reasoning on the candidate itself (the no-committee campaign path):
    a HYPOTHESIS-framed mechanism + conditional translational_path, deterministically composable."""
    return PublicCandidateRecord(
        candidate_id=candidate_id, title="alpelisib × PIK3CA",
        summary="Alpelisib engages mutant PI3Kα across the canine-HSA × human-AS axis.",
        mechanism="alpelisib is hypothesized to engage PIK3CA; mutation-selective PI3Kα inhibition.",
        translational_path="If alpelisib-PIK3CA engagement holds and the cross-species axis replicates, advance the pairing.",
        targets=["PIK3CA"], candidate_therapies=["alpelisib"], biomarkers=["PIK3CA mutation"],
        evidence_refs=["PMID:1", "PMID:2"],
    )


def test_spine_grounded_from_candidate_mechanism(tmp_path):
    """The spine turns each lane into a step in an argument, grounded in the named compound/target +
    the lane's REAL pre-registered kill criterion. Cites alpelisib, PIK3CA, and the 4.0 docking threshold."""
    svc = _svc(tmp_path)
    r = svc.build_moonshot_rubric("c-spine", candidate_record=_grounded_candidate())
    assert r.premises and r.premises[0].is_specified is True
    assert "alpelisib" in r.mechanistic_premise.lower()
    dock = next(t for t in r.test_plan if t.lane == "docking")
    assert "alpelisib" in dock.probes[0] and "PIK3CA" in dock.probes[0]
    assert dock.why_it_bears  # restates the lane's own kill-criterion rationale, bound to the mechanism
    # interpretation pre-commits every outcome from the REAL kill criterion (anti-HARKing)
    assert "gnina_cnn_affinity" in dock.interpretation.refutes and "4.0" in dock.interpretation.refutes
    assert "never accepted as proof" in dock.interpretation.supports
    # inference chain is falsification-first: every link carries the refutation that breaks it
    assert r.inference_chain and all(link.if_broken for link in r.inference_chain)
    assert r.inference_chain[0].from_lanes  # one link per lane in VOI order
    # the earned payoff is grounded + capped at survived_known_confounds (never "proven")
    assert r.expected_payoff.is_specified is True
    assert "survived_known_confounds" in r.expected_payoff.caveat


def test_spine_premise_unstated_when_no_reasoning(tmp_path):
    """When the thesis states no mechanism/rationale, the spine renders an HONEST 'premise_unstated'
    marker + a reasoning_unstated_upstream note — it never fabricates biology."""
    svc = _svc(tmp_path)
    svc.seed_validation_ready_candidate("c-bare", title="bare candidate", evidence_refs=["curate:x"],
                                        targets=["PIK3CA"], candidate_therapies=["alpelisib"])
    r = svc.build_moonshot_rubric("c-bare")
    assert r.premises[0].is_specified is False
    assert "premise_unstated" in r.premises[0].basis
    assert any("reasoning_unstated_upstream" in n for n in r.assembly_notes)
    # probes stay entity-grounded (name the compound/target) but carry NO invented mechanism clause
    dock = next(t for t in r.test_plan if t.lane == "docking")
    assert "alpelisib" in dock.probes[0]
    assert "requires" not in dock.probes[0]  # the 'that <mechanism> requires' clause is omitted, not faked


def test_spine_vocabulary_is_falsification_first(tmp_path):
    """No composed reasoning string overclaims: the words 'proven'/'cure' never appear, and the status
    word 'refuted' never leaks into interpretation/inference prose (vocabulary is supports/refutes/neutral)."""
    svc = _svc(tmp_path)
    r = svc.build_moonshot_rubric("c-spine", candidate_record=_grounded_candidate())
    # Scan COMPOSED prose only — the fixed payoff.caveat legitimately contains "never proven" (the ceiling).
    blob = json.dumps(
        [p.model_dump() for p in r.premises]
        + [link.model_dump() for link in r.inference_chain]
        + [t.interpretation.model_dump() for t in r.test_plan]
        + [r.expected_payoff.if_survives, r.expected_payoff.translational_claim, r.expected_payoff.next_step]
    ).lower()
    assert "proven" not in blob and "cure" not in blob and "guaranteed" not in blob
    # the caveat constant DOES (and must) pin the ceiling
    assert "never proven" in r.expected_payoff.caveat
    interp_inf = json.dumps(
        [link.model_dump() for link in r.inference_chain] + [t.interpretation.model_dump() for t in r.test_plan]
    ).lower()
    assert "refuted" not in interp_inf  # 'refutes'/'refuting' OK; 'refuted' is a rollup STATUS, not prose


def test_spine_smoke_md_interpretation_is_not_stability_proof(tmp_path):
    """A <=1000-step smoke MD run can fail-fast but NEVER confirm — interpretation.supports is structurally
    replaced so a sanity run is never sold as binding-pose-stability evidence."""
    svc = _svc(tmp_path)
    svc.seed_validation_ready_candidate("c-md", title="md candidate", evidence_refs=["curate:x"],
                                        targets=["PIK3CA"], candidate_therapies=["alpelisib"])
    r = svc.build_moonshot_rubric("c-md")
    md = next(t for t in r.test_plan if t.lane == "md")
    assert md.maturity == "smoke"
    assert "NOT binding-pose-stability evidence" in md.interpretation.supports
