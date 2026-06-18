"""MoonshotRubric — the pre-registered "whole shabang" per moonshot (Increment 8).

Verifies the assembler (build_moonshot_rubric) + the publication gate. Hermetic: a fake SMILES
resolver (no network) + the real data/target_library.json (PIK3CA/KDR verified, MTOR unverified).
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository

from hsa_research.ingestion_bridge.contracts import (
    MoonshotRubric, PublicCandidateGenerateRequest, PublicCandidateRecord, TherapyIdea, TherapyIdeaRecord,
)
from hsa_research.ingestion_bridge.service import _public_candidate_moonshot_gate


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
