"""End-to-end demo: a real frontier therapy hypothesis through the twog machine.

Seeds a structured therapy idea (spleen-tropic LNP-mRNA + mTOR-axis for canine splenic
hemangiosarcoma → human angiosarcoma), then runs it through the pipeline we built:

    therapy idea
      → validation decision (validation_ready)
      → generate_public_candidate_snapshot   (Phase-0 engine)
      → assess_candidate_validation_readiness (Phase 1 gate)
      → run_compute_validation_flow (mock provider, no GPU)  (Phase 2 loop)
      → accept_proof_capsule  → promote_proof_capsule_to_candidate
      → candidate evidence grows, readiness re-assessed

The COMPUTE is mocked (the mock ComputeRunner — no real docking/GPU). The LOOP, the gate,
the capsule, and the promotion are the real service code. Evidence refs are illustrative
placeholders for a demo, not asserted citations.

Run:  cd ~/twog-cleanup && . .venv/bin/activate && python scripts/demo_frontier_therapy.py
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from uuid import UUID, uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hsa_research.ingestion_bridge.contracts import (  # noqa: E402
    PublicCandidateGenerateRequest,
    TherapyIdea,
    TherapyIdeaRecord,
    ValidationAssayContext,
    ValidationDecisionPacket,
    ValidationDecisionRecord,
    ValidationRequest,
    ValidationRequestQueueItem,
)
from hsa_research.ingestion_bridge.local_store import SQLiteResearchRepository  # noqa: E402
from hsa_research.ingestion_bridge.service import HSAResearchService, _public_candidate_id  # noqa: E402


def h(title: str) -> None:
    print("\n" + "=" * 78 + f"\n  {title}\n" + "=" * 78)


def main() -> None:
    db = Path(tempfile.gettempdir()) / "twog_frontier_demo.sqlite3"
    db.unlink(missing_ok=True)
    repo = SQLiteResearchRepository(db, seed=False)
    service = HSAResearchService(repo)

    h("1. THE FRONTIER HYPOTHESIS  (seeded as a therapy idea)")
    idea = TherapyIdea(
        title="Spleen-tropic LNP-mRNA immunopayload + mTOR axis for canine splenic hemangiosarcoma",
        hypothesis=(
            "Lipid-nanoparticle biodistribution to spleen/liver and vascular endothelium is "
            "mechanistically matched to splenic HSA (an endothelial cancer), enabling localized "
            "mRNA-encoded IL-12 immune activation that, combined with next-generation mTOR "
            "inhibition of the PI3K/AKT/mTOR endothelial survival axis, converts a cold vascular "
            "tumor toward responsiveness — with direct translational relevance to human angiosarcoma."
        ),
        rationale=(
            "LNPs pool in liver/spleen and endothelium — a delivery 'bug' for most tumors but a "
            "bullseye for splenic HSA, which IS endothelial and splenic. Local IL-12 mRNA solves "
            "IL-12's systemic-toxicity barrier; PTEN-loss-driven mTOR activity is the endothelial "
            "survival axis to co-target. Spontaneous canine HSA is the highest-fidelity "
            "immunocompetent model of human angiosarcoma."
        ),
        candidate_therapies=[
            "LNP-mRNA-IL12 (localized)",
            "next-gen mTORC1-selective inhibitor (RMC-5552-class)",
            "propranolol (beta-adrenergic blockade)",
            "CD47-blocking mRNA (optional co-payload)",
        ],
        targets=["KDR/VEGFR2", "KIT", "PIK3CA", "AKT1", "MTOR", "PTEN", "ADRB1", "ADRB2", "CD47"],
        biomarkers=["PTEN loss", "pS6 (mTOR activity)", "VEGFR2 expression", "TAM density"],
        mechanism=(
            "Spleen/endothelium-tropic LNP delivers IL-12 mRNA locally (avoiding systemic IL-12 "
            "toxicity) to inflame a cold vascular tumor; concurrent mTORC1-selective inhibition "
            "starves the PTEN-loss endothelial proliferative program and supports T-cell memory — "
            "immune + anti-proliferative synergy."
        ),
        evidence_refs=[
            "LNP-spleen-tropism:SORT-lipids (illustrative)",
            "canine-HSA:PTEN-loss-mTOR (illustrative)",
            "intratumoral-IL12-mRNA:therapeutic-window (illustrative)",
        ],
        evidence_strength="medium",
        translational_path=(
            "Canine splenic HSA → human angiosarcoma: shared endothelial origin, KDR/MYC and "
            "beta-adrenergic biology, the cold-tumor problem. A response in dogs de-risks a human AS program."
        ),
        risks=[
            "HSA neoantigen landscape poorly defined — favor microenvironment-flip over neoantigen vaccine",
            "IL-12 / mTOR-inhibitor timing-sequencing antagonism risk",
            "LNP first-pass hepatic uptake may dominate over splenic delivery",
            "vet cold-chain / mRNA-LNP manufacturing",
            "field history of dog->human translation failures",
        ],
        next_experiments=[
            "Quantify PTEN-loss / mTOR-active signature frequency in canine splenic HSA (public omics)",
            "Measure LNP biodistribution to the splenic-HSA niche vs hepatic first-pass",
            "Dock / co-fold a next-gen mTOR inhibitor against canine MTOR",
            "Test IL-12 + mTOR-inhibitor sequencing for effector-function antagonism",
        ],
        priority_score=0.84,
    )
    record = repo.upsert_therapy_idea(
        TherapyIdeaRecord(idea=idea, topic="vascular-cancer frontier", status="ready_for_promotion", score=0.84)
    )
    candidate_id = _public_candidate_id(record)
    print(f"  title:        {idea.title}")
    print(f"  targets:      {', '.join(idea.targets)}")
    print(f"  therapies:    {', '.join(idea.candidate_therapies)}")
    print(f"  candidate_id: {candidate_id}")

    h("2. A VALIDATION DECISION marks the program validation-ready")
    decision = ValidationDecisionPacket(
        decision_id=f"validation_decision:{candidate_id}",
        packet_id=f"validation_packet:{candidate_id}",
        candidate_id=candidate_id,
        source_type="therapy_idea",
        source_id=str(idea.idea_id),
        therapy_idea_id=idea.idea_id,
        title="Promote spleen-tropic LNP-mRNA + mTOR program for external validation",
        outcome="promote_broader_program",
        confidence=0.74,
        validation_ready=True,
        specific_claim_viability="uncertain",
        broader_program_signal="strong",
        rationale="Broad program signal is strong enough for inspectable, recommend-only validation.",
        recommended_downstream_action="Create candidate record; run gated computational validation.",
        decisive_questions=[
            "Does PTEN-loss / mTOR-active signature enrich in canine splenic HSA cohorts?",
            "Does a next-gen mTOR inhibitor engage canine MTOR with acceptable selectivity?",
        ],
        evidence_tasks=[
            "Attach a docking/co-fold readout for the mTOR-inhibitor arm.",
            "Attach a canine-HSA omics expression review for the target set.",
        ],
        evidence_summary={"evidence_refs": idea.evidence_refs},
    )
    repo.upsert_validation_decision(ValidationDecisionRecord.from_decision(decision))
    print("  decision.validation_ready = True")
    print(f"  decisive_questions: {len(decision.decisive_questions)} | evidence_tasks: {len(decision.evidence_tasks)}")

    h("3. GENERATE the public candidate snapshot  (the engine)")
    gen = service.generate_public_candidate_snapshot(
        PublicCandidateGenerateRequest(
            therapy_idea_id=idea.idea_id,
            require_moonshot_grade=False,
            pipeline_version="frontier-demo-v1",
            commit_sha="demo",
        )
    )
    cand = service.get_public_candidate(candidate_id)
    print(f"  public_status:   {cand.public_status}")
    print(f"  snapshot hash:   {gen.snapshot.content_hash[:16]}…  (stable across regeneration)")
    print(f"  evidence_refs:   {len(cand.evidence_refs)}  (copied from the idea)")

    h("4. VALIDATION-READY GATE  (Phase 1)")
    readiness = service.assess_candidate_validation_readiness(candidate_id)
    print(f"  ready:        {readiness.ready}")
    print(f"  reasons:      {readiness.reasons}")
    print(f"  blockers:     {readiness.blockers or '(none)'}")
    print(f"  open_questions (auto-derived from the decision):")
    for q in readiness.open_questions:
        print(f"     - {q}")

    h("5. RUN THE COMPUTE LOOP  (Phase 2, mock provider — no GPU)")
    queue_item = repo.upsert_validation_request_queue_item(
        ValidationRequestQueueItem(
            plan_id=uuid4(),
            task_id=uuid4(),
            brief_id=uuid4(),
            topic="Dock next-gen mTOR inhibitor against canine MTOR",
            task_type="docking",
            title="Dock mTORC1-selective inhibitor against MTOR",
            objective="Computational engagement check for the mTOR-axis arm.",
            rationale="Decisive question #2: does the inhibitor engage canine MTOR?",
            validation_request=ValidationRequest(
                validation_type="docking",
                target_name="MTOR",
                candidate_name="next-gen mTORC1-selective inhibitor",
                objective="Dock the mTOR inhibitor against canine MTOR.",
                require_approval=True,
                assay_context=ValidationAssayContext(
                    disease_context="canine hemangiosarcoma and human angiosarcoma",
                    species=["canine", "human"],
                    model_system="Computational structure model with explicit provenance.",
                    assay_type="in silico structural validation",
                    readout="binding plausibility and failure modes",
                    endpoint="computational plausibility",
                ),
            ),
        )
    )
    flow = service.run_compute_validation_flow(candidate_id, queue_item.queue_item_id, runner_kind="mock")
    print(f"  workspace:    {flow.get('workspace_id')}")
    print(f"  compute job:  {flow.get('compute_job_id')}  → status {flow.get('compute_job_status')}")
    print(f"  capsule:      {flow.get('capsule_id')}  (packet_type=compute_artifact, status {flow.get('capsule_status')})")
    print(f"  errors:       {flow.get('errors') or '(none)'}")
    capsule_id = UUID(flow["capsule_id"])

    h("6. OPERATOR ACCEPTS + PROMOTES  (the write gate)")
    accepted = service.accept_proof_capsule(capsule_id, reviewer="chase")
    print(f"  capsule accepted → {accepted.status}")
    before = service.get_public_candidate(candidate_id)
    promotion = service.promote_proof_capsule_to_candidate(capsule_id, reviewer="chase")
    after = service.get_public_candidate(candidate_id)
    print(f"  promoted:     {promotion['promoted']}")
    print(f"  evidence_refs: {len(before.evidence_refs)} → {len(after.evidence_refs)}  (capsule evidence merged)")
    print(f"  new ref:      {[r for r in after.evidence_refs if r.startswith('proof_capsule:')]}")
    print(f"  capsule now:  {repo.get_proof_capsule(capsule_id).status}")
    print(f"  re-assessed validation_ready: {promotion['readiness_ready']}")

    h("DONE — the hypothesis traversed the full machine")
    print(f"  candidate page id would be:  /candidates/{candidate_id}")
    print(f"  demo db (inspect if you like): {db}")
    print("  (compute was mocked; the gate, capsule, promotion, and provenance are real service code.)")


if __name__ == "__main__":
    main()
