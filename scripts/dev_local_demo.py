"""DEV-ONLY local demo: serve the real web API against a fresh local SQLite DB, seeded with a published
moonshot candidate (PIK3CA) + a supporting docking capsule, so the EVIDENCE page renders the REAL
MoonshotRubric my code generates (not the mock fixture). No Neon, no GPU, no spend.

    PYTHONPATH=src python scripts/dev_local_demo.py [--port 8000]

Then run the web app (web/.env.local already points at http://127.0.0.1:8000 with USE_MOCKS=false):
    cd web && npm run dev    →    open http://localhost:3000/evidence and click the PIK3CA capsule.
"""

from __future__ import annotations

import argparse
import pathlib

from hsa_research.ingestion_bridge.contracts import (
    ProofCapsuleRecord, ProofCapsuleSummary, ProofCapsuleTarget,
    PublicCandidateGenerateRequest, TherapyIdea, TherapyIdeaRecord,
)
from hsa_research.ingestion_bridge.local_store import SQLiteResearchRepository
from hsa_research.ingestion_bridge.service import HSAResearchService
from hsa_research.ingestion_bridge.web_api import run_api_server

DB_PATH = pathlib.Path("/tmp/twog_moonshot_demo.sqlite3")


def _seed(db_path: pathlib.Path) -> str:
    if db_path.exists():
        db_path.unlink()  # fresh each run so the demo is deterministic
    repo = SQLiteResearchRepository(db_path, seed=False)
    svc = HSAResearchService(repo)

    idea = TherapyIdea(
        title="Cross-species PI3Kα strategy in canine HSA × human angiosarcoma",
        hypothesis="Alpelisib engages mutant PI3Kα across the canine-HSA × human-AS axis; the PIK3CA-mutant subset should respond to mutation-selective inhibition.",
        rationale="Mutation-selective PI3Kα inhibition is a cross-species precision strategy; the conserved pocket lets a human-verified structure stand in for the canine target.",
        # mechanism frames the LOAD-BEARING engagement hypothesis (grounded) — it does NOT assert
        # immunosuppression as fact (twog's own omics crux on that axis returned neutral).
        mechanism="Mutation-selective PI3Kα inhibition at the PIK3CA pocket — alpelisib, a PI3Kα-selective inhibitor, is hypothesized to engage the mutant site across the canine HSA × human AS axis.",
        translational_path="the PIK3CA-mutant canine-HSA × human-AS subset becomes a mutation-selective treatment candidate worth advancing",
        candidate_therapies=["alpelisib"], targets=["PIK3CA"], biomarkers=["PIK3CA mutation"],
        evidence_refs=["PMID:1", "PMID:2", "PMID:3"], evidence_strength="medium",
        risks=["alpelisib hyperglycemia in dogs", "sparse canine PIK3CA-mutant cohorts"],
        next_experiments=["confirm the cross-species axis in an orthogonal mutant-vs-WT cohort"], priority_score=0.9,
    )
    repo.upsert_therapy_idea(
        TherapyIdeaRecord(idea=idea, topic="PI3Ka", status="ready_for_promotion", score=0.9))

    # First publish to learn the deterministic candidate_id.
    res = svc.generate_public_candidate_snapshot(
        PublicCandidateGenerateRequest(therapy_idea_id=idea.idea_id, require_moonshot_grade=True, persist=True))
    if res.candidate is None:
        raise SystemExit(f"moonshot did not publish; errors={res.errors}")
    cid = res.candidate.candidate_id

    # A supporting docking capsule with a REAL content-hash + Ed25519 signature from a registered "lab",
    # so the provenance surface demonstrates a verifiable, re-derivable receipt (not a placeholder hash).
    from hsa_research.ingestion_bridge import provenance
    from hsa_research.ingestion_bridge.contracts import ProofCapsuleProducer, ProofCapsuleSubmitRequest

    priv, pub = provenance.generate_keypair()
    svc.register_collaborator(principal="demo-structural-lab", name="Demo Structural Lab",
                              public_key=pub, auth_subject="demo-structural-lab")
    producer = ProofCapsuleProducer(producer_type="agent", name="twog_compute")
    target = ProofCapsuleTarget(section="docking")
    summary = ProofCapsuleSummary(
        title="Falsify: alpelisib for PIK3CA-driven HSA",
        finding="gnina docked alpelisib against verified PIK3CA (4JPS): CNN affinity 6.1. Signal: supports.",
        why_it_matters="Dock the compound to test whether it engages the proposed PI3Kα pocket.",
        limitations=["docking is an estimate, not measured binding", "unaudited until the tumor-purity confound clears"],
    )
    payload = {"signal": "supports", "confidence": 0.8, "validation_type": "docking", "provenance_flag": "pass"}
    req = ProofCapsuleSubmitRequest(
        workspace_id=res.candidate.trace_id, checkout_manifest_hash="sha256:" + "d" * 24,
        candidate_id=cid, packet_type="compute_artifact", requested_action="docking_or_md_review",
        producer=producer, target=target, summary=summary, payload=payload, submitted_by="demo-structural-lab",
    )
    chash = svc.capsule_content_hash_for_submission(req)  # the canonical hash anyone can recompute
    repo.upsert_proof_capsule(ProofCapsuleRecord(
        workspace_id=req.workspace_id, checkout_manifest_hash=req.checkout_manifest_hash,
        candidate_id=cid, packet_type="compute_artifact", requested_action="docking_or_md_review",
        status="submitted", producer=producer, target=target, summary=summary, payload=payload,
        content_hash=chash, signature=provenance.sign(chash, priv), submitted_by="demo-structural-lab",
    ))

    # RESOLVE → PIN → DISPLAY: retrieve the molecular inputs once (PubChem SMILES, UniProt sequence) + the
    # curated redock-verified PIK3CA receptor, and PIN them onto the candidate so the rubric renders them
    # RESOLVED — deterministically, from stored values, never a live fetch at rubric-build/hash time.
    from hsa_research.ingestion_bridge import input_resolvers as _ir, target_library as _tl

    _ALPELISIB_SMILES = "CC1=C(SC(=N1)NC(=O)N2CCCC2C(=O)N)C3=CC(=NC=C3)C(C)(C)C(F)(F)F"  # offline fallback
    resolvers = _ir.NetworkInputResolvers()
    try:
        smiles = resolvers.compound_smiles("alpelisib") or _ALPELISIB_SMILES
    except Exception:
        smiles = _ALPELISIB_SMILES
    try:
        sequence = resolvers.protein_sequence("PIK3CA")
    except Exception:
        sequence = None
    lib = _tl.load_target_library()
    dock_cfg = _tl.curated_docking_config(lib, "PIK3CA", ligand_smiles=smiles, ligand_name="alpelisib") or {}
    lane_inputs: dict = {"docking": dock_cfg} if dock_cfg else {}
    if sequence:
        lane_inputs["cofolding"] = {"protein_sequence": sequence, "ligand_smiles": smiles,
                                    "ligand_name": "alpelisib", "target": "PIK3CA"}
    if dock_cfg.get("receptor_pdb"):
        lane_inputs["md"] = {"protein_pdb": dock_cfg["receptor_pdb"], "compound_smiles": smiles}
    cand = repo.get_public_candidate(cid)
    repo.upsert_public_candidate(
        cand.model_copy(update={"metadata": {**(cand.metadata or {}), "lane_inputs": lane_inputs}})
    )
    print(f"  resolved+pinned: SMILES {smiles[:36]}…  receptor {len(dock_cfg.get('receptor_pdb',''))} chars"
          f"  sequence {len(sequence) if sequence else 0} aa")

    # Re-publish (idempotent): the rebuilt rubric now SEES the supporting docking capsule + the resulting
    # open tumor-purity confound (docking reads "supports · unaudited", the audit is queued) AND the pinned
    # molecular inputs (SMILES/receptor/sequence resolved, not "missing").
    svc.generate_public_candidate_snapshot(
        PublicCandidateGenerateRequest(therapy_idea_id=idea.idea_id, require_moonshot_grade=True, persist=True))
    print(f"seeded: candidate={cid}  (open its capsule on /evidence to see the rubric)")
    return cid


def main() -> None:
    parser = argparse.ArgumentParser(description="DEV: serve the real web API over a seeded local SQLite DB.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    _seed(DB_PATH)

    def _service_factory() -> HSAResearchService:
        return HSAResearchService(SQLiteResearchRepository(DB_PATH, seed=False))

    print(f"twog web API (local SQLite demo) → http://{args.host}:{args.port}  · public reads open")
    run_api_server(service_factory=_service_factory, verify_token=lambda _t: None,
                   host=args.host, port=args.port, allow_origin="*")


if __name__ == "__main__":
    main()
