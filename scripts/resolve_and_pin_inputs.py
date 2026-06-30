"""Resolve every lane input for the validation-ready roster via the retrieval stack and PIN it to each
candidate, so the rubric shows SMILES/proteins/structures IN ORDER and the lanes are genuinely ready to
run real GPU. Honors the rule "nothing tested without all inputs resolved" — pinning makes inputs_ready
true deterministically (read-side needs no live call).

  docking  : curated redock-verified receptor (target_library) + PubChem SMILES
  cofolding: UniProt protein sequence (target) + PubChem SMILES
  md       : the curated receptor_pdb + PubChem SMILES

  --dry-run : resolve + report coverage, write NOTHING (network reads only).

    NEON_DATABASE_URL=... PYTHONPATH=src:. python scripts/resolve_and_pin_inputs.py [--dry-run]
"""

from __future__ import annotations

import argparse

from hsa_research.ingestion_bridge import target_library as _tl
from hsa_research.ingestion_bridge.input_catalog import CatalogResolvers, InputCatalog
from hsa_research.ingestion_bridge.input_resolvers import NetworkInputResolvers
from hsa_research.ingestion_bridge.contracts import PublicCandidateLibraryRequest
from hsa_research.ingestion_bridge.postgres_store import PostgresResearchRepository
from hsa_research.ingestion_bridge.service import HSAResearchService

from scripts.run_web_api import _database_url


def _resolve_for(candidate, resolvers, lib) -> tuple[dict, list[str]]:
    """Return (lane_inputs, resolved_lane_names) for a candidate using the retrieval stack."""
    lane_inputs: dict = {}
    resolved: list[str] = []
    target = candidate.targets[0] if candidate.targets else None
    therapy = candidate.candidate_therapies[0] if candidate.candidate_therapies else None
    if not target or not therapy:
        return lane_inputs, resolved

    smiles = None
    try:
        smiles = resolvers.compound_smiles(therapy)
    except Exception:
        smiles = None

    # docking + md: need the curated, redock-VERIFIED receptor (the spend gate) + SMILES
    dock_cfg = _tl.curated_docking_config(lib, target, ligand_smiles=smiles, ligand_name=therapy) if smiles else None
    if dock_cfg and dock_cfg.get("receptor_pdb") and smiles:
        lane_inputs["docking"] = dock_cfg
        resolved.append("docking")
        lane_inputs["md"] = {"protein_pdb": dock_cfg["receptor_pdb"], "compound_smiles": smiles}
        resolved.append("md")

    # cofolding: UniProt protein sequence + SMILES
    if smiles:
        seq = None
        try:
            seq = resolvers.protein_sequence(target)
        except Exception:
            seq = None
        if seq:
            lane_inputs["cofolding"] = {"protein_sequence": seq, "ligand_smiles": smiles,
                                        "ligand_name": therapy, "target": target}
            resolved.append("cofolding")
    return lane_inputs, resolved


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=100)
    args = ap.parse_args()

    svc = HSAResearchService(PostgresResearchRepository(_database_url(), seed=False))
    catalog = InputCatalog()
    resolvers = CatalogResolvers(NetworkInputResolvers(), catalog)  # read-through: reuse, fetch-once
    print(f"input catalog: {catalog.counts} (resolved-once reuse)\n", flush=True)
    lib = _tl.load_target_library()
    cands = [c for c in svc.list_public_candidates(PublicCandidateLibraryRequest(limit=args.limit)).candidates
             if c.validation_ready]
    print(f"{'DRY-RUN: ' if args.dry_run else ''}resolving inputs for {len(cands)} validation-ready candidates\n", flush=True)

    pinned = 0
    cov = {"docking": 0, "cofolding": 0, "md": 0, "none": 0}
    for c in cands:
        li, resolved = _resolve_for(c, resolvers, lib)
        for k in ("docking", "cofolding", "md"):
            if k in resolved:
                cov[k] += 1
        if not resolved:
            cov["none"] += 1
        print(f"  {c.candidate_id:28} → {resolved or 'NONE (no SMILES / unverified target)'}", flush=True)
        if not args.dry_run and li:
            # MERGE lane_inputs (never clobber existing curated inputs, e.g. an omics cohort) — only add
            # the lanes we resolved; leave any pre-existing lane configs intact.
            existing = dict((c.metadata or {}).get("lane_inputs") or {})
            existing.update(li)
            updated = c.model_copy(update={"metadata": {**(c.metadata or {}), "lane_inputs": existing}})
            svc.repository.upsert_public_candidate(updated)
            pinned += 1

    print(f"\ncoverage: docking={cov['docking']} cofolding={cov['cofolding']} md={cov['md']} · none={cov['none']}", flush=True)
    print(("DRY-RUN — nothing written." if args.dry_run else f"PINNED inputs to {pinned} candidates on Neon."), flush=True)


if __name__ == "__main__":
    main()
