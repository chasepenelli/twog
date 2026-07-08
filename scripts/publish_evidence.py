"""Publish twog's evidence as an open, signed, citable dataset bundle.

twog's give-back: most labs can only share claims you must trust. twog shares VERIFIABLE evidence —
content-hashed, Ed25519-signed, re-checkable — AND the thing almost nobody publishes: its FAILURES
(refutations + caught confounds). This builds a self-contained bundle ready to upload to Zenodo /
HuggingFace Datasets:

    <out>/
      crates/<candidate_id>/        one RO-Crate per candidate's evidence dossier (Process Run Crate)
      failure_corpus.json           every refutation + caught confound across all candidates
      verified_targets.json         the redock-verified target catalog (reusable docking inputs)
      dataset_metadata.json         Zenodo-style deposition metadata (title/creators/license/keywords)
      README.md                     what this is, the license, how to verify it, how to cite it

Read-only over the ledger. License: CC-BY-4.0.

    PYTHONPATH=src:. python scripts/publish_evidence.py --out dist/twog-evidence --ts 2026-06-17
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

from hsa_research.ingestion_bridge import rocrate_export, target_library
from hsa_research.ingestion_bridge.contracts import ProofCapsuleLibraryRequest
from hsa_research.ingestion_bridge.postgres_store import PostgresResearchRepository
from hsa_research.ingestion_bridge.service import HSAResearchService

from scripts.run_real_demo import _database_url

LICENSE = "CC-BY-4.0"
LICENSE_URL = "https://creativecommons.org/licenses/by/4.0/"


def _failure_corpus(service: HSAResearchService, candidate_ids: list[str]) -> list[dict]:
    rows: list[dict] = []
    for cid in candidate_ids:
        for entry in service.get_failure_corpus(cid):
            rows.append(json.loads(entry.model_dump_json()))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="dist/twog-evidence")
    ap.add_argument("--ts", default="", help="ISO date stamp for the bundle (avoids nondeterminism)")
    ap.add_argument("--zip-crates", action="store_true")
    args = ap.parse_args()

    out = Path(args.out)
    (out / "crates").mkdir(parents=True, exist_ok=True)

    repo = PostgresResearchRepository(_database_url(), seed=False)
    service = HSAResearchService(repo)

    # candidate_ids that actually have evidence (group the capsule ledger)
    caps = service.list_proof_capsules(ProofCapsuleLibraryRequest(limit=500)).capsules
    by_candidate: dict[str, int] = defaultdict(int)
    for c in caps:
        by_candidate[c.candidate_id] += 1
    candidate_ids = sorted(by_candidate)
    print(f"Found {len(caps)} capsules across {len(candidate_ids)} candidates.")

    # 1. one RO-Crate per candidate dossier
    exported, signal_counts = [], defaultdict(int)
    for cid in candidate_ids:
        dest = out / "crates" / cid
        try:
            rocrate_export.candidate_to_crate(repo, cid, dest, write_zip=args.zip_crates)
            exported.append(cid)
            print(f"  + crate: {cid} ({by_candidate[cid]} capsule(s))")
        except Exception as exc:
            print(f"  - skip {cid}: {type(exc).__name__}: {exc}")
    for c in caps:
        signal_counts[str((c.payload or {}).get("signal", "none"))] += 1

    # 2. the Failure Corpus — the negative-results dataset (the differentiated give-back)
    failures = _failure_corpus(service, candidate_ids)
    (out / "failure_corpus.json").write_text(json.dumps({
        "_doc": "Refutations + caught confounds twog accumulated. Negative results, openly shared so "
                "no one repeats them. Derived from the signed proof-capsule ledger.",
        "license": LICENSE, "generated": args.ts, "count": len(failures), "entries": failures,
    }, indent=2, default=str))

    # 3. the verified-target catalog — reusable, redock-verified docking inputs
    lib = target_library.load_target_library()
    targets = [
        {"target": k, "verified": v.get("verified"), "pdb_id": v.get("pdb_id"),
         "redock_rmsd": v.get("redock_rmsd"), "note": v.get("note")}
        for k, v in (lib.get("entries") or {}).items()
    ]
    (out / "verified_targets.json").write_text(json.dumps({
        "_doc": "Redock-verified target structures (symmetry-corrected RMSD <= 2 A + PoseBusters). "
                "Reusable docking inputs; 'verified:false' = refused at the spend gate.",
        "license": LICENSE, "generated": args.ts, "entries": targets,
    }, indent=2, default=str))

    # 4. Zenodo-style deposition metadata
    (out / "dataset_metadata.json").write_text(json.dumps({
        "title": "twog — autonomous comparative-oncology falsification evidence",
        "upload_type": "dataset",
        "description": "Signed, re-checkable proof capsules (RO-Crate), a failure corpus of refutations "
                       "and caught confounds, and a redock-verified target catalog from twog, an "
                       "autonomous engine that tries to falsify hypotheses in canine hemangiosarcoma × "
                       "human angiosarcoma. Nothing is auto-promoted; every result is independently verifiable.",
        "license": "cc-by-4.0",
        "keywords": ["comparative oncology", "angiosarcoma", "hemangiosarcoma", "falsification",
                     "molecular docking", "proof capsule", "RO-Crate", "negative results", "provenance"],
        "creators": [{"name": "twog"}],
        "version": args.ts or "unversioned",
    }, indent=2))

    # 5. README — the human entry point
    (out / "README.md").write_text(_readme(exported, len(caps), dict(signal_counts), len(failures), targets, args.ts))

    print(f"\nPublished bundle → {out}")
    print(f"  crates: {len(exported)} · capsules: {len(caps)} {dict(signal_counts)} · "
          f"failures: {len(failures)} · targets: {len(targets)}")
    print("  Upload crates/ + the JSONs to Zenodo (dataset_metadata.json) or HuggingFace Datasets.")


def _readme(crates, n_caps, signals, n_fail, targets, ts) -> str:
    verified = sum(1 for t in targets if t.get("verified"))
    sig_str = " · ".join(f"{v} {k}" for k, v in sorted(signals.items(), key=lambda kv: -kv[1])) or "—"
    return f"""# twog — open evidence dataset

> *An engine that tries to be wrong.* twog is an autonomous comparative-oncology falsification engine
> (canine hemangiosarcoma × human angiosarcoma). This bundle is its evidence, given away openly.

**Generated:** {ts or "unversioned"} · **License:** [{LICENSE}]({LICENSE_URL})

Most AI and most labs hand you claims you have to *trust*. twog hands you evidence you can *verify* —
and, unusually, it also publishes its **failures**.

## What's inside
- **`crates/<candidate_id>/`** — {len(crates)} RO-Crates (Process Run Crate profile), one per hypothesis
  dossier. Each holds the proof capsules, the compute provenance, the pre-registered kill-criteria, and
  a provenance-audit verdict per result. Total: **{n_caps} proof capsules** ({sig_str}).
- **`failure_corpus.json`** — **{n_fail}** refutations + caught confounds. Negative results, shared so
  no one wastes effort repeating them. This is the part nobody else publishes.
- **`verified_targets.json`** — {verified} redock-verified target structures (reusable docking inputs;
  refused targets are kept too, with the reason).

## How to verify it (don't trust — check)
Every proof capsule is **content-hashed** (the hash *is* its identity) and, where a key was held,
**Ed25519-signed**. Re-hash the scientific content and compare; verify signatures against the
producer's public key. The RO-Crates are readable by `runcrate`, WorkflowHub, Galaxy, and nf-core.

## How to cite
See `dataset_metadata.json`. Cite the dataset DOI (assigned on Zenodo deposit).

## Caveats (twog states its limits)
Docking/cofolding results are *in-silico estimates*, not measured binding. A "standing" hypothesis
survived an honest attempt to kill it — it is not a proven therapy. Nothing here is auto-promoted; a
human holds the terminal write-gate. Candidate idea-grounding references in seeds may be curation
placeholders pending citation.
"""


if __name__ == "__main__":
    main()
