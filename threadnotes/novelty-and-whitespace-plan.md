# Plan — Novelty gate + cross-species white-space generation

_From the 4-scout research sweep (2026-06-17). Goal: stop re-deriving known biochem; generate
genuinely novel, frontier-aligned comparative-oncology hypotheses, prove they aren't already published,
and let the capsules run + store the simulations. The capsule (back half) is solid — this is the front
of the funnel._

## Headline findings
- **The "comprehensive science DB search" repo Chase remembered is almost certainly [BioMCP](https://github.com/genomoncology/biomcp)** (MIT, MCP-native, 527★): one server unifying PubMed/PubTator3/Europe PMC/Semantic Scholar + cBioPortal/OncoKB/CIViC/ClinVar + ChEMBL/OpenTargets/ClinicalTrials. The "MCP research agents" = **FutureHouse Owl** ("has anyone done X before" — a novelty oracle) + **PaperQA2**.
- **A free novelty gate needs no LLM:** Europe PMC `hitCount` (no key) + PubMed E-utilities `<Count>` (MeSH precision, free key) + Semantic Scholar `total` + citation weighting. The count fields ARE the prior-art signal.
- **No single KG does dog↔human.** Cross-species white space = Open Targets (human AS targets) → Ensembl Compara ortholog bridge → Monarch (phenologs) → ICDC + canine HSA genomics (GSE95183 / the Oct-2025 "shared selection landscape" preprint: PIK3CA mutated in 31.8% of dog HSA).
- **Validation built in:** PIK3CA→angiosarcoma is well-published → a perfect NEGATIVE CONTROL (the gate must REJECT it as not-novel). Our 20 just-docked known drugs should mostly be rejected too — that's how we know the gate works.
- `PiVOT` could not be confirmed as a real resource — ICDC + CanISO are the substitutes. Flag to Chase.

## Phase 1 — Novelty / prior-art gate (FREE, no LLM, build FIRST)
New `novelty_gate.py`: `assess_novelty(compound, target, disease) -> NoveltyVerdict {score 0..1, prior_art_counts, queries, sources}`.
- **Backbone (free):** Europe PMC `hitCount` (primary, no key, full-text+preprints) + PubMed E-utilities via Biopython (`<Count>`, MeSH-precise, free key → 10 req/s). Semantic Scholar `total` + citation weighting as a third vote later.
- **Recipe:** tiered boolean counts `n_CTD, n_CD, n_CT, n_TD` → `novelty = 1/(1+log1p(weighted prior art))`. Synonym-expand compound (brand/INN/ChEMBL) + target (gene/protein) or everything looks falsely novel.
- **Comparative twist:** run the triple for human (angiosarcoma) AND dog (hemangiosarcoma) separately; **"explored in human, NOT in dog" = high translational-novelty quadrant** (twog's sweet spot).
- **Integration:** gate inside `generate_candidate_ideas` (the input analog of the docking spend-gate) — refuse/deprioritize high-prior-art ideas BEFORE docking. Persist the verdict (score + counts + the exact queries) into candidate metadata → it rides into the **capsule as provenance** ("we checked; here's proof it wasn't already known").
- **Caching + backoff** mandatory (rate limits); identity headers (Entrez tool+email).
- **Acceptance test:** PIK3CA→angiosarcoma scores LOW (rejected); a Tdark target / absent triple scores HIGH. Re-score the 20 known drugs → most should fail the gate.

## Phase 2 — Cross-species white-space generation (deterministic, grounded)
New generation `source="cross_species"` in `candidate_generator.py`:
1. **Open Targets GraphQL** → targets/drugs associated with human angiosarcoma (EFO/MONDO id).
2. **Ensembl Compara REST** (`/homology/symbol/human/<GENE>?target_species=canis_lupus_familiaris;type=orthologues`) → canine ortholog (the join key; pin Dog10K assembly).
3. **Canine HSA evidence** (ICDC GraphQL + GSE95183 / shared-selection matrix) → is the ortholog studied in dog HSA?
4. **White space = studied in human AS, ortholog exists, NOT in canine HSA** (or the reverse) → candidate hypothesis.
5. Run each through the Phase-1 novelty gate → keep the novel ones → resolvability gate (verified target) → seed.
- **Growth loop:** white-space will surface targets NOT yet in the verified target library (only PIK3CA/KDR today) → a queue for `verify_target_library.py` to add. This is how the verified-target catalog grows (and the give-back dataset with it).

## Phase 3 — Frontier-agentic generation (LATER; needs spend OK 💸)
SciMON (Apache-2.0, novelty-bounded loop) + SciAgents (Apache-2.0, KG path-sampling = graph-native Swanson ABC, natural for cross-species A→B→C) patterns, on twog's orchestrator: an agent PROPOSES hypotheses grounded in the fused KG, kept honest by the novelty + resolvability + falsification gates. Reimplement AI-Scientist-v2's novelty-check pattern (don't vendor — RAIL license). Uses paid LLM API → confirm with Chase before wiring.

## What to wire as MCP / infra
- **BioMCP** (MIT) as an MCP server → the orchestrator/agents get structured prior-art (PubTator3 co-occurrence, cBioPortal frequencies, CIViC/OncoKB) in one surface. Start the Phase-1 gate on direct Europe PMC/PubMed HTTP (zero infra); add BioMCP as the richer second source.
- **FutureHouse Owl** as an optional managed novelty oracle / second vote (free tier; paid past it 💸).
- Open Targets + Ensembl + Monarch + ICDC: direct REST/GraphQL (all free); cache locally.

## Recommended order
1. **Phase 1 novelty gate** (free, immediate) — wire into `generate_candidate_ideas`; validate on the PIK3CA negative control + the 20 known drugs.
2. **Phase 2 cross-species source** — Open Targets → ortholog → canine evidence → novelty gate → seed.
3. Grow the verified-target library to whatever Phase 2 surfaces.
4. **Phase 3 frontier-agentic** generation once 1+2 prove out (spend decision).

## Spend / licensing flags
- Phase 1 + 2 data = FREE (Europe PMC, PubMed, Open Targets/Apache-2.0, Ensembl, Monarch, ICDC). OpenAlex now keyed ($1/day) — optional. DrugBank full data is non-commercial; ChEMBL is share-alike; Hetionet stale (2017) — use PrimeKG/OT instead. Phase 3 LLM generation = real API spend.
