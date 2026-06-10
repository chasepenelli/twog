# Frontier Scan — tooling & patterns for twog

> Verified June 2026 by three scouting passes (agentic compute/sandboxes · open AI-for-bio ·
> agentic-science/provenance). Every repo/URL below was checked to exist with the noted license
> and recency. Honest hype-vs-real calls are kept. This is a reference, not a commitment — it
> feeds the Phase-3 lane/provider build and the agentic-architecture roadmap.

## How to read this against twog's needs

- **N1** — a real sandboxed GPU provider to fill the empty `ComputeRunner` seam, able to run
  scientific containers, write **checkpoints** to durable storage, and resume a paused job.
- **N2** — autonomous multi-stage compute pipelines (dock→MD) that checkpoint at stage boundaries
  and hand off via a workspace lease.
- **N3** — a tool-using orchestrator runtime with structured output + provenance.
- **Lanes** — concrete scientific payloads exposed as expert-gated compute lanes.
- **Thesis** — "the audit trail is the product" + "operator approval is the write gate."

## Checkpointing has TWO shapes (corrected after the hours-long-GPU-docking note)

Real docking on server GPUs runs for hours (screening libraries, gnina CNN rescoring, ensemble /
flexible-receptor / blind docking, ML docking). So docking is a **long-running, checkpoint-worthy
lane**, same tier as MD. But "checkpoint a compute" is two different problems:

| shape | example | checkpoint mechanism | difficulty |
|---|---|---|---|
| **Work-queue** | a screening campaign (N independent dockings) | persist each completed ligand result + a cursor over the library | **easy & clean** — resume mid-library, zero recompute, splits across workers, hands off trivially |
| **Single long run** | one exhaustive flexible docking; an MD trajectory | engine-native restart (OpenMM `.chk`); **Vina has no mid-search restart** | **hard** — needs process/GPU-level checkpointing (cuda-checkpoint / CRIUgpu / Cedana) |

The work-queue shape maps directly onto the pipeline fan-out (each ligand = a sub-job; completed
ones persisted in the stage record). The single-long-run shape is where the GPU-checkpoint stack
below earns its place.

---

# 1 — Compute providers, sandboxes & checkpointing

### GPU sandbox providers
- **Modal** — https://modal.com/docs/guide/sandbox-snapshots — Python-native serverless GPU + Sandbox API. Strongest GPU story; mature. Filesystem (indefinite), directory (30-day, mountable), and experimental memory (7-day) snapshots; GPU mem snapshots for fast cold starts. **Caveat: GPU + live-memory-snapshot do NOT compose** — "pause/resume" is snapshot-then-restore-as-new-sandbox. **For twog (N1/N2):** fastest path to a real provider; do stage-boundary checkpoints as *artifacts to durable storage*, not live GPU pause.
- **RunPod** — https://www.runpod.io/ — serverless + pod GPU (A100/H100), per-second billing, no egress, explicitly markets MD/Monte-Carlo workloads, ~20-40% cheaper than Modal. FlashBoot caches container snapshots but **no true user-facing mid-job GPU checkpoint**. **For twog (N1):** cheapest credible GPU backend; pair with Cedana for checkpointing.
- **Northflank** — https://northflank.com/ — PaaS running agent sandboxes + on-demand GPU (H100/A100/L4), **persistent volumes, no session time limits**. **For twog (N1/N2):** strong when a dock→MD run lasts days and must persist large trajectory artifacts. (Comparison numbers are vendor-sourced; weigh accordingly.)
- **Daytona** — https://github.com/daytonaio/daytona (~72k★, very active, $24M Series A, agent-infra pivot Feb 2025) — open-source secure runtime, ~200ms starts, sandbox snapshots incl. a prebuilt `daytona-gpu`. **For twog (N1/N3):** open + self-hostable with GPU snapshots — own your infra and provenance. (Stars partly legacy; judge on capability.)
- **E2B** — https://e2b.dev/docs/sandbox/persistence — canonical agent sandbox (Firecracker microVMs). **Best-in-class pause/resume semantics** (full memory+fs+processes, paused indefinitely, ~1s resume) **but managed E2B is CPU-only**; GPU needs self-hosting OSS on bare metal. **For twog (N1/N3):** model the checkout/lease API after this; use for CPU-side orchestrator tool execution, not MD.
- **Morph Cloud (Infinibranch)** — https://cloud.morph.so/docs/documentation/snapshots — microVMs that snapshot/branch/restore full VM state in <250ms, incl. branching a *live* VM. GPU limited. **For twog (N2):** branch-a-live-VM fits speculative pipeline fan-out + lease handoff (still needs a separate GPU lane).
- **Runloop (Devboxes)** — https://docs.runloop.ai/docs/devboxes/overview — agent devboxes with **Blueprints** (env templates) + snapshot suspend/resume. GPU limited. **For twog (N1/N3):** Blueprints map to per-lane scientific containers; CPU-orchestration option.
- **forkd** — https://github.com/deeplethe/forkd (~2k★, v0.5.2 @ 2026-06-08, very active) — open Firecracker fork()-for-microVMs; spawn 100 children in ~100ms, snapshot chains (layered differential w/ parent hashes). **For twog (N2):** self-hostable branch/snapshot fan-out substrate. Hype check: early, single-org, CPU-microVM (no GPU passthrough).
- **Firecracker / Cloud Hypervisor** (substrate) — https://github.com/firecracker-microvm/firecracker — **Firecracker deliberately has no GPU passthrough** (PCIe work paused 2025). Self-hosted GPU sandboxes need **Cloud Hypervisor/QEMU + VFIO**. Important constraint: "Firecracker = GPU sandbox" is a common wrong assumption.

### GPU checkpoint / resume stack (for the single-long-run case)
- **`NVIDIA/cuda-checkpoint`** — https://github.com/NVIDIA/cuda-checkpoint — official CLI that checkpoints/restores CUDA state (device mem→host, release/reacquire GPU, rebuild streams/contexts), pairs with CRIU. Driver 550+; 580+ adds GPU migration. x64-only; no UVM/IPC yet. **The enabling primitive for mid-job GPU pause/resume.** Consume via CRIU/Cedana.
- **CRIUgpu** — https://arxiv.org/html/2502.16631v1 (Feb 2025, merged CRIU 4.0+) — transparent unified CPU+GPU snapshots, no steady-state overhead, validated on H100/A100/MI210. **Credible open path to durable checkpoint + resume**, incl. stage-boundary dock→MD. Operationally heavy.
- **`cedana/cedana`** — https://github.com/cedana/cedana (active to 2026-06, AGPL-3.0) — productized save/migrate/resume for CPU+GPU containers; SLURM/K8s native, ~1-3% overhead, policy restores. **Closest turnkey "checkpoint-as-a-service" to put under RunPod/Northflank** (N1/N2). Honesty: small community, AGPL matters for integration, GPU C/R is finicky industry-wide.

### Agent runtimes / harnesses (N3 — structured output, not chat)
- **Pydantic AI (+ durable runtime / Kitaru)** — https://github.com/pydantic/pydantic-ai — typed agent framework: structured outputs, tool schemas, retries; durable runtime persists each model/tool/MCP call as a checkpoint boundary. **Cleanest fit for twog's structured-output, provenance-first orchestrator** — typed proof-capsule outputs + per-call checkpoints map ~1:1 onto the validation queue and lane invocations.
- **LangGraph** — https://docs.langchain.com/oss/python/langgraph/durable-execution — stateful orchestration w/ checkpointing + first-class human-in-the-loop. Fits dock→MD with HITL gates as native constructs.
- **Temporal** — https://temporal.io/ — durable-execution engine; every step auto-checkpointed, runs for days/months; 2025-26 added OpenAI Agents SDK / Google ADK integrations. **Most battle-tested backbone for the macro-pipeline + lease lifecycle** while the GPU C/R layer handles in-process state (complementary, not competing).
- **OpenAI Agents SDK** — https://github.com/openai/openai-agents-python — lightweight, provider-agnostic; structured `output_type`, handoffs, guardrails, tracing. Good if you want first-class lane→lane handoffs + tracing; pairs with Temporal.

### Top 3 to fill the ComputeRunner provider seam
1. **Modal** — fastest to a *real* working provider; genuine GPU + durable fs/directory snapshots at stage boundaries. Architect checkpoints as artifacts-to-storage at stage edges (GPU+memory-snapshot don't compose).
2. **RunPod + Cedana** — cost-efficient + capability-complete: cheap H100/A100 + true GPU mid-job checkpoint/resume/migration. More moving parts, but the honest answer to "pause a long MD/docking run and resume."
3. **Daytona (self-host) or Northflank** — open/persistent option. Daytona if owning the stack/provenance matters; Northflank for managed no-time-limit + persistent volumes + GPU.

Cross-cutting: wrap orchestration in **Pydantic AI or Temporal** (N3 + N2 stage-checkpoint/lease). If self-hosting GPU isolation, it must be **Cloud Hypervisor/QEMU+VFIO, not Firecracker**.

---

# 2 — Open AI-for-bio scientific compute (candidate lanes)

### Structure & protein-ligand co-folding
- **Boltz-2** — https://github.com/jwohlwend/boltz — **top pick**. MIT/Recursion co-folding model; predicts complex structure **and** binding affinity (binder-vs-decoy + affinity value), ~FEP accuracy at ~1000× speed. v2.2.1 (Sep 2025), ~4k★, **MIT (commercial-friendly)** — rare for a frontier open-weight model. NVIDIA GPU recommended. **Lane:** co-fold VEGFR2/KDR or KIT against candidate ligands (toceranib analogs, sunitinib, propranolol/β-adrenergic) → structure + affinity in one provenance-tracked call; serves dog→human angiosarcoma translation.
- **Chai-1** — https://github.com/chaidiscovery/chai-lab — AF3-class multimodal predictor. **Code Apache-2.0, weights non-commercial.** Strong cross-check to Boltz-2 (consensus = stronger provenance); flag the weight license for any commercial path.
- **AlphaFold3** — https://github.com/google-deepmind/alphafold3 — gold standard; code Apache-2.0 but **weights gated, academic-only, by request**. Reference benchmark only; weight friction makes it a poor first build given Boltz-2/Chai exist.
- **ESM3 / ESMFold** — https://github.com/evolutionaryscale/esm — generative protein model; fast single-seq folding. **Mixed licensing** (ESM3-small-open & ESM C 600M non-commercial; ESM C 300M/codebase open). **Lane:** fast apo-structure + variant/design (KIT exon-11 mutants, VEGFR2 variants). Watch commercial restrictions.

### Docking & virtual screening
- **AutoDock Vina** — https://github.com/ccsb-scripps/AutoDock-Vina — reference open engine, very mature, **Apache-2.0**. The current twog Vina lane; keep it. Weak spots: flexible-ligand handling, scoring bias.
- **gnina** — https://github.com/gnina/gnina — **recommended docking upgrade**. CNN-rescored docking (smina/Vina lineage), **Apache-2.0**, GPU-accelerated. 2025 benchmarks (MDPI Molecules 30/16/3361) show materially better true/false-positive discrimination than Vina. Same inputs, stronger scoring, commercial-friendly — natural "v2" of the docking lane. *(Note: CNN scoring is part of why real docking runs take hours.)*
- **DiffDock** — deep-learning generative docking. Honest: fair 2024-25 comparisons (arXiv 2412.02889) find Vina & gnina **beat it ~20-25 pts at 2Å RMSD**. Expose as experimental only; don't decide on it.
- **Uni-Mol Docking V2** — https://github.com/deepmodeling/Uni-Mol — ML pose prediction; V2 reports >77% poses <2Å on PoseBusters. Good pose-quality cross-check beside gnina. Check per-component license.

### Molecular dynamics & free energy
- **OpenMM** — https://github.com/openmm/openmm — GPU MD engine; the de facto open core. Keep as the MD substrate (the smoke test is on the right foundation).
- **OpenFE (Open Free Energy)** — https://github.com/OpenFreeEnergy/openfe — **recommended MD/FEP upgrade**. Permissive (**MIT**) alchemical RBFE/FEP on OpenMM + OpenFF; v1.11.0 (Apr 2025); `alchemiscale` for HPC/cloud scale. **Lane:** rigorous ΔΔG binding free energies for a congeneric series vs VEGFR2/KIT — the physics-grounded confirmation of Boltz-2's fast affinity estimates (great provenance story: ML screen → FEP confirm). Heaviest compute.

### ADMET / property / tox
- **ADMET-AI** — https://github.com/swansonk14/admet_ai — **clear pick**. Chemprop-RDKit GNN on 41 TDC datasets; CLI + API + web; contextualizes vs approved DrugBank drugs. Bioinformatics 2024, top TDC rank, `pip install admet_ai`, **CPU-friendly**. **Lane:** cheap, fast early gate — every ligand gets a tox/ADMET profile *before* expensive GPU docking/co-folding.

### Frontier agentic-for-bio
- **Biomni** — https://github.com/snap-stanford/Biomni — general biomedical agent (LLM + RAG planning + code exec over 25 domains), ~2.8k★, **core Apache-2.0** (tools carry own licenses). Architecturally *adjacent to twog itself* (an orchestrator, not an atomic lane) — mine for tool-integration patterns; doesn't fit the "one atomic gated computation" model.
- **REINVENT 4** — https://github.com/MolecularAI/REINVENT4 — RL generative molecule design (de novo, scaffold hop, R-group, linker), **Apache-2.0**, used in real pharma. **Lane:** generative front-end — design novel VEGFR2/KIT binders, then feed docking → ADMET → co-folding.

### Recommended first 3 lanes (given vascular-cancer focus)
1. **Boltz-2 co-fold + affinity** — highest leverage, MIT, structure + affinity in one call; the core "does this ligand engage this target?" question.
2. **gnina docking** (upgrades the Vina lane) — Apache-2.0, GPU, better hit discrimination; cheap high-throughput screening + a second independent binding signal to cross-check Boltz-2.
3. **ADMET-AI triage** — CPU, fast, free; early gate that maximizes signal per GPU-dollar.

Together a clean cascade: **ADMET triage → gnina docking → Boltz-2 co-fold+affinity**, all
MIT/Apache-2.0. **Defer:** OpenFE/FEP (physics confirmation, heaviest) and REINVENT 4 (generative
front-end). **Avoid as first builds:** AlphaFold3 (gated weights), DiffDock (underperforms).

---

# 3 — Agentic science, provenance & verification (the thesis layer)

### Steal directly (open, mature, thesis-aligned)
- **PaperQA2 (FutureHouse)** — https://github.com/Future-House/paper-qa — high-accuracy RAG over scientific PDFs with inline citations; refuses when evidence is thin. Apache, production-ready. **For twog:** the closest existing "citation-grounded claim → durable record." Harvest its contextual-summarization + relevance-confidence scoring as the evidence-synthesis front-end feeding the claim ledger.
- **Kosmos** — https://arxiv.org/abs/2511.02824 (Nov 2025, Edison/FutureHouse) — autonomous discovery agent where **every statement links to generated code or a primary citation**, backed by a **structured world model** (queryable entity/relationship/result DB updated per task). Commercial, but architecture fully described. **For twog:** the best external proof of "audit trail is the product." Harvest (a) the structured world model as the claim graph / candidate store; (b) its **accuracy-by-claim-type** data — 85.5% data-analysis, 82.1% literature, **57.9% interpretation** → gate interpretation/inference claims hardest.
- **AI co-scientist (Google DeepMind)** — https://research.google/blog/accelerating-scientific-breakthroughs-with-an-ai-co-scientist/ (Feb 2025, Nature) — multi-agent Generation→Reflection→Ranking→Evolution→Meta-review with **tournament debate + Elo ranking**. Closed, mechanisms published. **For twog:** the **Reflection-as-peer-reviewer + tournament/Elo** is the blueprint for the critique/validation committee — promote only tournament winners past the operator gate. (Discount the "made discoveries" PR; trust the architecture.)

### Strong harvestable patterns
- **Stanford Virtual Lab** — https://www.nature.com/articles/s41586-025-09442-9 (Nature, Jul 2025) — PI + specialist agents + a dedicated **"Scientific Critic" agent**; designed COVID nanobodies validated in wet-lab (rare real validation). Open code. **For twog:** keep the Critic as a distinct agent with flag/veto power feeding the validation queue.
- **Agent-as-a-Judge (Meta)** — https://github.com/metauto-ai/agent-as-a-judge (Oct 2024, arXiv 2410.10934) — evaluates the *entire reasoning trajectory*, ~90% human alignment vs 60-70% for vanilla LLM-judge. Open. **For twog:** your audit trail *is* a trajectory — this scores the whole provenance chain so the gate can reject a claim whose *derivation* is flawed even if the conclusion looks fine.
- **RAGAS / DeepEval / TruLens** — https://github.com/explodinggradients/ragas · https://github.com/confident-ai/deepeval — open eval harnesses with **faithfulness** (each statement traces to context?) + **claim-decomposition** metrics. **For twog:** an off-the-shelf factuality gate; wire DeepEval faithfulness into CI so no claim promotes without passing a threshold.
- **Chain-of-Verification (CoVe) + Self-RAG** — https://aclanthology.org/2024.findings-acl.212.pdf — draft→verify-questions→answer independently→revise; self-critique pre-filters. **For twog:** cheap single-agent self-critique *before* the operator gate — reduces operator load (the write-gate bottleneck).

### Narrower / benchmarks
- **DebateCV** — arXiv 2507.19090 (2025) — two opposing debaters + moderator; route *contested* biomedical claims to adversarial debate vs single-judge.
- **SciClaimHunt / SciFact** — arXiv 2502.10003 (2025) — evidence-based scientific-claim-verification datasets; use to **validate/calibrate your own gate** and fine-tune a claim-verifier.
- **ChemCrow / Coscientist** — arXiv 2304.05376 / Nature s41586-023-06792-0 (2023) — tool-augmented chemistry agents; harvest the **expert-curated typed-tools-behind-the-agent** pattern (relevant to compute-behind-gates). Foundational, not frontier.

### Hype caution
- **Sakana AI-Scientist-v2** — https://github.com/SakanaAI/AI-Scientist-v2 (arXiv 2504.08066) — genuinely open, impressive engineering (agentic tree search, ~$15/paper, a workshop-accepted paper), **but optimizes for autonomous output volume — the opposite of twog's gated, provenance-first thesis.** Harvest the tree-search execution; reject the "remove all human templates" stance — it's the anti-pattern to the write-gate.
- Google AI co-scientist & Kosmos **discovery claims** are press-forward; trust published architectures, discount "breakthrough" marketing until independently replicated.

### Top 3 patterns to steal
1. **Kosmos-style statement-level provenance binding + structured world model** — make a claim un-storable unless it carries a code-output/citation link; back the ledger with a queryable entity/relationship graph. Set gate strictness by claim type (hardest on interpretation, 57.9%).
2. **Co-scientist tournament + dedicated Critic → ranked promotion** — pairwise debate + Elo + a separate Scientific-Critic; only tournament-winning, critic-cleared claims reach the operator write-gate (so approval is the *final* gate on an already-filtered set).
3. **Trajectory-level eval as the programmable pre-gate** — Agent-as-a-Judge (scores the provenance chain) + DeepEval faithfulness/claim-decomposition + CoVe self-critique, stacked as automated CI gates; validate against SciClaimHunt.

Net architecture twog could assemble mostly from open parts today: **PaperQA2** synthesis front-end
→ **Kosmos-style provenance-bound claim graph** as the durable store → **co-scientist-style critique
tournament** feeding the operator gate → **Agent-as-a-Judge + DeepEval** as the automated pre-gate.
