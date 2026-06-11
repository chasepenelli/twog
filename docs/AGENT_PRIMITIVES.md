# New agent primitives for twog — research + design

A creative-agentic design pass: what autonomous-discovery primitives we should build that we
*don't have today*. Research first, then proposals. Rigor bearing held throughout — a primitive that
buys speed by quietly lowering our (3,3) standard is a regression, not a win.

## 0. The cross-cutting lesson from the field (read first)

Every system below optimizes for **autonomy and breadth**. The *recurring failure mode* is
**ungrounded confidence**: Sakana's AI Scientist hallucinated the GPU it ran on (claimed V100, used
H100), fabricated citations, and put a positive spin on negative results
([Sakana, *Nature* 2026](https://sakana.ai/ai-scientist-nature/);
[eval, arXiv:2502.14297](https://arxiv.org/html/2502.14297v2)). That is *exactly* the thing twog's
proof-capsule provenance + falsification-first design exists to prevent.

So the design principle for everything here: **make autonomy and rigor the same act.** Every new
autonomous move must (a) emit a verifiable capsule, and (b) pass a falsification/confound gate before
it counts. A primitive that just lets agents move faster is not interesting; a primitive that lets
them move faster *and* raises the evidentiary bar is.

## 1. Research survey — one transferable idea each, fit vs fight, sourced

| System | Core mechanism | The one transferable idea | Fits or fights twog |
|---|---|---|---|
| **K-Dense Analyst** ([arXiv:2508.07043](https://arxiv.org/abs/2508.07043)) | Dual-loop, 10 nested agents: a **planning loop** decoupled from an **implementation loop**, every task validated in a secure env. +59% over the base model on BixBench. | **Wrap planning in its own validation loop** — the *plan* is critiqued/verified before any execution, not just the result. | **Fits.** Our lanes validate *execution*; we have no tight loop validating the *plan of a crux*. We do this by hand. |
| **SciToolAgent** ([Nat. Comput. Sci.](https://www.nature.com/articles/s43588-025-00849-y), [arXiv:2507.20280](https://arxiv.org/abs/2507.20280)) | A **tool knowledge graph** (dependencies, prerequisites, compatibility) drives graph-RAG tool selection + composition + a safety check. | **A machine-readable graph of primitives** so agents *compose* tools instead of following hardcoded flows. | **Fits hard.** We have many research primitives (entity resolution, compound similarity, ortholog mapping, bioactivity indexing, lanes) but no composition graph — the agent picks lanes by hand. |
| **ChemGraph** ([Commun. Chem.](https://www.nature.com/articles/s42004-025-01776-9), [arXiv:2506.06363](https://arxiv.org/abs/2506.06363)) | Planner/Executor/Aggregator for comp-chem; the agent **selects the simulation method** (semi-empirical / ML-potential / DFT) from the task. | **Automated protocol selection** for simulations. | **Fits.** Directly addresses the MD-protocol decisions we make by hand (solvent model, force field, box, minimization). |
| **BioDiscoveryAgent** ([arXiv:2405.17631](https://arxiv.org/abs/2405.17631), Roohani/Stanford) | Closed-loop **experiment design**: LLM + tools (lit search, gene search, AI critique) picks the next batch to maximize hits; +21% over 5 rounds; fully interpretable with citations. | **Active selection of the next experiment** to maximize information — navigate the hypothesis space. | **Fits.** Our "what to test next" (purity → endothelial → cross-species → druggability) was *all human*. |
| **CellVoyager** ([Nat. Methods](https://www.nature.com/articles/s41592-026-03029-6), [bioRxiv](https://www.biorxiv.org/content/10.1101/2025.06.03.657517v1)) | Autonomous scRNA-seq exploration: generates a hypothesis + stepwise "blueprint," **self-critiques the blueprint**, executes conditioned on prior analyses. 80% of hypotheses rated interesting. | **Self-critiqued blueprints that build on prior runs.** | **Fits, with a caveat.** Hypothesis-gen + self-critique is the move; the caveat is "interesting" ≠ "correct" — needs our falsification gate on top. |
| **AI Scientist** ([Sakana, *Nature*](https://sakana.ai/ai-scientist-nature/)) | Idea → code → experiment → paper → **automated review** (~70% with self-reflection + ensembling). | The **automated reviewer / self-critique** is reusable — *but its failures are the warning label* (hallucinated hardware, positive spin, conservative bias). | **Both.** The review pattern fits; its failure modes are precisely what our provenance + write-gate must defend against. Validates our moat. |
| **K-Dense skills library** ([GitHub](https://github.com/K-Dense-AI/scientific-agent-skills), 140+ skills, Agent-Skills standard) | A ready-made skills library: OpenMM/MDAnalysis MD, docking, ADMET, ChEMBL mining, single-cell, 78+ DBs — Claude-Code-compatible. | **Adopt breadth instead of hand-building it** — back our lanes with an open skills standard. | **Fits as plumbing, fights as policy.** Importing 140 skills wholesale would dilute our gated/audited rigor — each must flow through the write-gate + emit a capsule, or it's just unaudited breadth. |

Framing reference: *From AI for Science to Agentic Science* survey ([arXiv:2508.14111](https://arxiv.org/html/2508.14111v1)).

## 2. twog through that lens — where a human makes the call today

Grounded in *this session's actual receipts* (not hypotheticals):

| Manual judgment we made | Should be a primitive | Closest prior art |
|---|---|---|
| Recognized the immune signal was **tumor-purity**, designed the endothelial/stroma controls | **Confound Auditor** | (none — this is our differentiator) |
| Chose the **next falsification** each step (purity → endothelial → cross-species → druggability) | **Active Falsification Planner** | BioDiscoveryAgent |
| **Loop-capping** call (cap vs model 148 missing residues; the proximity analysis) | **Protocol Selector** | ChemGraph |
| **MD remediation** (positive energy → minimize-to-convergence; `--no_kernels`; the openmm/cuda pin) | **Protocol Remediator** | ChemGraph + our checkpoint rig |
| **Deconvolver scoping** (immune-only; coarse-vs-fine lineages when CCC was low) | Protocol Selector (granularity) | — |
| Picked **which lane** for a hypothesis (dock vs cofold vs MD) | **Primitive Composition Graph** | SciToolAgent |
| Framed **controls** (VEGFR2 as the off-target) | Active Falsification Planner (controls) | BioDiscoveryAgent |

Everything in column 1 was a *creative judgment* that determined whether the result was trustworthy.
Those are the primitives.

## 3. Proposed primitives

Each: what it is · what it unlocks · how it composes · effort · limitations/failure modes ·
**[NEW]** vs **[EXT]** · **prototype-now** vs **research-first**.

### P1 — The Confound Auditor  **[NEW] · prototype-now**
**What.** An adversarial agent that, for *any* `supports` signal, enumerates the orthogonal
explanations that could make it an artifact (tumor purity, batch, cell-composition, normalization,
leakage, base-rate) and **runs the controls** before the capsule may promote. Emits a *confound
capsule* with the covariate-adjusted result and a verdict.
**Unlocks.** The purity-confound catch — which we did by hand and which would otherwise have sent us
chasing a ghost — becomes **automatic and mandatory**. No positive signal promotes without surviving
its confounds.
**Composes.** Sits *on* the write-gate (a `supports` capsule is provisional until audited); re-runs
the existing omics/stats engine with covariates; writes lessons to the Failure Corpus (P2).
**Effort.** Medium. The control library (purity/batch/composition adjustment) + our stats engine
exist; the "which confounds apply here" reasoning is LLM-over-a-confound-taxonomy.
**Limitations / failure modes.** Catches only *known* confound classes — a novel artifact escapes;
can **over-reject** real signals (a conservatism trap, cf. the AI-Scientist reviewer); the control
may need data that doesn't exist (then it must say "unauditable," not "clean"). It proves *a signal
survives known artifacts*, never *the signal is true*.

### P2 — Claim–Evidence Ledger + Failure Corpus  **[EXT of provenance] · prototype-now (ledger) / research-first (learning loop)**
**What.** Structured research memory beyond Postgres rows: **claims** as nodes; **capsules** as
hash-linked, signed evidence edges (supports/refutes/neutral + confidence); the lineage DAG we
already ship; *and* a **Failure Corpus** — every refuted hypothesis, caught confound, crashed run,
and protocol fix captured as a queryable lesson. (Real entries from this week alone: the
`cuequivariance_torch` crash, the positive-energy minimize, the `openmm+cuda-version` conflict, the
immune→purity confound.)
**Unlocks.** Agents stop repeating mistakes (those four would be retrieved as priors before acting);
the crux arc becomes a replayable trace; the flywheel's "agentic-reasoning corpus" endpoint becomes
a real, accumulating asset.
**Composes.** Built on the Ed25519/lineage primitive we just shipped + decision events; it is the
substrate P1/P3/P4 read from and write to.
**Effort.** Medium for the ledger (extends the capsule store); medium-high for the *learning loop*
(making retrieval actually change behavior without ossifying it).
**Limitations / failure modes.** Garbage-in (a mislabeled lesson misleads broadly); retrieval
relevance is genuinely hard; **over-weighting failures → conservatism** (the same trap as the
AI-Scientist reviewer that rejected 9/10 papers). Needs a decay/weighting policy.

### P3 — The Protocol Remediator (self-healing compute)  **[NEW] · prototype-now (known classes) / research-first (open diagnosis)**
**What.** Detects an unphysical/failed compute result (positive MD energy, NaN, a crashed lane, a
low-CNN dock), **diagnoses** the likely cause from a protocol-failure library + the run logs,
**adjusts** the protocol (more minimization; solvent/FF; box; a runtime flag), and **re-runs via the
checkpoint rig** — emitting the remediation as a capsule.
**Unlocks.** The fixes we did by hand this session — minimize-to-convergence, `--no_kernels`,
dropping the `cuda-version` pin, a GBn2→TIP3P/PME swap — become an autonomous capability.
**Composes.** Wraps the lanes + the checkpoint/resume rig; reads known fixes from the Failure Corpus
(P2); ChemGraph-style protocol reasoning.
**Effort.** Medium for known failure classes (a detector + remediation table + re-run loop);
research-first for open-ended diagnosis.
**Limitations / failure modes.** **Can mask a result that *should* fail** (auto-fixing a "bad" energy
that actually signals a wrong hypothesis) — the most dangerous failure here, and why every
remediation must emit a capsule the Confound Auditor can challenge; a remediation loop can burn
compute/$$ (needs a budget gate, like our spend rule).

### P4 — The Active Falsification Planner  **[NEW] · research-first**
**What.** Given the Claim–Evidence Ledger, propose the **next cheapest test that would most change
our belief**, prioritizing the experiment most likely to *kill* the leading hypothesis
(BioDiscoveryAgent's active selection, but value-of-information-weighted and **falsification-first**),
and **self-critique the plan** (CellVoyager) before it runs.
**Unlocks.** The crux arc we navigated by hand becomes an agent move — the system chooses its own
next falsification and pre-registers the kill-criterion.
**Composes.** Reads the ledger; emits a hypothesis + pre-registered crux + the lane (via P5); gated
by the write-gate.
**Effort.** Medium-high.
**Limitations / failure modes.** The AI-Scientist trap — **plausible but scientifically shallow**
proposals; drift toward *confirmation* if the falsification framing isn't structurally enforced;
chasing locally-cheap tests over globally-important ones. This one genuinely needs research before
it's trustworthy — ship it *suggesting* tests to a human first, autonomous later.

### P5 — The Primitive Composition Graph  **[EXT of lane registry] · prototype-now**
**What.** A machine-readable graph of *our* primitives (entity resolution, compound similarity,
ortholog mapping, bioactivity indexing, the lanes, the registry, the deconvolver) encoding what each
*consumes/produces*, prerequisites, and compatibility — so agents compose them into higher-order
moves via graph-RAG instead of hardcoded flows.
**Unlocks.** Moves agents can't make today emerge by composition — e.g., the cross-species
druggability pipeline ("human target → ortholog-map to canine → fetch canine bioactivity → pick the
lane → confound-audit") that we *assembled by hand* could be auto-planned.
**Composes.** The meta-layer P3/P4 plan over. We already have the seeds: `lane_image_plan`,
`describe_sandbox_environment` — they declare I/O; this upgrades them to a graph.
**Effort.** Medium (declare an I/O schema per primitive + the graph + graph-RAG planner).
**Limitations / failure modes.** A wrong edge → a nonsensical composition; **graph–code drift**
(the graph claims a capability the code lost); planners can emit invalid chains — needs a validator
(K-Dense's planning loop) before execution.

### P6 — The Provenance Auditor  **[EXT of provenance] · prototype-now**
**What.** An adversarial check that a capsule's *claimed* provenance (who/what hardware/what data)
matches its content-hash + signature + the actual run manifest — the machine-verifiable answer to
the AI-Scientist "claimed V100, used H100" failure.
**Unlocks.** Fabricated/drifted provenance is caught at the gate; "what the agent says it did" must
equal "what it did."
**Composes.** Extends the Ed25519/lineage we shipped; runs at the write-gate alongside P1.
**Effort.** Low.
**Limitations.** Verifies *integrity, not truth* — it catches a lie about the run, not a correctly-
signed wrong conclusion. (That's P1's job.)

## 4. The single highest-leverage primitive to build first

**P1 — the Confound Auditor.** Reasons:
1. **It automates the single highest-value judgment we actually made** — catching the tumor-purity
   confound. Without it, the headline PIK3CA→immune result would have been a confident artifact. That
   one move is the difference between a real finding and a ghost.
2. **It is uniquely ours.** The field builds autonomy + breadth (K-Dense skills, SciToolAgent) and
   *reviewers* (AI Scientist) — none builds a mandatory, adversarial confound gate that *runs the
   controls*. This is the (3,3) standard made executable.
3. **It raises the bar instead of trading it for speed.** It is the literal embodiment of "autonomy
   and rigor are the same act": the more autonomously the system runs, the *more* every positive
   signal gets adversarially challenged before it counts.
4. **It's prototype-able now** — the controls (purity/composition/batch adjustment) and the stats
   engine exist; it needs a confound taxonomy + the gate wiring, and a thin slice of P2 (a ledger to
   record verdicts) comes with it.

Honest caveat on P1 itself, kept up front: a confound auditor that only knows N confound classes will
*miss the N+1th*, and if tuned conservative it will *kill real signals* — so it must report
`survived known confounds` / `unauditable` / `refuted`, never `true`, and its rejections must
themselves be falsifiable. That's the discipline that keeps it a (3,3) win rather than a new way to
fool ourselves.

*Build order after P1:* P2 (the ledger it writes to) → P6 (cheap, closes the provenance loop) →
P5 (composition graph) → P3 (remediator) → P4 (planner, research-first, human-in-loop first).
