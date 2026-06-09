# TWOG — Current State

> **Purpose of this document.** This is the file future-you reads after months away,
> before touching any code. It records what actually works, what's half-built, what's
> parked, where the data and money live, and what the cleanup plan is. It is descriptive,
> not aspirational — if something here is a hope rather than a fact, it says so.
>
> Written 2026-06-09 during a repo cleanup pass. Supersedes nothing in the code;
> it just tells the truth about it.

---

## 0. TL;DR

TWOG is a working canine-hemangiosarcoma research engine: ingest open biomedical evidence
→ typed research records → agent synthesis/critique → operator-approved candidate records →
public pages at twog.bio. The thesis is sound and the engineering hygiene is good. **The
problem is shape, not soul:** ~88K LOC of Python in one tightly-coupled package centered on a
13,380-line `service.py` god-object that imports nearly everything, so the system is hard to
re-enter after a break and hard to change without fear.

This pass does the safe, high-value cleanup (this document, a cost map, a test-split plan)
and **deliberately does not** perform the risky physical refactors (splitting the package,
deleting the test suite) because doing them blindly would break the build or destroy
coverage. Those are sequenced below as reviewed work.

---

## 1. The repo/machine map (read this first — it's the confusing part)

There are **two GitHub projects and several local copies**. They are easy to confuse.

| Thing | What it is | Last git commit | Status |
|---|---|---|---|
| `github.com/chasepenelli/twog` | **The current project** (this repo). Monorepo: Python/Dagster backend at root + Next.js twog.bio site in `twog/`. | `origin/main` @ **2026-05-27** | **CANONICAL** |
| `github.com/chasepenelli/hsa-autoresearch` | The **predecessor**. Older "agent committee" architecture. | **2026-04-29** | **ARCHIVED** — do not develop |

**Local checkouts (as of 2026-06-09):**
- `~/twog-cleanup` — fresh clone of `origin/main`, branch `chore/cleanup-pass`. **This document lives here.**
- `~/Documents/Codex/.../twog-system-tightening/twog` — frontend checkout. Has a local commit **2026-06-04** ("Add guarded public candidate publish gate") that is **AHEAD of origin/main and unpushed**, plus ~12 dirty files. ⚠️ Your most recent frontend work lives only here. Push or back it up.
- `~/TWOG/hsa-dagster/twog` — **not a git repo**; a Dagster scratch sandbox (`.tmp_dagster_home_*`). Disposable.
- `~/hsa-autoresearch` — the predecessor's working copy. **2,110 uncommitted files** of agent output that accumulated while its launchd agents ran unattended for ~6 weeks. Archived, agents now disabled (see §2).

**Implication:** the backend hasn't been committed since May 27; recent energy went to the
frontend (unpushed). That gap is the "stall" — not abandonment, just divergence between what's
running, what's committed, and where the work happened.

---

## 2. Background processes — STOPPED 2026-06-09

The predecessor `~/hsa-autoresearch` had **five `launchd` agents** running unattended on the
laptop (not in this repo — they belonged to the old project):

| Agent | Cadence | Ran |
|---|---|---|
| `com.hsaresearch.orchestrator` | loop / 600s, auto-restart | `orchestrator.py --mode loop` |
| `com.hsaresearch.designloop` | continuous, auto-restart | `design_loop_runner.py` |
| `com.hsaresearch.mdrunner` | continuous, auto-restart | `md_runner.py` |
| `com.hsaresearch.director` | every 8h | `director_agent.py` (+ Remotion video) |
| `com.hsaresearch.healthcheck` | hourly | `health_check.py` |

All five were `launchctl unload -w` (disabled, survives reboot). The `.plist` files remain in
`~/Library/LaunchAgents/` so this is reversible. Orphaned Remotion `chrome-headless` renderers
and a stale `next dev` server were also killed. **No cron jobs, no `at` jobs, no other timed
triggers exist.** Verified: `launchctl print-disabled` shows all five disabled.

---

## 3. What actually works (the spine, end-to-end)

This is the load-bearing path that is real and should be protected:

```
open sources ─▶ harvesters ─▶ typed research records ─▶ claim extraction
   └─▶ stored in SQLite (local) / Postgres-Neon (hosted) via repository abstraction
   └─▶ agent runs recorded in append-only ledger (agent_runner)
   └─▶ operator approval gate ─▶ public candidate records ─▶ twog.bio
```

**Spine modules (keep, protect, test):**

| Module | LOC | Role |
|---|---|---|
| `service.py` | 13,380 | Orchestration hub. **God-object — see §5.** |
| `dagster_assets.py` | 9,216 | Dagster asset/job graph (96 local imports) |
| `contracts.py` | 6,734 | Typed Pydantic models for every boundary |
| `local_store.py` / `postgres_store.py` | 5,061 / 4,817 | Storage impls (SQLite / Neon Postgres) |
| `harvesters_v2.py` | 4,275 | Source ingestion |
| `cli.py` | 4,149 | Command-line entry |
| `mcp_server.py` | 3,116 | MCP server surface |
| `repository.py` | 2,532 | Storage abstraction (protocol) |
| `agent_runner.py` | 173 | **Provenance ledger — the cleanest, most reusable piece** |
| `command_center_web.py`, `candidate_contribution_intake.py`, `claim_extractor.py`, `embeddings.py`, `full_text_ops.py`, `entity_resolution.py`, `scrape_parsers.py`, `source_health.py`, `query_policy.py`, `research_primitives.py`, `source_followup.py`, `evidence_fit.py`, `structured_orchestration.py` | — | Supporting spine |

**Governing rule (keep it sacred):** *LLMs argue and synthesize. Operator approval is the
write gate.* Nothing an LLM produces becomes durable public state without a human stamp.

---

## 4. What's parked / experimental (the attic)

These are real features but lower-traffic experiments — candidates to extract out of the core
package **once `service.py` is decoupled** (§5). They are NOT dead; they are NOT yet cleanly
separable.

`therapy_committee` · `research_brief_agent` · `research_brief_evaluation` · `omics_readouts` ·
`omics_locus_signals` · `x_topic_monitor` · `x_topic_review` · `x_linked_article_review` ·
`validation_planning` · `validation_gap_source_pack` · `validation_tool_catalog` ·
`research_program_board` · `research_followup_resolver` · `research_followup_refinement` ·
`agent_performance` · `research_workspaces` · `scraper_bridge`

**`runpod_workers/md_smoke/`** — GPU molecular-dynamics smoke test. Scaffolded, approval-gated,
pay-per-use (zero idle cost). This is the seed of the "compute expedition" pattern, not a
running system.

---

## 5. The core problem: the `service.py` god-object (why the split isn't a file move)

`service.py` (13,380 lines) imports **15+ of the "attic" modules at top level**
(`therapy_committee`, `research_brief_agent`, `omics_readouts`, `validation_planning`,
`research_program_board`, …, lines 244–367) and has **49 distinct local imports total**.
`dagster_assets.py` has **96**.

**Consequence:** you cannot `git mv` any experiment to an `attic/` folder without breaking
`service.py` on import, which takes down the whole package. There are **no safe leaf modules**.
A real spine/attic separation must therefore start by decomposing `service.py`, not by moving
files. Attempting the move first would make the repo *worse* — the exact failure mode this
cleanup exists to prevent.

**Sequenced decoupling plan (reviewed work, not done in this pass):**
1. **Map the surface of `service.py`** — list every public function/class and which caller
   (`cli`, `dagster_assets`, `mcp_server`, web) actually uses it. (Mechanical, low-risk.)
2. **Carve `service.py` into domain modules** behind a thin facade that re-exports, so callers
   don't break while internals move. (One domain at a time, tests green between each.)
3. **Convert attic-module imports to lazy/local imports** at their call sites (Dagster already
   does this for `structured_orchestration` / `x_topic_monitor` — follow that pattern).
4. **Only then** physically relocate attic modules into a subpackage. Imports no longer break
   because nothing imports them at module top level.
5. Each step is a small PR with the test suite (§6) green. No big-bang refactor.

---

## 6. Tests — DO NOT DELETE; SPLIT

`tests/test_ingestion_bridge_contracts.py` is one **967KB / 23,634-line file with 502 real
behavioral tests** — including spine-critical ones: `test_agent_runner_creates_trace_and_run_manifest`,
`test_run_manifest_repository_roundtrip_memory_and_sqlite`, `test_therapy_ideas_round_trip_and_committee_from_brief`.

**This is genuine coverage, not generated cruft.** (An earlier assessment mischaracterized it;
that was wrong.) The problem is purely maintainability: 502 tests in one file is unnavigable
and scary to edit.

**Plan:** split into per-area modules (`test_agent_runner.py`, `test_repository.py`,
`test_contracts.py`, `test_validation.py`, `test_therapy_committee.py`, …), sharing common
fixtures via `conftest.py`. Preserve every test. Run the full suite before and after to prove
zero coverage loss. (`test_runpod_md_worker.py`, 211 lines, is already fine.)

---

## 7. Costs & idle footprint

**Good news — idle CI is free:** all 9 GitHub workflows are `workflow_dispatch` or `push`
triggered. **No `schedule:`/cron anywhere**, so nothing bills automatically from GitHub.

**Recurring-cost surface (needs dashboard access to confirm — check these):**

| Service | Refs in repo | Idle cost | Action |
|---|---|---|---|
| **Dagster+** (hosted) | 646 | **Bills while a deployment is live** | ⚠️ Check Dagster+ deployment status; run Dagster locally or pause the hosted deployment. Biggest likely idle cost. |
| **Neon** Postgres | 104 | Low on autoscale/free tier; an always-on branch can bill | Check project; let it scale to zero. Local SQLite path already exists for offline work. |
| **Vercel** (twog.bio) | — | Hobby tier ≈ free | Likely fine; confirm tier. |
| **RunPod** GPU | 114 | **Zero when nothing is launched** (pay-per-use) | No action; this is the right model for compute expeditions. |
| **OpenRouter / OpenAI / Anthropic** | 119 / 20 / 11 | Zero idle (pay-per-call) | No action. |

**Target idle cost: ~$0/month when not actively working.** The two things
to verify on a dashboard are Dagster+ and Neon.

---

## 8. Operating model (how to keep it moving)

This is a long-running project worked in bursts between other work, not a continuous sprint.
Design for **survivability across absences**, not throughput:

- **Idle should cost ~nothing** (§7) so a 2-month gap is free.
- **Re-entry should take one evening** — this document is step one of that.
- **One cheap heartbeat** (later): a single weekly job — pull new HSA/angiosarcoma literature →
  triage → propose claims to the review queue → after your approval, update public pages. The
  approval tap is the part worth showing up for; everything around it is automatic.
- **Compute as expeditions, not platform:** a few times a year, take one validation question,
  rent one GPU box (the `md_smoke` worker), run it approval-gated, land the artifact, shut it
  down. Infrastructure earns its way one expedition at a time. Distribution/checkpointing only
  if a queue of named, valuable jobs provably outgrows one machine.

---

## 9. Immediate next actions (in order)

1. **Push or back up the unpushed June 4 frontend work** in `~/Documents/Codex/...` — it's the
   only copy (§1). Highest urgency: it's unbacked-up.
2. **Check Dagster+ and Neon dashboards** (§7); pause/downgrade to hit ~$0 idle.
3. **Commit this document** to `origin/main` so the map survives.
4. **Split the test file** (§6) — safe, mechanical, makes everything afterward less scary.
5. **Begin `service.py` decoupling** step 1 (§5) — map its surface. Do not move files yet.
6. Decide on the weekly heartbeat (§8) when you next have a burst of energy.
