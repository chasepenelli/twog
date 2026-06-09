# service.py Decoupling Map (Phase 0, step 1)

> Read-only analysis. **No code moved yet.** This is the map that makes the later refactor
> safe and incremental. See `ROADMAP.md` §P0 and `CURRENT_STATE.md` §5.

## The shape of the problem

`service.py` (13,380 lines) is **one god-class plus a helper pile**:
- `HSAResearchService` — **133 methods**, lines 1070→end. This is the real API surface.
- **284 private module-level functions** (`_foo`) — the helper pile the class leans on.
- 2 tiny private dataclasses (`_SourceFollowupLinkStats`, `_ObservedEvidenceRef`).
- 4 public module functions: `get_service`, `build_default_repository`,
  `reset_service_for_tests`, `summarize_validation_autopilot_result`.

Callers (`cli.py`, `dagster_assets.py`, `mcp_server.py`, `command_center_web.py`, tests) reach
everything through `get_service()` → `HSAResearchService` methods. So the **facade to preserve
is `HSAResearchService`'s method names** — keep those callable and nothing downstream breaks.

## The carving seams (methods clustered by domain)

| Domain | ~methods | Proposed service module |
|---|---|---|
| research (briefs, programs, followups) | 28 | `services/research_service.py` |
| validation (queue, planning, dispatch) | 18 | `services/validation_service.py` |
| agent (runs, performance) | 7 | `services/agent_service.py` |
| reward events | 5 | `services/reward_service.py` |
| omics (readouts, locus signals) | 5 | `services/omics_service.py` |
| compute (jobs, submit, poll) | 5 | `services/compute_service.py` |
| therapy committee | 4 | `services/therapy_service.py` |
| public candidates | 4 | `services/public_candidate_service.py` |
| md expert gate | 3 | (fold into `compute_service`) |
| proof capsules | 2 | `services/proof_capsule_service.py` |
| source / source-followup | 3 | `services/source_service.py` |
| evidence / entity / search / retrieval | ~7 | `services/evidence_service.py` |
| x/twitter topic | 3 | (attic candidate — low traffic) |

Verb shape (lifecycle): mostly `get`/`list`/`run`/`build` (read + orchestrate), only a handful
of `submit`/`create`/`queue`/`update` (the mutating surface — the part the expert gate protects).

## The incremental refactor (each step ships green; nothing big-bang)

1. **Create `services/` package + a `_ServiceBase`** holding the shared deps the methods use
   (repository, config, runner handles). No behavior change.
2. **Extract one domain at a time** (start with the most self-contained — `compute` or
   `proof_capsule`, ~5–7 methods): move those methods into a domain service class that takes
   `_ServiceBase` deps; in `HSAResearchService`, replace the method bodies with one-line
   delegations (`return self._compute.submit_compute_job(...)`). **Facade unchanged → callers
   unchanged.** Run the (split) test suite green before moving on.
3. **Repeat per domain**, biggest/messiest (`research`, 28) last once the pattern is proven.
4. **Pull the 284 `_helpers` along with their domain** as each extraction happens; truly shared
   ones land in `services/_shared.py`.
5. **Only after the class is thin**, revisit whether the attic modules (CURRENT_STATE §4) can
   finally move — by then `service.py` no longer imports them at top level.

**Order rationale:** facade-first delegation means the 13k-line file shrinks monotonically while
the public API is frozen. No caller (`cli`, `dagster_assets`, `mcp_server`, web, tests) changes
until the very end, if ever. This is the opposite of a risky rewrite.

**Prerequisite:** the test suite must be split and green first (CURRENT_STATE §6) — it's the
safety net that proves each extraction preserved behavior.
