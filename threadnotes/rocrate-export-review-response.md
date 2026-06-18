# Response — `rocrate_export.py` review (all blocking items fixed)

_Re: `rocrate-export-review.md`. Thanks — the `refutes` catch was a real bug and the fixture genuinely
masked it. All four priority items addressed; tests now assert against real engine values._

## 🔴 `refutes` bug — FIXED (was silently wrong, you were right)
- Code now checks the real vocabulary `"supports"|"refutes"|"neutral"|"none"`; `"refuted"` is gone.
- Rollup is now the faithful **three-state** `LeadingHypothesisStatus`:
  - any capsule meets its **pre-registered kill criterion** → `refuted` (kill signal read from the
    compute-job prereg block's `observed_signal_kills`, default `"refutes"`, mirroring `failure_corpus`'s
    `signal == kill_criterion["observed_signal_kills"]` — so a `neutral`-kills plan is honored too);
  - else a real non-neutral survivor → `standing`;
  - else only `neutral`/`none` → `underpowered` (no longer collapsed to `standing`).
- The rollup carries `twog:statusDerivation` = "derived from capsule signals + pre-registered kill
  criteria" — honest that it's derived, since the authoritative `FalsificationLoopResult` is a returned
  object, not a persisted row.
- Test now seeds `"refutes"` (the value the engine actually emits) and asserts `refuted`; a new
  `test_rollup_three_states_are_faithful` pins `standing` vs `underpowered`. This is the test that would
  have failed before — it now guards the bug.

## 🟠 Gate verdict — PROMOTED TO v1 (the crate's missing soul)
- Every capsule now gets a `provenance_auditor.audit(capsule, job)` verdict stamped as a
  `twog:ProvenanceVerdict` entity (`twog:status`, `twog:ok`, `twog:checksPassed`, `twog:mismatches`),
  `about` the CreateAction (or the capsule when the claim doesn't resolve to a job). Recomputed via the
  pure/deterministic auditor (no I/O), so even a never-audited capsule gets a verdict. The capsule links
  to it via `twog:provenanceVerdict`.
- Verified on a real crate: `verified | checks: compute_job_id, candidate_id, checkout_manifest_hash,
  candidate_snapshot_hash, validation_type, compute_job_completed`.
- **Confound verdict** is NOT yet included — `provenance_auditor` is pure (no I/O), but a confound verdict
  needs lane controls/config to recompute. Proposed: project a *persisted* `confound_verdict` from
  `capsule.metadata` when present (pure read), rather than recompute. Left as a fast follow — say the word
  and I'll add it.

## 🟡 Smaller notes
- **Renamed `campaign_to_crate` → `candidate_to_crate`** (+ CLI/docstrings). Doc now states it is a
  *candidate evidence dossier*, explicitly NOT the literal `RunManifest(manifest_type="falsification_campaign")`.
- **`"Hypothesis"` → `"twog:Hypothesis"`** (and the verdict is `"twog:ProvenanceVerdict"`) — no more
  fake schema.org types.
- **`runcrate` CI validation** — still a follow-up (#4 below); `conformsTo` is declared but unproven until then.

## Dep drift — your fix taken
- `rocrate` is now an **optional extra** (`[project.optional-dependencies] rocrate`), lazily imported,
  off the hot path. Reverted the original `uv add` and re-locked: the diff is now **narrow — only `arcp` +
  `rocrate` added**, none of the prior sqlalchemy/requests/typer/structlog churn.
- One unavoidable correction surfaced: the committed `uv.lock` pinned `cryptography 47.0.0`, which already
  **violated** pyproject's `cryptography>=42,<46`; re-locking corrected it to `45.0.7`. Not my drift — a
  pre-existing stale-lock inconsistency. The full Ed25519/provenance/foreign-provenance suite passes on
  45.0.7 (124 tests in the regression slice). Flagging in case you want to look at why the lock was stale.
- Run/CI: `uv run --extra rocrate pytest tests/test_rocrate_export.py` (3 passed).

## Remaining follow-ups (non-blocking)
1. Wire into `cli.py` (`hsa-ingestion-bridge`) as a subcommand (currently `python -m ...rocrate_export`).
2. `runcrate`/`roc-validator` CI check to make `conformsTo` honest.
3. Project persisted `confound_verdict` from capsule metadata alongside the provenance verdict.
4. Embed (not just reference) small on-disk artifacts under `var/hsa_research/artifacts/` with real `contentSize`.

Files still **untracked / uncommitted** (new module + test) + `pyproject.toml`/`uv.lock` modified — left
for you to branch/commit alongside the in-flight work.
