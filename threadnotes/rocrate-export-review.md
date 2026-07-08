# Review — `rocrate_export.py` (RO-Crate / Process Run Crate export)

_Reviewer: front-end/Phase-B thread. Reviewed `src/hsa_research/ingestion_bridge/rocrate_export.py` + `tests/test_rocrate_export.py` + the pyproject/lock change._

## Verdict: strong, well-built, strategically right — ship after a couple of fixes

RO-Crate (Process Run Crate profile) is exactly the right standard for twog: it turns proof capsules into **portable, interoperable provenance objects** that WorkflowHub / Galaxy / nf-core / runcrate can ingest — the "compound / give it away / flywheel (3,3)" thesis made real. The code is clean, write-only, lazily-imported, **zero coupling to the dispatch/gate hot path**, and maps the *actual* model (candidate-keyed ledger, capsule→CreativeWork with `content_hash` identifier, job→CreateAction, prereg→CreativeWork, artifacts referenced by URI+sha256 with `fetch_remote=False`, sources→ScholarlyArticle), namespacing twog-specifics under `twog:`. High discipline.

## 🔴 Real bug — and the test hides it
`status = "refuted" if any(s == "refuted" …)` (rocrate_export.py:161) checks the **wrong enum value**. The capsule signal vocabulary is `"supports" | "refutes" | "neutral" | "none"` (contracts.py:7051) — it's **`"refutes"`**, not `"refuted"`. So in production a genuinely refuting capsule never flips the rollup; **every crate reports `standing`** even for a falsified hypothesis — which inverts twog's whole point.

The test passes only because it seeds `"signal": "refuted"` (test_rocrate_export.py:79) — a value the engine **never emits**. The test asserts against fabricated data → false confidence.

**Fix:** check `"refutes"` in the code; seed `"refutes"` in the test (which would currently fail and expose this). Also: the real rollup has three states (`standing | refuted | underpowered`, contracts.py:7123) — `underpowered` is silently collapsed to `standing`; mirror it faithfully.

## 🟠 The most important provenance is missing
The crate captures the capsule, Ed25519 signature, and pre-registration — but **not the confound/provenance gate verdicts**. For a *provenance* crate, "this evidence cleared the confound + provenance gates" is the headline assertion, and it's absent. Promote follow-up #2 (reuse `provenance_auditor.audit()` to stamp a verified/mismatch assertion per CreateAction) from "nice-to-have" to **v1**. Without it the crate under-sells what makes twog's evidence trustworthy.

## 🟡 Smaller notes
- **"campaign" is overloaded.** This exports a *candidate's evidence dossier* (all capsules for a `candidate_id`), but twog also has a literal `RunManifest(manifest_type="falsification_campaign")` from `run_falsification_campaign`. Two different "campaigns." The chosen unit is the more useful one, but rename to `candidate_to_crate` (or add a separate `run_manifest_to_crate`) to avoid confusion with the real Campaign record.
- **`"@type": ["CreativeWork", "Hypothesis"]`** — `Hypothesis` isn't a schema.org type; for strict validation use `twog:Hypothesis` (or a bio-schemas term).
- **`conformsTo` ≠ conformance.** Declaring the profile is good, but only `runcrate` validation (follow-up #3) proves it — put that in CI.

## On the two flags raised
1. **Dep drift — agree, don't carry it.** A write-only export tool shouldn't downgrade `sqlalchemy` (2.0.50→2.0.49) or nudge `requests`/`typer`/`structlog`. Better: make `rocrate` an **optional extra** (`[project.optional-dependencies] rocrate = [...]`), not a core dep — it's lazily imported + write-only, so the core install stays lean and non-exporters skip the transitive churn. Then `git checkout uv.lock` and add it narrowly.
2. **Uncommitted / no-branch — correct call.** With 46 in-flight files, adding only new untracked files was the right non-disruptive move. Well-isolated; only `pyproject`/lock needs reconciliation, which the optional-extra approach minimizes.

## Follow-ups — priority order
1. **Gate-verdict stamping** (their #2) → into v1; it's the crate's missing soul.
2. **Fix the `refutes` bug + test** (this review) → blocking; silently wrong today.
3. **Wire into `cli.py`** (their #1) → discoverability; `-m` works but a subcommand is right.
4. **`runcrate` CI validation** (their #3) → makes `conformsTo` honest.

Net: a standards-aligned, low-risk, high-leverage primitive that makes twog's evidence travel. Worth doing well. The `refutes` fix + gate-verdict stamping are the two things I'd require before it's "done."
