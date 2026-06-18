# twog — "get everything connected" checklist + audience explainer

_Goal: the whole system up, running, doing its thing, with the website live and (most) everything wired._

Status key: ✅ done · 🟡 partial · ⬜ to do · 💸 costs money (confirm with Chase first) · 🔑 a decision

---

## The system, in one picture

```
   ENGINE (falsification loop)  ──writes──▶  NEON (Postgres: candidates, capsules, runs)
        │  proposes → attacks → keeps                     │
        │  what survives, on a cadence                    │ reads
        ▼                                                 ▼
   MODAL (GPU compute: docking,                      API  (web_api.dispatch + run_api_server)
   cofold, MD, omics)                                     │
                                                          │ HTTPS/JSON
                                                          ▼
                                            WEBSITE (Next.js: STATE / EVIDENCE / RUNS)
                                                          │
                                            twog.bio (marketing homepage it plugs into)

   [Tier 2 only] WorkOS (login)  +  custodial key vault (signing)  ▶  CONTRIBUTE (gated)
```

What's already real: **Engine ✅**, **Neon ✅**, **Modal ✅**, the **API logic ✅** (built + tested), the
**public website ✅** (built, reviewer-approved). What's missing is the last mile that connects them.

---

## TIER 1 — "It's alive": the public site shows the real engine working (no login needed)

This is the fastest path to "up, running, and doing its thing." It needs NO auth, NO key vault — just
connecting the four pieces that already exist.

1. ⬜ 💸 🔑 **Turn the engine loop on, budget-capped.** The autonomous falsification scheduler is built
   but parked (stopped). Switch it to run on a cadence with the existing caps ($0.50/candidate, ~$2/tick).
   → *Confirm spend + cadence with Chase before flipping on (ongoing Modal GPU cost).*
2. 🟡 **Deploy the API.** ✅ BUILT + working locally: added a PUBLIC read surface (`/public/state`,
   `/public/capsules[/:id]`, `/public/campaigns[/:id]`, `/public/candidates[/:id]`) in `web_api.py`,
   a presenter layer (`web_presenters.py`) projecting raw records → the site's display shapes, and an
   entrypoint (`scripts/run_web_api.py`) serving it against Neon. Public reads need no auth; gated
   operator/contribute routes still require a (not-yet-wired) WorkOS token. ⬜ Remaining: deploy to a
   real host. → *Pick a host (Render / Fly / Railway / a small VM).* 🔑
3. ✅ **Point the website at the API.** `web/.env.local` sets `NEXT_PUBLIC_USE_MOCKS=false` +
   `NEXT_PUBLIC_API_BASE_URL`; public api clients now hit `/public/*` (operator actions stay gated).
4. ✅ **Reconcile the data shape.** Done via the presenter layer — `signal`←`payload.signal`,
   `confidence`←`payload.confidence`, `claim`←`summary.title`, `readout`←`summary.finding`,
   rollup/rows ← `output_refs`, etc. Verified against the 7 real capsules + 2 real campaigns in Neon.
   720 backend tests green (incl. new presenter + public-route tests); front-end typecheck clean.
5. ⬜ 💸 🔑 **Deploy the website + domain.** Host the Next app (Vercel is the natural fit) and wire the
   domain so `twog.bio` → marketing homepage and the app lives at a path/subdomain (e.g. app.twog.bio).
   → *Confirm domain/subdomain choice.*
6. ✅ (locally) **Smoke test the chain.** Real Neon data renders on STATE/EVIDENCE/RUNS end-to-end:
   Neon → API → Next → browser, all three pages 200, detail pages 200, bad ids 404. Re-verify in the
   cloud once 1/2/5 land.

**Tier 1 done = the public can watch the real engine do its thing, end to end.**

---

## TIER 2 — Collaborators can contribute (gated; the CONTRIBUTE surface)

Needed only when outside labs/people submit evidence — not required for the public "watch it" launch.

7. ⬜ 💸 🔑 **Stand up WorkOS.** Create the WorkOS project, fill the env keys (`.env.example` already
   lists them), and implement the `verify_token` seam (JWKS verify → stable user id). Mapping to twog
   principals is already coded. → *Free tier likely fine; confirm.*
8. ⬜ **Build the custodial key vault.** Platform-held Ed25519 keys, encrypted at rest on Neon, so
   collaborators can sign capsules with zero key UX (self-held keys already supported as the opt-in).
9. ⬜ 🔑 **Harden authz for prod.** Flip `TWOG_REQUIRE_REGISTERED_PRINCIPALS=true` (deny-unknown) and
   add the staged `(candidate_id, content_hash)` dedup UNIQUE index once real concurrent submitters exist.
10. ⬜ **Rebuild the CONTRIBUTE surface.** The old `/contribute` + `/operate` pages are stale (wrong
    brand); rebuild them on the current design system, wired to the apply→approve + submit flow.
11. ⬜ **Operator surface.** Pending-applicant queue, approve/revoke, candidate library, the write-gate
    (accept/promote), budget — all backed by service methods that already exist.

---

## What you do NOT need to do (already handled)

- ✅ The science engine, the falsification loop, the proof-capsule model, the spend-gate (refuse-on-
  unverified inputs), provenance, the campaign runner — all built, tested (700+ tests), proven on real GPU.
- ✅ The Neon database (live), Modal compute (deployed), the API routing logic, the public website design.

---

## The simple explanation (for the audience / the site)

**One line:**
> twog is a research engine that tries to prove itself **wrong** — hunting the cancers dogs and people
> share, out in the open.

**Short version:**
> Dogs and people get strikingly similar cancers. twog is an autonomous engine that takes a promising
> idea about one of them and runs real experiments-in-silico to try to *destroy* it — keeping only the
> ideas tough enough to survive. It runs around the clock, with no human steering it, and every result
> is a sealed, signed **proof capsule** anyone can open and re-check. Most AI hands you an answer you
> have to trust; twog hands you **evidence you can verify**.

**How it works, in three steps:**
1. **It guesses, then attacks.** It poses a hypothesis and runs the cheapest experiment that could kill it.
2. **It refuses to fool itself.** It won't spend a cent of compute on data it can't verify — and a result
   only counts if it survived an honest attempt to break it.
3. **It shows its work.** Every outcome (even the failures, which it keeps) is a signed proof capsule:
   the claim, the evidence, the confidence, and the limits — all re-checkable.

**Why it matters:** faster, cheaper answers for cancers that hurt both species; the failures are kept so
nobody repeats them; and it's free and open.
