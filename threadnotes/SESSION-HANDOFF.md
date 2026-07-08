# Session handoff — resume point

_Written 2026-07-01. Read this first when picking back up._

## TL;DR
Active work is an **in-flight public web / landing redesign** on branch `moonshot-rubric`
in `/Users/home/twog-cleanup` (the live repo). Nothing new is committed since
`98e8124`; there are **24 uncommitted changes** (12 modified, plenty untracked) that
represent the current design iteration. Decide whether to commit, keep iterating, or
discard before switching context.

## Session note: Fable 5 model
- Tried to switch to `claude-fable-5` / `fable` — **rejected at account level**, not a bug.
  Message: "Claude Fable 5 is currently unavailable" → https://www.anthropic.com/news/fable-mythos-access
- It's a real model ID behind staged "Fable Mythos" access; our account isn't entitled.
- Staying on **Opus 4.8** (`claude-opus-4-8`). `/fast` still available on Opus.
- Action if wanted: request access via the link above. No code/config change fixes it.

## Where the code stands (`/Users/home/twog-cleanup`, branch `moonshot-rubric`)
Branch is NOT merged/deployed (per memory: moonshot-rubric inc8 complete but held).

Recent commits (newest first):
- `98e8124` home (4/n): hard-and-heavy heart hero + 'this week' signature moment + live-data fix
- `fa18bce` evidence redesign (3/n): candidate-centric evidence list + conversational runs vocab
- `ec7a303` evidence redesign (2/n): capsule page — public teaser + login-gated full plan
- `0a745d6` evidence redesign (1/n): shared design language + conversational home front door
- `3c28e7f` remove all run pricing/cost from public website (+ operator console)
- `7667e6c` rubric polish: de-dup premise, canonical test-plan order, gradable badge

## Uncommitted / in-flight (the actual "pick back up" surface)
Modified:
- `web/app/{globals.css,layout.tsx,page.tsx}` — landing/home redesign
- `web/components/state/{count-up.tsx,live-activity.tsx}` + `web/lib/api/config.ts`
- `scripts/{run_real_campaign.py,run_web_api.py}`, `src/.../ingestion_bridge/postgres_store.py`
- `web/package.json` + `twog/package.json` (+ lockfiles)

Untracked (new, not yet added):
- `web/components/motion/`, `web/components/state/refusal-monument.tsx`, `.../scroll-rail.tsx`
- `twog/app/v2..v5/` and `twog/components/v2/` — multiple design iterations parked side-by-side
- `threadnotes/homepage-redesign.md`, `threadnotes/landing-design-package.{html,pdf}`

Design intent lives in `threadnotes/homepage-redesign.md` and `landing-design-package.*`.
Note the `v2/v3/v4/v5` app dirs = parallel explorations; pick a winner or they'll rot.

## Guardrails carried from memory (don't re-derive)
- Public surfaces must read as a **grounded scientific argument** — transparent about gaps,
  never fabricated confidence. Pricing/cost already stripped from public site (commit 3c28e7f). See [[chase-grounded-argument-pref]].
- **Always confirm before any paid spend** (GPU/compute/API). See [[always-confirm-spend]].
- Engine idles at $0 when out of runnable work; that's intended. See [[tier1-and-autogen]].

## Suggested first moves next session
1. `cd /Users/home/twog-cleanup && git status` — confirm the 24 changes are still here.
2. Skim `threadnotes/homepage-redesign.md` for the design target.
3. Decide: which of `twog/app/v2..v5` is the keeper → delete the rest.
4. Run the web API + Next front end locally to eyeball current state before committing.
5. Commit the redesign as `home (5/n)` or discard, then reassess merge/deploy of `moonshot-rubric`.
