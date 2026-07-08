# TWOG Homepage Redesign — unified brief (5-expert synthesis)

_Date 2026-06-23. Team: IA/layout, visual/UI, copy, interaction/motion, growth. All five converged independently on the same spine._

## The spine (one line)
**"An AI that tries to prove itself wrong — watch it, live, in public."**
Lead with the working machine, not a manifesto. The current page *explains* a philosophy in 8 dense expert sections; the new page *shows* science happening and invites you in.

## Non-negotiables (the brand = honesty)
- Never auto-promoted; a human is the only "yes." Make it loud, not fine print.
- **Celebrate killed ideas** — a falsified hypothesis is a win, designed with the same energy as a pass.
- Real ~$0.10 cost shown as a virtue (cheap disproof). No fake metrics, no manufactured urgency, no walling the ledger behind signup. Every number links to a real record.
- Don't fake activity. The engine is allowed to be quiet; design makes "idle" read as a coiled, frugal machine.

## New information architecture — 8 dense sections → 6
1. **HERO + LIVE LEDGER (fused, above the fold).** Bold claim left, the ledger *already streaming* right. Promise + visible proof on one screen = the whole hook.
2. **"What you're watching" — the loop in 5 plain steps.** Guess → Bet against it → Lock the rules → Run it on a GPU (~$0.10) → Show the receipt. (Merges the old loop / operating-loop / proof-network / validation sections — 4 → 1.)
3. **The one real result.** alpelisib × PI3Kα, −9.8 kcal/mol, "supports" — **NOT promoted**. One concrete un-hyped result out-converts ten claims.
4. **Why dogs, why now.** Canine hemangiosarcoma — urgent, real, mirrors human cancers. The section that earns the *gut*, not just the head. (Email capture lands here, where caring peaks.)
5. **Get involved — the participation ladder.** Watch → Follow → Suggest → Contribute → Run an agent. A real band, not a footer tail.
6. **Footer / built in public.** Inspect first record, architecture, GitHub, contact. Honest integrity claims repeated on purpose.

**Cut:** the "durable judgment" thesis, 3 of 4 redundant mechanism sections, raw jargon (ProofCapsule, confound/provenance gates, comparative-oncology wedge, validation-grade handoff), the marker chips + separate CTA band.

## Copy (recommended hero — direction A)
- **Headline:** "An AI that tries to prove itself wrong."  (_wrong_ in clay with a hand-drawn underline)
- **Subhead:** "TWOG is a research engine hunting cancer-drug ideas that might help dogs — and someday us. It doesn't chase wins. It attacks its own best ideas, in public, and shows every receipt."
- **Primary CTA:** `Watch it work →` (scrolls into / lands on the ledger). **Secondary:** `Get involved`.
- **12-year-old version:** "A robot scientist that tries cancer-drug ideas, does its best to prove each one is wrong, and posts everything so anyone can check."
- **Taglines:** "Falsification first. Receipts always." · "It attacks its own best ideas — so the survivors mean something." · "Cancer research, in public, for ten cents a test."
- Ship an **"in plain English" glossary toggle** (kill-criterion → the pass/fail line set before the test; docking → physics sim of whether a drug fits its target; etc.).

### Live Ledger — example event voice (replaying real history honestly)
`09:14 Proposed a test to kill candidate HSA-014 — let's see if it survives`
`09:14 Locked the pass/fail bar 🔒 (can't move it now — that's the point)`
`09:15 Fetched my own molecular inputs — no hand-feeding`
`09:16 Docking on GPU… $0.11`
`09:18 Result: idea did NOT survive. Good — one less dead end`
`09:31 alpelisib × PI3Kα → −9.8 kcal/mol. Supports the idea ✓`
`09:31 …but NOT promoted. A human decides what's real`

## Visual direction — "a lab notebook that's alive"
- Keep the credible cream + violet. **Warm it:** promote `--paper-warm #fafaf7` to page bg; add `--clay #c2542e` (human "killed", friendlier than red) and `--honey #e8a33d` ("running"). Add `--ink-soft #2a2a2a` for big display. Never introduce blue.
- **Mono leads** (every kicker/code/cost/verdict is IBM Plex Mono). Display scale widens to `clamp(40px,6vw,76px)`. Serif gets one job: hypothesis pull-quotes.
- **The 54px grid becomes the lab bench** the ledger sits on; grid lines spark `--lavender` when an event lands.
- **Signature element — the Proof Stamp:** a slightly rotated (−4°), letter-spaced mono rubber-stamp — `✓ SURVIVED` / `✗ KILLED` / `◷ RUNNING` with its `EXP-####` code beneath. Recurs in the hero, every ledger verdict, section dividers, and one big `✗ KILLED` in the footer (the thesis in one mark). Seeing a rotated mono stamp anywhere = TWOG.

## Live Ledger component spec (public, NOT the dark dev terminal)
- On-paper, readable across a room. Row = `[3px color spine | type | mono time | human claim | cost chip | verdict stamp]`.
- Spine color by event: propose `--purple` · pre-register `--violet` · dock `--honey` · gate `--lavender` · decide `--green`(survived)/`--clay`(killed).
- **Transport:** SSE (`GET /api/ledger/stream`, `text/event-stream`); poll `?since=<id>` every 15s fallback. Cheap when idle.
- **Liveness:** header status capsule — green `LIVE` + telemetry-pulse only when an event arrived <90s ago; else amber `IDLE · last event Nh ago`. **Cost-of-science counter** rolls per digit on each `dock`; idle shows truth `$0.00 today — runs only when a test is worth it`.
- New rows: FLIP, newest on top, ≤1 animation per 90ms (bursts read as a heartbeat). Auto-scroll with pause-on-hover (`▲ 3 new · resume`). Click-to-expand → mini proof (the **hash locked before the run** next to the result — the credibility payload).
- **States:** live / idle (+ Highlight Reel: a failed one, a standing one, cheapest disproof) / replaying (`REPLAY` watermark, instant mount, `— live —` seam) / empty (annotated `EXAMPLE` row, never fabricated) / error (reconnect w/ backoff, last state stays readable).

## Motion system
- Signature curve `--ease-telemetry: cubic-bezier(.16,1,.3,1)`. Tokens: `--dur-tick 120ms`, `--dur-row 380ms`, `--dur-reveal 640ms`.
- Reusable **telemetry-pulse** keyframe on any genuinely-live element (only while a run is active — honest).
- **5 scroll moments:** hero word-by-word settle → ledger "locks in" (surroundings dim, feed brightens) → method rail with a token flowing along the pipeline → `$0.10` counter punch that docks into the live header → get-involved snap.
- **The "aha":** a `dock` row ticks a real elapsed timer (unfaked irregularity = authenticity) → `gate` lands a PASS/**FAIL-celebrated** stamp → cost ticks `+$0.10` → row auto-expands to show the pre-locked hash beside the result for ~4s.
- Full `prefers-reduced-motion` path (opacity-only, counters snap, pulse off) + a persistent manual toggle. Animate transform/opacity only; pause when `document.hidden`; cap ~60 rows.

## Participation ladder (growth)
| Rung | CTA label | Friction | Payoff |
|---|---|---|---|
| Watch | `Watch science happen →` (no signup) | zero | instant proof it's real |
| Follow | `Get the next result in your inbox` | 1 field, shown **at the moment a run resolves** | closure / identity |
| Subscribe | `Read the build-in-public log →` | Substack | narrative, belonging |
| Suggest | `Suggest something to disprove →` | short form, optional handle for credit | their idea in a public queue |
| Contribute | `Return a ProofCapsule →` | auth | name on a permanent record |
| Run an agent | `Spin up a validation agent →` (~$0.10/run, you keep custody) | high | co-author / deepest belonging |
- **Above-the-fold CTA:** primary `Watch it work →`, secondary `Suggest something to disprove`. Don't email-gate the ledger — watching freely *is* the proof.
- **"Choose your way in"** — 4 honest hooks (dog owners / builders / scientists / backers), one element, four doors.
- **Honest momentum:** "N experiments run · $Y spent disproving bad ideas · M public records" (each links to a record); "3 tested, 2 killed, 1 standing"; "last experiment resolved 6h ago"; recent contributor handles. Never fake counts/testimonials/countdowns.

## Build plan (proposed phases)
- **P1 — Live Ledger component + new hero**, built on a safe preview route (`/preview` or `/v2`) so the live homepage isn't disturbed. Ledger seeded by **replaying the real alpelisib run history** + featured candidate (honest), component shaped to accept SSE later. New CSS tokens (clay/honey/ink-soft) + the Proof Stamp + telemetry-pulse.
- **P2 — `/api/ledger/stream` SSE endpoint** reading real ledger rows (the RO-Crate/proof-capsule data); wire idle/replay/empty states to real engine state.
- **P3 — remaining sections** (5-step loop, real-result card, why-dogs, participation ladder, choose-your-way-in) + copy swap + glossary toggle.
- **P4 — motion/scroll choreography pass** + reduced-motion + a11y audit, then promote `/preview` → `/`.

Full per-expert deliverables are in the conversation thread (IA outline + wireframes, visual CSS sketches, full copy incl. glossary, motion spec, growth ladder).
