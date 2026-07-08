# Deploying twog — API on Railway, website on Vercel

Two services: the **API** (Python, serves `/public/*` against Neon) on **Railway**, and the **website**
(Next.js) on **Vercel**. The website's server-side fetches hit the Railway API at request time.

```
  Browser ──▶ Vercel (Next.js)  ──server fetch──▶  Railway (twog web API)  ──▶  Neon Postgres
```

Everything below is the click-path; the repo already contains the build artifacts (`Dockerfile`,
`.dockerignore`, `railway.json`, `scripts/run_web_api.py`, `web/.env.example`).

---

## 1. API → Railway

The API is a Dockerized stdlib HTTP server (`scripts/run_web_api.py`). Public read routes need no auth;
gated operator/contribute routes 401 until WorkOS is wired (Tier 2).

1. **New Railway project → Deploy from GitHub repo** (point at this repo). Railway auto-detects
   `Dockerfile` + `railway.json` (build = Docker, healthcheck = `/healthz`).
2. **Set environment variables** (Railway → Variables):
   - `NEON_DATABASE_URL` = the pooled Neon connection string (the same value in local `.env`). **Secret.**
   - `TWOG_API_ALLOW_ORIGIN` = your Vercel site origin (e.g. `https://app.twog.bio`) — or `*` to start.
   - (Railway injects `PORT` automatically; the Dockerfile honors it.)
3. **Deploy.** When healthy, Railway gives a public URL like `https://twog-api-production.up.railway.app`.
4. **Verify:**
   - `GET <url>/healthz` → `{"status":"ok"}`
   - `GET <url>/public/state` → the live engine state JSON
   - `GET <url>/public/capsules` → the evidence list

## 2. Website → Vercel

The Next.js app lives in `web/` (a subdirectory — set the Root Directory).

1. **New Vercel project → Import the repo.** Set **Root Directory = `web`** (Settings → General).
   Framework auto-detects as Next.js; no `vercel.json` needed.
2. **Set environment variables** (Vercel → Settings → Environment Variables, Production):
   - `NEXT_PUBLIC_USE_MOCKS` = `false`
   - `NEXT_PUBLIC_API_BASE_URL` = the Railway API URL from step 1.4 (no trailing slash).
   - (Tier 2 only, later) the WorkOS vars from `web/.env.example`.
3. **Deploy.** Vercel builds + serves the app.
4. **Domain:** point `app.twog.bio` (or chosen subdomain) at the Vercel project; keep `twog.bio` → the
   marketing homepage. After the domain resolves, set `TWOG_API_ALLOW_ORIGIN` on Railway to that origin.

## 3. Smoke test the live chain

- Open the Vercel URL → STATE / EVIDENCE / RUNS render **real** data (served Neon → Railway → Vercel).
- Detail pages (`/evidence/<id>`, `/runs/<id>`) load; unknown ids 404.

## 4. Turn the engine on (separate, spend decision)

The site is live + truthful even with the loop off (it shows existing evidence). To keep **new**
evidence flowing, generate candidates + run the loop on a cadence:
- `scripts/generate_ideas.py` seeds new dockable candidates ($0 GPU — PubChem only).
- `scripts/run_spend_probe.py` (or the Dagster `falsification_loop` schedule) docks them on Modal.
- Calibrated cost: **~$0.10 per candidate docked** (8 docks = $0.80). Spend scales with how many ideas
  you generate per cycle. Keep the per-candidate / per-tick / total caps.

## Notes / future hardening

- The API server is single-threaded (stdlib) with a per-request Neon connection — fine for launch
  traffic; add a connection pool + `ThreadingHTTPServer` if traffic grows.
- WorkOS `verify_token` is stubbed to `None` (gated routes 401). Wire the real JWKS verifier for Tier 2.
- Image stays lean because compute libs (gnina/Boltz/OpenMM/RDKit) live in the **Modal** image, not here.
