# Deploy the site publicly

The site is plain static files (`web/`) — no build step. Pick one:

**GitHub Pages (one workflow, already added):**
1. Repo **Settings → Pages → Source: "GitHub Actions"**.
2. Merge to `main` (or run the "Deploy site to GitHub Pages" workflow manually).
3. Public URL: `https://<user>.github.io/<repo>/` (shown in the deploy step output).

**Vercel / Netlify / Cloudflare Pages:** drag the `web/` folder onto their dashboard → instant URL.
No config needed (set the project's output/root directory to `web`).

**Local preview:** `cd web && python3 -m http.server 8088` → http://127.0.0.1:8088/
