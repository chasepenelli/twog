"""Fetch a JS-gated URL (e.g. PMC proof-of-work supplementary files) via a headless browser.

Reusable: a headless Chromium runs the page's JS (solving anti-bot PoW / interstitials natively),
then we pull the real bytes through the same cookie-bearing context. Handles the recurring case of
scientific supplements behind PMC's cloudpmc-viewer-pow gate. Usage: fetch_gated.py <url> <outfile>
"""
import sys
from playwright.sync_api import sync_playwright

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")


def fetch(url: str, out: str) -> int:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(user_agent=UA, accept_downloads=True)
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        # let the PoW JS solve + set its cookie
        for _ in range(60):
            if any(c["name"] == "cloudpmc-viewer-pow" for c in ctx.cookies()):
                break
            page.wait_for_timeout(500)
        page.wait_for_timeout(1500)
        resp = ctx.request.get(url)  # cookie now present -> real bytes
        body = resp.body()
        with open(out, "wb") as fh:
            fh.write(body)
        browser.close()
    return len(body)


if __name__ == "__main__":
    n = fetch(sys.argv[1], sys.argv[2])
    head = open(sys.argv[2], "rb").read(8)
    print(f"saved {sys.argv[2]} ({n} bytes) magic={head!r}")
