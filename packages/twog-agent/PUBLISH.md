# Publishing twog-agent to PyPI

This package ships as `twog-agent` on PyPI so non-technical users can
run `pipx install twog-agent` without a git URL or `uvx twog-agent`
without an install at all.

## One-time setup

1. **Create a PyPI account** at https://pypi.org/account/register/
   (and a TestPyPI account at https://test.pypi.org for dry runs).
2. **Create a project-scoped API token** in your PyPI account settings
   once the package exists (or use an account-scoped token for the
   first upload). Store it in `~/.pypirc` or pass it as
   `--token "${PYPI_API_TOKEN}"` per command.
3. **Reserve the name** `twog-agent` on PyPI (the first successful
   upload reserves it for your account).

## Each release

```bash
# 1. Bump the version in pyproject.toml (twog-agent's, not the root).
#    Match src/twog_agent/__init__.py::__version__.

# 2. Stage the source + build the wheel.
./build.sh

# 3. Smoke-test the wheel in a fresh venv.
uv venv /tmp/twog-agent-release-check
uv pip install --python /tmp/twog-agent-release-check/bin/python dist/twog_agent-*.whl
/tmp/twog-agent-release-check/bin/twog-agent --help
rm -rf /tmp/twog-agent-release-check

# 4. Push to TestPyPI first.
uv publish --publish-url https://test.pypi.org/legacy/ dist/twog_agent-*.whl \
  --token "${TEST_PYPI_API_TOKEN}"

# 5. Verify TestPyPI install picks up the new wheel.
uv pip install --index-url https://test.pypi.org/simple/ twog-agent

# 6. Push to real PyPI.
uv publish dist/twog_agent-*.whl --token "${PYPI_API_TOKEN}"

# 7. Verify production.
pipx install twog-agent
twog-agent --help
```

## What the wheel contains

The wheel is intentionally slim. After install you get:

```
twog-agent --help            # CLI: list/checkout/submit/status/whoami/install/doctor/mcp
twog-agent mcp               # Stdio MCP server (Claude Desktop / Codex / Cursor)
twog-agent install           # One-command client setup for normies
```

Runtime deps (under 30 packages, ~10 MB installed):

- `httpx` — HTTP client
- `mcp` — Anthropic's Model Context Protocol SDK
- `cryptography` — ed25519 signing

No dagster, no psycopg2, no numpy. The agent talks to the public TWOG
HTTP API; everything heavy lives in the hosted research pipeline.

## What's NOT in the wheel

- The skill bundles (`skills/`) — they're documentation; the installer
  pulls them from the source repo when run from a checkout. For a
  PyPI-only install, the `twog-agent install` command falls back to
  pointing users at the GitHub source.
- The Dagster pipeline, Postgres adapter, Neon migrations — these live
  in the kitchen-sink `hsa-dagster` install, not here.

## CI hook (when ready)

Add a GitHub Actions workflow that:

1. Triggers on `git tag v*`
2. Runs `./packages/twog-agent/build.sh`
3. Uploads to PyPI via `uv publish` with the secret token
4. Updates the `version` in pyproject.toml automatically via release-please or similar.

Not wired yet — manual publish is fine for the first few releases.
