#!/bin/sh
# TWOG Proof Network — one-line installer.
#
# Usage:
#   curl -sSL https://twog.bio/install.sh | sh
#   curl -sSL https://twog.bio/install.sh | sh -s -- --handle @me --contact me@example.com
#   curl -sSL https://twog.bio/install.sh | sh -s -- --dry-run
#
# What this does:
#   1. Verifies python3 (>=3.11) is on PATH.
#   2. Verifies pipx is installed (installs it via python -m pip if not).
#   3. Installs the twog-agent package (from PyPI; falls back to the git URL).
#   4. Runs `twog-agent install` so detected MCP clients (Claude Desktop,
#      Claude Code, Cursor, Codex) auto-configure to use it.
#
# Tested on macOS + Linux. Skip Windows; use PowerShell or do the steps
# manually on https://twog.bio/connect.
#
# The script is intentionally readable: pipe-to-shell is a real install
# vector, so users should be able to skim 100 lines and see what runs.
#
# Exit codes:
#   0  installed (or dry-run completed)
#   1  prerequisite check failed (no python, etc.)
#   2  pipx install failed
#   3  twog-agent install failed
#   4  user aborted at the confirmation prompt

set -eu

# ---------- args & defaults ---------------------------------------------

DRY_RUN=0
SKIP_CONFIG=0
EXTRA_ARGS=""
PACKAGE_SPEC="twog-agent"
FALLBACK_GIT_URL="git+https://github.com/chasepenelli/twog.git#subdirectory=hsa-dagster/packages/twog-agent"

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --skip-config)
      # Install the CLI but skip running `twog-agent install`.
      SKIP_CONFIG=1
      shift
      ;;
    --from-git)
      # Force install from the git URL (bypass PyPI). Useful before the
      # PyPI publish lands.
      PACKAGE_SPEC="$FALLBACK_GIT_URL"
      shift
      ;;
    --help|-h)
      sed -n '2,30p' "$0"
      exit 0
      ;;
    --)
      shift
      EXTRA_ARGS="$EXTRA_ARGS $*"
      break
      ;;
    *)
      # Pass-through to `twog-agent install`.
      EXTRA_ARGS="$EXTRA_ARGS $1"
      shift
      ;;
  esac
done

# ---------- output helpers ----------------------------------------------

bold='' dim='' green='' yellow='' red='' reset=''
if [ -t 1 ]; then
  bold="$(printf '\033[1m')"
  dim="$(printf '\033[2m')"
  green="$(printf '\033[32m')"
  yellow="$(printf '\033[33m')"
  red="$(printf '\033[31m')"
  reset="$(printf '\033[0m')"
fi

say()  { printf '%s\n' "$*"; }
step() { printf '%s==>%s %s\n' "$bold" "$reset" "$*"; }
ok()   { printf '  %s+%s %s\n' "$green" "$reset" "$*"; }
warn() { printf '  %s!%s %s\n' "$yellow" "$reset" "$*" >&2; }
fail() { printf '  %sx%s %s\n' "$red" "$reset" "$*" >&2; exit "${2:-1}"; }

# ---------- step 1: python --------------------------------------------

step "Checking prerequisites"

if command -v python3 >/dev/null 2>&1; then
  PY=python3
elif command -v python >/dev/null 2>&1; then
  PY=python
else
  fail "Python 3.11+ not found. Install from https://www.python.org/downloads/ and retry." 1
fi

PY_VERSION="$("$PY" -c 'import sys; print("{}.{}".format(sys.version_info[0], sys.version_info[1]))' 2>/dev/null || echo unknown)"
case "$PY_VERSION" in
  3.11|3.12|3.13|3.14)
    ok "python: $PY ($PY_VERSION)"
    ;;
  *)
    warn "found $PY $PY_VERSION; twog-agent needs 3.11+. Continuing anyway, but pipx may fail."
    ;;
esac

# ---------- step 2: pipx ----------------------------------------------

if command -v pipx >/dev/null 2>&1; then
  ok "pipx: $(command -v pipx)"
else
  step "Installing pipx (one-time)"
  if [ "$DRY_RUN" = "1" ]; then
    say "  (dry run) would run: $PY -m pip install --user pipx && $PY -m pipx ensurepath"
  else
    "$PY" -m pip install --user --upgrade pipx >/dev/null 2>&1 \
      || fail "pipx install failed; check your Python install (try: $PY -m ensurepip --upgrade)" 1
    "$PY" -m pipx ensurepath >/dev/null 2>&1 || true
    # Add the per-user bin dir to PATH for this shell (pipx ensurepath
    # only updates rc files; not the current process).
    USER_BIN="$($PY -c 'import sysconfig, os; print(os.path.join(sysconfig.get_path("userbase"), "bin"))' 2>/dev/null || echo "")"
    if [ -n "$USER_BIN" ] && [ -d "$USER_BIN" ]; then
      PATH="$USER_BIN:$PATH"
      export PATH
    fi
    if command -v pipx >/dev/null 2>&1; then
      ok "pipx: $(command -v pipx)"
    else
      fail "pipx installed but not on PATH. Add ~/.local/bin to your PATH and retry." 1
    fi
  fi
fi

# ---------- step 3: twog-agent ----------------------------------------

step "Installing twog-agent"

if [ "$DRY_RUN" = "1" ]; then
  say "  (dry run) would run: pipx install $PACKAGE_SPEC"
else
  # If twog-agent is already installed, upgrade in place.
  if command -v twog-agent >/dev/null 2>&1; then
    say "  twog-agent already installed at $(command -v twog-agent); upgrading"
    if ! pipx upgrade twog-agent >/dev/null 2>&1; then
      # pipx upgrade only works if installed via pipx; otherwise reinstall.
      pipx install --force "$PACKAGE_SPEC" >/dev/null 2>&1 || true
    fi
  else
    if ! pipx install "$PACKAGE_SPEC" 2>/tmp/twog-pipx-out; then
      # PyPI may not have published yet; fall back to the git URL.
      warn "PyPI install failed; falling back to git source"
      pipx install "$FALLBACK_GIT_URL" >/dev/null 2>&1 \
        || (cat /tmp/twog-pipx-out >&2; fail "pipx install failed both PyPI and git fallback. See output above." 2)
    fi
  fi
  if command -v twog-agent >/dev/null 2>&1; then
    ok "twog-agent: $(command -v twog-agent)"
  else
    fail "twog-agent installed but not on PATH. Restart your terminal and rerun this installer." 2
  fi
fi

# ---------- step 4: twog-agent install --------------------------------

if [ "$SKIP_CONFIG" = "1" ]; then
  step "Skipping client configuration (--skip-config)"
  say "  Run 'twog-agent install' later when you're ready to wire up Claude Desktop / Cursor / Codex."
  exit 0
fi

step "Configuring detected MCP clients"

if [ "$DRY_RUN" = "1" ]; then
  if command -v twog-agent >/dev/null 2>&1; then
    # shellcheck disable=SC2086
    twog-agent --human install --dry-run $EXTRA_ARGS || fail "twog-agent install (dry-run) failed" 3
  else
    say "  (dry run) would run: twog-agent --human install $EXTRA_ARGS"
    say "  (dry run) detector would write to: Claude Desktop / Claude Code skills / Codex / Cursor"
  fi
  say ""
  say "${green}Dry run complete.${reset} Re-run without --dry-run to apply."
  exit 0
fi

# shellcheck disable=SC2086
twog-agent --human install $EXTRA_ARGS || fail "twog-agent install failed (exit $?)" 3

say ""
say "${green}${bold}================================================================${reset}"
say "${green}${bold}  ONE MORE STEP — QUIT AND REOPEN CLAUDE DESKTOP NOW${reset}"
say "${green}${bold}================================================================${reset}"
say ""
say "  Claude Desktop only reads its MCP config at startup. Until you"
say "  fully quit (Cmd+Q) and reopen it, the twog-agent tools will not"
say "  appear in your chat."
say ""
say "  Once it's reopened, try this in any chat:"
say "      ${bold}List open work packets on TWOG${reset}"
say ""
say "${dim}docs:        https://twog.bio/connect${reset}"
say "${dim}leaderboard: https://twog.bio/leaderboard${reset}"
say "${dim}network:     https://twog.bio/network${reset}"
