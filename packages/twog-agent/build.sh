#!/usr/bin/env bash
# Build the slim twog-agent wheel for PyPI.
#
# We can't symlink into <repo>/src/twog_agent because hatch builds
# happen in a temp directory and the symlink target won't resolve.
# Instead we stage a fresh copy of the agent source into this package
# directory, run ``uv build``, then clean up. The agent source of truth
# stays at <repo>/src/twog_agent; this script just packages it.
#
# Usage:
#   ./build.sh           # builds dist/twog_agent-<version>-py3-none-any.whl
#   ./build.sh clean     # remove dist/ and the staged copy
#
# After building, smoke-test the wheel in a clean venv:
#   uv venv /tmp/twog-agent-test
#   /tmp/twog-agent-test/bin/uv pip install dist/twog_agent-*.whl
#   /tmp/twog-agent-test/bin/twog-agent --help
#
# To publish (requires a PyPI API token):
#   uv publish dist/twog_agent-*.whl --token "${PYPI_API_TOKEN}"

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
SOURCE="$REPO_ROOT/src/twog_agent"
STAGED="$HERE/twog_agent"

clean() {
  rm -rf "$STAGED" "$HERE/dist"
  echo "cleaned: $STAGED + $HERE/dist"
}

if [ "${1:-}" = "clean" ]; then
  clean
  exit 0
fi

if [ ! -d "$SOURCE" ]; then
  echo "ERROR: source not found at $SOURCE" >&2
  exit 1
fi

# Stage a clean copy of the agent source.
rm -rf "$STAGED"
cp -r "$SOURCE" "$STAGED"
# Strip __pycache__ so the wheel stays small.
find "$STAGED" -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

echo "staged $(find "$STAGED" -type f -name '*.py' | wc -l | tr -d ' ') Python files into $STAGED"

# Build the wheel + sdist.
rm -rf "$HERE/dist"
(cd "$HERE" && uv build)

# Show what landed.
ls -la "$HERE/dist"

# Leave the staged copy in place so subsequent re-runs are fast; ./build.sh clean removes it.
echo "wheel ready: $(ls "$HERE/dist"/*.whl | head -1)"
