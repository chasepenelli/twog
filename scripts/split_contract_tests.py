"""One-shot splitter: break tests/test_ingestion_bridge_contracts.py (502 tests in
one 23k-line file) into per-domain modules + a shared tests/_helpers.py.

Strategy (safety-first):
- Parse with ast. Top-level `def test_*` nodes are tests; everything else
  (imports, assignments, helper defs, classes) is the shared header.
- _helpers.py = the shared header verbatim, with an explicit __all__ exporting
  every top-level name it binds, so `from tests._helpers import *` resolves every
  bare name a test could reference (including module imports and constants).
- Each test goes to a domain bucket by name keyword (priority-ordered).
- No test source is edited — only relocated verbatim. Run pytest before/after;
  count must stay 502 passed.
"""
from __future__ import annotations

import ast
from pathlib import Path

SRC = Path("tests/test_ingestion_bridge_contracts.py")
HELPERS = Path("tests/_helpers.py")

# (filename, [keywords]) — first match wins; order matters.
BUCKETS = [
    ("test_agents.py", ["agent_runner", "run_manifest", "agent_run", "agent_perf", "_agent_"]),
    ("test_repository.py", ["repository", "sqlite", "postgres", "_store", "storage"]),
    ("test_compute.py", ["compute", "_md_", "md_expert", "runpod", "docking", "proof_capsule", "workspace", "checkout"]),
    ("test_validation.py", ["validation", "validation_tool", "validation_queue"]),
    ("test_therapy.py", ["therapy"]),
    ("test_omics.py", ["omics", "locus"]),
    ("test_research.py", ["research_brief", "research_program", "research_followup", "_brief", "followup", "reward"]),
    ("test_dagster.py", ["dagster"]),
    ("test_sources.py", ["scrape", "source", "harvest", "parser", "x_topic", "x_linked", "unpaywall", "full_text", "chunk", "entity"]),
    ("test_candidates.py", ["candidate", "contribution", "public_candidate", "claim_curator", "claim_extract"]),
]
CATCHALL = "test_misc.py"

tree = ast.parse(SRC.read_text())
lines = SRC.read_text().splitlines(keepends=True)

shared_nodes, test_nodes = [], []
for node in tree.body:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_"):
        test_nodes.append(node)
    else:
        shared_nodes.append(node)


def seg(node) -> str:
    # include any decorators
    start = min([d.lineno for d in getattr(node, "decorator_list", [])] + [node.lineno]) - 1
    end = node.end_lineno
    return "".join(lines[start:end])


# names bound by the shared header -> __all__
bound: list[str] = []
for node in shared_nodes:
    if isinstance(node, (ast.Import,)):
        for a in node.names:
            bound.append((a.asname or a.name).split(".")[0])
    elif isinstance(node, ast.ImportFrom):
        for a in node.names:
            if a.name == "*":
                continue
            bound.append(a.asname or a.name)
    elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        bound.append(node.name)
    elif isinstance(node, ast.Assign):
        for t in node.targets:
            if isinstance(t, ast.Name):
                bound.append(t.id)
    elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        bound.append(node.target.id)
bound = sorted(set(n for n in bound if not n.startswith("__")))
# `import *` skips single-underscore names, so import those explicitly in each file
underscored = sorted(n for n in bound if n.startswith("_"))

header_src = "".join(seg(n) for n in shared_nodes)
all_src = "__all__ = [\n" + "".join(f"    {n!r},\n" for n in bound) + "]\n"
HELPERS.write_text(header_src + "\n\n" + all_src)

# bucket tests
def pick(name: str) -> str:
    low = name.lower()
    for fname, kws in BUCKETS:
        if any(k in low for k in kws):
            return fname
    return CATCHALL

buckets: dict[str, list[str]] = {}
for node in test_nodes:
    buckets.setdefault(pick(node.name), []).append(seg(node))

_explicit = (
    "from tests._helpers import (  # noqa: F401\n"
    + "".join(f"    {n},\n" for n in underscored)
    + ")\n"
) if underscored else ""
PREAMBLE = (
    '"""Split from test_ingestion_bridge_contracts.py — see scripts/split_contract_tests.py.\n'
    'Shared imports/helpers live in tests/_helpers.py."""\n'
    "from __future__ import annotations\n\n"
    "from tests._helpers import *  # noqa: F401,F403\n"
    + _explicit
    + "\n"
)

written = {}
for fname, srcs in buckets.items():
    body = PREAMBLE + "\n\n".join(s.rstrip() + "\n" for s in srcs)
    Path("tests", fname).write_text(body)
    written[fname] = len(srcs)

SRC.unlink()  # remove the megafile

print(f"shared header: {len(shared_nodes)} nodes, __all__={len(bound)} names -> {HELPERS}")
print(f"tests split: {sum(written.values())} across {len(written)} files")
for f, n in sorted(written.items(), key=lambda x: -x[1]):
    print(f"  {n:4d}  tests/{f}")
