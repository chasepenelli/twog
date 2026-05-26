#!/usr/bin/env python3
"""Pre-flight validation for a proof capsule JSON.

Mirrors the gates the TWOG server enforces (twog/lib/proof-capsules.ts +
twog/lib/proof-capsule-quality.ts). A capsule that passes here will pass
on the server too, modulo per-handle rate limits (which depend on
recent history, not the capsule content).

Use this before ``twog-agent capsule submit`` to catch obvious
problems locally instead of burning a rate-limit slot on an invalid
submission.

Usage:
    validate_capsule.py --file capsule.json
    cat capsule.json | validate_capsule.py
Exit codes:
    0  passes
    5  invalid (matches server's INVALID_PACKET exit code semantics)
    2  bad CLI args
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


PACKET_TYPES = {
    "citation_repair", "claim_critique", "evidence_addition", "omics_note",
    "docking_replication", "md_review", "validation_proposal",
    "demotion_case", "methods_review", "freeform",
}
CONTRIBUTOR_KINDS = {"human", "agent", "team", "lab", "company"}
REVIEW_ROUTES = {"operator_review", "validation", "compute_review"}

TITLE_MIN_CHARS = 6
ANALYSIS_MIN_CHARS_HARD = 20      # twog/lib/proof-capsules.ts line 299
ANALYSIS_MIN_CHARS_QUALITY = 80   # twog/lib/proof-capsule-quality.ts THIN_ANALYSIS_MIN_CHARS
ANALYSIS_MIN_WORDS = 12           # twog/lib/proof-capsule-quality.ts THIN_ANALYSIS_MIN_WORDS
REPETITION_MIN_LEN = 6            # twog/lib/proof-capsule-quality.ts REPETITION_MIN_LEN
REPETITION_MIN_COUNT = 4          # twog/lib/proof-capsule-quality.ts REPETITION_MIN_COUNT
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _check_top_level(capsule: dict[str, Any], problems: list[str]) -> None:
    candidate_id = capsule.get("candidate_id")
    if not isinstance(candidate_id, str) or not candidate_id.strip():
        problems.append("candidate_id is required")

    capsule_type = capsule.get("capsule_type")
    if capsule_type not in PACKET_TYPES:
        problems.append(f"capsule_type must be one of {sorted(PACKET_TYPES)} (got {capsule_type!r})")

    title = capsule.get("title")
    if not isinstance(title, str):
        problems.append("title is required")
    elif len(title.strip()) < TITLE_MIN_CHARS:
        problems.append(f"title must be at least {TITLE_MIN_CHARS} characters (got {len(title.strip())})")


def _check_analysis(capsule: dict[str, Any], problems: list[str]) -> None:
    analysis = capsule.get("analysis_summary")
    if not isinstance(analysis, str):
        problems.append("analysis_summary is required")
        return
    text = analysis.strip()
    if len(text) < ANALYSIS_MIN_CHARS_HARD:
        problems.append(
            f"analysis_summary must be at least {ANALYSIS_MIN_CHARS_HARD} characters "
            f"(got {len(text)})"
        )
        return
    if len(text) < ANALYSIS_MIN_CHARS_QUALITY:
        problems.append(
            f"analysis_summary is too thin: needs ≥{ANALYSIS_MIN_CHARS_QUALITY} chars (got {len(text)})"
        )
    words = [w for w in re.split(r"\s+", text) if w]
    if len(words) < ANALYSIS_MIN_WORDS:
        problems.append(
            f"analysis_summary is too thin: needs ≥{ANALYSIS_MIN_WORDS} words (got {len(words)})"
        )
    # Repetition guard: no token of len ≥ 6 may appear ≥ 4 times
    counts: dict[str, int] = {}
    for w in words:
        normalized = w.lower().strip(".,;:!?\"'()[]{}-—–")
        if len(normalized) >= REPETITION_MIN_LEN:
            counts[normalized] = counts.get(normalized, 0) + 1
    repeated = [w for w, c in counts.items() if c >= REPETITION_MIN_COUNT]
    if repeated:
        problems.append(
            f"analysis_summary repeats tokens too often: {', '.join(sorted(repeated))} "
            f"(each ≥{REPETITION_MIN_COUNT} times)"
        )


def _check_contributor(capsule: dict[str, Any], problems: list[str]) -> None:
    contributor = capsule.get("contributor")
    if not isinstance(contributor, dict):
        problems.append("contributor block is required")
        return
    handle = contributor.get("handle")
    if not isinstance(handle, str) or not handle.strip():
        problems.append("contributor.handle is required")
    elif not handle.startswith("@"):
        problems.append("contributor.handle must start with @ (e.g. @your-handle)")
    contact = contributor.get("contact")
    if not isinstance(contact, str) or not contact.strip():
        problems.append("contributor.contact is required")
    elif not EMAIL_PATTERN.match(contact.strip()):
        problems.append(f"contributor.contact doesn't look like an email: {contact!r}")
    kind = contributor.get("kind", "human")
    if kind not in CONTRIBUTOR_KINDS:
        problems.append(f"contributor.kind must be one of {sorted(CONTRIBUTOR_KINDS)} (got {kind!r})")


def _check_artifacts(capsule: dict[str, Any], problems: list[str]) -> None:
    manifest = capsule.get("artifact_manifest", [])
    if manifest is None:
        return
    if not isinstance(manifest, list):
        problems.append("artifact_manifest must be a list")
        return
    for i, entry in enumerate(manifest):
        if not isinstance(entry, dict):
            problems.append(f"artifact_manifest[{i}] must be an object")
            continue
        if not entry.get("label"):
            problems.append(f"artifact_manifest[{i}].label is required")
        ch = entry.get("content_hash")
        if not isinstance(ch, str) or not ch.strip():
            problems.append(f"artifact_manifest[{i}].content_hash is required")


def _check_optional_enums(capsule: dict[str, Any], problems: list[str]) -> None:
    route = capsule.get("requested_review_route")
    if route is not None and route not in REVIEW_ROUTES:
        problems.append(f"requested_review_route must be one of {sorted(REVIEW_ROUTES)} (got {route!r})")


def validate_capsule(capsule: dict[str, Any]) -> list[str]:
    """Return a list of validation problems; empty means the capsule passes."""
    problems: list[str] = []
    _check_top_level(capsule, problems)
    _check_analysis(capsule, problems)
    _check_contributor(capsule, problems)
    _check_artifacts(capsule, problems)
    _check_optional_enums(capsule, problems)
    return problems


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--file", help="Path to capsule.json (default: stdin)")
    p.add_argument("--quiet", action="store_true", help="Suppress 'passes' line on success")
    args = p.parse_args(argv)

    if args.file:
        try:
            text = Path(args.file).expanduser().read_text(encoding="utf-8")
        except OSError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
    else:
        text = sys.stdin.read()

    try:
        capsule = json.loads(text)
    except json.JSONDecodeError as exc:
        print(f"error: capsule is not valid JSON: {exc}", file=sys.stderr)
        return 5

    if not isinstance(capsule, dict):
        print("error: capsule must be a JSON object", file=sys.stderr)
        return 5

    problems = validate_capsule(capsule)
    if problems:
        print("capsule fails validation:", file=sys.stderr)
        for prob in problems:
            print(f"  - {prob}", file=sys.stderr)
        return 5

    if not args.quiet:
        print("capsule passes all local validation checks")
    return 0


if __name__ == "__main__":
    sys.exit(main())
