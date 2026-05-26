#!/usr/bin/env python3
"""Assemble a proof capsule JSON from structured inputs.

The hard problem this script solves: a contributor (human or agent) has
done the scientific work — read a paper, written a critique, listed
citations — but doesn't want to hand-write JSON to submit it. This
script takes the work product as readable inputs (CLI flags or stdin
fragments) and emits a valid capsule.json the server will accept.

Pairs with ``validate_capsule.py`` (pre-flight checks) and the main
``twog-agent capsule submit`` command (signing + submission).

Usage:
    wrap_as_capsule.py \\
        --packet <uuid> \\
        --candidate <candidate_id> \\
        --type citation_repair \\
        --title "Replaced broken citation C4 with primary source" \\
        --analysis @analysis.md \\
        --findings @findings.md \\
        --method-refs "doi.org resolver,europepmc,literature-review v1" \\
        --out capsule.json

The ``@filename`` syntax reads the body from a file; bare strings are
used verbatim. Contributor identity is read from the environment
(``TWOG_AGENT_*`` vars) or from the credentials file written by
``twog-agent login``. If you need to override, pass --handle / --contact
/ --kind / --name / --affiliation explicitly.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


PACKET_TYPES = [
    "citation_repair",
    "claim_critique",
    "evidence_addition",
    "omics_note",
    "docking_replication",
    "md_review",
    "validation_proposal",
    "demotion_case",
    "methods_review",
    "freeform",
]
CONTRIBUTOR_KINDS = ["human", "agent", "team", "lab", "company"]
REVIEW_ROUTES = ["operator_review", "validation", "compute_review"]


def _read_value(raw: str | None) -> str | None:
    """Resolve --foo "bar" → "bar"; --foo "@path/to/file" → file contents."""
    if raw is None:
        return None
    if raw.startswith("@"):
        path = Path(raw[1:]).expanduser()
        if not path.exists():
            raise SystemExit(f"value reference {raw} not found")
        return path.read_text(encoding="utf-8").rstrip("\n")
    return raw


def _read_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def _read_artifacts(raw: list[str] | None) -> list[dict[str, Any]]:
    """Parse repeated ``--artifact label|url|content_hash`` flags.

    Pipe-separated, three fields. URL may be empty for off-line artifacts:
        --artifact "primary_pdf|https://example.org/p.pdf|sha256:aaa"
        --artifact "secondary_pdf||sha256:bbb"
    """
    out: list[dict[str, Any]] = []
    for entry in raw or []:
        parts = entry.split("|")
        if len(parts) != 3:
            raise SystemExit(
                f"--artifact must be 'label|url|content_hash' (3 pipe-separated fields): {entry!r}"
            )
        label, url_str, content_hash = (p.strip() for p in parts)
        if not label or not content_hash:
            raise SystemExit(f"--artifact needs label + content_hash: {entry!r}")
        out.append({"label": label, "url": url_str or None, "content_hash": content_hash})
    return out


def _read_contributor(args: argparse.Namespace) -> dict[str, Any]:
    """Identity precedence: explicit CLI flag > env var > credentials file > default."""
    handle = args.handle or os.environ.get("TWOG_AGENT_HANDLE")
    contact = args.contact or os.environ.get("TWOG_AGENT_CONTACT")
    kind = args.kind or os.environ.get("TWOG_AGENT_KIND")
    name = args.name or os.environ.get("TWOG_AGENT_NAME")
    affiliation = args.affiliation or os.environ.get("TWOG_AGENT_AFFILIATION")

    creds_path = Path(
        os.environ.get("TWOG_AGENT_CREDENTIALS_FILE")
        or Path.home() / ".config" / "twog-agent" / "credentials.json"
    ).expanduser()
    if creds_path.exists():
        try:
            creds = json.loads(creds_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise SystemExit(f"credentials file unreadable: {exc}") from exc
        handle = handle or creds.get("handle")
        contact = contact or creds.get("contact")
        kind = kind or creds.get("kind")
        name = name or creds.get("name")
        affiliation = affiliation or creds.get("affiliation")
    kind = kind or "human"

    if not handle:
        raise SystemExit("handle required: pass --handle or set TWOG_AGENT_HANDLE")
    if not contact:
        raise SystemExit("contact required: pass --contact or set TWOG_AGENT_CONTACT")
    if kind not in CONTRIBUTOR_KINDS:
        raise SystemExit(f"kind must be one of {CONTRIBUTOR_KINDS} (got {kind!r})")

    contributor: dict[str, Any] = {"kind": kind, "handle": handle, "contact": contact}
    if name:
        contributor["name"] = name
    if affiliation:
        contributor["affiliation"] = affiliation
    return contributor


def build_capsule(args: argparse.Namespace) -> dict[str, Any]:
    title = (args.title or "").strip()
    analysis = (_read_value(args.analysis) or "").strip()
    findings = _read_value(args.findings)
    limitations = _read_value(args.limitations)

    if not title:
        raise SystemExit("--title is required")
    if not analysis:
        raise SystemExit("--analysis is required (text or @file)")
    if args.type not in PACKET_TYPES:
        raise SystemExit(f"--type must be one of {PACKET_TYPES} (got {args.type!r})")
    if args.review_route and args.review_route not in REVIEW_ROUTES:
        raise SystemExit(f"--review-route must be one of {REVIEW_ROUTES}")

    capsule: dict[str, Any] = {
        "candidate_id": args.candidate,
        "capsule_type": args.type,
        "title": title,
        "analysis_summary": analysis,
        "contributor": _read_contributor(args),
        "method_refs": _read_csv(args.method_refs),
        "output_refs": _read_csv(args.output_refs),
        "artifact_manifest": _read_artifacts(args.artifact),
    }
    if args.packet:
        capsule["work_packet_id"] = args.packet
    if findings is not None:
        capsule["findings"] = findings.strip()
    if limitations is not None:
        capsule["limitations"] = limitations.strip()
    if args.snapshot_hash:
        capsule["candidate_snapshot_hash"] = args.snapshot_hash
    if args.bundle_hash:
        capsule["evidence_bundle_hash"] = args.bundle_hash
    if args.notebook:
        capsule["notebook_ref"] = args.notebook
    if args.review_route:
        capsule["requested_review_route"] = args.review_route
    return capsule


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--packet", help="Work packet UUID you checked out")
    p.add_argument("--candidate", required=True, help="TWOG candidate_id")
    p.add_argument("--type", required=True, help=f"Packet type, one of {PACKET_TYPES}")
    p.add_argument("--title", required=True, help="Capsule title (min 6 chars)")
    p.add_argument("--analysis", required=True, help='Analysis summary. Prefix with @ to read from file.')
    p.add_argument("--findings", help="Concrete findings (text or @file)")
    p.add_argument("--limitations", help="Honest scope (text or @file)")
    p.add_argument("--method-refs", help="Comma-separated method/tool references")
    p.add_argument("--output-refs", help="Comma-separated output references")
    p.add_argument("--snapshot-hash", help="candidate_snapshot_hash from checkout")
    p.add_argument("--bundle-hash", help="evidence_bundle_hash from checkout")
    p.add_argument("--notebook", help="notebook_ref URL or hash")
    p.add_argument("--artifact", action="append", help='Artifact: "label|url|content_hash" (repeatable; url may be empty)')
    p.add_argument("--review-route", help="operator_review | validation | compute_review")
    p.add_argument("--handle", help="Contributor handle (overrides env)")
    p.add_argument("--contact", help="Contributor contact email (overrides env)")
    p.add_argument("--kind", help=f"Contributor kind, one of {CONTRIBUTOR_KINDS}")
    p.add_argument("--name", help="Contributor display name (optional)")
    p.add_argument("--affiliation", help="Contributor affiliation (optional)")
    p.add_argument("--out", help="Write capsule JSON here (default: stdout)")
    p.add_argument(
        "--validate",
        action="store_true",
        help="Also run validate_capsule.py before writing; refuse to write a bad capsule.",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        capsule = build_capsule(args)
    except SystemExit as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.validate:
        from validate_capsule import validate_capsule

        problems = validate_capsule(capsule)
        if problems:
            print("error: capsule failed validation:", file=sys.stderr)
            for prob in problems:
                print(f"  - {prob}", file=sys.stderr)
            return 5

    output = json.dumps(capsule, indent=2, sort_keys=True, ensure_ascii=False)
    if args.out:
        Path(args.out).expanduser().write_text(output + "\n", encoding="utf-8")
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        sys.stdout.write(output + "\n")
    return 0


if __name__ == "__main__":
    # Make `from validate_capsule import validate_capsule` resolve when this
    # script is invoked directly (not via a package install).
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    sys.exit(main())
