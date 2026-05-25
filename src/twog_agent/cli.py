"""twog-agent CLI — autonomous client for the TWOG Proof Network.

Every command:

- Reads identity + endpoint config from environment variables.
- Prints JSON to stdout by default (``--human`` switches to a terse summary).
- Exits with a deterministic code so a wrapping shell can branch on the
  verdict without parsing prose. See :mod:`twog_agent.exits`.

Environment variables:

    TWOG_SITE_URL          Base URL of the TWOG public site (default https://twog.bio).
    TWOG_AGENT_HANDLE      Stable handle reputation accrues to (e.g. @my-agent).
    TWOG_AGENT_CONTACT     Contact address for follow-up (required by the server).
    TWOG_AGENT_KIND        One of human|agent|team|lab|company. Default: agent.
    TWOG_AGENT_NAME        Display name for the contributor.
    TWOG_AGENT_AFFILIATION Optional affiliation string.
    TWOG_AGENT_ID          Optional stable agent identifier.
    TWOG_AGENT_WEBSITE     Optional URL.
    TWOG_AGENT_PRIVKEY     Optional base64 ed25519 seed for signed capsules.

The CLI never persists credentials. ``TWOG_AGENT_PRIVKEY`` is only used
to sign in-memory at submit time and is not written to disk.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from twog_agent.client import (
    NetworkUnavailable,
    ProofNetworkClient,
    ProofNetworkError,
    resolve_site_url,
)
from twog_agent.content_hash import compute_proof_capsule_content_hash
from twog_agent.exits import Exit, NON_TERMINAL_STATUSES, TERMINAL_STATUS_EXIT
from twog_agent.signing import sign_content_hash


# ---------- Identity helpers --------------------------------------------


def _env(name: str) -> str | None:
    value = os.environ.get(name)
    return value.strip() if value else None


def _contributor_from_env() -> dict[str, Any]:
    """Resolve contributor identity from env, with credentials.json as a fallback.

    Precedence:
      1. Explicit env vars (TWOG_AGENT_HANDLE, …) — wins for one-off overrides.
      2. ``~/.twog-agent/credentials.json`` written by ``twog-agent login``.
      3. Unset (caller decides; for ``agent`` kind we default to that).

    This is the single source of truth for "who am I right now" across
    every subcommand: list/checkout/submit/status/whoami/install.
    """

    creds = _read_credentials_if_present()
    handle = _env("TWOG_AGENT_HANDLE") or (creds.handle if creds else None)
    contact = _env("TWOG_AGENT_CONTACT") or (creds.contact if creds else None)
    contributor: dict[str, Any] = {
        "kind": _env("TWOG_AGENT_KIND") or (creds.kind if creds else None) or "agent",
        "handle": handle,
        "contact": contact,
    }
    for key, env_key, cred_attr in (
        ("name", "TWOG_AGENT_NAME", "name"),
        ("affiliation", "TWOG_AGENT_AFFILIATION", "affiliation"),
        ("agent_id", "TWOG_AGENT_ID", "agent_id"),
        ("website", "TWOG_AGENT_WEBSITE", "website"),
    ):
        value = _env(env_key) or (getattr(creds, cred_attr, None) if creds else None)
        if value:
            contributor[key] = value
    return contributor


def _merge_contributor(
    explicit: dict[str, Any] | None, env: dict[str, Any]
) -> dict[str, Any]:
    """Capsule-file contributor wins; env fills the gaps."""

    merged: dict[str, Any] = dict(env)
    if explicit:
        for key, value in explicit.items():
            if value:
                merged[key] = value
    return merged


# ---------- Output helpers ----------------------------------------------


def _emit(payload: Any, *, human: bool = False, fallback_summary: str | None = None) -> None:
    if human and fallback_summary is not None:
        sys.stdout.write(fallback_summary.rstrip() + "\n")
        return
    json.dump(payload, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")


def _eprint(*parts: str) -> None:
    sys.stderr.write(" ".join(str(p) for p in parts).rstrip() + "\n")


# ---------- Error → exit code mapping -----------------------------------

_ERROR_CODE_TO_EXIT: dict[str, Exit] = {
    "work_packet_storage_not_configured": Exit.STORAGE_NOT_CONFIGURED,
    "proof_capsule_storage_not_configured": Exit.STORAGE_NOT_CONFIGURED,
    "candidate_contribution_storage_not_configured": Exit.STORAGE_NOT_CONFIGURED,
    "work_packet_not_found": Exit.NOT_FOUND,
    "proof_capsule_not_found": Exit.NOT_FOUND,
    "public_candidate_not_found": Exit.NOT_FOUND,
    "invalid_proof_capsule_submission": Exit.INVALID_PACKET,
    "invalid_proof_capsule_signature": Exit.INVALID_PACKET,
    "proof_capsule_submission_too_large": Exit.INVALID_PACKET,
    "proof_capsule_rate_limit_exceeded": Exit.RATE_LIMITED,
}


def _exit_for_proof_network_error(error: ProofNetworkError) -> Exit:
    return _ERROR_CODE_TO_EXIT.get(error.code, Exit.GENERIC_ERROR)


# ---------- Subcommands -------------------------------------------------


def cmd_packets_list(args: argparse.Namespace) -> int:
    with ProofNetworkClient(site_url=args.site_url) as client:
        try:
            payload = client.list_work_packets(
                statuses=args.status or None,
                packet_types=args.type or None,
                candidate_ids=args.candidate_id or None,
                limit=args.limit,
            )
        except ProofNetworkError as exc:
            _eprint(str(exc))
            return _exit_for_proof_network_error(exc)
        except NetworkUnavailable as exc:
            _eprint(str(exc))
            return Exit.NETWORK_ERROR

    if args.human:
        rows = payload.get("work_packets") or []
        if not rows:
            sys.stdout.write("(no open work packets)\n")
        for row in rows:
            sys.stdout.write(
                f"{row['work_packet_id']}  {row['packet_type']:<20} "
                f"{row['difficulty']:<10} {row['candidate_id'][:24]:<24} {row['title']}\n"
            )
        return Exit.SUCCESS
    _emit(payload)
    return Exit.SUCCESS


def cmd_packets_checkout(args: argparse.Namespace) -> int:
    with ProofNetworkClient(site_url=args.site_url) as client:
        try:
            payload = client.checkout_work_packet(args.packet_id)
        except ProofNetworkError as exc:
            _eprint(str(exc))
            return _exit_for_proof_network_error(exc)
        except NetworkUnavailable as exc:
            _eprint(str(exc))
            return Exit.NETWORK_ERROR

    if args.out:
        Path(args.out).write_text(json.dumps(payload, indent=2, default=str) + "\n")
        if args.human:
            sys.stdout.write(f"wrote {args.out}\n")
        return Exit.SUCCESS

    if args.human:
        wp = payload["work_packet"]
        cand = payload["candidate"]
        sys.stdout.write(
            f"packet:  {wp['work_packet_id']}\n"
            f"type:    {wp['packet_type']}\n"
            f"candidate: {cand['candidate_id']} ({cand.get('display_id') or ''})\n"
            f"snapshot:  {cand.get('snapshot_short_hash') or cand.get('snapshot_content_hash')}\n"
        )
        return Exit.SUCCESS
    _emit(payload)
    return Exit.SUCCESS


def _build_submission_body(
    *,
    capsule_file: Path,
    packet_id: str | None,
    candidate_id_override: str | None,
    checkout: dict[str, Any] | None,
) -> dict[str, Any]:
    raw = json.loads(capsule_file.read_text())
    if not isinstance(raw, dict):
        raise SystemExit("capsule file must contain a JSON object")
    body: dict[str, Any] = dict(raw)

    # Resolve candidate_id: explicit arg > file value > checkout-derived.
    candidate_id = (
        candidate_id_override
        or body.get("candidate_id")
        or (checkout or {}).get("candidate", {}).get("candidate_id")
    )
    if not candidate_id:
        raise SystemExit(
            "candidate_id is required: pass --candidate-id, set it in the file, "
            "or use --packet so checkout can resolve it."
        )
    body["candidate_id"] = candidate_id

    # Fill in work_packet_id from --packet if not present.
    if packet_id and not body.get("work_packet_id"):
        body["work_packet_id"] = packet_id

    # Fill snapshot/evidence hashes from checkout if missing.
    if checkout:
        candidate = checkout.get("candidate") or {}
        bundle = (checkout.get("evidence_bundle_summary") or {}).get("snapshot") or {}
        if not body.get("candidate_snapshot_hash") and candidate.get("snapshot_content_hash"):
            body["candidate_snapshot_hash"] = candidate["snapshot_content_hash"]
        if not body.get("evidence_bundle_hash"):
            body["evidence_bundle_hash"] = (
                bundle.get("content_hash") or candidate.get("snapshot_content_hash")
            )

    # Merge contributor identity (env fills missing fields).
    explicit_contributor = body.get("contributor") if isinstance(body.get("contributor"), dict) else None
    merged = _merge_contributor(explicit_contributor, _contributor_from_env())
    if not merged.get("handle"):
        raise SystemExit(
            "contributor.handle is required. Set TWOG_AGENT_HANDLE or put it in the capsule file."
        )
    if not merged.get("contact"):
        raise SystemExit(
            "contributor.contact is required. Set TWOG_AGENT_CONTACT or put it in the capsule file."
        )
    body["contributor"] = merged
    return body


def _attach_content_hash_and_signature(body: dict[str, Any]) -> None:
    """Compute the canonical content_hash client-side and (optionally) sign it.

    We always attach a client-computed ``content_hash`` so the server can
    use it for dedup and the signature (if present) is bound to the same
    digest the server would compute. The hashing algorithm mirrors
    ``hsa_research.ingestion_bridge.contracts._proof_capsule_content_hash``
    — a regression test pins the two implementations together.

    If ``TWOG_AGENT_PRIVKEY`` is set, signs the content_hash and attaches
    the signature in the format ``ed25519:<pubkey-b64>:<sig-b64>``.
    Reviewers verify out-of-band; the server stores the signature but does
    not enforce verification yet.
    """

    if not body.get("content_hash"):
        body["content_hash"] = compute_proof_capsule_content_hash(body)

    if body.get("signature"):
        return  # caller already supplied a signature

    try:
        signature = sign_content_hash(body["content_hash"])
    except RuntimeError as exc:
        raise SystemExit(str(exc))
    if signature is not None:
        body["signature"] = signature.as_packet_string()


def cmd_capsule_submit(args: argparse.Namespace) -> int:
    capsule_file = Path(args.file)
    if not capsule_file.is_file():
        _eprint(f"capsule file not found: {capsule_file}")
        return Exit.INVALID_ARGS

    checkout: dict[str, Any] | None = None
    if args.checkout:
        try:
            checkout = json.loads(Path(args.checkout).read_text())
        except (OSError, ValueError) as exc:
            _eprint(f"failed to read checkout file: {exc}")
            return Exit.INVALID_ARGS

    try:
        body = _build_submission_body(
            capsule_file=capsule_file,
            packet_id=args.packet,
            candidate_id_override=args.candidate_id,
            checkout=checkout,
        )
    except SystemExit as exc:
        _eprint(str(exc))
        return Exit.INVALID_PACKET

    _attach_content_hash_and_signature(body)

    with ProofNetworkClient(site_url=args.site_url) as client:
        try:
            payload = client.submit_proof_capsule(body)
        except ProofNetworkError as exc:
            _eprint(str(exc))
            return _exit_for_proof_network_error(exc)
        except NetworkUnavailable as exc:
            _eprint(str(exc))
            return Exit.NETWORK_ERROR

    if args.human:
        capsule = payload.get("proof_capsule") or {}
        sys.stdout.write(
            f"capsule:    {capsule.get('proof_capsule_id')}\n"
            f"content:    {capsule.get('content_hash')}\n"
            f"status:     {capsule.get('status')}\n"
            f"status_url: {capsule.get('status_url')}\n"
        )
        return Exit.SUCCESS
    _emit(payload)
    return Exit.SUCCESS


def cmd_capsule_status(args: argparse.Namespace) -> int:
    deadline = time.time() + max(1.0, args.timeout) if args.wait else None
    poll_interval = max(1.0, args.poll_interval)

    with ProofNetworkClient(site_url=args.site_url) as client:
        last_payload: dict[str, Any] | None = None
        while True:
            try:
                payload = client.get_proof_capsule(args.capsule_id)
            except ProofNetworkError as exc:
                _eprint(str(exc))
                return _exit_for_proof_network_error(exc)
            except NetworkUnavailable as exc:
                _eprint(str(exc))
                return Exit.NETWORK_ERROR

            last_payload = payload
            capsule = payload.get("proof_capsule") or {}
            status = capsule.get("status")

            if not args.wait:
                break
            if status in TERMINAL_STATUS_EXIT:
                break
            if status in NON_TERMINAL_STATUSES:
                if deadline is not None and time.time() >= deadline:
                    if args.human:
                        sys.stdout.write(f"timed out waiting; last status={status}\n")
                    else:
                        _emit(payload)
                    return Exit.WAIT_TIMEOUT
                time.sleep(poll_interval)
                continue
            # Unknown status; surface and exit generic.
            _eprint(f"unexpected capsule status: {status}")
            _emit(payload)
            return Exit.GENERIC_ERROR

    capsule = (last_payload or {}).get("proof_capsule") or {}
    status = capsule.get("status")
    if args.human:
        sys.stdout.write(
            f"capsule:  {capsule.get('proof_capsule_id')}\n"
            f"status:   {status}\n"
            f"reviewed: {capsule.get('reviewed_at') or '-'}\n"
        )
    else:
        _emit(last_payload or {})

    if status in TERMINAL_STATUS_EXIT:
        return TERMINAL_STATUS_EXIT[status]
    return Exit.SUCCESS


def cmd_do(args: argparse.Namespace) -> int:
    """Pipeline: checkout → submit → (optional) wait for verdict."""

    with ProofNetworkClient(site_url=args.site_url) as client:
        try:
            checkout = client.checkout_work_packet(args.packet)
        except ProofNetworkError as exc:
            _eprint(str(exc))
            return _exit_for_proof_network_error(exc)
        except NetworkUnavailable as exc:
            _eprint(str(exc))
            return Exit.NETWORK_ERROR

        capsule_file = Path(args.capsule)
        if not capsule_file.is_file():
            _eprint(f"capsule file not found: {capsule_file}")
            return Exit.INVALID_ARGS

        try:
            body = _build_submission_body(
                capsule_file=capsule_file,
                packet_id=args.packet,
                candidate_id_override=args.candidate_id,
                checkout=checkout,
            )
        except SystemExit as exc:
            _eprint(str(exc))
            return Exit.INVALID_PACKET

        _attach_content_hash_and_signature(body)

        try:
            submission = client.submit_proof_capsule(body)
        except ProofNetworkError as exc:
            _eprint(str(exc))
            return _exit_for_proof_network_error(exc)
        except NetworkUnavailable as exc:
            _eprint(str(exc))
            return Exit.NETWORK_ERROR

        capsule = submission.get("proof_capsule") or {}
        capsule_id = capsule.get("proof_capsule_id")
        result: dict[str, Any] = {
            "phase": "submitted",
            "checkout": checkout,
            "submission": submission,
        }

        if not args.wait or not capsule_id:
            if args.human:
                sys.stdout.write(
                    f"capsule:    {capsule_id}\n"
                    f"content:    {capsule.get('content_hash')}\n"
                    f"status:     {capsule.get('status')}\n"
                    f"status_url: {capsule.get('status_url')}\n"
                )
            else:
                _emit(result)
            return Exit.SUCCESS

        deadline = time.time() + max(1.0, args.timeout)
        poll_interval = max(1.0, args.poll_interval)
        status = capsule.get("status")
        last_status_payload: dict[str, Any] = submission
        while status in NON_TERMINAL_STATUSES:
            if time.time() >= deadline:
                result["phase"] = "wait_timeout"
                result["final_status"] = last_status_payload
                if args.human:
                    sys.stdout.write(f"timed out waiting; last status={status}\n")
                else:
                    _emit(result)
                return Exit.WAIT_TIMEOUT
            time.sleep(poll_interval)
            try:
                last_status_payload = client.get_proof_capsule(capsule_id)
            except ProofNetworkError as exc:
                _eprint(str(exc))
                return _exit_for_proof_network_error(exc)
            except NetworkUnavailable as exc:
                _eprint(str(exc))
                return Exit.NETWORK_ERROR
            status = (last_status_payload.get("proof_capsule") or {}).get("status")

        result["phase"] = "verdict"
        result["final_status"] = last_status_payload

    final = (result["final_status"].get("proof_capsule") or {}) if result.get("final_status") else {}
    if args.human:
        sys.stdout.write(
            f"capsule:  {final.get('proof_capsule_id')}\n"
            f"verdict:  {final.get('status')}\n"
            f"reviewed: {final.get('reviewed_at') or '-'}\n"
        )
    else:
        _emit(result)

    final_status = final.get("status")
    if final_status in TERMINAL_STATUS_EXIT:
        return TERMINAL_STATUS_EXIT[final_status]
    return Exit.SUCCESS


def cmd_mcp(args: argparse.Namespace) -> int:  # noqa: ARG001
    """Run the stdio MCP server.

    Imports :mod:`twog_agent.mcp_server` lazily so the CLI's other
    subcommands keep working when the optional ``mcp`` SDK isn't installed.
    """

    try:
        from twog_agent.mcp_server import main as mcp_main
    except ImportError as exc:  # pragma: no cover - defensive
        _eprint(
            "the 'mcp' SDK is not installed; reinstall twog-agent "
            f"with the mcp extra. ({exc})"
        )
        return Exit.GENERIC_ERROR
    return int(mcp_main())


def _prompt(label: str, *, default: str | None = None) -> str:
    """Stdin-only prompt for the install flow. Honors a non-tty stdin
    gracefully — if stdin is not a tty, we fall back to the default and
    return it without blocking (so headless installs still work as long
    as the env or flags supply the values)."""

    suffix = f" [{default}]" if default else ""
    try:
        if not sys.stdin.isatty():
            return default or ""
        sys.stdout.write(f"{label}{suffix}: ")
        sys.stdout.flush()
        line = sys.stdin.readline().strip()
        return line or (default or "")
    except (EOFError, KeyboardInterrupt):
        return default or ""


def _resolve_install_identity(args: argparse.Namespace) -> tuple[str, str, str, str | None]:
    """Resolve handle / contact / kind / name.

    Precedence: explicit --flag > env var > credentials.json > interactive prompt.
    Stored credentials let a logged-in user run ``twog-agent install`` with
    no args and no env vars at all.
    """

    from twog_agent.install import VALID_KINDS, looks_like_email, normalize_handle

    creds = _read_credentials_if_present()

    def from_creds(attr: str) -> str | None:
        return getattr(creds, attr, None) if creds else None

    handle_raw = args.handle or _env("TWOG_AGENT_HANDLE") or from_creds("handle")
    if not handle_raw:
        handle_raw = _prompt("Your handle (e.g. @my-agent)", default=None)
    handle = normalize_handle(handle_raw or "")
    if not handle or handle == "@":
        raise SystemExit("handle is required (e.g. @my-agent)")

    contact = args.contact or _env("TWOG_AGENT_CONTACT") or from_creds("contact")
    if not contact:
        contact = _prompt("Contact email", default=None)
    if not contact:
        raise SystemExit("contact is required")
    if not looks_like_email(contact):
        _eprint(f"warning: '{contact}' doesn't look like an email; continuing anyway")

    kind = args.kind or _env("TWOG_AGENT_KIND") or from_creds("kind") or "agent"
    if kind not in VALID_KINDS:
        raise SystemExit(
            f"kind must be one of {', '.join(VALID_KINDS)} (got {kind!r})"
        )

    name = args.name or _env("TWOG_AGENT_NAME") or from_creds("name") or None
    return handle, contact.strip(), kind, name


def cmd_install(args: argparse.Namespace) -> int:
    """Auto-configure detected MCP clients to use the TWOG MCP server."""

    from twog_agent.install import (
        InstallPlan,
        SOUL_BUNDLES,
        detect_clients,
        run_install,
    )

    try:
        handle, contact, kind, name = _resolve_install_identity(args)
    except SystemExit as exc:
        _eprint(str(exc))
        return Exit.INVALID_ARGS

    targets = detect_clients()
    if not targets:
        message = (
            "no MCP-capable clients detected.\n"
            "  Looked for: Claude Desktop, Claude Code skills, Codex, Cursor.\n"
            "  Easiest fix: install Claude Desktop from https://claude.ai/download,\n"
            "    then rerun this command.\n"
            "  Already installed but at a non-standard path? See the snippet at\n"
            "    https://twog.bio/connect for the JSON to paste manually."
        )
        if args.dry_run:
            if args.human:
                sys.stdout.write(
                    "identity:  handle=" + handle + "  kind=" + kind + "  contact set\n"
                    "dry_run:   True\n\n"
                    "  · (no MCP clients detected — nothing to preview)\n\n"
                    + message + "\n"
                )
            else:
                _emit(
                    {
                        "dry_run": True,
                        "targets": [],
                        "overall_change": False,
                        "message": "no MCP-capable clients detected",
                    }
                )
            return Exit.SUCCESS
        _eprint(message)
        return Exit.GENERIC_ERROR

    site_url = args.site_url or _env("TWOG_SITE_URL") or "https://twog.bio"
    creds_for_priv = _read_credentials_if_present()
    privkey = (
        _env("TWOG_AGENT_PRIVKEY")
        or (creds_for_priv.ed25519_private_key_b64 if creds_for_priv else None)
    )

    if args.skills_only:
        # Skip JSON config writes; only install skill bundles into Claude Code.
        targets = [t for t in targets if t.name == "Claude Code skills"]
        if not targets:
            _eprint("no skills target detected (~/.claude/skills/).")
            return Exit.GENERIC_ERROR

    bundles = tuple(args.skill) if args.skill else SOUL_BUNDLES
    plan = InstallPlan(
        handle=handle,
        contact=contact,
        kind=kind,
        name=name,
        site_url=site_url,
        privkey=privkey,
        targets=targets,
        skill_bundles=bundles,
        use_symlink=not args.copy_skills,
    )
    report = run_install(plan, dry_run=args.dry_run)
    payload = report.as_payload()
    payload["dry_run"] = args.dry_run

    if args.human:
        sys.stdout.write(
            f"identity:  handle={handle}  kind={kind}  contact set\n"
            f"binary:    {payload['binary']}\n"
            f"site:      {site_url}\n"
            f"dry_run:   {args.dry_run}\n\n"
        )
        for target in report.targets:
            line = f"  · {target.target.name:<22}  "
            line += f"{target.config_change or '—':<10}"
            if target.backup:
                line += f"  (backup: {target.backup.name})"
            if target.skills and target.skills.installed:
                line += f"  +skills: {', '.join(target.skills.installed)}"
            if target.skills and target.skills.errors:
                line += f"  !skill-errors: {len(target.skills.errors)}"
            if target.error:
                line += f"  ERROR: {target.error}"
            sys.stdout.write(line + "\n")
        sys.stdout.write("\n")
        if report.overall_change and not args.dry_run:
            sys.stdout.write(
                "================================================================\n"
                "  ONE MORE STEP — QUIT AND REOPEN CLAUDE DESKTOP NOW\n"
                "================================================================\n"
                "\n"
                "  Claude Desktop only reads its MCP config at startup. Until you\n"
                "  fully quit (Cmd+Q) and reopen it, the twog-agent tools will not\n"
                "  appear in your chat.\n"
                "\n"
                "  Once it's reopened, try this in any chat:\n"
                "      'List open work packets on TWOG'\n"
            )
        elif args.dry_run:
            sys.stdout.write("Dry run only; re-run without --dry-run to apply.\n")
        else:
            sys.stdout.write("Nothing changed.\n")
    else:
        _emit(payload)
    return Exit.SUCCESS


def cmd_uninstall(args: argparse.Namespace) -> int:
    from twog_agent.install import detect_clients, run_uninstall

    targets = detect_clients()
    if not targets:
        _eprint("no MCP-capable clients detected to uninstall from.")
        return Exit.GENERIC_ERROR
    report = run_uninstall(targets=targets, remove_skills=not args.keep_skills)
    if args.human:
        for target in report.targets:
            line = f"  · {target.target.name:<22}  {target.config_change or '—'}"
            if target.backup:
                line += f"  (backup: {target.backup.name})"
            if target.error:
                line += f"  ERROR: {target.error}"
            sys.stdout.write(line + "\n")
    else:
        _emit(report.as_payload())
    return Exit.SUCCESS


def cmd_doctor(args: argparse.Namespace) -> int:
    from twog_agent.install import run_doctor

    report = run_doctor()
    if args.human:
        sys.stdout.write(
            f"binary:           {report['binary']}\n"
            f"on PATH:          {report['binary_on_path']}\n"
            f"skills source:    {report['skills_source'] or '(not bundled)'}\n\n"
            f"Detected clients:\n"
        )
        if not report["detected"]:
            sys.stdout.write("  (none)\n")
        for record in report["detected"]:
            sys.stdout.write(f"  · {record['name']}\n")
            sys.stdout.write(f"      config:  {record['config_path']}\n")
            sys.stdout.write(f"      exists:  {record['config_exists']}\n")
            if "twog_entry" in record:
                sys.stdout.write(f"      twog:    {record['twog_entry']}\n")
            if record.get("other_servers"):
                sys.stdout.write(
                    f"      others:  {', '.join(record['other_servers'])}\n"
                )
            if record.get("parse_error"):
                sys.stdout.write(f"      parse:   {record['parse_error']}\n")
    else:
        _emit(report)
    return Exit.SUCCESS


def _read_credentials_if_present():
    """Lazy-load credentials.py so the import cost is paid only when needed."""

    try:
        from twog_agent.credentials import load_credentials  # noqa: WPS433

        return load_credentials()
    except RuntimeError as exc:
        _eprint(f"credentials read error: {exc}")
        return None
    except Exception:  # noqa: BLE001
        return None


def cmd_login(args: argparse.Namespace) -> int:
    """Create or update the local credentials store.

    Idempotent: re-running ``login`` updates the file in place,
    preserves the existing ed25519 key unless --regenerate-key is set.
    """

    from twog_agent.credentials import (
        Credentials,
        credentials_path,
        generate_ed25519_keypair,
        load_credentials,
        permissions_ok,
        public_key_from_private,
        public_view,
        save_credentials,
    )
    from twog_agent.install import VALID_KINDS, looks_like_email, normalize_handle

    existing = load_credentials() if not args.regenerate_key else None

    handle_raw = args.handle or _env("TWOG_AGENT_HANDLE") or (existing.handle if existing else None)
    if not handle_raw:
        handle_raw = _prompt("Your handle (e.g. @my-agent)", default=None)
    handle = normalize_handle(handle_raw or "")
    if not handle or handle == "@":
        _eprint("handle is required")
        return Exit.INVALID_ARGS

    contact = args.contact or _env("TWOG_AGENT_CONTACT") or (existing.contact if existing else None)
    if not contact:
        contact = _prompt("Contact email", default=None)
    if not contact:
        _eprint("contact is required")
        return Exit.INVALID_ARGS
    if not looks_like_email(contact):
        _eprint(f"warning: '{contact}' doesn't look like an email; continuing anyway")

    kind = args.kind or _env("TWOG_AGENT_KIND") or (existing.kind if existing else "agent")
    if kind not in VALID_KINDS:
        _eprint(f"kind must be one of {', '.join(VALID_KINDS)} (got {kind!r})")
        return Exit.INVALID_ARGS

    name = args.name or _env("TWOG_AGENT_NAME") or (existing.name if existing else None)
    affiliation = args.affiliation or _env("TWOG_AGENT_AFFILIATION") or (
        existing.affiliation if existing else None
    )
    agent_id_value = args.agent_id or _env("TWOG_AGENT_ID") or (
        existing.agent_id if existing else None
    )
    website = args.website or _env("TWOG_AGENT_WEBSITE") or (
        existing.website if existing else None
    )

    # Keypair handling: re-use if --regenerate-key not set and we have one.
    if args.regenerate_key or not (existing and existing.ed25519_private_key_b64):
        priv_b64, pub_b64 = generate_ed25519_keypair()
    else:
        priv_b64 = existing.ed25519_private_key_b64
        # Re-derive in case the stored pub key drifted.
        pub_b64 = public_key_from_private(priv_b64)

    creds = Credentials(
        handle=handle,
        contact=contact.strip(),
        kind=kind,
        name=name,
        affiliation=affiliation,
        agent_id=agent_id_value,
        website=website,
        ed25519_private_key_b64=priv_b64,
        ed25519_public_key_b64=pub_b64,
        github_identity=existing.github_identity if existing else None,
        created_at=(existing.created_at if existing and not args.regenerate_key else None) or None,  # type: ignore[arg-type]
    )
    # If created_at was None above (new install or regenerate), the
    # dataclass default fired. Save and report.
    written = save_credentials(creds)

    if args.human:
        sys.stdout.write(
            f"wrote:       {written}\n"
            f"handle:      {creds.handle}\n"
            f"kind:        {creds.kind}\n"
            f"pubkey:      {creds.ed25519_public_key_b64}\n"
            f"perms ok:    {permissions_ok()}\n\n"
            "Done. Subsequent twog-agent commands will pick this up automatically.\n"
            "Run 'twog-agent install' to wire the credentials into your MCP clients.\n"
        )
    else:
        _emit({"credentials_path": str(written), "credentials": public_view(creds)})
    return Exit.SUCCESS


def cmd_logout(args: argparse.Namespace) -> int:
    from twog_agent.credentials import credentials_path, delete_credentials

    if args.dry_run:
        existing = credentials_path()
        if args.human:
            sys.stdout.write(
                f"would remove: {existing}{'(exists)' if existing.exists() else ' (no-op)'}\n"
            )
        else:
            _emit({"would_remove": str(existing), "exists": existing.exists()})
        return Exit.SUCCESS
    removed = delete_credentials()
    if args.human:
        sys.stdout.write("removed credentials\n" if removed else "no credentials to remove\n")
    else:
        _emit({"removed": removed, "path": str(credentials_path())})
    return Exit.SUCCESS


def cmd_dxt_build(args: argparse.Namespace) -> int:
    """Build a Claude Desktop Extension (.dxt) the user can drag-and-drop install."""

    from twog_agent.dxt import DxtBuildOptions, validate_dxt, write_dxt

    binary = args.binary or os.environ.get("TWOG_AGENT_DXT_BINARY") or "twog-agent"
    portability_warning: str | None = None
    if not args.binary and not os.environ.get("TWOG_AGENT_DXT_BINARY"):
        import shutil as _sh

        resolved = _sh.which("twog-agent")
        if resolved:
            # Detect paths that exist only on the builder's machine (venv,
            # repo checkout, sandbox). A baked-in absolute path like that
            # produces a .dxt that silently fails on any other laptop, so
            # default to the bare name and let the target machine's PATH
            # resolve it.
            looks_local = any(
                token in resolved
                for token in ("/.venv/", "/venv/", "/site-packages/", "/Documents/", "/Desktop/", "/tmp/")
            )
            if looks_local:
                portability_warning = (
                    f"resolved twog-agent at {resolved}\n"
                    f"  This path is local to your machine. Keeping the manifest as plain\n"
                    f"  'twog-agent' so the .dxt installs anywhere twog-agent is on PATH.\n"
                    f"  If the target host can't find it, rebuild with --binary <path>."
                )
            else:
                binary = resolved

    output = Path(args.output).expanduser().resolve()
    options = DxtBuildOptions(output=output, binary=binary)
    written = write_dxt(options)
    manifest = validate_dxt(written)

    if args.human:
        if portability_warning:
            sys.stdout.write("note: " + portability_warning + "\n\n")
        sys.stdout.write(
            f"wrote:    {written}\n"
            f"size:     {written.stat().st_size} bytes\n"
            f"binary:   {binary}\n"
            f"version:  {manifest['version']}\n"
            f"tools:    {len(manifest['tools'])} declared\n"
            f"prompts:  {len(manifest['user_config'])} user_config fields\n\n"
            f"Open Claude Desktop and drag {written.name} onto the window.\n"
            f"Fill the prompts when asked. The agent connects on next chat.\n"
        )
        return Exit.SUCCESS
    _emit({
        "output": str(written),
        "size_bytes": written.stat().st_size,
        "manifest": manifest,
    })
    return Exit.SUCCESS


def cmd_contributor_whoami(args: argparse.Namespace) -> int:
    handle = args.handle or _env("TWOG_AGENT_HANDLE")
    if not handle:
        _eprint("handle required: pass --handle or set TWOG_AGENT_HANDLE")
        return Exit.INVALID_ARGS

    with ProofNetworkClient(site_url=args.site_url) as client:
        try:
            payload = client.get_contributor(handle)
        except ProofNetworkError as exc:
            _eprint(str(exc))
            return _exit_for_proof_network_error(exc)
        except NetworkUnavailable as exc:
            _eprint(str(exc))
            return Exit.NETWORK_ERROR

    if args.human:
        summary = payload.get("summary") or {}
        sys.stdout.write(
            f"handle:        {payload.get('handle')}\n"
            f"tier:          {payload.get('tier')}\n"
            f"proof points:  {payload.get('proof_points')}\n"
            f"accepted:      {summary.get('accepted_capsule_count')}\n"
            f"routed:        {summary.get('routed_capsule_count')}\n"
            f"candidates:    {summary.get('candidate_count')}\n"
        )
        return Exit.SUCCESS
    _emit(payload)
    return Exit.SUCCESS


# ---------- Arg parsing -------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="twog-agent",
        description=(
            "Autonomous CLI for the TWOG Proof Network. Every command is "
            "scriptable: env-driven config, JSON-by-default output, "
            "deterministic exit codes."
        ),
    )
    parser.add_argument(
        "--site-url",
        default=None,
        help="Override TWOG_SITE_URL (default https://twog.bio).",
    )
    parser.add_argument(
        "--human",
        action="store_true",
        help="Print a terse human-readable summary instead of JSON.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # `packets list` and `packets checkout`
    packets = subparsers.add_parser("packets", help="Work packet operations")
    packets_subs = packets.add_subparsers(dest="packet_command", required=True)

    packets_list = packets_subs.add_parser("list", help="List open work packets")
    packets_list.add_argument("--status", action="append", default=[], help="Status filter; repeatable.")
    packets_list.add_argument("--type", action="append", default=[], help="packet_type filter; repeatable.")
    packets_list.add_argument(
        "--candidate-id", action="append", default=[], help="Restrict to a candidate; repeatable."
    )
    packets_list.add_argument("--limit", type=int, default=25)
    packets_list.set_defaults(handler=cmd_packets_list)

    packets_checkout = packets_subs.add_parser(
        "checkout", help="Pull the checkout payload for a packet"
    )
    packets_checkout.add_argument("packet_id", help="work_packet_id (UUID)")
    packets_checkout.add_argument(
        "--out",
        default=None,
        help="Write checkout JSON to this path instead of stdout.",
    )
    packets_checkout.set_defaults(handler=cmd_packets_checkout)

    # `capsule submit` and `capsule status`
    capsule = subparsers.add_parser("capsule", help="Proof capsule operations")
    capsule_subs = capsule.add_subparsers(dest="capsule_command", required=True)

    capsule_submit = capsule_subs.add_parser("submit", help="Submit a proof capsule")
    capsule_submit.add_argument("--file", required=True, help="Path to capsule JSON file")
    capsule_submit.add_argument(
        "--packet", default=None, help="work_packet_id to bind the capsule to (overrides the file)"
    )
    capsule_submit.add_argument(
        "--candidate-id",
        default=None,
        help="candidate_id (overrides the file; otherwise resolved from checkout/file)",
    )
    capsule_submit.add_argument(
        "--checkout",
        default=None,
        help="Optional path to a checkout JSON file used to backfill missing fields.",
    )
    capsule_submit.set_defaults(handler=cmd_capsule_submit)

    capsule_status = capsule_subs.add_parser(
        "status", help="Read a capsule's current status (optionally poll)"
    )
    capsule_status.add_argument("capsule_id", help="proof_capsule_id (UUID)")
    capsule_status.add_argument(
        "--wait",
        action="store_true",
        help="Poll until the capsule reaches a terminal status (or --timeout).",
    )
    capsule_status.add_argument(
        "--timeout", type=float, default=600.0, help="Seconds to wait when --wait is set."
    )
    capsule_status.add_argument(
        "--poll-interval", type=float, default=10.0, help="Seconds between polls."
    )
    capsule_status.set_defaults(handler=cmd_capsule_status)

    # `do` — full pipeline
    do = subparsers.add_parser(
        "do",
        help="Checkout a packet, submit a capsule, optionally wait for a verdict",
    )
    do.add_argument("--packet", required=True, help="work_packet_id (UUID)")
    do.add_argument("--capsule", required=True, help="Path to capsule JSON file")
    do.add_argument(
        "--candidate-id",
        default=None,
        help="candidate_id override; otherwise resolved from checkout.",
    )
    do.add_argument("--wait", action="store_true", help="Poll for a terminal verdict.")
    do.add_argument("--timeout", type=float, default=600.0)
    do.add_argument("--poll-interval", type=float, default=10.0)
    do.set_defaults(handler=cmd_do)

    # `mcp` — stdio MCP server entrypoint
    mcp = subparsers.add_parser(
        "mcp",
        help="Run the stdio Model Context Protocol server (for Claude Desktop, Codex, etc.)",
    )
    mcp.set_defaults(handler=cmd_mcp)

    # `install` — one-command client setup
    install = subparsers.add_parser(
        "install",
        help=(
            "Auto-configure Claude Desktop / Claude Code / Codex / Cursor to use "
            "the TWOG MCP server. Writes mcpServers.twog and symlinks the agent "
            "skills. Run after pipx install twog-agent."
        ),
    )
    install.add_argument("--handle", default=None, help="Contributor handle, e.g. @my-agent.")
    install.add_argument("--contact", default=None, help="Contact email.")
    install.add_argument("--kind", default=None, help="human|agent|team|lab|company (default: agent).")
    install.add_argument("--name", default=None, help="Optional display name.")
    install.add_argument(
        "--skill",
        action="append",
        default=[],
        help=(
            "Skill bundle to install; repeatable. Defaults to all 5 (generic + 4 souls). "
            "Pass --skill twog-agent for the generic skill only."
        ),
    )
    install.add_argument(
        "--copy-skills",
        action="store_true",
        help="Copy skill bundles instead of symlinking (better for pipx-installed setups).",
    )
    install.add_argument(
        "--skills-only",
        action="store_true",
        help="Only install skills; don't write any client JSON config.",
    )
    install.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would happen without writing anything.",
    )
    install.set_defaults(handler=cmd_install)

    uninstall = subparsers.add_parser(
        "uninstall",
        help="Remove the TWOG MCP entry from detected client configs (and skills).",
    )
    uninstall.add_argument(
        "--keep-skills",
        action="store_true",
        help="Leave installed skill bundles in place.",
    )
    uninstall.set_defaults(handler=cmd_uninstall)

    doctor = subparsers.add_parser(
        "doctor",
        help="Report what MCP clients we can see and whether twog is wired up.",
    )
    doctor.set_defaults(handler=cmd_doctor)

    login = subparsers.add_parser(
        "login",
        help=(
            "Create or update local credentials (~/.twog-agent/credentials.json). "
            "Stores handle, contact, kind, and an ed25519 keypair so subsequent "
            "commands skip the prompts and signed capsules are automatic."
        ),
    )
    login.add_argument("--handle", default=None)
    login.add_argument("--contact", default=None)
    login.add_argument("--kind", default=None, help="human|agent|team|lab|company")
    login.add_argument("--name", default=None, help="Optional display name.")
    login.add_argument("--affiliation", default=None)
    login.add_argument("--agent-id", default=None, help="Stable agent identifier (for agent kind).")
    login.add_argument("--website", default=None)
    login.add_argument(
        "--regenerate-key",
        action="store_true",
        help="Force a fresh ed25519 keypair (rotates the soul; previous signatures still verify).",
    )
    login.set_defaults(handler=cmd_login)

    logout = subparsers.add_parser(
        "logout",
        help="Remove the local credentials file (forces handle re-entry on next command).",
    )
    logout.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be removed without removing anything.",
    )
    logout.set_defaults(handler=cmd_logout)

    dxt_build = subparsers.add_parser(
        "dxt-build",
        help=(
            "Build a Claude Desktop Extension (.dxt) the user can drag-and-drop install. "
            "Lets non-technical contributors connect without editing any JSON."
        ),
    )
    dxt_build.add_argument(
        "--output",
        default="./twog.dxt",
        help="Path to write the .dxt file (default ./twog.dxt).",
    )
    dxt_build.add_argument(
        "--binary",
        default=None,
        help=(
            "Override the twog-agent binary path baked into the manifest. "
            "Default: resolve via shutil.which on the build machine."
        ),
    )
    dxt_build.set_defaults(handler=cmd_dxt_build)

    # `contributor whoami`
    contributor = subparsers.add_parser(
        "contributor", help="Contributor profile (proof portfolio, tier, points)"
    )
    contributor_subs = contributor.add_subparsers(dest="contributor_command", required=True)
    whoami = contributor_subs.add_parser("whoami", help="Render the contributor profile")
    whoami.add_argument("--handle", default=None, help="Override TWOG_AGENT_HANDLE")
    whoami.set_defaults(handler=cmd_contributor_whoami)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return Exit.INVALID_ARGS
    # Default the env-derived site URL if not overridden, so the early
    # client construction sees a consistent value.
    if not args.site_url:
        args.site_url = resolve_site_url()
    try:
        return int(handler(args))
    except KeyboardInterrupt:
        return Exit.GENERIC_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
