"""JSON REST API for the twog web app (Contribute + Operate surfaces).

This is the boundary the Next.js front end calls. It deliberately does the MINIMUM and delegates:
authentication is resolved upstream (a WorkOS session token → a stable auth_subject → a twog
``principal`` via ``service.resolve_collaborator_by_auth_subject``); AUTHORIZATION is the service's
already-hardened ``_authorize`` (scopes) + the confound/provenance/human gates — this layer never
re-implements them, it just maps identity in and exceptions to HTTP status out.

``dispatch()`` is pure over (service, method, path, principal, body) so it is fully unit-testable
without HTTP. ``run_api_server`` is a thin stdlib wrapper that resolves the bearer token to a
principal and calls dispatch. Reads require an ACTIVE collaborator (the app has no public surface in
v1); write/operator actions are enforced by the service when handed ``principal.principal`` as actor.
"""

from __future__ import annotations

from typing import Any, Callable
from uuid import UUID

from .contracts import (
    CollaboratorRecord,
    ProofCapsuleLibraryRequest,
    ProofCapsuleSubmitRequest,
    PublicCandidateLibraryRequest,
)
from .service import CollaboratorAccessError, HSAResearchService, WorkspaceLeaseError
from . import web_presenters as _present


class ApiError(Exception):
    """An HTTP-mappable API error (status + client-safe message)."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


def _require_principal(principal: CollaboratorRecord | None) -> CollaboratorRecord:
    if principal is None:
        raise ApiError(401, "authentication required")
    return principal


def _require_active(principal: CollaboratorRecord | None) -> CollaboratorRecord:
    p = _require_principal(principal)
    if p.status != "active":
        raise ApiError(403, f"collaborator is '{p.status}', not active")
    return p


def _require_operator(principal: CollaboratorRecord | None) -> CollaboratorRecord:
    p = _require_active(principal)
    if p.role != "operator":
        raise ApiError(403, "operator role required")
    return p


def _dump(obj: Any) -> Any:
    if hasattr(obj, "model_dump"):
        return obj.model_dump(mode="json")
    if isinstance(obj, list):
        return [_dump(x) for x in obj]
    return obj


def dispatch(
    service: HSAResearchService,
    *,
    method: str,
    path: str,
    principal: CollaboratorRecord | None = None,
    body: dict[str, Any] | None = None,
) -> tuple[int, Any]:
    """Route a request to the service and return (http_status, json_payload). Raises nothing — every
    failure becomes an (status, {"error": ...}) tuple. ``principal`` is the already-resolved actor."""
    body = body or {}
    seg = [s for s in path.strip("/").split("/") if s != ""]
    actor = (principal.principal if principal else None)

    try:
        # ---- health check (no DB) — for the platform load balancer / uptime probe ----------------
        if method == "GET" and seg == ["healthz"]:
            return 200, {"status": "ok"}

        # ---- PUBLIC read surface (no auth) — powers the public STATE / EVIDENCE / RUNS site -----
        # These return PRESENTED (display-shaped) data via web_presenters, never raw records. The
        # auth-gated operator/contribute routes below return raw records to authenticated principals.
        if method == "GET" and seg[:1] == ["public"]:
            pub = seg[1:]
            if pub == ["state"]:
                cands = service.list_public_candidates(PublicCandidateLibraryRequest(limit=500)).candidates
                caps = service.list_proof_capsules(ProofCapsuleLibraryRequest(limit=500)).capsules
                mans = service.list_run_manifests(manifest_type="falsification_campaign", limit=100)
                return 200, _present.present_engine_state(cands, caps, mans)
            if pub == ["capsules"]:
                caps = service.list_proof_capsules(
                    ProofCapsuleLibraryRequest(statuses=list(_present.PUBLIC_CAPSULE_STATUSES), limit=200)
                ).capsules
                return 200, [_present.present_capsule(c) for c in caps]
            if len(pub) == 2 and pub[0] == "capsules":
                caps = service.list_proof_capsules(ProofCapsuleLibraryRequest(capsule_id=UUID(pub[1]))).capsules
                if not caps:
                    raise ApiError(404, "capsule not found")
                return 200, _present.present_capsule(caps[0])
            if len(pub) == 3 and pub[0] == "capsules" and pub[2] == "provenance":
                bundle = service.public_capsule_provenance(UUID(pub[1]))
                if bundle is None:
                    raise ApiError(404, "capsule not found")
                return 200, bundle  # already display-shaped + independently verifiable
            if pub == ["activity"]:
                # the live engine feed — merged reverse-chron, honest idle/online from real job state
                feed = _present.present_activity_feed(
                    agent_runs=service.list_agent_runs(limit=25),
                    compute_jobs=service.list_compute_jobs(limit=25),
                    capsules=service.list_proof_capsules(
                        ProofCapsuleLibraryRequest(statuses=list(_present.PUBLIC_CAPSULE_STATUSES), limit=25)
                    ).capsules,
                    manifests=service.list_run_manifests(manifest_type="falsification_campaign", limit=10),
                )
                return 200, feed
            if pub == ["campaigns"]:
                mans = service.list_run_manifests(manifest_type="falsification_campaign", limit=100)
                return 200, [_present.present_manifest(m) for m in mans]
            if len(pub) == 2 and pub[0] == "campaigns":
                rec = service.get_run_manifest(UUID(pub[1]))
                if rec is None:
                    raise ApiError(404, "campaign not found")
                return 200, _present.present_manifest(rec)
            if pub == ["candidates"]:
                cands = service.list_public_candidates(PublicCandidateLibraryRequest(limit=500)).candidates
                return 200, [_present.present_candidate(c) for c in cands]
            if len(pub) == 2 and pub[0] == "candidates":
                rec = service.get_public_candidate(pub[1])
                if rec is None:
                    raise ApiError(404, "candidate not found")
                return 200, _present.present_candidate(rec)
            if len(pub) == 3 and pub[0] == "candidates" and pub[2] == "rubric":
                rec = service.get_public_candidate(pub[1])
                if rec is None:
                    raise ApiError(404, "candidate not found")
                rubric = service.get_public_candidate_rubric(pub[1])
                if rubric is None:
                    # candidate exists but carries no moonshot rubric (e.g. published non-moonshot-grade)
                    raise ApiError(404, "no moonshot rubric for this candidate")
                return 200, _present.present_rubric(rubric)
            raise ApiError(404, f"no public route for /{'/'.join(seg)}")

        # ---- identity --------------------------------------------------------------------------
        if method == "GET" and seg == ["me"]:
            p = _require_principal(principal)
            return 200, _dump(p)

        # ---- collaborator lifecycle ------------------------------------------------------------
        if method == "POST" and seg == ["collaborators", "apply"]:
            # An authenticated-but-not-yet-collaborator user applies (principal may be None).
            rec = service.request_collaborator_access(
                principal=body["principal"], name=body["name"], public_key=body["public_key"],
                contact=body.get("contact"), auth_subject=body.get("auth_subject"), note=body.get("note"),
            )
            return 201, _dump(rec)
        if method == "GET" and seg == ["collaborators"]:
            _require_operator(principal)
            return 200, _dump(service.list_collaborators(status=(body.get("status") if body else None)))
        if method == "POST" and len(seg) == 3 and seg[0] == "collaborators" and seg[2] in {"approve", "revoke"}:
            _require_operator(principal)
            cid = UUID(seg[1])
            if seg[2] == "approve":
                rec = service.approve_collaborator(cid, approved_by=actor, scopes=body.get("scopes"),
                                                   role=body.get("role", "collaborator"))
            else:
                rec = service.revoke_collaborator(cid)
            if rec is None:
                raise ApiError(404, "collaborator not found")
            return 200, _dump(rec)

        # ---- candidates / campaigns / target library (reads; auth-gated) ------------------------
        if method == "GET" and seg == ["candidates"]:
            _require_active(principal)
            return 200, _dump(service.list_public_candidates(PublicCandidateLibraryRequest(limit=200)).candidates)
        if method == "GET" and len(seg) == 2 and seg[0] == "candidates":
            _require_active(principal)
            rec = service.get_public_candidate(seg[1])
            if rec is None:
                raise ApiError(404, "candidate not found")
            return 200, _dump(rec)
        if method == "GET" and seg == ["campaigns"]:
            _require_active(principal)
            return 200, _dump(service.list_run_manifests(manifest_type="falsification_campaign", limit=100))
        if method == "GET" and len(seg) == 2 and seg[0] == "campaigns":
            _require_active(principal)
            rec = service.get_run_manifest(UUID(seg[1]))
            if rec is None:
                raise ApiError(404, "campaign not found")
            return 200, _dump(rec)
        if method == "GET" and seg == ["target-library"]:
            _require_active(principal)
            from . import target_library

            lib = target_library.load_target_library()
            return 200, _dump(list((lib.get("entries") or {}).values()))

        # ---- sandbox + contributions -----------------------------------------------------------
        if method == "POST" and seg == ["sandbox", "open"]:
            _require_active(principal)
            bundle = service.open_collaborator_sandbox(
                actor, UUID(body["workspace_id"]),
                validation_type=body.get("validation_type", "docking"),
                candidate_id=body.get("candidate_id"),
            )
            if bundle is None:
                raise ApiError(404, "workspace not found")
            return 200, bundle
        if method == "POST" and seg == ["contributions"]:
            _require_active(principal)
            kind = body.get("kind")
            if kind == "candidate_proposal":
                rec = service.submit_candidate_proposal(
                    actor, candidate_id=body["candidate_id"], title=body["title"],
                    rationale=body.get("rationale", ""), targets=body.get("targets"),
                    candidate_therapies=body.get("candidate_therapies"),
                    evidence_refs=body.get("evidence_refs"), biomarkers=body.get("biomarkers"),
                )
                return 201, _dump(rec)
            if kind == "evidence_capsule":
                request = ProofCapsuleSubmitRequest(**body["capsule"])
                result = service.submit_external_proof_capsule(request)
                return (201 if result.accepted else 422), _dump(result)
            raise ApiError(400, f"unknown contribution kind: {kind!r}")
        if method == "GET" and seg == ["contributions", "mine"]:
            p = _require_active(principal)
            caps = service.list_proof_capsules(ProofCapsuleLibraryRequest(limit=200)).capsules
            return 200, _dump([c for c in caps if c.submitted_by == p.principal])

        # ---- operator write gate (capsules) ----------------------------------------------------
        if method == "GET" and seg == ["capsules"]:
            _require_operator(principal)
            caps = service.list_proof_capsules(ProofCapsuleLibraryRequest(status="submitted", limit=200)).capsules
            return 200, _dump(caps)
        if method == "POST" and len(seg) == 3 and seg[0] == "capsules" and seg[2] in {"accept", "promote"}:
            _require_principal(principal)  # the service enforces the operator scope precisely
            cid = UUID(seg[1])
            if seg[2] == "accept":
                rec = service.accept_proof_capsule(cid, reviewer=actor, note=body.get("note"))
            else:
                rec = service.promote_proof_capsule_to_candidate(cid, reviewer=actor)
            if rec is None:
                raise ApiError(404, "capsule not found")
            return 200, _dump(rec)

        # ---- candidate proposals (operator review queue) ---------------------------------------
        if method == "GET" and seg == ["proposals"]:
            _require_operator(principal)
            return 200, _dump(service.list_candidate_proposals())
        if method == "POST" and len(seg) == 3 and seg[0] == "proposals" and seg[2] == "decide":
            _require_principal(principal)  # service enforces promote_candidate
            rec = service.decide_candidate_proposal(
                seg[1], approved_by=actor, approve=bool(body.get("approve")), note=body.get("note"),
            )
            if rec is None:
                raise ApiError(404, "proposal not found")
            return 200, _dump(rec)

        raise ApiError(404, f"no route for {method} /{'/'.join(seg)}")

    except ApiError as exc:
        return exc.status, {"error": exc.message}
    except CollaboratorAccessError as exc:
        return 403, {"error": str(exc)}
    except WorkspaceLeaseError as exc:
        return 409, {"error": str(exc)}
    except (KeyError, ValueError, TypeError) as exc:
        return 400, {"error": f"bad request: {exc}"}


def resolve_principal_from_token(
    service: HSAResearchService,
    token: str | None,
    *,
    verify_token: Callable[[str], str | None],
) -> CollaboratorRecord | None:
    """Resolve a bearer token to a twog principal: verify_token (the WorkOS seam) yields the stable
    auth_subject (WorkOS user id), which maps to a collaborator. None token / unverifiable / unmapped
    → None (the app then treats the caller as unauthenticated). The WorkOS verification is injected so
    this stays testable and the SDK is only needed where the server is actually run."""
    if not token:
        return None
    try:
        subject = verify_token(token)
    except Exception:
        return None
    return service.resolve_collaborator_by_auth_subject(subject)


def run_api_server(
    *,
    service_factory: Callable[[], HSAResearchService],
    verify_token: Callable[[str], str | None],
    host: str = "127.0.0.1",
    port: int = 8000,
    allow_origin: str = "*",
) -> None:
    """Thin stdlib HTTP wrapper around dispatch() — the servable form of the API. Resolves the bearer
    token to a principal (the injected WorkOS verifier → auth_subject → collaborator) and delegates to
    dispatch. CORS-enabled for the Next.js origin. The verifier is injected so the WorkOS SDK is only
    needed at the deployment that actually runs this (tests exercise dispatch/resolution directly)."""
    import json
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        def _principal(self) -> CollaboratorRecord | None:
            auth = self.headers.get("Authorization", "")
            token = auth[7:].strip() if auth.lower().startswith("bearer ") else None
            return resolve_principal_from_token(service_factory(), token, verify_token=verify_token)

        def _send(self, status: int, payload: Any) -> None:
            data = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", allow_origin)
            self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.end_headers()
            self.body_bytes = self.wfile.write(data)

        def _handle(self, method: str) -> None:
            length = int(self.headers.get("Content-Length", 0) or 0)
            raw = self.rfile.read(length) if length else b""
            try:
                body = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                return self._send(400, {"error": "invalid JSON body"})
            path = self.path.split("?", 1)[0]
            try:
                status, payload = dispatch(
                    service_factory(), method=method, path=path, principal=self._principal(), body=body
                )
            except Exception as exc:  # never leak a stack trace to the client
                return self._send(500, {"error": f"internal error: {type(exc).__name__}"})
            self._send(status, payload)

        def do_OPTIONS(self) -> None:  # CORS preflight
            self._send(204, {})

        def do_GET(self) -> None:
            self._handle("GET")

        def do_POST(self) -> None:
            self._handle("POST")

        def log_message(self, *args: Any) -> None:  # quiet by default
            pass

    # Threaded so the front-end's periodic polls (e.g. the live ledger) don't serialize behind one
    # another and reset the connection — each request gets its own short-lived per-request service/conn.
    ThreadingHTTPServer((host, port), Handler).serve_forever()
