"""HTTP client for the TWOG Proof Network.

This is intentionally tiny: every method is a one-shot request that
returns parsed JSON or raises a typed exception. The CLI layer maps those
exceptions to deterministic exit codes; the client itself stays
transport-only so tests can mock at the HTTPX level.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import httpx


DEFAULT_SITE_URL = "https://twog.bio"
USER_AGENT = "twog-agent/0.1"


@dataclass
class ProofNetworkError(Exception):
    """A typed HTTP failure from the Proof Network API.

    ``code`` is the server's ``error`` slug (e.g. ``"work_packet_not_found"``).
    ``status`` is the HTTP status. ``details`` is the server's ``details``
    array when present; otherwise empty.
    """

    code: str
    status: int
    message: str = ""
    details: list[str] | None = None

    def __str__(self) -> str:
        head = f"{self.code} ({self.status})"
        if self.message:
            head = f"{head}: {self.message}"
        if self.details:
            head = f"{head}\n  - " + "\n  - ".join(self.details)
        return head


@dataclass
class NetworkUnavailable(Exception):
    """Transport-level failure (DNS, TLS, connection refused, 5xx)."""

    reason: str

    def __str__(self) -> str:
        return self.reason


def resolve_site_url() -> str:
    return (os.environ.get("TWOG_SITE_URL") or DEFAULT_SITE_URL).rstrip("/")


class ProofNetworkClient:
    """Minimal HTTP client over the TWOG Proof Network."""

    def __init__(
        self,
        *,
        site_url: str | None = None,
        timeout: float = 30.0,
        client: httpx.Client | None = None,
    ) -> None:
        self.site_url = (site_url or resolve_site_url()).rstrip("/")
        self.timeout = timeout
        self._client = client
        self._owns_client = client is None

    def __enter__(self) -> ProofNetworkClient:
        if self._client is None:
            self._client = httpx.Client(
                timeout=self.timeout,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            )
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        if self._client is not None and self._owns_client:
            self._client.close()
            self._client = None

    # -- HTTP helpers --------------------------------------------------

    def _full_url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return f"{self.site_url}{path if path.startswith('/') else '/' + path}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
        params: list[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        if self._client is None:
            # Allow ad-hoc use without a context manager.
            with self as bound:
                return bound._request(method, path, json_body=json_body, params=params)

        url = self._full_url(path)
        if params:
            url = f"{url}?{urlencode(params)}"
        try:
            response = self._client.request(method, url, json=json_body)
        except httpx.HTTPError as exc:
            raise NetworkUnavailable(f"{method} {url}: {exc}") from exc

        # Both error and success responses are JSON for our API. Status 5xx
        # without a body is mapped to NetworkUnavailable so the CLI exits
        # with the retryable network code.
        try:
            payload = response.json()
        except ValueError:
            if response.status_code >= 500:
                raise NetworkUnavailable(
                    f"{method} {url}: {response.status_code} (non-JSON body)"
                )
            payload = {}

        if response.is_success:
            return payload

        if response.status_code >= 500:
            raise NetworkUnavailable(
                f"{method} {url}: {response.status_code} {payload.get('error') or ''}".strip()
            )

        raise ProofNetworkError(
            code=str(payload.get("error") or f"http_{response.status_code}"),
            status=response.status_code,
            message=str(payload.get("message") or ""),
            details=list(payload.get("details") or []),
        )

    # -- Work packets --------------------------------------------------

    def list_work_packets(
        self,
        *,
        statuses: list[str] | None = None,
        packet_types: list[str] | None = None,
        candidate_ids: list[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        params: list[tuple[str, str]] = []
        for status in statuses or []:
            params.append(("status", status))
        for packet_type in packet_types or []:
            params.append(("packet_type", packet_type))
        for candidate_id in candidate_ids or []:
            params.append(("candidate_id", candidate_id))
        if limit is not None:
            params.append(("limit", str(limit)))
        return self._request("GET", "/api/work-packets", params=params)

    def get_work_packet(self, work_packet_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/work-packets/{work_packet_id}")

    def checkout_work_packet(self, work_packet_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/work-packets/{work_packet_id}/checkout")

    # -- Proof capsules ------------------------------------------------

    def submit_proof_capsule(self, body: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/api/proof-capsules", json_body=body)

    def get_proof_capsule(self, proof_capsule_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/proof-capsules/{proof_capsule_id}")

    # -- Contributors --------------------------------------------------

    def get_contributor(self, handle: str) -> dict[str, Any]:
        return self._request("GET", f"/api/contributors/{handle}")

    # -- Network feed --------------------------------------------------

    def get_network_feed(self) -> dict[str, Any]:
        return self._request("GET", "/api/network/feed")
