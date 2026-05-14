"""Compute runner adapters for approval-first validation jobs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
import os
import urllib.error
import urllib.request
from typing import Any

from .contracts import ComputeJobRecord


RUNPOD_DEFAULT_BASE_URL = "https://api.runpod.ai"


class ComputeRunnerConfigError(RuntimeError):
    """Raised when a compute runner lacks required environment configuration."""


class ComputeRunnerRequestError(RuntimeError):
    """Raised when a compute runner API request fails."""


@dataclass(frozen=True)
class RunPodComputeRunner:
    api_key: str
    endpoint_id: str
    base_url: str = RUNPOD_DEFAULT_BASE_URL
    timeout_seconds: int = 60
    auth_scheme: str = "Bearer"

    @classmethod
    def from_env(cls) -> "RunPodComputeRunner":
        api_key = os.getenv("RUNPOD_API_KEY", "").strip()
        endpoint_id = os.getenv("HSA_RUNPOD_ENDPOINT_ID", "").strip()
        base_url = os.getenv("HSA_RUNPOD_BASE_URL", RUNPOD_DEFAULT_BASE_URL).strip() or RUNPOD_DEFAULT_BASE_URL
        timeout_seconds = int(os.getenv("HSA_RUNPOD_TIMEOUT_SECONDS", "60"))
        auth_scheme = os.getenv("HSA_RUNPOD_AUTH_SCHEME", "Bearer").strip()
        if not api_key:
            raise ComputeRunnerConfigError("RUNPOD_API_KEY is required for live RunPod submission.")
        if not endpoint_id:
            raise ComputeRunnerConfigError("HSA_RUNPOD_ENDPOINT_ID is required for live RunPod submission.")
        return cls(
            api_key=api_key,
            endpoint_id=endpoint_id,
            base_url=base_url.rstrip("/"),
            timeout_seconds=timeout_seconds,
            auth_scheme=auth_scheme,
        )

    def submit(self, record: ComputeJobRecord) -> dict[str, Any]:
        payload = {"input": _runpod_worker_input(record)}
        response = self._request_json("POST", f"/v2/{self.endpoint_id}/run", payload)
        job_id = str(response.get("id") or response.get("job_id") or response.get("runpod_job_id") or "").strip()
        if not job_id:
            raise ComputeRunnerRequestError("RunPod submission response did not include a job id.")
        return {
            "runpod_job_id": job_id,
            "external_run_id": job_id,
            "status": _runpod_response_to_compute_status(response),
            "output_payload": {"runpod_submit_response": response},
            "metadata": {
                "runpod_endpoint_id": self.endpoint_id,
                "runpod_base_url": self.base_url,
                "runpod_submitted_at": datetime.now(UTC).isoformat(),
            },
        }

    def poll(self, record: ComputeJobRecord) -> dict[str, Any]:
        job_id = record.runpod_job_id or record.external_run_id
        if not job_id:
            raise ComputeRunnerConfigError("Compute job has no RunPod job id to poll.")
        response = self._request_json("GET", f"/v2/{self.endpoint_id}/status/{job_id}")
        status = _runpod_response_to_compute_status(response)
        return {
            "status": status,
            "output_payload": {"runpod_status_response": response},
            "last_error": _runpod_error_message(response) if status in {"failed", "cancelled"} else None,
            "metadata": {
                "runpod_endpoint_id": self.endpoint_id,
                "runpod_base_url": self.base_url,
                "runpod_polled_at": datetime.now(UTC).isoformat(),
            },
        }

    def cancel(self, record: ComputeJobRecord) -> dict[str, Any]:
        job_id = record.runpod_job_id or record.external_run_id
        if not job_id:
            raise ComputeRunnerConfigError("Compute job has no RunPod job id to cancel.")
        response = self._request_json("POST", f"/v2/{self.endpoint_id}/cancel/{job_id}")
        return {
            "status": "cancelled",
            "output_payload": {"runpod_cancel_response": response},
            "metadata": {
                "runpod_endpoint_id": self.endpoint_id,
                "runpod_base_url": self.base_url,
                "runpod_cancelled_at": datetime.now(UTC).isoformat(),
            },
        }

    def _request_json(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            method=method,
            headers={
                "Authorization": self._authorization_header(),
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise ComputeRunnerRequestError(f"RunPod HTTP {exc.code}: {body[:1000]}") from exc
        except urllib.error.URLError as exc:
            raise ComputeRunnerRequestError(f"RunPod request failed: {exc.reason}") from exc
        try:
            decoded = json.loads(body) if body else {}
        except json.JSONDecodeError as exc:
            raise ComputeRunnerRequestError("RunPod response was not valid JSON.") from exc
        if not isinstance(decoded, dict):
            raise ComputeRunnerRequestError("RunPod response JSON was not an object.")
        return decoded

    def _authorization_header(self) -> str:
        if not self.auth_scheme:
            return self.api_key
        if self.api_key.lower().startswith(self.auth_scheme.lower() + " "):
            return self.api_key
        return f"{self.auth_scheme} {self.api_key}"


def _runpod_status_to_compute_status(status: str) -> str:
    normalized = status.strip().upper()
    if normalized in {"IN_QUEUE", "PENDING", "QUEUED"}:
        return "submitted"
    if normalized in {"IN_PROGRESS", "RUNNING", "PROCESSING"}:
        return "running"
    if normalized in {"COMPLETED", "SUCCEEDED", "SUCCESS"}:
        return "completed"
    if normalized in {"CANCELLED", "CANCELED"}:
        return "cancelled"
    if normalized in {"FAILED", "TIMED_OUT", "ERROR"}:
        return "failed"
    return "submitted"


def _runpod_response_to_compute_status(response: dict[str, Any]) -> str:
    status = _runpod_status_to_compute_status(str(response.get("status") or "UNKNOWN"))
    output = response.get("output")
    if status == "completed" and isinstance(output, dict):
        output_status = str(output.get("status") or "").strip().lower()
        if output_status in {"failed", "error"}:
            return "failed"
        if output_status in {"cancelled", "canceled"}:
            return "cancelled"
    return status


def _runpod_worker_input(record: ComputeJobRecord) -> dict[str, Any]:
    worker_input = {
        "compute_job_id": str(record.compute_job_id),
        "queue_item_id": str(record.queue_item_id) if record.queue_item_id else None,
        "validation_type": record.validation_type,
        "title": record.title,
        "objective": record.objective,
        "payload": record.input_payload,
        "expected_outputs": record.expected_outputs,
        "metadata": record.metadata,
    }
    validation_request = record.input_payload.get("validation_request")
    explicit_input: dict[str, Any] = {}
    if isinstance(validation_request, dict):
        request_metadata = validation_request.get("metadata")
        if isinstance(request_metadata, dict) and isinstance(request_metadata.get("runpod_input"), dict):
            explicit_input.update(request_metadata["runpod_input"])
    if isinstance(record.metadata.get("runpod_input"), dict):
        explicit_input.update(record.metadata["runpod_input"])
    if isinstance(record.input_payload.get("runpod_input"), dict):
        explicit_input.update(record.input_payload["runpod_input"])
    worker_input.update(explicit_input)
    return worker_input


def _runpod_error_message(response: dict[str, Any]) -> str | None:
    error = response.get("error") or response.get("errorMessage") or response.get("message")
    if error:
        return str(error)[:2000]
    output = response.get("output")
    if not isinstance(output, dict):
        return None
    output_error = output.get("error") or output.get("errorMessage") or output.get("message") or output.get("last_error")
    if output_error:
        return str(output_error)[:2000]
    errors = output.get("errors")
    if isinstance(errors, list) and errors:
        first = errors[0]
        if isinstance(first, dict):
            parts = [
                str(first.get("stage") or "").strip(),
                str(first.get("message") or "").strip(),
                str(first.get("stderr_tail") or "").strip(),
            ]
            message = ": ".join(part for part in parts if part)
            return message[:2000] if message else json.dumps(first, sort_keys=True)[:2000]
        return str(first)[:2000]
    if str(output.get("status") or "").strip().lower() in {"failed", "error"}:
        return "RunPod worker returned failed status without an error message."
    return None
