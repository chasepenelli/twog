"""Compute runner seam for approval-first validation jobs.

The RunPod execution provider was REMOVED — it never worked reliably (the worker
failed in ligand preparation; see docs/DAGSTER_REVIEW.md §4 and ROADMAP.md P3).
A replacement will be built from scratch or use a different tool.

What remains here is the provider-agnostic *seam*: a ComputeRunner protocol plus a
registry. The expert gate, validation queue, proof-capsule model, and compute-job
ledger are unchanged and provider-independent. Until a provider is registered,
get_compute_runner() raises ComputeRunnerConfigError, so no job can silently
execute — submission is blocked safely, not run against a dead endpoint.

To add a provider (ROADMAP P3): implement ComputeRunner and call
register_compute_runner("<kind>", factory).
"""

from __future__ import annotations

from typing import Any, Callable, Protocol, runtime_checkable

from .contracts import ComputeJobRecord


class ComputeRunnerConfigError(RuntimeError):
    """Raised when no compute provider is configured (or a provider lacks config)."""


class ComputeRunnerRequestError(RuntimeError):
    """Raised when a registered compute provider's request fails."""


@runtime_checkable
class ComputeRunner(Protocol):
    """Provider seam. Each method returns a dict carrying at least a ``status`` key.

    submit(record) -> {status, external_run_id, runpod_job_id?, output_payload, metadata}
    poll(record)   -> {status, output_payload, last_error, metadata}
    cancel(record) -> {status, output_payload, metadata}
    """

    def submit(self, record: ComputeJobRecord) -> dict[str, Any]: ...
    def poll(self, record: ComputeJobRecord) -> dict[str, Any]: ...
    def cancel(self, record: ComputeJobRecord) -> dict[str, Any]: ...


ComputeRunnerFactory = Callable[[], ComputeRunner]

_PROVIDERS: dict[str, ComputeRunnerFactory] = {}


def register_compute_runner(runner_kind: str, factory: ComputeRunnerFactory) -> None:
    """Register a compute provider factory for a given ``runner_kind``."""
    _PROVIDERS[runner_kind] = factory


def available_compute_runners() -> tuple[str, ...]:
    """Return the runner_kinds with a registered provider (empty until P3)."""
    return tuple(sorted(_PROVIDERS))


def get_compute_runner(record: ComputeJobRecord) -> ComputeRunner:
    """Resolve the provider for ``record.runner_kind`` or raise if none is registered."""
    factory = _PROVIDERS.get(record.runner_kind)
    if factory is None:
        raise ComputeRunnerConfigError(
            f"compute_provider_not_configured: no '{record.runner_kind}' compute provider is "
            "registered. The RunPod provider was removed (non-functional); implement a "
            "ComputeRunner and register it via register_compute_runner() — see ROADMAP.md P3."
        )
    return factory()


class MockComputeRunner:
    """Deterministic in-process provider for proving the Phase-2 loop without any GPU.

    Selected explicitly via runner_kind="mock". submit() returns a 'completed' result whose
    output_payload conforms to the compute-artifact shape the capsule builder reads. It does NOT
    run any real computation — never use it for scientific results."""

    def submit(self, record: ComputeJobRecord) -> dict[str, Any]:
        run_id = f"mock:{record.compute_job_id}"
        return {
            "status": "completed",
            "external_run_id": run_id,
            "runpod_job_id": run_id,
            "output_payload": {
                "provider": "mock",
                "findings": f"Mock compute completed for '{record.title}' ({record.validation_type or 'compute'}).",
                "limitations": ["Mock provider output; not scientifically meaningful."],
                "source_refs": [],
                "metrics": {"mock": True},
                # Honest mock: no real computation, so the directional signal is neutral/low-confidence.
                "signal": "neutral",
                "confidence": 0.0,
            },
            "metadata": {"provider": "mock"},
        }

    def poll(self, record: ComputeJobRecord) -> dict[str, Any]:
        return {
            "status": "completed",
            "output_payload": record.output_payload or {"provider": "mock"},
            "last_error": None,
            "metadata": {"provider": "mock"},
        }

    def cancel(self, record: ComputeJobRecord) -> dict[str, Any]:
        return {"status": "cancelled", "output_payload": {}, "metadata": {"provider": "mock"}}


register_compute_runner("mock", lambda: MockComputeRunner())
