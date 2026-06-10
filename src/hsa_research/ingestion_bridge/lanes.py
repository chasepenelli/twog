"""Compute lane registry — the pluggable lane pattern (ROADMAP P3 / PHASE3_PLAN 3a).

A LaneSpec describes ONE atomic, optionally gated computation: its validation_type, an optional
gate (the SAME expert-gate machinery — the lane only supplies the gate callable/checklist), its
compute profile, and whether it supports checkpointing. MD is the first instance; new lanes
(omics, docking, boltz, admet) register the same way. ``submit_compute_job`` dispatches the gate
by lane instead of a hardcoded ``validation_type == "md"`` branch.

Lanes are ATOMIC. Chaining lanes into multi-stage pipelines is a separate layer (deferred — the
affordances are noted in PHASE3_PLAN). Lanes are keyed by ``validation_type`` because that is the
discriminator carried on a ComputeJobRecord.

The gate signature is ``(service, compute_job_record) -> (error_or_None, metadata)`` so a lane can
reuse a service method (e.g. MD's ``_md_live_submit_gate``) without this module importing service.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

# (service, compute_job_record) -> (error_str_or_None, gate_metadata)
LaneGate = Callable[[Any, Any], "tuple[str | None, dict[str, Any]]"]


@dataclass(frozen=True)
class LaneSpec:
    lane_key: str
    validation_type: str
    gate: LaneGate | None = None  # None = ungated (e.g. ADMET, omics)
    compute_profile: str = "gpu"
    supports_checkpointing: bool = False
    description: str = ""


_LANES: dict[str, LaneSpec] = {}


def register_lane(spec: LaneSpec) -> None:
    """Register (or replace) a lane, keyed by its validation_type."""
    _LANES[spec.validation_type] = spec


def get_lane(validation_type: str | None) -> LaneSpec | None:
    """Resolve the lane for a compute job's validation_type, or None if unregistered."""
    if not validation_type:
        return None
    return _LANES.get(validation_type)


def available_lanes() -> tuple[str, ...]:
    """Return the registered validation_types."""
    return tuple(sorted(_LANES))
