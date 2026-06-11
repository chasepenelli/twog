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
class LaneEnvironment:
    """Declarative spec of what a ready-to-work sandbox for a lane needs — the "open a sandbox and
    it has everything" contract. A provisioner (e.g. a Modal Sandbox/Image + Volume) materializes
    this; the spec itself is dependency-free so it can be inspected, diffed, and reproduced.

    image_ref: container image (registry tag) the lane runs in.
    tools: binaries expected on PATH (gnina, boltz, ...).
    python_packages / conda_packages: deps to install.
    data_refs: datasets to stage into the sandbox (accessions / volume paths).
    skills: skill refs to install for an agent/collaborator working the lane.
    gpu: GPU hint ("T4"/"A100"/"") — "" = CPU.
    """

    image_ref: str = ""
    builder: str = "debian_slim"  # how to build it: "debian_slim" | "micromamba" | "registry"
    tools: tuple[str, ...] = ()
    python_packages: tuple[str, ...] = ()  # PINNED pip specs (e.g. "boltz==2.2.1")
    conda_packages: tuple[str, ...] = ()  # PINNED conda specs (e.g. "openmm=8.5.1")
    channels: tuple[str, ...] = ("conda-forge",)
    data_refs: tuple[str, ...] = ()
    skills: tuple[str, ...] = ()
    gpu: str = ""
    notes: str = ""


@dataclass(frozen=True)
class LaneSpec:
    lane_key: str
    validation_type: str
    gate: LaneGate | None = None  # None = ungated (e.g. ADMET, omics)
    compute_profile: str = "gpu"
    supports_checkpointing: bool = False
    description: str = ""
    environment: LaneEnvironment | None = None  # what a ready sandbox for this lane contains


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


# Utility (non-lane) image plans — e.g. the one-shot R/Seurat .rds -> h5ad converter.
UTILITY_IMAGE_PLANS: dict[str, dict[str, Any]] = {
    "rds_convert": {
        "builder": "micromamba",
        "base": "",
        "gpu": "",
        "conda": ["r-base", "r-seurat", "r-sceasy", "anndata", "python=3.11"],
        "pip": [],
        "channels": ["conda-forge", "bioconda"],
        "notes": "one-shot Seurat .rds -> h5ad (sceasy); emits AnnData once to a Volume",
    },
}


def lane_image_plan(validation_type: str | None) -> dict[str, Any] | None:
    """Translate a lane's declared LaneEnvironment into a CONCRETE, inspectable image build plan
    (builder + base + pip/conda specs + channels + gpu). A provisioner consumes this to construct a
    real modal.Image; the plan stays dependency-free so the translation is unit-testable without
    modal. Returns None for an unregistered lane / no environment."""
    lane = get_lane(validation_type)
    if lane is None or lane.environment is None:
        return None
    env = lane.environment
    return {
        "lane_key": lane.lane_key,
        "validation_type": lane.validation_type,
        "builder": env.builder,
        "base": env.image_ref,
        "pip": list(env.python_packages),
        "conda": list(env.conda_packages),
        "channels": list(env.channels),
        "gpu": env.gpu,
        "tools": list(env.tools),
    }


def resolve_sandbox_environment(
    validation_type: str | None,
    *,
    extra_data_refs: tuple[str, ...] = (),
    extra_skills: tuple[str, ...] = (),
) -> dict[str, Any] | None:
    """Resolve a concrete, inspectable sandbox manifest for a lane — "what a ready sandbox contains"
    — merging the lane's declared environment with any candidate/workspace-specific data + skills.
    Returns None if the lane is unregistered or has no environment. A provisioner consumes this to
    materialize the actual sandbox (Modal Image+Volume); the manifest stays dependency-free."""
    lane = get_lane(validation_type)
    if lane is None or lane.environment is None:
        return None
    env = lane.environment
    return {
        "lane_key": lane.lane_key,
        "validation_type": lane.validation_type,
        "compute_profile": lane.compute_profile,
        "supports_checkpointing": lane.supports_checkpointing,
        "image_ref": env.image_ref,
        "gpu": env.gpu,
        "tools": list(env.tools),
        "python_packages": list(env.python_packages),
        "conda_packages": list(env.conda_packages),
        "data_refs": list(dict.fromkeys([*env.data_refs, *extra_data_refs])),
        "skills": list(dict.fromkeys([*env.skills, *extra_skills])),
        "notes": env.notes,
    }
