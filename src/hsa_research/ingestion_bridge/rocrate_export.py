"""Export a twog falsification campaign as a portable RO-Crate (Process Run Crate profile).

A "campaign" here is every ``ProofCapsuleRecord`` sharing one ``candidate_id`` — the unit of the
falsification ledger (there is no surrogate run id; see frontier-scout/dossiers/ro-crate.md). Each
capsule is already content-hashed and optionally Ed25519-signed; RO-Crate is the *packaging* layer it
never had: one directory (or zip) holding a JSON-LD provenance graph (schema.org) that downstream
tools (Workflow-Hub / Galaxy / nf-core / runcrate) can read.

This is a WRITE-ONLY projection over existing ledger rows — it reads via the ``ResearchRepository``
protocol and never touches the dispatch/gate hot path. Mapping (twog -> RO-Crate):

    candidate_id (the dossier)           -> root Dataset + mainEntity hypothesis (CreativeWork)
    ProofCapsuleRecord                   -> result CreativeWork (content_hash = identifier)
    ComputeJobRecord                     -> CreateAction (instrument/object/result/agent + times)
    runner_kind / container_image        -> SoftwareApplication instrument
    falsification_preregistration block  -> CreativeWork (preregistration_hash = identifier)
    provenance_auditor.audit(...)        -> CreativeWork integrity assertion, `about` the CreateAction
    ProofCapsuleProducer                 -> Person | SoftwareApplication agent
    ProofCapsuleArtifactRef              -> File data entity (referenced by URI; sha256 recorded)
    ProofCapsuleSourceRef                -> ScholarlyArticle citation

"Dossier" = every capsule for one ``candidate_id``. This is deliberately NOT the literal
``RunManifest(manifest_type="falsification_campaign")`` record — to avoid overloading "campaign", the
public entry point is ``candidate_to_crate``.

``rocrate`` (ro-crate-py, Apache-2.0) is imported lazily so importing this module never hard-requires
the dependency; only ``campaign_to_crate`` needs it.
"""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import provenance_auditor
from .contracts import ComputeJobRecord, ProofCapsuleRecord
from .repository import ResearchRepository

if TYPE_CHECKING:  # pragma: no cover - typing only
    from rocrate.rocrate import ROCrate

# Process Run Crate profile — the community profile for "one tool run with provenance". Aligning to it
# (rather than ad-hoc types) is what makes the crate validate against an existing consumer profile.
PROCESS_RUN_CRATE_PROFILE = "https://w3id.org/ro/wfrun/process/0.5"
DEFAULT_LICENSE = "https://creativecommons.org/licenses/by/4.0/"


def _iso(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


def _clean(props: dict[str, Any]) -> dict[str, Any]:
    """Drop None/empty values so the JSON-LD graph stays free of null noise."""
    return {k: v for k, v in props.items() if v not in (None, "", [], {})}


def _producer_agent(crate: "ROCrate", capsule: ProofCapsuleRecord, seen: dict[str, Any]) -> Any:
    """Add (once) the capsule's producer as a Person or SoftwareApplication agent; return its node."""
    from rocrate.model.contextentity import ContextEntity
    from rocrate.model.person import Person

    p = capsule.producer
    is_software = p.producer_type in {"agent", "sandbox", "system"} or bool(p.agent_name or p.model_name)
    label = p.agent_name or p.model_name or p.name or "unknown"
    agent_id = f"#agent-{'sw' if is_software else 'person'}-{label}".replace(" ", "_")
    if agent_id in seen:
        return seen[agent_id]
    if is_software:
        node = crate.add(ContextEntity(crate, agent_id, _clean({
            "@type": "SoftwareApplication",
            "name": label,
            "softwareVersion": p.model_name,
            "description": f"twog producer ({p.producer_type})",
        })))
    else:
        node = crate.add(Person(crate, agent_id, _clean({"name": p.name, "contactPoint": p.contact})))
    seen[agent_id] = node
    return node


def _add_artifacts(crate: "ROCrate", capsule: ProofCapsuleRecord) -> list[dict[str, str]]:
    """Register each artifact ref as a referenced File data entity; return @id refs for `result`."""
    from rocrate.model.contextentity import ContextEntity

    refs: list[dict[str, str]] = []
    for idx, art in enumerate(capsule.artifacts):
        props = _clean({
            "name": art.description or art.artifact_type or Path(art.artifact_uri).name or "artifact",
            "encodingFormat": art.artifact_type,
            "sha256": art.content_hash,
            "description": art.description,
        })
        if "://" in art.artifact_uri:
            # Reference remote artifacts by URI — don't copy bytes into the crate (GPU outputs are big).
            node = crate.add_file(art.artifact_uri, fetch_remote=False, validate_url=False, properties=props)
        else:
            # Not a dereferenceable URI (bare hash / opaque id) — record as a File entity by reference.
            node = crate.add(ContextEntity(crate, f"#artifact-{capsule.capsule_id}-{idx}", {
                "@type": "File", **props, "identifier": art.artifact_uri,
            }))
        refs.append({"@id": node.id})
    return refs


def _add_sources(crate: "ROCrate", capsule: ProofCapsuleRecord) -> list[dict[str, str]]:
    """Register each source ref as a ScholarlyArticle; return @id refs for the capsule's `citation`."""
    from rocrate.model.contextentity import ContextEntity

    refs: list[dict[str, str]] = []
    for idx, src in enumerate(capsule.source_refs):
        ident = src.doi or (f"PMID:{src.pmid}" if src.pmid else None) or (f"PMC{src.pmcid}" if src.pmcid else None)
        node = crate.add(ContextEntity(crate, f"#source-{capsule.capsule_id}-{idx}", _clean({
            "@type": "ScholarlyArticle",
            "name": src.title or src.source_id or ident or src.url,
            "url": src.url,
            "identifier": ident,
            "description": src.claim_supported,
        })))
        refs.append({"@id": node.id})
    return refs


def _add_prereg(crate: "ROCrate", job: ComputeJobRecord) -> dict[str, str] | None:
    """Lift the pre-registration block (embedded in compute-job metadata) into its own CreativeWork."""
    from rocrate.model.contextentity import ContextEntity

    block = (job.metadata or {}).get("falsification_preregistration")
    if not isinstance(block, dict) or not block:
        return None
    node = crate.add(ContextEntity(crate, f"#prereg-{job.compute_job_id}", _clean({
        "@type": "CreativeWork",
        "name": "Falsification pre-registration",
        "identifier": block.get("preregistration_hash"),
        "description": "Pre-registered kill-criterion locked before dispatch (anti-p-hacking).",
        "text": json.dumps(block, sort_keys=True, default=str),
    })))
    return {"@id": node.id}


def _resolve_job(repository: ResearchRepository, capsule: ProofCapsuleRecord) -> ComputeJobRecord | None:
    raw_job_id = (capsule.payload or {}).get("compute_job_id")
    if not raw_job_id:
        return None
    try:
        from uuid import UUID
        return repository.get_compute_job(UUID(str(raw_job_id)))
    except Exception:
        return None


def _kill_signal_for(job: ComputeJobRecord | None) -> str:
    """The observed signal that COUNTS AS A KILL for this capsule's pre-registered criterion.

    Defaults to ``"refutes"`` (the engine default, contracts.py ``observed_signal_kills``); a prereg
    block whose kill criterion fires on ``"neutral"`` overrides it. Mirrors failure_corpus's check
    (``signal == kill_criterion["observed_signal_kills"]``)."""
    if job is None:
        return "refutes"
    block = (job.metadata or {}).get("falsification_preregistration")
    if isinstance(block, dict):
        crit = block.get("kill_criterion")
        if isinstance(crit, dict) and crit.get("observed_signal_kills"):
            return str(crit["observed_signal_kills"]).lower()
    return "refutes"


def _hypothesis_status(pairs: list[tuple[ProofCapsuleRecord, ComputeJobRecord | None]]) -> str:
    """Faithful three-state rollup (contracts.py ``LeadingHypothesisStatus``) DERIVED from capsule
    signals (the authoritative ``FalsificationLoopResult`` is a returned object, not a persisted row):
      - any capsule meets its pre-registered kill criterion -> ``refuted``
      - else any capsule carries a real (non-neutral) readout that survived -> ``standing``
      - else (only ``neutral``/``none`` readouts) -> ``underpowered`` (NOT silently "standing")
    """
    signals = [str((c.payload or {}).get("signal", "")).lower() for c, _ in pairs]
    if any(sig == _kill_signal_for(job) and sig for (c, job), sig in zip(pairs, signals)):
        return "refuted"
    if any(sig in {"supports", "refutes"} for sig in signals):
        return "standing"
    return "underpowered"


def candidate_to_crate(
    repository: ResearchRepository,
    candidate_id: str,
    out_path: str | Path,
    *,
    write_zip: bool = False,
    capsule_limit: int = 500,
) -> Path:
    """Project one candidate's evidence dossier (all its proof capsules) into an RO-Crate at
    ``out_path`` (a directory, or a ``.zip`` with ``write_zip=True``).

    Returns the written path. Raises ``LookupError`` if the candidate has no proof capsules.
    """
    from rocrate.model.contextentity import ContextEntity
    from rocrate.rocrate import ROCrate

    capsules = repository.list_proof_capsules(candidate_id=candidate_id, limit=capsule_limit)
    if not capsules:
        raise LookupError(f"no proof capsules for candidate_id={candidate_id!r}; nothing to export")
    pairs = [(capsule, _resolve_job(repository, capsule)) for capsule in capsules]

    crate = ROCrate()
    crate.name = f"twog candidate evidence dossier — {candidate_id}"
    crate.license = DEFAULT_LICENSE

    # Hypothesis rollup = the dossier's mainEntity. Status is the faithful three-state derivation.
    status = _hypothesis_status(pairs)
    hypothesis = crate.add(ContextEntity(crate, f"#hypothesis-{candidate_id}", _clean({
        "@type": ["CreativeWork", "twog:Hypothesis"],  # twog: term — Hypothesis is not a schema.org type
        "name": f"Leading hypothesis for {candidate_id}",
        "identifier": candidate_id,
        "creativeWorkStatus": status,
        "description": capsules[0].summary.why_it_matters,
        "twog:statusDerivation": "derived from capsule signals + pre-registered kill criteria",
    })))
    crate.mainEntity = hypothesis
    crate.description = (
        f"Autonomous falsification evidence dossier for candidate {candidate_id}: "
        f"{len(capsules)} proof capsule(s), leading hypothesis {status}."
    )

    agents: dict[str, Any] = {}
    tools: dict[str, Any] = {}
    latest: datetime | None = None

    for capsule, job in pairs:
        latest = max([t for t in (latest, capsule.updated_at, capsule.created_at) if t], default=latest)
        agent = _producer_agent(crate, capsule, agents)

        capsule_node = crate.add(ContextEntity(crate, f"#capsule-{capsule.capsule_id}", _clean({
            "@type": "CreativeWork",
            "name": capsule.summary.title,
            "text": capsule.summary.finding,
            "description": capsule.summary.why_it_matters,
            "creativeWorkStatus": capsule.status,
            "identifier": capsule.content_hash,
            "dateCreated": _iso(capsule.created_at),
            "version": capsule.lineage_index,
            "about": {"@id": hypothesis.id},
            "creator": {"@id": agent.id},
        })))
        # Provenance extras as plain properties (kept off schema.org core so consumers can ignore them).
        signal = (capsule.payload or {}).get("signal")
        if signal:
            capsule_node["twog:signal"] = signal
        if capsule.signature:
            capsule_node["twog:ed25519Signature"] = capsule.signature
        citations = _add_sources(crate, capsule)
        if citations:
            capsule_node["citation"] = citations

        # The compute job that produced this capsule (resolved up front) becomes the CreateAction.
        result_refs: list[dict[str, str]] = [{"@id": capsule_node.id}, *_add_artifacts(crate, capsule)]
        object_refs: list[dict[str, str]] = [{"@id": hypothesis.id}]
        action_node: Any = None

        if job is not None:
            runner = job.runner_kind or "mock"
            tool_id = f"#tool-{runner}"
            tool = tools.get(tool_id)
            if tool is None:
                tool = crate.add(ContextEntity(crate, tool_id, _clean({
                    "@type": "SoftwareApplication",
                    "name": runner,
                    "softwareVersion": job.container_image,
                    "description": f"twog compute runner ({job.compute_profile})",
                })))
                tools[tool_id] = tool
            prereg = _add_prereg(crate, job)
            if prereg:
                object_refs.append(prereg)
            action_node = crate.add(ContextEntity(crate, f"#action-{job.compute_job_id}", _clean({
                "@type": "CreateAction",
                "name": job.title,
                "description": job.objective,
                "instrument": {"@id": tool.id},
                "agent": {"@id": agent.id},
                "object": object_refs,
                "result": result_refs,
                "startTime": _iso(job.started_at),
                "endTime": _iso(job.completed_at),
                "actionStatus": (
                    "CompletedActionStatus" if job.status == "completed" else "FailedActionStatus"
                ),
                "twog:validationType": job.validation_type,
                "twog:providerJobId": job.provider_job_id,
                "twog:dagsterRunId": job.dagster_run_id,
                "twog:costActualUsd": job.cost_actual_usd,
                "twog:costEstimateUsd": job.cost_estimate_usd,
            })))

        # Stamp the integrity gate verdict — "did this evidence's claimed run check out?" — the headline
        # assertion of a *provenance* crate. Recomputed (the auditor is pure/deterministic, no I/O) so a
        # never-audited capsule still gets a verdict; `about` points at the action, or the capsule when
        # the claim didn't resolve to a job.
        verdict = provenance_auditor.audit(capsule, job)
        about_id = action_node.id if action_node is not None else capsule_node.id
        verdict_node = crate.add(ContextEntity(crate, f"#provenance-{capsule.capsule_id}", _clean({
            "@type": ["CreativeWork", "twog:ProvenanceVerdict"],
            "name": f"Provenance audit: {verdict['status']}",
            "about": {"@id": about_id},
            "description": verdict["rationale"],
            "twog:gate": "provenance_auditor",
            "twog:status": verdict["status"],
            "twog:ok": verdict["ok"],
            "twog:checksPassed": verdict["checks_passed"],
            "twog:mismatches": verdict["mismatches"],
        })))
        capsule_node["twog:provenanceVerdict"] = {"@id": verdict_node.id}

    crate.datePublished = _iso(latest)

    # Declare conformance to the Process Run Crate profile (best-effort; never fail the export over it).
    try:
        crate.metadata["conformsTo"] = [
            {"@id": "https://w3id.org/ro/crate/1.1"},
            {"@id": PROCESS_RUN_CRATE_PROFILE},
        ]
        crate.add(ContextEntity(crate, PROCESS_RUN_CRATE_PROFILE, {
            "@type": "CreativeWork", "name": "Process Run Crate", "version": "0.5",
        }))
    except Exception:  # pragma: no cover - defensive only
        pass

    out_path = Path(out_path)
    if write_zip:
        crate.write_zip(str(out_path))
    else:
        crate.write(str(out_path))
    return out_path


def main(argv: list[str] | None = None) -> int:
    """CLI: ``python -m hsa_research.ingestion_bridge.rocrate_export --candidate <id> --out <path>``."""
    import argparse

    parser = argparse.ArgumentParser(description="Export a twog campaign as an RO-Crate.")
    parser.add_argument("--candidate", required=True, help="candidate_id of the campaign to export")
    parser.add_argument("--out", default="campaign-crate", help="output directory (or .zip with --zip)")
    parser.add_argument("--zip", action="store_true", help="write a .zip instead of a directory")
    args = parser.parse_args(argv)

    from .storage import build_research_repository

    repository = build_research_repository()
    path = candidate_to_crate(repository, args.candidate, args.out, write_zip=args.zip)
    print(f"wrote RO-Crate to {path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
