"""RO-Crate export of a candidate's evidence dossier (all proof capsules for one candidate_id).

Seeds the ledger directly (upsert_compute_job + upsert_proof_capsule, mirroring
test_provenance_auditor's fixtures), exports a crate, then asserts the JSON-LD graph carries the
hypothesis rollup, the CreateAction with its provenance, the pre-registration, the provenance gate
verdict, the artifact, and the source — and that ro-crate-py can re-parse what we wrote.

Signal vocabulary is the REAL one (contracts.py: "supports"|"refutes"|"neutral"|"none"); the rollup
states are the real LeadingHypothesisStatus ("refuted"|"standing"|"underpowered").
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from hsa_research.ingestion_bridge.local_store import SQLiteResearchRepository
from hsa_research.ingestion_bridge.contracts import (
    ComputeJobRecord,
    ProofCapsuleArtifactRef,
    ProofCapsuleProducer,
    ProofCapsuleRecord,
    ProofCapsuleSourceRef,
    ProofCapsuleSummary,
    ProofCapsuleTarget,
)
from hsa_research.ingestion_bridge import rocrate_export

pytest.importorskip("rocrate", reason="ro-crate-py not installed; `uv sync --extra rocrate`")

_MANIFEST = "sha256:" + "a" * 24


def _summary(title="capsule"):
    return ProofCapsuleSummary(
        title=title, finding="finding text here", why_it_matters="why it matters here",
        limitations=["single pose"],
    )


def _bare_capsule(repo, cid, signal):
    """A capsule with a signal but no compute job — for rollup-state coverage."""
    return repo.upsert_proof_capsule(
        ProofCapsuleRecord(
            workspace_id=uuid4(), checkout_manifest_hash=_MANIFEST, candidate_id=cid,
            packet_type="compute_artifact", requested_action="docking_or_md_review",
            target=ProofCapsuleTarget(section="docking"), summary=_summary(),
            payload={"signal": signal}, content_hash="sha256:" + signal + "0" * 16,
        )
    )


def _seed_refuting_campaign(repo, cid="vr-rocrate-1"):
    job = repo.upsert_compute_job(
        ComputeJobRecord(
            title="Dock alpelisib vs PIK3CA",
            objective="Falsify the binding hypothesis via spend-gated docking",
            status="completed",
            runner_kind="modal",
            compute_profile="gpu_a100",
            validation_type="docking",
            candidate_id=cid,
            checkout_manifest_hash=_MANIFEST,
            provider_job_id="modal-abc123",
            dagster_run_id="dag-1",
            cost_actual_usd=0.1,
            container_image="ghcr.io/twog/docking@sha256:deadbeef",
            started_at=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
            completed_at=datetime(2026, 6, 15, 12, 20, tzinfo=UTC),
            metadata={
                "falsification_preregistration": {
                    "preregistration_hash": "sha256:preregXYZ",
                    "kill_criterion": {"observed_signal_kills": "refutes"},
                }
            },
        )
    )
    cap = repo.upsert_proof_capsule(
        ProofCapsuleRecord(
            workspace_id=uuid4(),
            checkout_manifest_hash=_MANIFEST,
            candidate_id=cid,
            packet_type="compute_artifact",
            requested_action="docking_or_md_review",
            producer=ProofCapsuleProducer(
                producer_type="agent", name="twog-runner", model_name="claude-opus-4-8"
            ),
            target=ProofCapsuleTarget(section="docking"),
            summary=ProofCapsuleSummary(
                title="Docking refutes the binding hypothesis",
                finding="Top pose passed PoseBusters but affinity was worse than the control.",
                why_it_matters="Pre-registered kill-criterion met -> hypothesis refuted.",
                limitations=["single pose", "implicit solvent"],
            ),
            # The REAL refuting value is "refutes" (not "refuted"); the engine never emits "refuted".
            payload={
                "compute_job_id": str(job.compute_job_id),
                "validation_type": "docking",
                "signal": "refutes",
            },
            artifacts=[
                ProofCapsuleArtifactRef(
                    artifact_uri="https://storage.twog.dev/pose.pdb",
                    content_hash="sha256:pose123",
                    artifact_type="chemical/x-pdb",
                    description="top docked pose",
                )
            ],
            source_refs=[
                ProofCapsuleSourceRef(
                    title="PIK3CA crystal structure",
                    doi="10.1234/abcd",
                    url="https://doi.org/10.1234/abcd",
                    claim_supported="receptor structure provenance",
                )
            ],
            content_hash="sha256:" + "c" * 24,
            signature="ed25519-sig-xyz",
        )
    )
    return cid, job, cap


def _graph_by_id(crate_dir):
    meta = json.loads((crate_dir / "ro-crate-metadata.json").read_text())
    return {node["@id"]: node for node in meta["@graph"]}


def _types(node):
    t = node.get("@type", [])
    return t if isinstance(t, list) else [t]


def test_candidate_to_crate_writes_full_provenance_graph(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "rc.sqlite3", seed=False)
    cid, job, cap = _seed_refuting_campaign(repo)

    out = rocrate_export.candidate_to_crate(repo, cid, tmp_path / "crate")
    assert (out / "ro-crate-metadata.json").exists()

    nodes = _graph_by_id(out)

    # Hypothesis is the mainEntity and a refuting capsule flips the rollup to "refuted" (the bug fix).
    hyp = nodes[f"#hypothesis-{cid}"]
    assert hyp["creativeWorkStatus"] == "refuted"
    assert nodes["./"]["mainEntity"]["@id"] == f"#hypothesis-{cid}"

    # The compute job became a CreateAction carrying its provenance.
    action = nodes[f"#action-{job.compute_job_id}"]
    assert "CreateAction" in _types(action)
    assert action["actionStatus"] == "CompletedActionStatus"
    assert action["twog:providerJobId"] == "modal-abc123"
    assert action["twog:costActualUsd"] == 0.1
    assert action["instrument"]["@id"] == "#tool-modal"

    # Pre-registration was lifted out of the compute-job metadata as its own entity.
    assert nodes[f"#prereg-{job.compute_job_id}"]["identifier"] == "sha256:preregXYZ"

    # Provenance GATE VERDICT is stamped, about the action, and verifies (all claimed fields match).
    verdict = nodes[f"#provenance-{cap.capsule_id}"]
    assert "twog:ProvenanceVerdict" in _types(verdict)
    assert verdict["twog:status"] == "verified"
    assert verdict["twog:ok"] is True
    assert verdict["about"]["@id"] == f"#action-{job.compute_job_id}"

    # Capsule, artifact, citation, and the link to its verdict are present.
    capsule = nodes[f"#capsule-{cap.capsule_id}"]
    assert capsule["identifier"] == "sha256:" + "c" * 24
    assert capsule["twog:signal"] == "refutes"
    assert capsule["twog:provenanceVerdict"]["@id"] == f"#provenance-{cap.capsule_id}"
    assert nodes["https://storage.twog.dev/pose.pdb"]["sha256"] == "sha256:pose123"
    source = next(n for n in nodes.values() if "ScholarlyArticle" in _types(n))
    assert source["identifier"] == "10.1234/abcd"

    # Round-trip: ro-crate-py can re-parse what we wrote.
    from rocrate.rocrate import ROCrate

    reloaded = ROCrate(str(out))
    assert reloaded.mainEntity is not None
    assert reloaded.mainEntity.id == f"#hypothesis-{cid}"


def test_rollup_three_states_are_faithful(tmp_path):
    """A non-neutral survivor is `standing`; only-neutral readouts are `underpowered` (NOT collapsed
    to standing). This is the assertion the original `"refuted"` bug + fabricated fixture hid."""
    repo = SQLiteResearchRepository(tmp_path / "rollup.sqlite3", seed=False)
    _bare_capsule(repo, "vr-supports", "supports")
    _bare_capsule(repo, "vr-neutral", "neutral")

    out_s = rocrate_export.candidate_to_crate(repo, "vr-supports", tmp_path / "s")
    out_u = rocrate_export.candidate_to_crate(repo, "vr-neutral", tmp_path / "u")

    assert _graph_by_id(out_s)["#hypothesis-vr-supports"]["creativeWorkStatus"] == "standing"
    assert _graph_by_id(out_u)["#hypothesis-vr-neutral"]["creativeWorkStatus"] == "underpowered"


def test_candidate_with_no_capsules_raises(tmp_path):
    repo = SQLiteResearchRepository(tmp_path / "empty.sqlite3", seed=False)
    with pytest.raises(LookupError):
        rocrate_export.candidate_to_crate(repo, "vr-nothing-here", tmp_path / "crate")
