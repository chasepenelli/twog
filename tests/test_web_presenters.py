"""web_presenters — the public-read projection (record -> display DTO).

Pure mapping tests: the public site's display contract (web/lib/types/domain.ts) must be satisfied by
these projections. No DB / HTTP. Records are built minimally but validly to mirror real engine output.
"""

from __future__ import annotations

from uuid import uuid4

from hsa_research.ingestion_bridge import web_presenters as present
from hsa_research.ingestion_bridge.contracts import (
    ProofCapsuleRecord,
    PublicCandidateRecord,
    RunManifestRecord,
)


def _capsule(**over):
    base = dict(
        workspace_id=uuid4(),
        checkout_manifest_hash="sha256:" + "a" * 16,
        candidate_id="alpelisib-pi3ka",
        packet_type="compute_artifact",
        requested_action="docking_or_md_review",
        target={"section": "docking", "method_ref": str(uuid4())},
        summary={
            "title": "Falsify: Alpelisib for PIK3CA-driven HSA",
            "finding": "gnina docked alpelisib: -9.4 kcal/mol. Signal: supports.",
            "why_it_matters": "Dock the compound to test whether it engages the site.",
            "limitations": ["docking is an estimate, not measured binding"],
        },
        content_hash="a" * 16,
        status="submitted",
        producer={"producer_type": "agent", "name": "twog_compute"},
        payload={"signal": "supports", "confidence": 0.85, "provenance_flag": "pass"},
    )
    base.update(over)
    return ProofCapsuleRecord.model_validate(base)


def test_present_capsule_projects_display_fields():
    out = present.present_capsule(_capsule())
    assert out["signal"] == "supports"
    assert out["validation_type"] == "docking"
    assert out["claim"] == "Falsify: Alpelisib for PIK3CA-driven HSA"
    assert out["readout"].startswith("gnina docked")
    assert out["confidence"] == 0.85
    assert out["provenance_verdict"] == "pass"
    assert out["produced_by"] == "twog_compute"
    assert out["limitations"] == ["docking is an estimate, not measured binding"]


def test_present_capsule_defends_bad_signal_and_missing_optionals():
    out = present.present_capsule(_capsule(payload={"signal": "bogus"}))
    assert out["signal"] == "neutral"  # invalid signal degrades, never crashes
    assert "confidence" not in out  # absent optional is omitted, not faked
    assert "provenance_verdict" not in out


def test_present_manifest_surfaces_rollup_and_rows():
    rec = RunManifestRecord.model_validate({
        "manifest_type": "falsification_campaign",
        "title": "Falsification campaign — 3 candidates",
        "output_refs": {
            "rollup": {"candidates_selected": 3, "any_promoted": False, "terminal_reasons": {"max_rounds": 3}},
            "rows": [{"candidate_id": "alpelisib-pi3ka", "terminal_reason": "max_rounds"}],
        },
        "metadata": {"runner_kind": "modal"},
    })
    out = present.present_manifest(rec)
    assert out["runner_kind"] == "modal"
    assert out["rollup"]["candidates_selected"] == 3
    assert out["rollup"]["any_promoted"] is False
    assert out["rows"][0]["candidate_id"] == "alpelisib-pi3ka"
    assert out["ran_at"]  # derived from created_at


def test_present_manifest_forces_any_promoted_invariant():
    rec = RunManifestRecord.model_validate({"manifest_type": "falsification_campaign", "output_refs": {}})
    assert present.present_manifest(rec)["rollup"]["any_promoted"] is False


def test_present_candidate_maps_public_fields():
    rec = PublicCandidateRecord.model_validate({
        "candidate_id": "pik3ca-crux",
        "title": "Is the PIK3CA-mutant HSA subset immunosuppressed?",
        "public_status": "evidence_supported",
        "validation_ready": True,
        "targets": ["PIK3CA"],
        "evidence_refs": ["GSE95183"],
    })
    out = present.present_candidate(rec)
    assert out["candidate_id"] == "pik3ca-crux"
    assert out["public_status"] == "evidence_supported"
    assert out["validation_ready"] is True
    assert out["targets"] == ["PIK3CA"]


def test_present_engine_state_counts_signals_from_capsules():
    caps = [
        _capsule(payload={"signal": "supports"}),
        _capsule(payload={"signal": "refutes"}),
        _capsule(payload={"signal": "neutral"}, target={"section": "omics"}),
    ]
    state = present.present_engine_state([], caps, [])
    assert state["online"] is True
    assert state["headline"]["validatedResults"] == 1
    assert state["headline"]["hypothesesFalsified"] == 1
    assert state["headline"]["computeLanes"] == 2  # docking + omics
    assert len(state["loop"]) == 5
    assert state["headline"]["testsPassing"] == present.ENGINE_TESTS_PASSING
