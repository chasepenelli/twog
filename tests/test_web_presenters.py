"""web_presenters — the public-read projection (record -> display DTO).

Pure mapping tests: the public site's display contract (web/lib/types/domain.ts) must be satisfied by
these projections. No DB / HTTP. Records are built minimally but validly to mirror real engine output.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
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


def test_present_capsule_surfaces_provenance_anchors():
    """The widened receipt carries the verifiable anchors (content_hash + lineage) so it's re-derivable."""
    out = present.present_capsule(_capsule(content_hash="sha256:abc", lineage_index=2,
                                           parent_content_hash="sha256:par", submitted_by="lab"))
    assert out["content_hash"] == "sha256:abc"
    assert out["lineage_index"] == 2
    assert out["parent_content_hash"] == "sha256:par"
    assert out["submitted_by"] == "lab"


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


def _rubric_dict(**over):
    """A minimal-but-valid MoonshotRubric content_payload() shape (what rides snapshot.payload)."""
    base = {
        "rubric_version": "moonshot-rubric-v1",
        "candidate_id": "alpelisib-pik3ca",
        "title": "Alpelisib × PIK3CA",
        "thesis": "Alpelisib engages PI3Ka across the canine-HSA × human-AS axis.",
        "moonshot_gate": {"passed": True, "weighted_score": 0.9, "reasons": ["evidence_anchor_present"], "blockers": []},
        "moonshot_grade": True,
        "moonshot_score": 0.9,
        "mechanistic_premise": "alpelisib is hypothesized to engage PIK3CA; mutation-selective PI3Kα inhibition.",
        "premises": [
            {"claim": "alpelisib is hypothesized to engage PIK3CA", "basis": "cross-species precision strategy",
             "supports_quality": "mutation-selective inhibition", "strength": "medium", "is_specified": True},
        ],
        "inference_chain": [
            {"step": 1, "from_lanes": ["docking"], "infers": "If docking survives, treat the mechanism as live and proceed to omics.",
             "if_broken": "A refuting docking readout (gnina_cnn_affinity < 4.0) breaks the chain at step 1."},
            {"step": 2, "from_lanes": ["omics"], "infers": "If omics survives, the claim has survived_known_confounds.",
             "if_broken": "A refuting cross-species axis breaks the chain at step 2."},
        ],
        "expected_payoff": {
            "if_survives": "If all lanes survive, the PIK3CA-mutant subset becomes a treatment candidate to advance.",
            "translational_claim": "a mutation-selective treatment candidate to advance",
            "next_step": "confirm the axis in an orthogonal cohort", "value_of_information": 0.65,
            "is_specified": True, "caveat": "survived_known_confounds, never proven; the operator write-gate is terminal.",
        },
        "net_signal": "supports",
        "net_confidence": 0.42,
        "signalful_capsule_count": 1,
        "has_falsifiable_plan": True,
        "ready_to_run": False,
        "runnable_lanes": ["docking", "omics"],
        "targets_needed": [
            {"target": "PIK3CA", "role": "primary", "verification": "verified", "uniprot": "P42336",
             "pdb_id": "4JPS", "chain": "A", "redock_rmsd": 1.8, "cocrystal_ligand_code": "1E8"},
        ],
        "compounds_needed": [
            {"name": "alpelisib", "role": "lead", "smiles": "CC1=...", "readiness": "resolved",
             "resolution_source": "pubchem", "intended_targets": ["PIK3CA"]},
        ],
        "test_plan": [
            {"order": 1, "lane": "docking", "validation_type": "docking", "test_objective": "Dock alpelisib vs PIK3CA.",
             "kill_criterion": {"metric": "gnina_cnn_affinity", "comparator": "<", "threshold": 4.0,
                                "observed_signal_kills": "refutes", "rationale": "No measurable engagement refutes."},
             "expected_signal_if_alive": "supports", "addresses_confound": None, "est_cost_usd": 0.1,
             "value_of_information": 0.8, "inputs_ready": True, "is_proposed": True, "autonomously_runnable": True,
             "maturity": "production", "standing": "queued",
             "probes": ["Can the modeled pose let alpelisib occupy the site at PIK3CA (PDB 4JPS)?"],
             "why_it_bears": "This lane interrogates mutation-selective PI3Kα inhibition — the docking precondition.",
             "interpretation": {"supports": "Consistent with engagement; remains unaudited — never accepted as proof.",
                                "refutes": "Observed gnina_cnn_affinity < 4.0 -> refutes target-mediated action.",
                                "neutral": "Inconclusive; belief unmoved."},
             "inputs": {"lane": "docking", "config_key": "docking", "readiness": "resolved",
                        "resolution_source": "curated_library", "required_keys": ["receptor_pdb", "ligand_smiles"],
                        "present_keys": ["receptor_pdb", "ligand_smiles"], "missing": [], "md_schedule": None}},
            {"order": 2, "lane": "md", "validation_type": "md", "test_objective": "MD smoke for pose stability.",
             "kill_criterion": {"metric": "ligand_pocket_rmsd_nm", "comparator": ">", "threshold": 0.5,
                                "observed_signal_kills": "refutes", "rationale": "Drift refutes a stable binder."},
             "expected_signal_if_alive": "supports", "addresses_confound": None, "est_cost_usd": 0.2,
             "value_of_information": 0.5, "inputs_ready": False, "is_proposed": False, "autonomously_runnable": True,
             "maturity": "smoke", "standing": "untested",
             "inputs": {"lane": "md", "config_key": "compute_input", "readiness": "missing", "resolution_source": "",
                        "required_keys": ["protein_pdb", "compound_smiles"], "present_keys": [], "missing": ["protein_pdb"],
                        "md_schedule": {"simulation_steps": 1000, "temperature": 300.0, "ph": 7.4,
                                        "force_field": "amber14", "solvent_model": "tip3p",
                                        "equilibration": "minimize -> NVT -> NPT", "preparation_method": "apo + pose"}}},
        ],
        "inputs_rollup": {"resolved_lanes": ["docking"], "needs_verification_lanes": [], "missing_lanes": ["md"],
                          "ready_to_run_lanes": ["docking"], "per_lane_missing": {"md": ["protein_pdb"]},
                          "blockers": ["md: protein_pdb"]},
        "confounds": {"open_confounds": [{"kind": "tumor_purity", "status": "open", "control_lane": "omics"}],
                      "controlled_confounds": [], "audit_policy": "No supports until controls survive."},
        "cross_species": {"species": ["canine", "human"], "disease_context": "canine HSA × human AS",
                          "replication_axis": "PI3Ka activation axis", "replication_lane": "omics",
                          "orthogonal_cohort_required": True, "kill_criterion": None, "evidence_to_date": []},
        "promotion": {"auto_promotable": False, "required_surviving_lanes": ["docking", "md"],
                      "required_confounds_controlled": ["tumor_purity"], "cross_species_replication_required": True,
                      "min_signalful_capsules": 2, "statement": "Promote only if every lane survives. Never auto."},
        "evidence_anchors": ["PMID:1", "PMID:2"],
        "risks": ["alpelisib hyperglycemia in dogs"],
        "assembly_notes": [],
    }
    base.update(over)
    return base


def test_present_rubric_projects_whole_shabang():
    out = present.present_rubric(_rubric_dict())
    assert out["title"] == "Alpelisib × PIK3CA"
    assert out["moonshot_grade"] is True and out["has_falsifiable_plan"] is True
    assert out["gradable"] is True  # a thesis was graded (non-empty moonshot_gate)
    # targets carry the 3-state docking verification (the spend gate)
    assert out["targets_needed"][0]["verification"] == "verified"
    assert out["targets_needed"][0]["pdb_id"] == "4JPS"
    # compounds carry SMILES-resolution status, never fabricated
    assert out["compounds_needed"][0]["readiness"] == "resolved"
    # ordered test plan with flattened kill criteria + standings
    tp = out["test_plan"]
    assert [t["order"] for t in tp] == [1, 2]
    assert tp[0]["kill_criterion"]["metric"] == "gnina_cnn_affinity"
    assert tp[0]["kill_criterion"]["kills_on"] == "refutes"
    assert tp[0]["standing"] == "queued" and tp[0]["is_proposed"] is True
    # the md lane carries its pre-registered MD SCHEDULE + smoke maturity flag
    md = tp[1]
    assert md["maturity"] == "smoke"
    assert md["inputs"]["md_schedule"]["simulation_steps"] == 1000
    # promotion is NEVER auto-satisfiable (typed invariant)
    assert out["promotion"]["auto_promotable"] is False
    # confounds + cross-species + rollup all surface
    assert out["confounds"]["open"][0]["kind"] == "tumor_purity"
    assert out["cross_species"]["replication_lane"] == "omics"
    assert out["inputs_rollup"]["ready_to_run_lanes"] == ["docking"]
    # reasoning spine: premise → per-test probes/interpretation → inference chain → payoff
    assert out["premises"][0]["is_specified"] is True and out["premises"][0]["strength"] == "medium"
    assert "alpelisib" in out["mechanistic_premise"]
    assert tp[0]["probes"] and "PIK3CA" in tp[0]["probes"][0]
    assert tp[0]["why_it_bears"] and "gnina_cnn_affinity" in tp[0]["interpretation"]["refutes"]
    assert [link["step"] for link in out["inference_chain"]] == [1, 2]
    assert all(link["if_broken"] for link in out["inference_chain"])  # falsification-first
    assert out["expected_payoff"]["is_specified"] is True
    assert "never proven" in out["expected_payoff"]["caveat"]


def test_present_rubric_failed_grade_stays_gradable():
    """A graded-but-FAILED candidate (non-empty gate, passed=False) stays gradable=True -> the UI shows
    'not moonshot-grade', NEVER the soft 'active candidate · under test' (reserved for never-graded roster
    candidates where the gate dict is empty). Locks the never-graded vs graded-and-failed distinction."""
    out = present.present_rubric(_rubric_dict(
        moonshot_gate={"passed": False, "blockers": ["frontier_weighted_score_below_0.80"], "reasons": []},
        moonshot_grade=False, moonshot_score=0.4))
    assert out["gradable"] is True  # non-empty gate => graded (even though it failed)
    assert out["moonshot_grade"] is False  # => header shows "not moonshot-grade", not "under test"


def test_present_rubric_degrades_on_empty_dict():
    out = present.present_rubric({})
    assert out["promotion"]["auto_promotable"] is False  # invariant holds even with no data
    assert out["test_plan"] == [] and out["targets_needed"] == []
    assert out["moonshot_grade"] is False and out["has_falsifiable_plan"] is False
    assert out["gradable"] is False  # no gate -> "under test", not an undercutting "not moonshot-grade"


def test_present_activity_feed_merges_reverse_chron_and_is_honest_about_idle():
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    jobs = [SimpleNamespace(runner_kind="modal", validation_type="docking", compute_profile="gpu_a100",
                            status="completed", candidate_id="c1", updated_at=t0, created_at=t0)]
    runs = [SimpleNamespace(agent_name="active_falsification_planner", status="completed",
                            completed_at=t0.replace(hour=2), started_at=t0, created_at=t0)]
    caps = [SimpleNamespace(payload={"signal": "refutes"}, target=SimpleNamespace(section="omics"),
                            status="submitted", candidate_id="c1", capsule_id="cap1",
                            updated_at=t0.replace(hour=3), created_at=t0)]
    mans = [SimpleNamespace(title="campaign — 3 candidates", status="completed", created_at=t0.replace(hour=1))]
    feed = present.present_activity_feed(agent_runs=runs, compute_jobs=jobs, capsules=caps, manifests=mans)
    # merged reverse-chron: capsule(03:00) > agent(02:00) > campaign(01:00) > compute(00:00)
    assert [e["type"] for e in feed["events"]] == ["capsule", "agent", "campaign", "compute"]
    assert feed["events"][0]["signal"] == "refutes"
    # no running job -> HONEST idle, never faked busy (no pricing surfaced on the public site)
    assert feed["idle"] is True and feed["running_jobs"] == 0 and "no runnable work" in feed["idle_reason"]
    assert "$" not in feed["idle_reason"]


def test_present_activity_feed_busy_and_mock_is_labelled():
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    jobs = [SimpleNamespace(runner_kind="mock", validation_type="docking", compute_profile="cpu",
                            status="running", candidate_id="c1", updated_at=t0, created_at=t0)]
    feed = present.present_activity_feed(compute_jobs=jobs)
    assert feed["idle"] is False and feed["running_jobs"] == 1
    assert "mock (CI)" in feed["events"][0]["title"]  # simulated activity is never passed off as real GPU


def test_present_activity_feed_counts_approved_as_in_flight():
    """A job that's 'approved' (post-gate, pre-dispatch) is work-in-progress — NOT idle. Surfaced by the
    first real Modal campaign, where docks sat in 'approved' and the feed wrongly read idle."""
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    jobs = [SimpleNamespace(runner_kind="modal", validation_type="docking", compute_profile="gpu_a100",
                            status="approved", candidate_id="c1", updated_at=t0, created_at=t0)]
    feed = present.present_activity_feed(compute_jobs=jobs)
    assert feed["idle"] is False and feed["running_jobs"] == 1


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
