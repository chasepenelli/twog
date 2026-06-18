/**
 * Mock fixtures for the twog falsification platform.
 *
 * These back EVERY api-client call when NEXT_PUBLIC_USE_MOCKS is on (the v1
 * default — there is no live backend yet). Fixtures are deliberately realistic:
 * a campaign whose rollup proves `any_promoted: false`, candidates in a mix of
 * states (standing / refuted / underpowered), pending + active collaborators, a
 * sandbox bundle exposing all four contribution modes, capsules carrying both
 * confound + provenance verdicts, and a verified-target catalog where MTOR was
 * REFUSED. Voice is falsification-first throughout ("standing", "refuted",
 * "nothing auto-promoted").
 *
 * API-CLIENT owns this file. lib/api/* imports these.
 */

import type {
  Candidate,
  Collaborator,
  MoonshotRubric,
  ProofCapsule,
  RunManifest,
  SandboxBundle,
  TargetLibraryEntry,
} from "@/lib/types/domain";

// ---------------------------------------------------------------------------
// Collaborators (pending + active + an operator)
// ---------------------------------------------------------------------------

export const MOCK_COLLABORATORS: Collaborator[] = [
  {
    collaborator_id: "col_operator_root",
    principal: "auth0|operator-root",
    name: "Chase Penelli",
    role: "operator",
    status: "active",
    scopes: [
      "lease_workspace",
      "submit_compute",
      "submit_capsule",
      "accept_capsule",
      "promote_candidate",
    ],
  },
  {
    collaborator_id: "col_active_kestrel",
    principal: "did:key:z6Mk-kestrel-lab",
    name: "Kestrel Comparative Oncology Lab",
    role: "collaborator",
    status: "active",
    scopes: ["lease_workspace", "submit_compute", "submit_capsule"],
    public_key: "z6MkpTHR8V2yQv7kestrelXXXXXXXXXXXXXXXXXXXXXX",
  },
  {
    collaborator_id: "col_active_marrow",
    principal: "auth0|marrow-vet-net",
    name: "Marrow Veterinary Network",
    role: "collaborator",
    status: "active",
    scopes: ["lease_workspace", "submit_capsule"],
  },
  {
    collaborator_id: "col_pending_dunes",
    principal: "auth0|dunes-canine-genomics",
    name: "Dunes Canine Genomics",
    role: "collaborator",
    status: "pending",
    scopes: [],
  },
  {
    collaborator_id: "col_pending_holloway",
    principal: "did:key:z6Mk-holloway",
    name: "Holloway Translational Group",
    role: "collaborator",
    status: "pending",
    scopes: [],
    public_key: "z6MkHollowayYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY",
  },
  {
    collaborator_id: "col_revoked_orsa",
    principal: "auth0|orsa-bio",
    name: "Orsa Bio (former)",
    role: "collaborator",
    status: "revoked",
    scopes: [],
  },
];

// ---------------------------------------------------------------------------
// Candidates (mix of validation_ready; falsification-first statuses)
// ---------------------------------------------------------------------------

export const MOCK_CANDIDATES: Candidate[] = [
  {
    candidate_id: "cand_pik3ca_immunosuppr",
    title: "PIK3CA activation drives tumor-microenvironment immunosuppression",
    public_status: "standing",
    validation_ready: true,
    evidence_refs: ["cap_pik3ca_supports", "doc_megquier_cohort"],
    targets: ["PIK3CA"],
  },
  {
    candidate_id: "cand_kdr_angiogenesis",
    title: "KDR/VEGFR2 inhibition slows canine hemangiosarcoma progression",
    public_status: "standing",
    validation_ready: true,
    evidence_refs: ["cap_kdr_supports", "cap_kdr_neutral"],
    targets: ["KDR", "VEGFA"],
  },
  {
    candidate_id: "cand_mtor_alpelisib_synergy",
    title: "MTOR co-inhibition is required for alpelisib durability",
    public_status: "refuted",
    validation_ready: false,
    evidence_refs: ["cap_mtor_refutes"],
    targets: ["MTOR", "PIK3CA"],
  },
  {
    candidate_id: "cand_braf_osteosarcoma",
    title: "BRAF V600E is recurrent in canine osteosarcoma",
    public_status: "underpowered",
    validation_ready: false,
    evidence_refs: [],
    targets: ["BRAF"],
  },
];

// ---------------------------------------------------------------------------
// Moonshot rubrics — the pre-registered "whole shabang" per candidate, keyed by
// candidate_id. A capsule detail page shows its candidate's rubric, so the reader
// sees the full falsification program a capsule is one receipt from.
// ---------------------------------------------------------------------------

export const MOCK_RUBRICS: Record<string, MoonshotRubric> = {
  cand_pik3ca_immunosuppr: {
    rubric_version: "moonshot-rubric-v1",
    candidate_id: "cand_pik3ca_immunosuppr",
    title: "PIK3CA activation drives tumor-microenvironment immunosuppression",
    thesis:
      "Alpelisib engages mutant PI3Kα across the canine-HSA × human-AS axis; the PIK3CA-mutant subset is " +
      "immunosuppressed and should respond to mutation-selective inhibition.",
    mechanistic_premise:
      "alpelisib is hypothesized to engage PIK3CA; mutation-selective PI3Kα inhibition is the load-bearing assumption.",
    premises: [
      {
        claim: "alpelisib is hypothesized to engage PIK3CA",
        basis: "Mutation-selective PI3Kα inhibition; cross-species precision strategy (canine HSA × human AS).",
        supports_quality: "mutation-selective inhibition at the PIK3CA pocket",
        strength: "medium",
        is_specified: true,
      },
    ],
    inference_chain: [
      {
        step: 1, from_lanes: ["docking"],
        infers: "If docking survives (engagement at PIK3CA/4JPS), treat mutation-selective inhibition as live and proceed to omics.",
        if_broken: "A refuting docking readout (gnina_cnn_affinity < 4.0) breaks the chain at step 1; the leading claim dies — any single refutation is sufficient.",
      },
      {
        step: 2, from_lanes: ["omics"],
        infers: "If the cross-species axis replicates, every pre-registered necessary condition has survived its kill criterion -> the claim has survived_known_confounds.",
        if_broken: "A refuting cross-species axis (cross_species_axis_direction signal_is refutes) breaks the chain at step 2; engagement without replication is not the thesis.",
      },
      {
        step: 3, from_lanes: ["md"],
        infers: "If MD survives, the docked pose is stable rather than a single-pose artifact.",
        if_broken: "A refuting MD readout (ligand_pocket_rmsd_nm > 0.5) breaks the chain at step 3.",
      },
    ],
    expected_payoff: {
      if_survives:
        "If all lanes survive their pre-registered kill criteria and the open confounds (tumor_purity) are controlled, the PIK3CA-mutant canine-HSA × human-AS subset becomes a mutation-selective treatment candidate worth advancing.",
      translational_claim: "a mutation-selective treatment candidate worth advancing",
      next_step: "Confirm the cross-species axis in an orthogonal mutant-vs-WT cohort.",
      value_of_information: 0.73,
      is_specified: true,
      caveat: "survived_known_confounds, never proven; the operator write-gate is terminal.",
    },
    moonshot_grade: true,
    moonshot_score: 0.9,
    moonshot_gate: {
      passed: true,
      weighted_score: 0.9,
      reasons: ["frontier_weighted_score>=0.80", "evidence_anchor_present", "mechanism_targets_and_therapy_defined"],
      blockers: [],
    },
    net_signal: "supports",
    net_confidence: 0.38,
    signalful_capsule_count: 1,
    has_falsifiable_plan: true,
    ready_to_run: true,
    runnable_lanes: ["docking", "md", "omics"],
    targets_needed: [
      { target: "PIK3CA", role: "primary", verification: "verified", uniprot: "P42336", pdb_id: "4JPS", chain: "A", redock_rmsd: 1.8, cocrystal_ligand_code: "1E8" },
    ],
    compounds_needed: [
      { name: "alpelisib", role: "lead", smiles: "CC(C)(C(=O)NC1=CN=C(C=C1)C(F)(F)F)N", readiness: "resolved", resolution_source: "pubchem", intended_targets: ["PIK3CA"] },
    ],
    test_plan: [
      {
        order: 1, lane: "docking", objective: "Dock alpelisib against the verified PIK3CA pocket to test engagement.",
        standing: "supports_unaudited", maturity: "production", inputs_ready: true, is_proposed: false,
        autonomously_runnable: true, est_cost_usd: 0.1, value_of_information: 0.8,
        kill_criterion: { metric: "gnina_cnn_affinity", comparator: "<", threshold: 4.0, kills_on: "refutes", rationale: "No measurable engagement refutes target-mediated action." },
        expected_signal_if_alive: "supports", addresses_confound: null,
        probes: ["Can the modeled pose let alpelisib occupy the site at PIK3CA (PDB 4JPS) that mutation-selective PI3Kα inhibition requires?"],
        why_it_bears: "This lane interrogates mutation-selective PI3Kα inhibition — the docking precondition the thesis depends on. No measurable engagement refutes target-mediated action.",
        interpretation: {
          supports: "Consistent with mutation-selective PI3Kα inhibition; the mechanism stays live but remains unaudited — never accepted as proof.",
          refutes: "Observed gnina_cnn_affinity < 4.0 -> no measurable engagement refutes target-mediated action. The target-mediated story for alpelisib collapses here.",
          neutral: "Inconclusive; belief unmoved — the lane does not license the next inference link.",
        },
        inputs: { readiness: "resolved", resolution_source: "curated_library", required_keys: ["receptor_pdb", "ligand_smiles"], missing: [], md_schedule: null },
      },
      {
        order: 2, lane: "omics", objective: "Audit the tumor-purity confound before trusting the supporting omics signal.",
        standing: "queued", maturity: "production", inputs_ready: true, is_proposed: true,
        autonomously_runnable: true, est_cost_usd: 0.02, value_of_information: 0.9,
        kill_criterion: { metric: "cross_species_axis_direction", comparator: "signal_is", threshold: "refutes", kills_on: "refutes", rationale: "A refuting cross-species axis kills the translational claim." },
        expected_signal_if_alive: "supports", addresses_confound: "tumor_purity",
        probes: ["Does the PIK3CA mutation axis move in the SAME direction across canine HSA × human AS?"],
        why_it_bears: "This lane interrogates the cross-species replication the whole moonshot rests on — and is where the tumor_purity confound lives. A refuting axis kills the translational claim.",
        interpretation: {
          supports: "Consistent with the cross-species axis; remains unaudited (tumor_purity not yet controlled) — never accepted as proof.",
          refutes: "Observed cross_species_axis_direction signal_is refutes -> a refuting axis kills the translational claim, even if every binding lane survived.",
          neutral: "Inconclusive (neutral); the axis is not established — belief unmoved.",
        },
        inputs: { readiness: "resolved", resolution_source: "candidate.metadata", required_keys: ["expression", "strata"], missing: [], md_schedule: null },
      },
      {
        order: 3, lane: "md", objective: "MD smoke to probe binding-pose stability of the docked complex.",
        standing: "untested", maturity: "smoke", inputs_ready: false, is_proposed: false,
        autonomously_runnable: true, est_cost_usd: 0.25, value_of_information: 0.5,
        kill_criterion: { metric: "ligand_pocket_rmsd_nm", comparator: ">", threshold: 0.5, kills_on: "refutes", rationale: "If the ligand drifts out of the pocket under MD, it is not a stable binder." },
        expected_signal_if_alive: "supports", addresses_confound: null,
        probes: ["Does the docked alpelisib-PIK3CA pose stay seated under dynamics (ligand_pocket_rmsd_nm <= 0.5) — stable engagement vs a single-pose artifact?"],
        why_it_bears: "This lane interrogates whether engagement is stable rather than a single-pose docking artifact. If the ligand drifts out of the pocket under MD, it is not a stable binder.",
        interpretation: {
          supports: "Pose did not drift within the <=1000-step smoke window; this is NOT binding-pose-stability evidence and cannot be read as engagement confirmation.",
          refutes: "Observed ligand_pocket_rmsd_nm > 0.5 -> the ligand drifts out of the pocket; it is not a stable binder. The target-mediated story for alpelisib collapses here.",
          neutral: "Inconclusive; belief unmoved — the lane does not license the next inference link.",
        },
        inputs: {
          readiness: "missing", resolution_source: "", required_keys: ["protein_pdb", "compound_smiles"], missing: ["protein_pdb"],
          md_schedule: {
            simulation_steps: 1000, temperature: 300.0, ph: 7.4, force_field: "protein=amber14; ligand=worker_default_openff_or_gaff",
            solvent_model: "tip3p",
            equilibration: "minimize -> NVT 100ps @300K (heavy-atom restrained) -> NPT 100ps (1 bar, restraints released) -> production sampling.",
            preparation_method: "Apo receptor = the verified structure's chain (waters/ions/HETATM stripped); ligand placed by the docking lane's top pose; protonation at pH 7.4.",
          },
        },
      },
    ],
    inputs_rollup: {
      resolved_lanes: ["docking", "omics"], needs_verification_lanes: [], missing_lanes: ["md"],
      ready_to_run_lanes: ["docking", "omics"], blockers: ["md: protein_pdb"],
    },
    confounds: {
      open: [{ kind: "tumor_purity", status: "open", control_lane: "omics" }],
      controlled: [],
      audit_policy: "No `supports` signal may be accepted until every open confound has a surviving control. The best attainable verdict is 'survived_known_confounds', never 'true'.",
    },
    cross_species: {
      species: ["canine", "human"], disease_context: "canine hemangiosarcoma and human angiosarcoma",
      replication_axis: "PIK3CA-mutant immunosuppression axis, replicated canine HSA → human AS",
      replication_lane: "omics", orthogonal_cohort_required: true,
      kill_criterion: { metric: "cross_species_axis_direction", comparator: "signal_is", threshold: "refutes", kills_on: "refutes", rationale: "A refuting cross-species expression axis kills the translational claim." },
      evidence_to_date: ["GSE95183 canine cohort AUC trend"],
    },
    promotion: {
      auto_promotable: false,
      required_surviving_lanes: ["docking", "md", "omics"],
      required_confounds_controlled: ["tumor_purity"],
      cross_species_replication_required: true,
      min_signalful_capsules: 2,
      statement:
        "Promote only if every test-plan lane survives its pre-registered kill criterion, all open confounds " +
        "are controlled, the cross-species axis replicates in an orthogonal cohort, and an operator accepts the " +
        "resulting capsules. Never auto-promoted.",
    },
    evidence_anchors: ["cap_pik3ca_supports", "doc_megquier_cohort"],
    risks: ["alpelisib hyperglycemia in dogs", "sparse canine PIK3CA-mutant cohorts"],
    assembly_notes: [],
  },
};

// ---------------------------------------------------------------------------
// Proof capsules (carry confound + provenance verdicts)
// ---------------------------------------------------------------------------

export const MOCK_CAPSULES: ProofCapsule[] = [
  {
    capsule_id: "cap_pik3ca_supports",
    candidate_id: "cand_pik3ca_immunosuppr",
    signal: "supports",
    validation_type: "cohort_association",
    status: "submitted",
    confound_verdict: "pass",
    provenance_verdict: "pass",
    claim: "The PIK3CA→endothelial axis is active in human angiosarcoma.",
    method: "Cross-species cohort association, tumor-purity adjusted.",
    readout: "p = 0.001 — after the immune story was falsified and reframed.",
    plain: "The same biology the engine found in dogs shows up in humans — strongly, after it threw out its first wrong guess.",
    confidence: 0.9,
    limitations: ["Public cohort; modest n.", "Association, not intervention."],
    produced_by: "twog engine",
  },
  {
    capsule_id: "cap_kdr_supports",
    candidate_id: "cand_kdr_angiogenesis",
    signal: "supports",
    validation_type: "docking_falsification",
    status: "accepted",
    signature: "ed25519:9f3a…kestrel",
    confound_verdict: "pass",
    provenance_verdict: "pass",
    claim: "Toceranib engages the VEGFR2 pocket.",
    method: "gnina CNN docking into the redock-verified VEGFR2 structure.",
    readout: "−7.19 kcal/mol · CNN pose 0.72 — survived the kill-criterion.",
    plain: "The drug sat snugly in the target pocket — well enough to survive the engine\u2019s hardest attempt to reject it.",
    confidence: 0.72,
    limitations: ["In silico.", "Human structure, conserved-pocket reasoning."],
    produced_by: "Kestrel Comparative Oncology Lab (BYOC)",
  },
  {
    capsule_id: "cap_kdr_neutral",
    candidate_id: "cand_kdr_angiogenesis",
    signal: "neutral",
    validation_type: "expression_panel",
    status: "submitted",
    confound_verdict: "pending",
    provenance_verdict: "pass",
    claim: "VEGFR2 expression alone stratifies HSA subsets.",
    method: "Bulk expression panel across the cohort.",
    readout: "AUC 0.55 — inconclusive; no separation.",
    plain: "This marker alone didn\u2019t tell the two groups apart — a non-result, not a yes or no.",
    confidence: 0.3,
    limitations: ["Underpowered.", "Confound control still pending."],
    produced_by: "twog engine",
  },
  {
    capsule_id: "cap_mtor_refutes",
    candidate_id: "cand_mtor_alpelisib_synergy",
    signal: "refutes",
    validation_type: "docking_falsification",
    status: "submitted",
    confound_verdict: "pass",
    provenance_verdict: "fail",
    claim: "Alpelisib co-engages mTOR (synergy hypothesis).",
    method: "Native-ligand redock QC on the proposed mTOR structure.",
    readout: "redock RMSD 5.91 Å — pose refused by the spyrmsd gate.",
    plain: "The 3-D structure didn\u2019t pass our trust check, so the engine refused to spend compute on it — caught before chasing a ghost.",
    confidence: 0.1,
    limitations: ["The structure failed verification; the input was refused before spend."],
    produced_by: "twog engine",
  },
  {
    capsule_id: "cap_braf_pending",
    candidate_id: "cand_braf_osteosarcoma",
    signal: "neutral",
    validation_type: "variant_call",
    status: "submitted",
    confound_verdict: "unknown",
    provenance_verdict: "pending",
    claim: "BRAF is a recurrent driver in canine osteosarcoma.",
    method: "Variant calling across the canine cohort.",
    readout: "pending — run queued.",
    plain: "This one hasn\u2019t been tested yet — it\u2019s in the queue.",
    confidence: 0.0,
    limitations: ["Not yet run."],
    produced_by: "twog engine",
  },
];

// ---------------------------------------------------------------------------
// Campaign / RunManifest (rollup proves any_promoted=false)
// ---------------------------------------------------------------------------

export const MOCK_CAMPAIGNS: RunManifest[] = [
  {
    manifest_id: "run_2026_06_alpelisib_falsify",
    title: "Alpelisib falsification campaign",
    ran_at: "2026-06-15",
    runner_kind: "modal_gpu_docking",
    rollup: {
      candidates_selected: 4,
      candidates_processed: 3,
      total_est_cost_usd: 0.42,
      budget_exhausted: false,
      any_promoted: false,
      terminal_reasons: {
        hypothesis_standing: 2,
        provenance_gate_failed: 1,
        underpowered_skipped: 1,
      },
      leading_hypothesis_status: {
        standing: 2,
        refuted: 1,
        underpowered: 1,
      },
    },
    rows: [
      {
        candidate_id: "cand_pik3ca_immunosuppr",
        terminal_reason: "hypothesis_standing",
        leading_hypothesis_status: "standing",
        total_est_cost_usd: 0.11,
        capsule_ids: ["cap_pik3ca_supports"],
        plain: "The immunosuppression story was killed, but the reframed PIK3CA\u2192endothelial claim held up.",
      },
      {
        candidate_id: "cand_kdr_angiogenesis",
        terminal_reason: "hypothesis_standing",
        leading_hypothesis_status: "standing",
        total_est_cost_usd: 0.18,
        capsule_ids: ["cap_kdr_supports", "cap_kdr_neutral"],
        plain: "VEGFR2 engagement survived docking; the expression-only angle stayed inconclusive.",
      },
      {
        candidate_id: "cand_mtor_alpelisib_synergy",
        terminal_reason: "provenance_gate_failed",
        leading_hypothesis_status: "refuted",
        total_est_cost_usd: 0.13,
        capsule_ids: ["cap_mtor_refutes"],
        plain: "The proposed mTOR structure failed its trust check \u2014 refused before any compute was spent.",
      },
      {
        candidate_id: "cand_braf_osteosarcoma",
        terminal_reason: "underpowered_skipped",
        leading_hypothesis_status: "underpowered",
        total_est_cost_usd: 0,
        capsule_ids: [],
        plain: "No runnable test resolved for BRAF this round \u2014 it is queued, not yet attempted.",
      },
    ],
  },
  {
    manifest_id: "run_2026_05_vegf_scan",
    title: "VEGF axis scan",
    ran_at: "2026-05-28",
    runner_kind: "modal_gpu_docking",
    rollup: {
      candidates_selected: 2,
      candidates_processed: 2,
      total_est_cost_usd: 0.27,
      budget_exhausted: true,
      any_promoted: false,
      terminal_reasons: {
        hypothesis_standing: 1,
        budget_exhausted: 1,
      },
      leading_hypothesis_status: {
        standing: 1,
        refuted: 1,
      },
    },
    rows: [
      {
        candidate_id: "cand_kdr_angiogenesis",
        terminal_reason: "hypothesis_standing",
        leading_hypothesis_status: "standing",
        total_est_cost_usd: 0.15,
        capsule_ids: ["cap_kdr_supports"],
      },
      {
        candidate_id: "cand_mtor_alpelisib_synergy",
        terminal_reason: "budget_exhausted",
        leading_hypothesis_status: "refuted",
        total_est_cost_usd: 0.12,
        capsule_ids: ["cap_mtor_refutes"],
      },
    ],
  },
];

// ---------------------------------------------------------------------------
// Sandbox bundle (all four contribution modes)
// ---------------------------------------------------------------------------

export const MOCK_SANDBOX_BUNDLE: SandboxBundle = {
  workspace_id: "ws_lease_kestrel_8a1f",
  gate_policy: "external_collaborator",
  lease_expires_at: "2026-06-22T17:00:00Z",
  sandbox: {
    image: "twog/sandbox:v2.1",
    runner: "modal",
    cpu: 4,
    gpu: "A10G",
    mem_gb: 16,
    scratch_gb: 50,
    network: "egress_allowlist",
    allowlist: ["rcsb.org", "ncbi.nlm.nih.gov"],
  },
  contribution_modes: [
    {
      mode: "evidence_capsule",
      what: "A signed unit of evidence (supports/refutes/neutral) for a candidate hypothesis.",
      artifact: "ProofCapsule",
      produced_on: "sandbox_runner",
      admission_gate: "confound + provenance",
      entry_point: "twog submit capsule --candidate <id>",
    },
    {
      mode: "target_library_entry",
      what: "A proposed verified target with structural + expression backing.",
      artifact: "TargetLibraryEntry",
      produced_on: "sandbox_runner",
      admission_gate: "structure + expression verification",
      entry_point: "twog submit target --gene <symbol>",
    },
    {
      mode: "candidate_proposal",
      what: "A new hypothesis to be entered into the falsification queue.",
      artifact: "Candidate",
      produced_on: "local",
      admission_gate: "operator triage",
      entry_point: "twog propose candidate",
    },
    {
      mode: "new_lane",
      what: "A new validation lane (assay / method) for producing evidence.",
      artifact: "LaneSpec",
      produced_on: "off-platform",
      admission_gate: "operator review + reproducibility check",
      entry_point: "twog propose lane",
    },
  ],
};

// ---------------------------------------------------------------------------
// Verified-target catalog (PIK3CA/KDR verified, MTOR refused)
// ---------------------------------------------------------------------------

export const MOCK_TARGET_LIBRARY: TargetLibraryEntry[] = [
  {
    target_id: "tgt_pik3ca",
    gene: "PIK3CA",
    verdict: "verified",
    structure_ref: "PDB:4JPS",
    note: "p110a kinase domain confirmed; alpelisib pocket well-resolved.",
    candidate_refs: ["cand_pik3ca_immunosuppr", "cand_mtor_alpelisib_synergy"],
  },
  {
    target_id: "tgt_kdr",
    gene: "KDR",
    verdict: "verified",
    structure_ref: "PDB:2OH4",
    note: "VEGFR2 kinase domain verified; canine ortholog expression confirmed.",
    candidate_refs: ["cand_kdr_angiogenesis"],
  },
  {
    target_id: "tgt_mtor",
    gene: "MTOR",
    verdict: "refused",
    note: "Pose verification failed (PoseBusters); refused at spend gate — bad MTOR pose.",
    candidate_refs: ["cand_mtor_alpelisib_synergy"],
  },
];
