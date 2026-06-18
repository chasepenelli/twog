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
