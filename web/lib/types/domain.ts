/**
 * Domain model for the twog comparative-oncology FALSIFICATION platform.
 *
 * Source of truth = the backend contract. These types MIRROR that model; do NOT
 * redesign or invent behavior beyond what is described here. The platform tries to
 * DISPROVE hypotheses. Nothing is ever auto-promoted — a human operator holds the
 * terminal accept/promote gate.
 *
 * SCAFFOLD owns this file. Other specialists import from "@/lib/types/domain".
 */

// ---------------------------------------------------------------------------
// Collaborators & access control
// ---------------------------------------------------------------------------

/** A scope grants permission to perform a specific action. */
export type Scope =
  | "lease_workspace"
  | "submit_compute"
  | "submit_capsule"
  | "accept_capsule" // operator-only
  | "promote_candidate"; // operator-only

/** Scopes that may only ever be held by an operator. */
export const OPERATOR_ONLY_SCOPES = [
  "accept_capsule",
  "promote_candidate",
] as const satisfies readonly Scope[];

export type CollaboratorRole = "operator" | "collaborator";

export type CollaboratorStatus = "pending" | "active" | "revoked";

export interface Collaborator {
  collaborator_id: string;
  /** Stable principal identity (e.g. auth subject / DID). */
  principal: string;
  name: string;
  role: CollaboratorRole;
  status: CollaboratorStatus;
  scopes: Scope[];
  /** Present when the collaborator holds their own signing key (self-held mode). */
  public_key?: string;
  /** Stable external login identity (WorkOS user id) this principal authenticates as. */
  auth_subject?: string | null;
}

// ---------------------------------------------------------------------------
// Sandbox bundle (leased workspace + how a collaborator may contribute)
// ---------------------------------------------------------------------------

/** The kinds of contributions a sandbox can accept. */
export type ContributionMode =
  | "evidence_capsule"
  | "target_library_entry"
  | "candidate_proposal"
  | "new_lane";

/** One contribution channel exposed by a leased sandbox. */
export interface ContributionModeSpec {
  mode: ContributionMode;
  /** Human description of what this mode produces. */
  what: string;
  /** The artifact type produced by this mode. */
  artifact: string;
  /** Where/how the artifact is produced (e.g. runner, local, off-platform). */
  produced_on: string;
  /** The gate an artifact must pass to be admitted. */
  admission_gate: string;
  /** Entry point (path / command / endpoint) for producing the artifact. */
  entry_point: string;
}

export type GatePolicy = "external_collaborator";

/** Free-form sandbox manifest describing the leased environment. */
export interface SandboxManifest {
  [key: string]: unknown;
}

export interface SandboxBundle {
  workspace_id: string;
  gate_policy: GatePolicy;
  /** ISO-8601 timestamp when the lease expires. */
  lease_expires_at: string;
  sandbox: SandboxManifest;
  contribution_modes: ContributionModeSpec[];
}

// ---------------------------------------------------------------------------
// Candidates (hypotheses under test)
// ---------------------------------------------------------------------------

export interface Candidate {
  candidate_id: string;
  title: string;
  /** Public-facing status string (e.g. "standing", "refuted", "survived"). */
  public_status: string;
  /** Whether the candidate has enough evidence to enter validation. */
  validation_ready: boolean;
  /** References to supporting/refuting evidence (capsule ids, doc ids, etc.). */
  evidence_refs: string[];
  /** Molecular / biological targets associated with the candidate. */
  targets: string[];
}

// ---------------------------------------------------------------------------
// Campaigns (RunManifest) — a falsification run over selected candidates
// ---------------------------------------------------------------------------

/** Aggregate outcome of a campaign. `any_promoted` is ALWAYS false. */
export interface CampaignRollup {
  candidates_selected: number;
  candidates_processed: number;
  total_est_cost_usd: number;
  budget_exhausted: boolean;
  /** Invariant: nothing auto-promotes. */
  any_promoted: false;
  /** Map of terminal_reason -> count. */
  terminal_reasons: Record<string, number>;
  /** Map of hypothesis status -> count (e.g. "standing", "refuted"). */
  leading_hypothesis_status: Record<string, number>;
}

/** Per-candidate row within a campaign report. */
export interface CampaignRow {
  candidate_id: string;
  terminal_reason: string;
  leading_hypothesis_status: string;
  total_est_cost_usd: number;
  capsule_ids: string[];
  /** Candidate-specific plain-language outcome (avoids identical boilerplate per status). */
  plain?: string;
}

export interface RunManifest {
  manifest_id: string;
  runner_kind: string;
  rollup: CampaignRollup;
  rows: CampaignRow[];
  /** Human-readable campaign title. */
  title?: string;
  /** ISO date the campaign ran. */
  ran_at?: string;
}

/** Alias: a Campaign is a RunManifest. */
export type Campaign = RunManifest;

// ---------------------------------------------------------------------------
// Proof capsules (units of evidence reviewed at the write gate)
// ---------------------------------------------------------------------------

/** The direction of evidence a capsule provides for its candidate. */
export type CapsuleSignal = "supports" | "refutes" | "neutral";

/** Outcome of an automated confound / provenance check. */
export type GateVerdict = "pass" | "fail" | "pending" | "unknown";

export interface ProofCapsule {
  capsule_id: string;
  candidate_id: string;
  signal: CapsuleSignal;
  validation_type: string;
  status: string;
  /** Cryptographic signature (present for self-held key submissions). */
  signature?: string;
  /** Verdict of the confound-control gate. */
  confound_verdict?: GateVerdict;
  /** Verdict of the provenance gate. */
  provenance_verdict?: GateVerdict;
  // ---- human-readable view fields (the digestible "receipt") -------------
  /** The plain-language claim this capsule tested. */
  claim?: string;
  /** How it was tested (lane / assay, human phrasing). */
  method?: string;
  /** The headline readout (e.g. "p=0.001", "redock 5.91 Å"). */
  readout?: string;
  /** Plain-language translation of the readout, for non-experts. */
  plain?: string;
  /** Confidence 0..1. */
  confidence?: number;
  /** Stated limitations — how far the claim goes. */
  limitations?: string[];
  /** Who produced it (principal / lab), for attribution. */
  produced_by?: string;
  /** The recomputable content hash (integrity anchor). */
  content_hash?: string | null;
  /** Hash-linked edit chain. */
  parent_content_hash?: string | null;
  lineage_index?: number | null;
  /** The principal that produced + signed it. */
  submitted_by?: string;
  /** The pinned candidate snapshot this capsule is bound to. */
  candidate_snapshot_hash?: string | null;
}

/**
 * The verifiable provenance bundle for a capsule — everything needed to RE-DERIVE trust without
 * trusting the operator: a recomputable content hash, the Ed25519 signature + signer public key +
 * verification verdict, the hash-linked lineage chain, and the deterministic provenance-audit checks.
 */
export interface CapsuleProvenance {
  capsule_id: string;
  content_hash: string | null;
  /** Plain-language description of how to recompute the content hash. */
  hashing: string;
  signed: boolean;
  signature: string | null;
  signer: string | null;
  /** The signer's Ed25519 public key (hex) — anyone can verify the signature with it. */
  signer_public_key: string | null;
  signature_valid: boolean;
  lineage: {
    index: number | null;
    parent_content_hash: string | null;
    chain_ok: boolean | null;
    versions: number | null;
  };
  /** Deterministic provenance-audit verdict; null for in-engine (custodial) capsules. */
  audit: { status?: string; checks_passed: string[]; mismatches: string[] } | null;
  /** The fold → hash → sign → attest → pin pipeline, as labels. */
  pipeline: string[];
  verifiable: boolean;
}

// ---------------------------------------------------------------------------
// Moonshot rubric — the pre-registered "whole shabang" behind a candidate
// ---------------------------------------------------------------------------

/** A pre-registered kill criterion (the anti-p-hacking threshold), flattened for display. */
export interface RubricKillCriterion {
  metric: string;
  comparator: string;
  threshold: string | number;
  /** The signal that KILLS the hypothesis if observed (usually "refutes"). */
  kills_on: string;
  rationale: string;
}

/** A protein the moonshot needs, with its 3-state docking-verification status (the spend gate). */
export interface RubricTarget {
  target: string;
  role: string;
  /** verified = in redock-passed library; unverified = in library, not passed; absent = not curated. */
  verification: "verified" | "unverified" | "absent";
  uniprot?: string | null;
  pdb_id?: string | null;
  chain?: string | null;
  redock_rmsd?: number | null;
  cocrystal_ligand_code?: string | null;
}

/** A ligand the moonshot needs, with SMILES-resolution status (NEVER fabricated). */
export interface RubricCompound {
  name: string;
  role: string;
  smiles?: string | null;
  readiness: "resolved" | "needs_verification" | "missing";
  resolution_source: string;
  intended_targets: string[];
}

/** The MD protocol the moonshot pre-registers (present even when inputs are unresolved). */
export interface RubricMDSchedule {
  simulation_steps: number;
  temperature: number;
  ph?: number | null;
  force_field?: string | null;
  solvent_model?: string | null;
  equilibration: string;
  preparation_method: string;
}

/** Pre-committed reading of every outcome of a lane test (anti-HARKing); vocabulary supports/refutes/neutral. */
export interface RubricInterpretation {
  supports: string;
  refutes: string;
  neutral: string;
}

/** One ordered step in the test plan: a lane + its pre-registered kill criterion + standing + the grounded
 *  reasoning that makes it a STEP IN AN ARGUMENT (what it probes, why it bears, what each outcome means). */
export interface RubricLaneTest {
  order: number;
  lane: string;
  objective: string;
  standing: "untested" | "queued" | "supports_unaudited" | "refuted" | "controlled";
  maturity: "smoke" | "production";
  inputs_ready: boolean;
  is_proposed: boolean;
  autonomously_runnable: boolean;
  est_cost_usd?: number;
  value_of_information?: number;
  kill_criterion: RubricKillCriterion | null;
  expected_signal_if_alive: string;
  /** The confound kind this test audits, if any. */
  addresses_confound?: string | null;
  /** The specific qualities of the target/mechanism this lane interrogates. */
  probes: string[];
  /** Why this lane bears on the thesis — restates the lane's own kill-criterion rationale. */
  why_it_bears?: string;
  interpretation: RubricInterpretation;
  inputs: {
    readiness: "resolved" | "needs_verification" | "missing";
    resolution_source?: string;
    required_keys: string[];
    missing: string[];
    md_schedule: RubricMDSchedule | null;
  };
}

/** The grounded "because of A" the moonshot rests on. is_specified=false when the thesis states none. */
export interface RubricPremise {
  claim: string;
  basis: string;
  supports_quality?: string;
  /** The idea's stated evidence tier, VERBATIM — never upgraded (the verdict ceiling). */
  strength: "high" | "medium" | "low" | "unknown";
  is_specified: boolean;
}

/** One link in the falsification-first inference chain (conjunctive-survival / single-refutation-kills). */
export interface RubricInferenceLink {
  step: number;
  from_lanes: string[];
  infers: string;
  /** The refutation (the lane's real kill criterion) that breaks the chain HERE. */
  if_broken: string;
}

/** The conclusion the program is trying to EARN — capped at survived_known_confounds. */
export interface RubricExpectedPayoff {
  if_survives: string;
  translational_claim: string;
  next_step: string;
  value_of_information?: number | null;
  is_specified: boolean;
  /** Fixed ceiling string: "survived_known_confounds, never proven; …". */
  caveat: string;
}

export interface RubricConfoundFlag {
  kind: string;
  status: "open" | "controlled" | "unauditable";
  control_lane?: string | null;
}

/** The pre-registered "whole shabang": what a moonshot must show and exactly how it will be tested. */
export interface MoonshotRubric {
  rubric_version: string;
  candidate_id: string;
  title: string;
  thesis: string;
  /** Reasoning spine: the grounded "because of A" premise the argument opens on. */
  mechanistic_premise: string;
  premises: RubricPremise[];
  /** The line of reasoning: if X survives, proceed to Y… (single refutation kills it). */
  inference_chain: RubricInferenceLink[];
  /** What it would mean if the whole chain survives — capped at survived_known_confounds. */
  expected_payoff: RubricExpectedPayoff;
  moonshot_grade: boolean;
  moonshot_score?: number;
  moonshot_gate: { passed: boolean; weighted_score?: number; reasons: string[]; blockers: string[] };
  net_signal: "supports" | "refutes" | "neutral" | "none";
  net_confidence: number;
  signalful_capsule_count: number;
  has_falsifiable_plan: boolean;
  ready_to_run: boolean;
  runnable_lanes: string[];
  targets_needed: RubricTarget[];
  compounds_needed: RubricCompound[];
  test_plan: RubricLaneTest[];
  inputs_rollup: {
    resolved_lanes: string[];
    needs_verification_lanes: string[];
    missing_lanes: string[];
    ready_to_run_lanes: string[];
    blockers: string[];
  };
  confounds: { open: RubricConfoundFlag[]; controlled: RubricConfoundFlag[]; audit_policy: string };
  cross_species: {
    species: string[];
    disease_context?: string;
    replication_axis: string;
    replication_lane?: string | null;
    orthogonal_cohort_required: boolean;
    kill_criterion: RubricKillCriterion | null;
    evidence_to_date: string[];
  };
  promotion: {
    /** Typed invariant — NEVER auto-promoted. */
    auto_promotable: false;
    required_surviving_lanes: string[];
    required_confounds_controlled: string[];
    cross_species_replication_required: boolean;
    min_signalful_capsules: number;
    statement: string;
  };
  evidence_anchors: string[];
  risks: string[];
  assembly_notes: string[];
}

// ---------------------------------------------------------------------------
// Verified-target catalog
// ---------------------------------------------------------------------------

/**
 * A verified-target catalog entry — the admitted artifact of the
 * `target_library_entry` contribution mode. `verdict` records whether the target
 * survived structural + expression verification ("verified") or was rejected
 * ("refused"); falsification-first language carries here too.
 */
export interface TargetLibraryEntry {
  target_id: string;
  gene: string;
  /** Outcome of the verification gate. */
  verdict: "verified" | "refused";
  /** Source structure (e.g. PDB id) when verified. */
  structure_ref?: string;
  /** One-line rationale for the verdict. */
  note: string;
  /** Candidate ids that reference this target. */
  candidate_refs: string[];
}

// ---------------------------------------------------------------------------
// Engine state (the public STATE / "research state" home)
// ---------------------------------------------------------------------------

export interface ComputeLaneState {
  lane: string;
  sublabel: string;
  compute: string;
  status: "verified" | "running" | "failed";
  lastResult: string;
}

export interface LoopStep {
  key: string;
  title: string;
  blurb: string;
  live?: boolean;
}

export interface EngineState {
  online: boolean;
  context: string; // "canine HSA × human AS"
  phase: string; // "phase 0 locked"
  tracks: string; // "free track live · paid track running"
  headline: {
    hypothesesFalsified: number;
    validatedResults: number;
    computeLanes: number;
    testsPassing: number;
    coverage: string; // "76.5%"
    bestRedockRmsd: string; // "1.80 Å"
  };
  loop: LoopStep[];
  lanes: ComputeLaneState[];
}

// ---------------------------------------------------------------------------
// Live engine feed — the REAL activity ledger (not a scripted narration)
// ---------------------------------------------------------------------------

/** One real event from the engine's ledgers: an agent reacting, a GPU lane dispatching, an evidence
 *  capsule landing, or a campaign running. Carries the record's actual status + timestamp. */
export interface ActivityEvent {
  type: "compute" | "agent" | "capsule" | "campaign";
  occurred_at: string | null;
  status: string;
  title: string;
  candidate_id?: string | null;
  lane?: string | null;
  signal?: "supports" | "refutes" | "neutral" | null;
  capsule_id?: string | null;
}

/** The merged, reverse-chronological activity feed + an HONEST idle/online signal derived from real job
 *  state (the engine genuinely idles $0 when out of runnable work). */
export interface ActivityFeed {
  events: ActivityEvent[];
  running_jobs: number;
  idle: boolean;
  idle_reason: string | null;
  last_event_at: string | null;
}
