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
