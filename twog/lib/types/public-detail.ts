// Display shapes for the public runs / evidence surfaces — a subset of the /web app's domain types,
// derived from the engine's Neon payloads (run_manifests / proof_capsules). No cost is ever surfaced.

export type Verdict = 'still-standing' | 'ruled-out' | 'needs-more';

export interface RunRollup {
  any_promoted: boolean;
  candidates_selected: number;
  candidates_processed: number;
  terminal_reasons: Record<string, number>;
  /** keys: standing / refuted / underpowered */
  leading_hypothesis_status: Record<string, number>;
}

export interface RunRow {
  candidate_id: string;
  promoted: boolean;
  rounds_run: number;
  capsule_ids: string[];
  compute_job_ids: string[];
  terminal_reason: string;
  leading_hypothesis_status: string;
}

export interface RunManifest {
  manifest_id: string;
  runner_kind: string;
  title: string;
  ran_at: string | null;
  rollup: RunRollup;
  rows: RunRow[];
}

export interface Capsule {
  capsule_id: string;
  candidate_id: string;
  status: string;
  signal: string | null; // refutes / supports / neutral
  validation_type: string | null; // docking / cofolding / md / omics
  claim: string;
  method: string;
  readout: string;
  why_it_matters: string;
  limitations: string[];
  cross_species: string | null; // public cross-species caveat for structure lanes (human ortholog used)
  held: boolean; // blocked at the confound gate (unauditable) — not confirmed evidence
  held_reason: string | null; // why it's held (e.g. missing pose-instability control)
  confidence: number | null;
  content_hash: string | null;
  preregistration: Record<string, unknown> | null; // falsification_preregistration (the locked kill-criterion)
  metrics: Record<string, unknown> | null;
  lineage_index: number | null;
  submitted_by: string | null;
  produced_by: string | null;
}

export interface Provenance {
  capsule_id: string;
  content_hash: string | null;
  signed: boolean;
  signature: string | null;
  signer: string | null;
  signature_valid: boolean | null;
  lineage_index: number | null;
}

// A public candidate (an "idea": drug × target) read live from Neon public_candidates. The autonomous
// falsification roster is lean — title (the question), targets, biomarkers, evidence_refs, status — so
// these shapes stay lean and surface richer narrative fields only when they're actually populated.
export interface CandidateCore {
  candidate_id: string;
  display_id: string | null; // human id (e.g. TWOG-…); null for the autonomous slug roster
  title: string; // raw, e.g. "Falsify: does carvedilol engage KDR …?"
  question: string; // publicQuestion(title) — the plain-language ask
  targets: string[];
  biomarkers: string[];
  public_status: string | null;
  validation_ready: boolean;
  priority_score: number | null;
  kind: string | null;
}

export interface CandidateSummary extends CandidateCore {
  verdict: Verdict; // derived from this candidate's capsule signals (still-standing / ruled-out / needs-more)
}

export interface CandidateDetail extends CandidateCore {
  evidence_refs: string[];
  content_hash: string | null;
  updated_at: string | null;
  // optional narrative — present only if non-empty in the payload (most autonomous candidates omit these)
  summary?: string;
  mechanism?: string;
  rationale_md?: string;
}
