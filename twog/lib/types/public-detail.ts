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
