import dataset from '@/data/public-candidates.json';

export type StringMap = Record<string, string | number | boolean | null | undefined>;

export interface PublicCandidateRecord {
  candidate_id: string;
  display_id?: string;
  title?: string;
  summary?: string;
  public_status?: string;
  candidate_kind?: string;
  candidate_therapies?: string[];
  targets?: string[];
  biomarkers?: string[];
  evidence_refs?: string[];
  risk_flags?: string[];
  priority_score?: number;
  content_hash?: string;
  updated_at?: string;
  rationale_md?: string;
}

export interface PublicCandidateSnapshot {
  snapshot_id?: string;
  content_hash?: string;
  snapshot_version?: number;
  pipeline_version?: string;
  created_at?: string;
  payload?: {
    rationale?: {
      hypothesis?: string;
      rationale_md?: string;
      mechanism?: string;
      translational_path?: string;
    };
    biology?: {
      targets?: string[];
      candidate_therapies?: string[];
      biomarkers?: string[];
    };
    evidence?: {
      evidence_refs?: string[];
      evidence_strength?: string;
      risks?: string[];
      next_experiments?: string[];
      validation_decisions?: CandidateValidationDecision[];
    };
    computational_evidence?: Array<Record<string, unknown>>;
    artifacts?: Array<Record<string, unknown>>;
    linked_records?: {
      compute_job_ids?: string[];
      therapy_idea_id?: string;
      validation_decision_ids?: string[];
    };
    literature?: LiteratureRecord[];
    reproducibility?: {
      pipeline_version?: string;
      commit_sha?: string;
      content_hash?: string;
      generated_at?: string;
    };
  };
}

export interface LiteratureRecord {
  ref?: string;
  title?: string;
  source_url?: string;
  source_key?: string;
  evidence_kind?: string;
  identifiers?: StringMap;
  supports?: string;
  resolved?: boolean;
  publication_year?: number;
  published_at?: string;
  section_labels?: string[];
  provenance?: {
    chunk_ids?: string[];
    research_object_ids?: string[];
    dedupe_keys?: string[];
  };
  dedupe?: {
    duplicate_count?: number;
    duplicate_citation_ids?: string[];
    merged_citation_ids?: string[];
    primary_citation_id?: string;
  };
}

export interface CandidateDecisionEvent {
  action?: string;
  actor?: string;
  occurred_at?: string;
  rationale_md?: string;
  prior_status?: string | null;
  new_status?: string | null;
}

export interface CandidateValidationDecision {
  decision_id?: string;
  outcome?: string;
  summary?: string;
  confidence?: number;
  validation_ready?: boolean;
  broader_program_signal?: string;
  specific_claim_viability?: string;
  blocking_reasons?: string[];
  confidence_changers?: string[];
  packet_id?: string;
}

export interface PublicCandidateDetail {
  candidate: PublicCandidateRecord;
  latest_snapshot?: PublicCandidateSnapshot | null;
  decision_events?: CandidateDecisionEvent[];
}

interface PublicCandidateDataset {
  generatedAt: string;
  source: string;
  candidates: PublicCandidateDetail[];
}

export const publicCandidateDataset = dataset as PublicCandidateDataset;

export const publicCandidates = publicCandidateDataset.candidates;

export function getFeaturedCandidate(): PublicCandidateDetail | undefined {
  return publicCandidates[0];
}

export function getCandidate(candidateId: string): PublicCandidateDetail | undefined {
  const normalized = candidateId.toLowerCase();
  return publicCandidates.find((entry) => {
    const stableId = entry.candidate.candidate_id.toLowerCase();
    const displayId = entry.candidate.display_id?.toLowerCase();
    return stableId === normalized || displayId === normalized;
  });
}

export function publicCandidatePayloadPath(candidateId: string): string {
  return `/api/public-candidates/${encodeURIComponent(candidateId)}`;
}

export function publicCandidateEvidenceBundlePath(candidateId: string): string {
  return `/api/public-candidates/${encodeURIComponent(candidateId)}/evidence-bundle`;
}

export function shortHash(value?: string | null): string {
  if (!value) return 'pending';
  return value.slice(0, 12);
}

export function formatPublicDate(value?: string | null): string {
  if (!value) return 'Not recorded';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat('en', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  }).format(date);
}

export function readableKind(value?: string | null): string {
  if (!value) return 'supporting context';
  return value.replaceAll('_', ' ');
}
