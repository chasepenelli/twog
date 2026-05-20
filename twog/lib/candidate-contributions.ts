import { Pool } from 'pg';
import { randomUUID } from 'node:crypto';
import type { PublicCandidateDetail } from '@/lib/public-candidates';
import { publicCandidatePayloadPath } from '@/lib/public-candidates';

const CONTRIBUTION_TYPES = [
  'evidence',
  'critique',
  'replication',
  'artifact',
  'validation_proposal',
  'compute_result',
] as const;

const RECORD_RELATIONS = [
  'supports',
  'challenges',
  'extends',
  'corrects',
  'requests_validation',
  'requests_compute',
] as const;

const REQUESTED_ACTIONS = [
  'evidence_review',
  'citation_repair',
  'validation_packet',
  'omics_readout',
  'docking_or_md_review',
  'no_action',
] as const;

export type ContributionType = (typeof CONTRIBUTION_TYPES)[number];
export type RecordRelation = (typeof RECORD_RELATIONS)[number];
export type RequestedSystemAction = (typeof REQUESTED_ACTIONS)[number];

export interface CandidateContributionPacket {
  contribution_type: ContributionType;
  contributor: {
    name?: string;
    affiliation?: string;
    contact: string;
  };
  title: string;
  summary: string;
  claim_or_question: string;
  relation_to_current_record: RecordRelation;
  evidence: Array<Record<string, unknown>>;
  artifacts: Array<Record<string, unknown>>;
  requested_system_action: RequestedSystemAction;
  conflicts_or_limitations?: string;
}

export interface CandidateContributionRecord {
  contribution_id: string;
  candidate_id: string;
  display_id?: string | null;
  snapshot_content_hash?: string | null;
  status: string;
  contribution_type: string;
  relation_to_current_record: string;
  requested_system_action: string;
  source_payload_url: string;
  created_at: string;
}

declare global {
  var twogCandidateContributionPool: Pool | undefined;
}

export class CandidateContributionError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly code: string,
    public readonly details?: string[]
  ) {
    super(message);
  }
}

function databaseUrl(): string | undefined {
  return (
    process.env.NEON_DATABASE_URL ??
    process.env.DATABASE_URL ??
    process.env.POSTGRES_URL ??
    process.env.HSA_DATABASE_URL
  );
}

export function isCandidateContributionStorageConfigured(): boolean {
  return Boolean(databaseUrl());
}

function pool(): Pool {
  const connectionString = databaseUrl();
  if (!connectionString) {
    throw new CandidateContributionError(
      'Candidate contribution storage is not configured.',
      503,
      'candidate_contribution_storage_not_configured',
      ['Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL.']
    );
  }

  if (!globalThis.twogCandidateContributionPool) {
    globalThis.twogCandidateContributionPool = new Pool({
      connectionString,
      max: 3,
      ssl: { rejectUnauthorized: false },
    });
  }

  return globalThis.twogCandidateContributionPool;
}

export async function ensureCandidateContributionSchema() {
  await pool().query(`
    create table if not exists candidate_contribution_intake (
      contribution_id uuid primary key,
      candidate_id text not null,
      display_id text,
      snapshot_content_hash text,
      source_payload_url text not null,
      status text not null default 'queued_for_intake',
      contribution_type text not null,
      relation_to_current_record text not null,
      requested_system_action text not null,
      contributor jsonb not null default '{}'::jsonb,
      evidence jsonb not null default '[]'::jsonb,
      artifacts jsonb not null default '[]'::jsonb,
      packet jsonb not null,
      review_notes text,
      promoted_queue_id text,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      reviewed_at timestamptz,
      constraint candidate_contribution_intake_status_check
        check (status in (
          'queued_for_intake',
          'triage_in_progress',
          'needs_more_information',
          'rejected',
          'accepted_for_evidence_review',
          'accepted_for_validation_queue',
          'accepted_for_compute_review',
          'archived'
        )),
      constraint candidate_contribution_intake_type_check
        check (contribution_type in (
          'evidence',
          'critique',
          'replication',
          'artifact',
          'validation_proposal',
          'compute_result'
        ))
    );

    create index if not exists candidate_contribution_intake_candidate_created_idx
      on candidate_contribution_intake (candidate_id, created_at desc);

    create index if not exists candidate_contribution_intake_status_created_idx
      on candidate_contribution_intake (status, created_at desc);

    create index if not exists candidate_contribution_intake_action_created_idx
      on candidate_contribution_intake (requested_system_action, created_at desc);
  `);
}

function asObject(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function asText(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

function asArrayOfObjects(value: unknown, limit: number): Array<Record<string, unknown>> {
  if (!Array.isArray(value)) return [];
  return value.slice(0, limit).filter((item) => item && typeof item === 'object' && !Array.isArray(item)) as Array<
    Record<string, unknown>
  >;
}

function pickAllowed<T extends readonly string[]>(value: string, allowed: T): T[number] | undefined {
  return (allowed as readonly string[]).includes(value) ? (value as T[number]) : undefined;
}

export function normalizeCandidateContributionPacket(body: unknown): CandidateContributionPacket {
  const raw = asObject(body);
  const source = asObject(raw.contribution_packet ?? raw);
  const contributor = asObject(source.contributor);
  const errors: string[] = [];

  const contributionType = pickAllowed(asText(source.contribution_type), CONTRIBUTION_TYPES);
  const relation = pickAllowed(asText(source.relation_to_current_record), RECORD_RELATIONS);
  const requestedAction = pickAllowed(asText(source.requested_system_action), REQUESTED_ACTIONS);
  const title = asText(source.title);
  const summary = asText(source.summary);
  const claimOrQuestion = asText(source.claim_or_question);
  const contact = asText(contributor.contact);

  if (!contributionType) errors.push(`contribution_type must be one of: ${CONTRIBUTION_TYPES.join(', ')}`);
  if (!relation) errors.push(`relation_to_current_record must be one of: ${RECORD_RELATIONS.join(', ')}`);
  if (!requestedAction) errors.push(`requested_system_action must be one of: ${REQUESTED_ACTIONS.join(', ')}`);
  if (title.length < 6) errors.push('title must be at least 6 characters.');
  if (summary.length < 20) errors.push('summary must be at least 20 characters.');
  if (claimOrQuestion.length < 12) errors.push('claim_or_question must be at least 12 characters.');
  if (contact.length < 6) errors.push('contributor.contact is required for follow-up.');

  if (errors.length > 0 || !contributionType || !relation || !requestedAction) {
    throw new CandidateContributionError(
      'Candidate contribution packet is invalid.',
      400,
      'invalid_candidate_contribution_packet',
      errors
    );
  }

  return {
    contribution_type: contributionType,
    contributor: {
      name: asText(contributor.name) || undefined,
      affiliation: asText(contributor.affiliation) || undefined,
      contact,
    },
    title,
    summary,
    claim_or_question: claimOrQuestion,
    relation_to_current_record: relation,
    evidence: asArrayOfObjects(source.evidence, 25),
    artifacts: asArrayOfObjects(source.artifacts, 25),
    requested_system_action: requestedAction,
    conflicts_or_limitations: asText(source.conflicts_or_limitations) || undefined,
  };
}

export async function createCandidateContribution(
  candidate: PublicCandidateDetail,
  packet: CandidateContributionPacket
): Promise<CandidateContributionRecord> {
  await ensureCandidateContributionSchema();

  const record = candidate.candidate;
  const contributionId = randomUUID();
  const sourcePayloadUrl = publicCandidatePayloadPath(record.candidate_id);
  const snapshotContentHash = record.content_hash ?? candidate.latest_snapshot?.content_hash ?? null;

  const result = await pool().query<CandidateContributionRecord>(
    `
      insert into candidate_contribution_intake (
        contribution_id,
        candidate_id,
        display_id,
        snapshot_content_hash,
        source_payload_url,
        contribution_type,
        relation_to_current_record,
        requested_system_action,
        contributor,
        evidence,
        artifacts,
        packet
      )
      values ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb)
      returning
        contribution_id,
        candidate_id,
        display_id,
        snapshot_content_hash,
        status,
        contribution_type,
        relation_to_current_record,
        requested_system_action,
        source_payload_url,
        created_at
    `,
    [
      contributionId,
      record.candidate_id,
      record.display_id ?? null,
      snapshotContentHash,
      sourcePayloadUrl,
      packet.contribution_type,
      packet.relation_to_current_record,
      packet.requested_system_action,
      JSON.stringify(packet.contributor),
      JSON.stringify(packet.evidence),
      JSON.stringify(packet.artifacts),
      JSON.stringify(packet),
    ]
  );

  return result.rows[0];
}
