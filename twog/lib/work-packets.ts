import { Pool } from 'pg';

export const WORK_PACKET_TYPES = [
  'citation_repair',
  'claim_critique',
  'evidence_addition',
  'omics_note',
  'docking_replication',
  'md_review',
  'validation_proposal',
  'demotion_case',
  'methods_review',
] as const;

export const WORK_PACKET_STATUSES = ['open', 'in_progress', 'completed', 'retired'] as const;
export const WORK_PACKET_DIFFICULTIES = ['light', 'moderate', 'heavy'] as const;
export const OPEN_WORK_PACKET_STATUSES: ReadonlyArray<WorkPacketStatus> = ['open', 'in_progress'];

export type WorkPacketType = (typeof WORK_PACKET_TYPES)[number];
export type WorkPacketStatus = (typeof WORK_PACKET_STATUSES)[number];
export type WorkPacketDifficulty = (typeof WORK_PACKET_DIFFICULTIES)[number];

// Public boundary: this shape backs GET /api/work-packets/:id and friends.
// Operator audit fields (created_by, retired_reason, internal metadata) are
// intentionally absent so the route handler cannot leak them by accident.
export interface WorkPacketPublicRecord {
  work_packet_id: string;
  candidate_id: string;
  packet_type: WorkPacketType;
  title: string;
  question: string;
  why_it_matters: string;
  target_claim_ids: string[];
  target_section: string | null;
  required_inputs: string[];
  suggested_methods: string[];
  expected_outputs: string[];
  acceptance_criteria: string[];
  reward_hint: string;
  difficulty: WorkPacketDifficulty;
  status: WorkPacketStatus;
  notebook_recommended: boolean;
  created_at: string;
  updated_at: string;
  url: string;
  checkout_url: string;
}

declare global {
  var twogWorkPacketPool: Pool | undefined;
}

export class WorkPacketError extends Error {
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

export function isWorkPacketStorageConfigured(): boolean {
  return Boolean(databaseUrl());
}

function pool(): Pool {
  const connectionString = databaseUrl();
  if (!connectionString) {
    throw new WorkPacketError(
      'Work packet storage is not configured.',
      503,
      'work_packet_storage_not_configured',
      ['Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL.']
    );
  }
  if (!globalThis.twogWorkPacketPool) {
    globalThis.twogWorkPacketPool = new Pool({
      connectionString,
      max: 3,
      ssl: { rejectUnauthorized: false },
    });
  }
  return globalThis.twogWorkPacketPool;
}

// Defensive schema bootstrap. Hosted environments should rely on
// twog/db/migrations/003_work_packets.sql for the authoritative shape; this
// idempotent block lets the public read path survive a missed migration.
export async function ensureWorkPacketSchema() {
  await pool().query(`
    create table if not exists work_packets (
      work_packet_id uuid primary key,
      candidate_id text not null,
      packet_type text not null,
      title text not null,
      question text not null,
      why_it_matters text not null default '',
      target_claim_ids jsonb not null default '[]'::jsonb,
      target_section text,
      required_inputs jsonb not null default '[]'::jsonb,
      suggested_methods jsonb not null default '[]'::jsonb,
      expected_outputs jsonb not null default '[]'::jsonb,
      acceptance_criteria jsonb not null default '[]'::jsonb,
      reward_hint text not null default '',
      difficulty text not null default 'moderate',
      status text not null default 'open',
      notebook_recommended boolean not null default false,
      created_by text not null default 'twog_system',
      retired_reason text,
      payload jsonb not null,
      created_at timestamptz not null default now(),
      updated_at timestamptz not null default now()
    );

    create index if not exists work_packets_candidate_status_idx
      on work_packets(candidate_id, status, updated_at desc);
    create index if not exists work_packets_status_created_idx
      on work_packets(status, created_at desc);
    create index if not exists work_packets_type_status_idx
      on work_packets(packet_type, status, updated_at desc);
  `);
}

function asStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter((item): item is string => typeof item === 'string')
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

function packetUrl(id: string): string {
  return `/api/work-packets/${encodeURIComponent(id)}`;
}

function packetCheckoutUrl(id: string): string {
  return `/api/work-packets/${encodeURIComponent(id)}/checkout`;
}

interface WorkPacketRow {
  work_packet_id: string;
  candidate_id: string;
  packet_type: string;
  title: string;
  question: string;
  why_it_matters: string | null;
  target_claim_ids: unknown;
  target_section: string | null;
  required_inputs: unknown;
  suggested_methods: unknown;
  expected_outputs: unknown;
  acceptance_criteria: unknown;
  reward_hint: string | null;
  difficulty: string;
  status: string;
  notebook_recommended: boolean | null;
  created_at: Date | string;
  updated_at: Date | string;
}

function toPublicRecord(row: WorkPacketRow): WorkPacketPublicRecord {
  return {
    work_packet_id: row.work_packet_id,
    candidate_id: row.candidate_id,
    packet_type: row.packet_type as WorkPacketType,
    title: row.title,
    question: row.question,
    why_it_matters: row.why_it_matters ?? '',
    target_claim_ids: asStringList(row.target_claim_ids),
    target_section: row.target_section,
    required_inputs: asStringList(row.required_inputs),
    suggested_methods: asStringList(row.suggested_methods),
    expected_outputs: asStringList(row.expected_outputs),
    acceptance_criteria: asStringList(row.acceptance_criteria),
    reward_hint: row.reward_hint ?? '',
    difficulty: (row.difficulty || 'moderate') as WorkPacketDifficulty,
    status: (row.status || 'open') as WorkPacketStatus,
    notebook_recommended: Boolean(row.notebook_recommended),
    created_at: row.created_at instanceof Date ? row.created_at.toISOString() : String(row.created_at),
    updated_at: row.updated_at instanceof Date ? row.updated_at.toISOString() : String(row.updated_at),
    url: packetUrl(row.work_packet_id),
    checkout_url: packetCheckoutUrl(row.work_packet_id),
  };
}

const PUBLIC_SELECT_COLUMNS = `
  work_packet_id::text as work_packet_id,
  candidate_id,
  packet_type,
  title,
  question,
  why_it_matters,
  target_claim_ids,
  target_section,
  required_inputs,
  suggested_methods,
  expected_outputs,
  acceptance_criteria,
  reward_hint,
  difficulty,
  status,
  notebook_recommended,
  created_at,
  updated_at
`;

function normalizeStatuses(statuses: ReadonlyArray<string> | undefined): WorkPacketStatus[] {
  if (!statuses || statuses.length === 0) {
    return [...OPEN_WORK_PACKET_STATUSES];
  }
  const allowed = new Set<string>(WORK_PACKET_STATUSES);
  const normalized: WorkPacketStatus[] = [];
  for (const status of statuses) {
    const value = String(status).trim();
    if (allowed.has(value) && !normalized.includes(value as WorkPacketStatus)) {
      normalized.push(value as WorkPacketStatus);
    }
  }
  return normalized.length > 0 ? normalized : [...OPEN_WORK_PACKET_STATUSES];
}

function normalizeLimit(limit: number | undefined): number {
  if (limit === undefined) return 25;
  const bounded = Math.max(1, Math.min(Math.trunc(limit), 200));
  return Number.isFinite(bounded) ? bounded : 25;
}

export interface ListWorkPacketsOptions {
  candidate_ids?: ReadonlyArray<string>;
  statuses?: ReadonlyArray<string>;
  packet_types?: ReadonlyArray<string>;
  limit?: number;
}

export async function listWorkPackets(
  options: ListWorkPacketsOptions = {}
): Promise<WorkPacketPublicRecord[]> {
  await ensureWorkPacketSchema();
  const statuses = normalizeStatuses(options.statuses);
  const limit = normalizeLimit(options.limit);
  const candidates = (options.candidate_ids ?? [])
    .map((value) => String(value).trim())
    .filter((value) => value.length > 0);
  const packetTypes = (options.packet_types ?? [])
    .map((value) => String(value).trim())
    .filter((value) => (WORK_PACKET_TYPES as readonly string[]).includes(value));

  const conditions: string[] = ['status = any($1)'];
  const params: unknown[] = [statuses];
  if (candidates.length > 0) {
    conditions.push(`candidate_id = any($${params.length + 1})`);
    params.push(candidates);
  }
  if (packetTypes.length > 0) {
    conditions.push(`packet_type = any($${params.length + 1})`);
    params.push(packetTypes);
  }
  params.push(limit);

  const result = await pool().query<WorkPacketRow>(
    `
      select ${PUBLIC_SELECT_COLUMNS}
      from work_packets
      where ${conditions.join(' and ')}
      order by
        case status when 'open' then 0 when 'in_progress' then 1 when 'completed' then 2 else 3 end,
        updated_at desc
      limit $${params.length}
    `,
    params
  );
  return result.rows.map(toPublicRecord);
}

export async function getWorkPacket(workPacketId: string): Promise<WorkPacketPublicRecord | null> {
  await ensureWorkPacketSchema();
  const result = await pool().query<WorkPacketRow>(
    `
      select ${PUBLIC_SELECT_COLUMNS}
      from work_packets
      where work_packet_id::text = $1
      limit 1
    `,
    [workPacketId]
  );
  return result.rows[0] ? toPublicRecord(result.rows[0]) : null;
}
