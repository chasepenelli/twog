import { Pool } from 'pg';
import { createHash, randomUUID } from 'node:crypto';
import { verifyProofCapsuleSignature } from '@/lib/proof-capsule-signing';
import {
  checkSubmissionRate,
  recordSubmissionRate,
} from '@/lib/proof-capsule-rate';
import { computeQualityFlags } from '@/lib/proof-capsule-quality';

export const PROOF_CAPSULE_TYPES = [
  'citation_repair',
  'claim_critique',
  'evidence_addition',
  'omics_note',
  'docking_replication',
  'md_review',
  'validation_proposal',
  'demotion_case',
  'methods_review',
  'freeform',
] as const;

export const PROOF_CAPSULE_STATUSES = [
  'submitted',
  'in_review',
  'needs_changes',
  'accepted',
  'rejected',
  'archived',
  'routed_to_validation',
  'routed_to_compute_review',
] as const;

export const PROOF_CAPSULE_PENDING_STATUSES = [
  'submitted',
  'in_review',
  'needs_changes',
] as const;

export const PROOF_CAPSULE_ACCEPTED_STATUSES = [
  'accepted',
  'routed_to_validation',
  'routed_to_compute_review',
] as const;

export const PROOF_CAPSULE_CONTRIBUTOR_KINDS = [
  'human',
  'agent',
  'team',
  'lab',
  'company',
] as const;

export type ProofCapsuleType = (typeof PROOF_CAPSULE_TYPES)[number];
export type ProofCapsuleStatus = (typeof PROOF_CAPSULE_STATUSES)[number];
export type ProofCapsuleContributorKind = (typeof PROOF_CAPSULE_CONTRIBUTOR_KINDS)[number];

export interface ProofCapsuleContributorPacket {
  kind: ProofCapsuleContributorKind;
  name?: string;
  handle?: string;
  affiliation?: string;
  contact: string;
  agent_id?: string;
  website?: string;
}

export interface ProofCapsuleArtifactPacket {
  label: string;
  url?: string;
  content_hash: string;
  mime_type?: string;
  size_bytes?: number;
  method_or_tool?: string;
  container_or_software_versions?: Record<string, unknown>;
  notes?: string;
}

export interface ProofCapsuleSubmissionPacket {
  capsule_type: ProofCapsuleType;
  work_packet_id?: string;
  title: string;
  contributor: ProofCapsuleContributorPacket;
  candidate_snapshot_hash?: string;
  evidence_bundle_hash?: string;
  method_refs: string[];
  notebook_ref?: string;
  analysis_summary: string;
  findings?: string;
  output_refs: string[];
  artifact_manifest: ProofCapsuleArtifactPacket[];
  limitations?: string;
  conflicts_or_disclosures?: string;
  task_manifest?: Record<string, unknown>;
  requested_review_route?: string;
  signature?: string;
}

// Public boundary: this is the shape returned by every public-facing read.
// Sensitive contributor fields (contact, agent_id, website) and operator
// review notes never appear here.
export interface ProofCapsulePublicRecord {
  proof_capsule_id: string;
  work_packet_id: string | null;
  candidate_id: string;
  capsule_type: ProofCapsuleType;
  title: string;
  contributor: {
    kind: ProofCapsuleContributorKind;
    name: string | null;
    handle: string | null;
    affiliation: string | null;
  };
  candidate_snapshot_hash: string | null;
  evidence_bundle_hash: string | null;
  method_refs: string[];
  notebook_ref: string | null;
  analysis_summary: string;
  findings: string;
  output_refs: string[];
  artifact_manifest: Array<{
    label: string;
    url: string | null;
    content_hash: string;
    mime_type: string | null;
    size_bytes: number | null;
    method_or_tool: string | null;
  }>;
  limitations: string;
  requested_review_route: string | null;
  content_hash: string;
  status: ProofCapsuleStatus;
  submitted_at: string;
  updated_at: string;
  reviewed_at: string | null;
  // Number of operator/evaluator review events attached to this capsule.
  // Surfaced so contributors can see "this was reviewed N times" without
  // any review notes leaking publicly. 0 means still in the intake queue.
  review_count: number;
  // Smell flags computed at read time from the public fields. Hints, not
  // rejections — operators use these to prioritize the review queue, and
  // contributors can self-correct before resubmitting.
  quality_flags: string[];
  status_url: string;
}

// Public view of the latest authoritative review (excludes llm_evaluator
// rows, which are recommendations). Rubric scores surface so readers can
// see WHY a capsule earned its proof points; reviewer_id and rationale
// stay private to the operator console.
export interface ProofCapsuleLatestReview {
  reviewer_type: string;
  verdict: string;
  reviewed_at: string | null;
  rubric: {
    scientific_usefulness: number | null;
    provenance_strength: number | null;
    reproducibility: number | null;
    actionability: number | null;
    novelty: number | null;
    clarity: number | null;
    downstream_impact: number | null;
  };
  // Weighted reward score 0-1 computed from the rubric. Surfaced so the
  // page can show pp earned without re-doing the math client-side.
  reward_score: number | null;
}

declare global {
  var twogProofCapsulePool: Pool | undefined;
}

export class ProofCapsuleError extends Error {
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

export function isProofCapsuleStorageConfigured(): boolean {
  return Boolean(databaseUrl());
}

function pool(): Pool {
  const connectionString = databaseUrl();
  if (!connectionString) {
    throw new ProofCapsuleError(
      'Proof capsule storage is not configured.',
      503,
      'proof_capsule_storage_not_configured',
      ['Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL.']
    );
  }
  if (!globalThis.twogProofCapsulePool) {
    globalThis.twogProofCapsulePool = new Pool({
      connectionString,
      max: 3,
      ssl: { rejectUnauthorized: false },
    });
  }
  return globalThis.twogProofCapsulePool;
}

// Defensive bootstrap. Hosted environments should rely on
// twog/db/migrations/004_proof_capsules.sql for the authoritative shape.
export async function ensureProofCapsuleSchema() {
  await pool().query(`
    create table if not exists proof_capsules (
      proof_capsule_id uuid primary key,
      work_packet_id uuid,
      candidate_id text not null,
      capsule_type text not null,
      title text not null,
      contributor jsonb not null default '{}'::jsonb,
      task_manifest jsonb not null default '{}'::jsonb,
      candidate_snapshot_hash text,
      evidence_bundle_hash text,
      method_refs jsonb not null default '[]'::jsonb,
      notebook_ref text,
      analysis_summary text not null,
      findings text not null default '',
      output_refs jsonb not null default '[]'::jsonb,
      artifact_manifest jsonb not null default '[]'::jsonb,
      limitations text not null default '',
      conflicts_or_disclosures text not null default '',
      requested_review_route text,
      content_hash text not null,
      signature text,
      status text not null default 'submitted',
      payload jsonb not null,
      submitted_at timestamptz not null default now(),
      updated_at timestamptz not null default now(),
      reviewed_at timestamptz
    );

    create index if not exists proof_capsules_candidate_status_idx
      on proof_capsules(candidate_id, status, submitted_at desc);
    create index if not exists proof_capsules_status_submitted_idx
      on proof_capsules(status, submitted_at desc);
    create index if not exists proof_capsules_content_hash_idx
      on proof_capsules(content_hash);
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

function asStringArray(value: unknown, max: number): string[] {
  if (!Array.isArray(value)) return [];
  const out: string[] = [];
  const seen = new Set<string>();
  for (const item of value.slice(0, max)) {
    if (typeof item !== 'string') continue;
    const trimmed = item.trim();
    if (!trimmed || seen.has(trimmed.toLowerCase())) continue;
    out.push(trimmed);
    seen.add(trimmed.toLowerCase());
  }
  return out;
}

function pickAllowed<T extends readonly string[]>(value: string, allowed: T): T[number] | undefined {
  return (allowed as readonly string[]).includes(value) ? (value as T[number]) : undefined;
}

function normalizeArtifact(input: unknown, index: number, errors: string[]): ProofCapsuleArtifactPacket | undefined {
  const raw = asObject(input);
  const label = asText(raw.label);
  const contentHash = asText(raw.content_hash);
  if (!label) errors.push(`artifact_manifest[${index}].label is required.`);
  if (!contentHash) errors.push(`artifact_manifest[${index}].content_hash is required.`);
  if (errors.length > 0) return undefined;
  return {
    label,
    url: asText(raw.url) || undefined,
    content_hash: contentHash,
    mime_type: asText(raw.mime_type) || undefined,
    size_bytes: typeof raw.size_bytes === 'number' ? raw.size_bytes : undefined,
    method_or_tool: asText(raw.method_or_tool) || undefined,
    container_or_software_versions: asObject(raw.container_or_software_versions),
    notes: asText(raw.notes) || undefined,
  };
}

export function normalizeProofCapsuleSubmission(body: unknown): ProofCapsuleSubmissionPacket {
  const raw = asObject(body);
  const source = asObject(raw.proof_capsule ?? raw);
  const contributor = asObject(source.contributor);
  const errors: string[] = [];

  const capsuleType = pickAllowed(asText(source.capsule_type), PROOF_CAPSULE_TYPES);
  const title = asText(source.title);
  const analysisSummary = asText(source.analysis_summary);
  const contact = asText(contributor.contact);
  const contributorKind =
    pickAllowed(asText(contributor.kind), PROOF_CAPSULE_CONTRIBUTOR_KINDS) ?? 'human';

  if (!capsuleType) errors.push(`capsule_type must be one of: ${PROOF_CAPSULE_TYPES.join(', ')}`);
  if (title.length < 6) errors.push('title must be at least 6 characters.');
  if (analysisSummary.length < 20)
    errors.push('analysis_summary must be at least 20 characters.');
  if (contact.length < 3) errors.push('contributor.contact is required for follow-up.');

  const artifactSource = Array.isArray(source.artifact_manifest) ? source.artifact_manifest : [];
  const artifactManifest: ProofCapsuleArtifactPacket[] = [];
  artifactSource.slice(0, 50).forEach((entry, index) => {
    const localErrors: string[] = [];
    const artifact = normalizeArtifact(entry, index, localErrors);
    if (localErrors.length === 0 && artifact) {
      artifactManifest.push(artifact);
    } else {
      errors.push(...localErrors);
    }
  });

  if (errors.length > 0 || !capsuleType) {
    throw new ProofCapsuleError(
      'Proof capsule submission is invalid.',
      400,
      'invalid_proof_capsule_submission',
      errors
    );
  }

  return {
    capsule_type: capsuleType,
    work_packet_id: asText(source.work_packet_id) || undefined,
    title,
    contributor: {
      kind: contributorKind,
      name: asText(contributor.name) || undefined,
      handle: asText(contributor.handle) || undefined,
      affiliation: asText(contributor.affiliation) || undefined,
      contact,
      agent_id: asText(contributor.agent_id) || undefined,
      website: asText(contributor.website) || undefined,
    },
    candidate_snapshot_hash: asText(source.candidate_snapshot_hash) || undefined,
    evidence_bundle_hash: asText(source.evidence_bundle_hash) || undefined,
    method_refs: asStringArray(source.method_refs, 50),
    notebook_ref: asText(source.notebook_ref) || undefined,
    analysis_summary: analysisSummary,
    findings: asText(source.findings) || undefined,
    output_refs: asStringArray(source.output_refs, 50),
    artifact_manifest: artifactManifest,
    limitations: asText(source.limitations) || undefined,
    conflicts_or_disclosures: asText(source.conflicts_or_disclosures) || undefined,
    task_manifest: asObject(source.task_manifest),
    requested_review_route: asText(source.requested_review_route) || undefined,
    signature: asText(source.signature) || undefined,
  };
}

function proofCapsuleStatusPath(id: string): string {
  return `/api/proof-capsules/${encodeURIComponent(id)}`;
}

// Canonical JSON serialization. Mirrors Python's
// ``json.dumps(payload, sort_keys=True)`` — every object's keys are
// sorted lexicographically at every nesting level, list order is
// preserved, and the separators match Python's default
// (``', '`` between entries, ``': '`` between key and value).
//
// JavaScript's built-in JSON.stringify(replacer) is not recursive, so we
// roll our own. The signing path depends on byte-identical output across
// the Python and TypeScript implementations; the regression test
// ``test_proof_capsule_content_hash_ts_matches_python`` pins them.
function canonicalJsonStringify(value: unknown): string {
  if (value === null || value === undefined) return 'null';
  if (typeof value === 'string') return JSON.stringify(value);
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new TypeError('cannot canonicalize non-finite number');
    }
    // Match Python's json default integer/float rendering (integers without ".0").
    return Number.isInteger(value) ? String(value) : JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return '[' + value.map((item) => canonicalJsonStringify(item)).join(', ') + ']';
  }
  if (typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    const keys = Object.keys(obj).sort();
    const parts = keys.map((key) => `${JSON.stringify(key)}: ${canonicalJsonStringify(obj[key])}`);
    return '{' + parts.join(', ') + '}';
  }
  throw new TypeError(`cannot canonicalize value of type ${typeof value}`);
}

// Mirrors the Python content_hash logic in
// src/hsa_research/ingestion_bridge/contracts.py::_proof_capsule_content_hash
// AND in src/twog_agent/content_hash.py.
// Keep all three implementations symmetric; the digest payload shape is
// part of the cross-language contract. Drift here = broken signatures.
function computeContentHash(packet: ProofCapsuleSubmissionPacket, candidateId: string): string {
  const manifestDigest = packet.artifact_manifest.map((artifact) => ({
    label: artifact.label,
    content_hash: artifact.content_hash,
    mime_type: artifact.mime_type ?? null,
    size_bytes: typeof artifact.size_bytes === 'number' ? artifact.size_bytes : null,
  }));
  const digest = {
    candidate_id: candidateId,
    work_packet_id: packet.work_packet_id ?? null,
    capsule_type: packet.capsule_type,
    title: packet.title,
    analysis_summary: packet.analysis_summary,
    findings: packet.findings ?? '',
    limitations: packet.limitations ?? '',
    candidate_snapshot_hash: packet.candidate_snapshot_hash ?? null,
    evidence_bundle_hash: packet.evidence_bundle_hash ?? null,
    method_refs: packet.method_refs,
    output_refs: packet.output_refs,
    notebook_ref: packet.notebook_ref ?? null,
    artifact_manifest: manifestDigest,
    contributor_handle: packet.contributor.handle ?? null,
  };
  const sorted = canonicalJsonStringify(digest);
  return 'sha256:' + createHash('sha256').update(sorted).digest('hex');
}

// Exported so a future cross-language unit test (vitest/jest) can pin the
// TS implementation against the Python ones in
// ``test_twog_agent_client_hash_matches_server``. The contract is
// currently end-to-end-tested by the live signed-submission path: a
// CLI-signed capsule whose signature verifies on this server confirms
// the TS and Python hashes agree byte-for-byte.
export function _internalComputeContentHashForTest(
  packet: ProofCapsuleSubmissionPacket,
  candidateId: string
): string {
  return computeContentHash(packet, candidateId);
}

function publicContributor(value: unknown): ProofCapsulePublicRecord['contributor'] {
  const raw = asObject(value);
  return {
    kind:
      pickAllowed(asText(raw.kind), PROOF_CAPSULE_CONTRIBUTOR_KINDS) ?? 'human',
    name: typeof raw.name === 'string' ? raw.name : null,
    handle: typeof raw.handle === 'string' ? raw.handle : null,
    affiliation: typeof raw.affiliation === 'string' ? raw.affiliation : null,
  };
}

function publicArtifactManifest(value: unknown): ProofCapsulePublicRecord['artifact_manifest'] {
  if (!Array.isArray(value)) return [];
  return value
    .filter((entry): entry is Record<string, unknown> => Boolean(entry) && typeof entry === 'object')
    .map((entry) => ({
      label: typeof entry.label === 'string' ? entry.label : '',
      url: typeof entry.url === 'string' ? entry.url : null,
      content_hash: typeof entry.content_hash === 'string' ? entry.content_hash : '',
      mime_type: typeof entry.mime_type === 'string' ? entry.mime_type : null,
      size_bytes: typeof entry.size_bytes === 'number' ? entry.size_bytes : null,
      method_or_tool: typeof entry.method_or_tool === 'string' ? entry.method_or_tool : null,
    }));
}

interface ProofCapsuleRow {
  proof_capsule_id: string;
  work_packet_id: string | null;
  candidate_id: string;
  capsule_type: string;
  title: string;
  contributor: unknown;
  candidate_snapshot_hash: string | null;
  evidence_bundle_hash: string | null;
  method_refs: unknown;
  notebook_ref: string | null;
  analysis_summary: string;
  findings: string | null;
  output_refs: unknown;
  artifact_manifest: unknown;
  limitations: string | null;
  requested_review_route: string | null;
  content_hash: string;
  status: string;
  submitted_at: Date | string;
  updated_at: Date | string;
  reviewed_at: Date | string | null;
  review_count: number | string | null;
}

function toPublicRecord(row: ProofCapsuleRow): ProofCapsulePublicRecord {
  const artifact_manifest = publicArtifactManifest(row.artifact_manifest);
  const method_refs = asStringArray(row.method_refs, 50);
  const analysis_summary = row.analysis_summary;
  const findings = row.findings ?? '';
  return {
    proof_capsule_id: row.proof_capsule_id,
    work_packet_id: row.work_packet_id,
    candidate_id: row.candidate_id,
    capsule_type: row.capsule_type as ProofCapsuleType,
    title: row.title,
    contributor: publicContributor(row.contributor),
    candidate_snapshot_hash: row.candidate_snapshot_hash,
    evidence_bundle_hash: row.evidence_bundle_hash,
    method_refs,
    notebook_ref: row.notebook_ref,
    analysis_summary,
    findings,
    output_refs: asStringArray(row.output_refs, 50),
    artifact_manifest,
    limitations: row.limitations ?? '',
    requested_review_route: row.requested_review_route,
    content_hash: row.content_hash,
    status: row.status as ProofCapsuleStatus,
    submitted_at:
      row.submitted_at instanceof Date ? row.submitted_at.toISOString() : String(row.submitted_at),
    updated_at:
      row.updated_at instanceof Date ? row.updated_at.toISOString() : String(row.updated_at),
    reviewed_at:
      row.reviewed_at instanceof Date
        ? row.reviewed_at.toISOString()
        : row.reviewed_at !== null
          ? String(row.reviewed_at)
          : null,
    review_count:
      row.review_count === null || row.review_count === undefined
        ? 0
        : Number(row.review_count),
    quality_flags: computeQualityFlags({
      capsule_type: row.capsule_type,
      title: row.title,
      analysis_summary,
      findings,
      method_refs,
      artifact_manifest,
    }),
    status_url: proofCapsuleStatusPath(row.proof_capsule_id),
  };
}

// Public boundary: ``review_count`` is a derived integer — operators get
// their notes/identities through the Python intake path only. The
// correlated subquery on proof_capsule_reviews is fine at the volumes
// we're operating at; if that ever changes, swap to a LEFT JOIN over a
// pre-aggregated counts view.
const PUBLIC_SELECT = `
  proof_capsule_id::text as proof_capsule_id,
  work_packet_id::text as work_packet_id,
  candidate_id,
  capsule_type,
  title,
  contributor,
  candidate_snapshot_hash,
  evidence_bundle_hash,
  method_refs,
  notebook_ref,
  analysis_summary,
  findings,
  output_refs,
  artifact_manifest,
  limitations,
  requested_review_route,
  content_hash,
  status,
  submitted_at,
  updated_at,
  reviewed_at,
  coalesce((
    select count(*)
    from proof_capsule_reviews r
    where r.proof_capsule_id = proof_capsules.proof_capsule_id
  ), 0) as review_count
`;

export async function submitProofCapsule(
  candidateId: string,
  packet: ProofCapsuleSubmissionPacket
): Promise<ProofCapsulePublicRecord> {
  await ensureProofCapsuleSchema();
  const contentHash = computeContentHash(packet, candidateId);

  // Verify the signature (if present) against the *server-computed* canonical
  // content_hash. A signature that doesn't verify is a hard 400 — better to
  // reject a malformed credential than persist it and have reviewers discover
  // the mismatch later.
  if (packet.signature) {
    const result = verifyProofCapsuleSignature(packet.signature, contentHash);
    if (!result.ok) {
      throw new ProofCapsuleError(
        'Proof capsule signature did not verify.',
        400,
        'invalid_proof_capsule_signature',
        result.reason ? [result.reason] : []
      );
    }
  }

  // Per-handle rate limit. We check *before* doing the dedup query so an
  // abusive handle cannot probe the table for free. The check is also
  // before record, so a misbehaving agent can stop and recover within
  // the trailing hour window without permanent damage.
  const handle = packet.contributor.handle ?? '';
  if (handle) {
    const rate = await checkSubmissionRate(handle);
    if (!rate.allowed) {
      throw new ProofCapsuleError(
        `Per-handle submission limit reached (${rate.current}/${rate.limit} in the last hour).`,
        429,
        'proof_capsule_rate_limit_exceeded',
        [
          `handle=${handle}`,
          `current=${rate.current}`,
          `limit=${rate.limit}`,
          'Wait until the trailing-hour window has more headroom, then retry.',
        ]
      );
    }
  }

  // Idempotent submit: return the existing capsule with the same content_hash.
  const existing = await pool().query<ProofCapsuleRow>(
    `select ${PUBLIC_SELECT} from proof_capsules where content_hash = $1 limit 1`,
    [contentHash]
  );
  if (existing.rows[0]) {
    if (handle) {
      // Even dedups cost a write to the rate ledger; abusive identical
      // re-submits should still trip the per-hour limit.
      await recordSubmissionRate(handle);
    }
    return toPublicRecord(existing.rows[0]);
  }

  const proofCapsuleId = randomUUID();
  const result = await pool().query<ProofCapsuleRow>(
    `
      insert into proof_capsules (
        proof_capsule_id,
        work_packet_id,
        candidate_id,
        capsule_type,
        title,
        contributor,
        task_manifest,
        candidate_snapshot_hash,
        evidence_bundle_hash,
        method_refs,
        notebook_ref,
        analysis_summary,
        findings,
        output_refs,
        artifact_manifest,
        limitations,
        conflicts_or_disclosures,
        requested_review_route,
        content_hash,
        signature,
        status,
        payload
      )
      values (
        $1, $2, $3, $4, $5,
        $6::jsonb, $7::jsonb,
        $8, $9,
        $10::jsonb, $11,
        $12, $13,
        $14::jsonb, $15::jsonb,
        $16, $17,
        $18, $19, $20, $21,
        $22::jsonb
      )
      returning ${PUBLIC_SELECT}
    `,
    [
      proofCapsuleId,
      packet.work_packet_id ?? null,
      candidateId,
      packet.capsule_type,
      packet.title,
      JSON.stringify(packet.contributor),
      JSON.stringify(packet.task_manifest ?? {}),
      packet.candidate_snapshot_hash ?? null,
      packet.evidence_bundle_hash ?? null,
      JSON.stringify(packet.method_refs),
      packet.notebook_ref ?? null,
      packet.analysis_summary,
      packet.findings ?? '',
      JSON.stringify(packet.output_refs),
      JSON.stringify(packet.artifact_manifest),
      packet.limitations ?? '',
      packet.conflicts_or_disclosures ?? '',
      packet.requested_review_route ?? null,
      contentHash,
      packet.signature ?? null,
      'submitted',
      JSON.stringify({
        ...packet,
        candidate_id: candidateId,
        content_hash: contentHash,
        proof_capsule_id: proofCapsuleId,
      }),
    ]
  );
  if (handle) {
    await recordSubmissionRate(handle);
  }
  return toPublicRecord(result.rows[0]);
}

export async function getProofCapsule(
  proofCapsuleId: string
): Promise<ProofCapsulePublicRecord | null> {
  await ensureProofCapsuleSchema();
  const result = await pool().query<ProofCapsuleRow>(
    `select ${PUBLIC_SELECT} from proof_capsules where proof_capsule_id::text = $1 limit 1`,
    [proofCapsuleId]
  );
  return result.rows[0] ? toPublicRecord(result.rows[0]) : null;
}

// Per-rubric weights mirror Python proof_capsules.compute_capsule_reward_from_rubric.
// Surfacing the weighted score on the capsule page so contributors and readers
// can see why a capsule earned its proof points.
const RUBRIC_WEIGHTS: Record<string, number> = {
  scientific_usefulness: 0.25,
  provenance_strength: 0.20,
  actionability: 0.20,
  reproducibility: 0.10,
  novelty: 0.10,
  downstream_impact: 0.10,
  clarity: 0.05,
};

const VERDICT_MULTIPLIER: Record<string, number> = {
  accepted: 1.0,
  routed_to_validation: 0.9,
  routed_to_compute_review: 0.9,
  needs_changes: 0.0,
  rejected: 0.0,
  archived: 0.3,
};

function weightedRubricScore(rubric: ProofCapsuleLatestReview['rubric'], verdict: string): number | null {
  const multiplier = VERDICT_MULTIPLIER[verdict];
  if (multiplier === undefined) return null;
  if (multiplier === 0) return 0;
  let weightedSum = 0;
  let weightTotal = 0;
  for (const [dim, weight] of Object.entries(RUBRIC_WEIGHTS)) {
    const value = rubric[dim as keyof typeof rubric];
    if (value === null || value === undefined) continue;
    const clamped = Math.max(0, Math.min(1, value));
    weightedSum += weight * clamped;
    weightTotal += weight;
  }
  if (weightTotal === 0) return null;
  return Number(((weightedSum / weightTotal) * multiplier).toFixed(4));
}

// Latest authoritative review (excludes llm_evaluator recommendations).
// Returns null when no operator-grade review exists. Sensitive operator
// fields (reviewer_id, rationale, required_changes) intentionally do NOT
// surface — only the public-facing rubric breakdown.
export async function getLatestPublicReview(
  proofCapsuleId: string
): Promise<ProofCapsuleLatestReview | null> {
  await ensureProofCapsuleSchema();
  const result = await pool().query<{
    reviewer_type: string;
    verdict: string;
    created_at: Date | string | null;
    scientific_usefulness: string | number | null;
    provenance_strength: string | number | null;
    reproducibility: string | number | null;
    actionability: string | number | null;
    novelty: string | number | null;
    clarity: string | number | null;
    downstream_impact: string | number | null;
  }>(
    `
      select
        reviewer_type,
        verdict,
        created_at,
        scientific_usefulness,
        provenance_strength,
        reproducibility,
        actionability,
        novelty,
        clarity,
        downstream_impact
      from proof_capsule_reviews
      where proof_capsule_id::text = $1
        and reviewer_type != 'llm_evaluator'
      order by created_at desc
      limit 1
    `,
    [proofCapsuleId]
  );
  if (result.rows.length === 0) return null;
  const row = result.rows[0];
  const numOrNull = (v: string | number | null): number | null =>
    v === null || v === undefined ? null : Number(v);
  const rubric = {
    scientific_usefulness: numOrNull(row.scientific_usefulness),
    provenance_strength: numOrNull(row.provenance_strength),
    reproducibility: numOrNull(row.reproducibility),
    actionability: numOrNull(row.actionability),
    novelty: numOrNull(row.novelty),
    clarity: numOrNull(row.clarity),
    downstream_impact: numOrNull(row.downstream_impact),
  };
  return {
    reviewer_type: row.reviewer_type,
    verdict: row.verdict,
    reviewed_at: row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at,
    rubric,
    reward_score: weightedRubricScore(rubric, row.verdict),
  };
}

export async function listProofCapsulesForCandidate(
  candidateId: string,
  options: { statuses?: ReadonlyArray<string>; limit?: number } = {}
): Promise<ProofCapsulePublicRecord[]> {
  await ensureProofCapsuleSchema();
  const allowed = new Set<string>(PROOF_CAPSULE_STATUSES);
  const statuses = (options.statuses ?? [...PROOF_CAPSULE_PENDING_STATUSES, ...PROOF_CAPSULE_ACCEPTED_STATUSES])
    .map((value) => String(value).trim())
    .filter((value) => allowed.has(value));
  const limit = Math.max(1, Math.min(Math.trunc(options.limit ?? 25), 200));

  const result = await pool().query<ProofCapsuleRow>(
    `
      select ${PUBLIC_SELECT}
      from proof_capsules
      where candidate_id = $1 and status = any($2)
      order by submitted_at desc
      limit $3
    `,
    [candidateId, statuses.length > 0 ? statuses : [...PROOF_CAPSULE_PENDING_STATUSES], limit]
  );
  return result.rows.map(toPublicRecord);
}

// Cross-candidate capsule listing for the /network mission board and for
// agent callers consuming `/api/work-packets` companion feeds. Returns
// public-safe rows only — sensitive contributor fields never appear.
export async function listProofCapsules(
  options: {
    statuses?: ReadonlyArray<string>;
    candidate_ids?: ReadonlyArray<string>;
    capsule_types?: ReadonlyArray<string>;
    limit?: number;
  } = {}
): Promise<ProofCapsulePublicRecord[]> {
  await ensureProofCapsuleSchema();
  const allowedStatuses = new Set<string>(PROOF_CAPSULE_STATUSES);
  const allowedTypes = new Set<string>(PROOF_CAPSULE_TYPES);
  const statuses = (options.statuses ?? [...PROOF_CAPSULE_PENDING_STATUSES, ...PROOF_CAPSULE_ACCEPTED_STATUSES])
    .map((value) => String(value).trim())
    .filter((value) => allowedStatuses.has(value));
  const candidateIds = (options.candidate_ids ?? [])
    .map((value) => String(value).trim())
    .filter((value) => value.length > 0);
  const capsuleTypes = (options.capsule_types ?? [])
    .map((value) => String(value).trim())
    .filter((value) => allowedTypes.has(value));
  const limit = Math.max(1, Math.min(Math.trunc(options.limit ?? 25), 200));

  const conditions: string[] = ['status = any($1)'];
  const params: unknown[] = [statuses.length > 0 ? statuses : [...PROOF_CAPSULE_PENDING_STATUSES]];
  if (candidateIds.length > 0) {
    conditions.push(`candidate_id = any($${params.length + 1})`);
    params.push(candidateIds);
  }
  if (capsuleTypes.length > 0) {
    conditions.push(`capsule_type = any($${params.length + 1})`);
    params.push(capsuleTypes);
  }
  params.push(limit);

  const result = await pool().query<ProofCapsuleRow>(
    `
      select ${PUBLIC_SELECT}
      from proof_capsules
      where ${conditions.join(' and ')}
      order by submitted_at desc
      limit $${params.length}
    `,
    params
  );
  return result.rows.map(toPublicRecord);
}
