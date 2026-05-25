import { Pool } from 'pg';

export const CONTRIBUTOR_TIERS = [
  'observer',
  'scout',
  'citation_repairer',
  'record_builder',
  'replication_contributor',
  'validation_contributor',
  'trusted_reviewer',
  'proof_partner',
] as const;

export type ContributorTier = (typeof CONTRIBUTOR_TIERS)[number];

const TIER_LABELS: Record<ContributorTier, string> = {
  observer: 'Observer',
  scout: 'Scout',
  citation_repairer: 'Citation Repairer',
  record_builder: 'Record Builder',
  replication_contributor: 'Replication Contributor',
  validation_contributor: 'Validation Contributor',
  trusted_reviewer: 'Trusted Reviewer',
  proof_partner: 'Proof Partner',
};

export function tierLabel(tier: string): string {
  return (TIER_LABELS as Record<string, string | undefined>)[tier] ?? tier.replace(/_/g, ' ');
}

export interface ContributorProfile {
  schema_version: 'twog-contributor-profile-v1';
  handle: string;
  display_name: string | null;
  affiliation: string | null;
  kind: string;
  tier: ContributorTier;
  proof_points: number;
  summary: {
    accepted_capsule_count: number;
    routed_capsule_count: number;
    candidate_count: number;
    reward_event_count: number;
    outcome_counts: Record<string, number>;
  };
  capsule_type_counts: Record<string, number>;
  candidates: string[];
  strongest_accepted_work: {
    score: number;
    proof_capsule_id: string | null;
    capsule_type: string | null;
    candidate_id: string | null;
    rationale: string | null;
  } | null;
  accepted_capsules: Array<{
    proof_capsule_id: string;
    candidate_id: string;
    capsule_type: string;
    title: string;
    status: string;
    submitted_at: string;
    reviewed_at: string | null;
    status_url: string | null;
  }>;
  routed_capsules: Array<{
    proof_capsule_id: string;
    candidate_id: string;
    capsule_type: string;
    title: string;
    status: string;
    submitted_at: string;
    reviewed_at: string | null;
    status_url: string | null;
  }>;
  profile_url: string;
  errors: string[];
}

declare global {
  var twogContributorProfilePool: Pool | undefined;
}

function databaseUrl(): string | undefined {
  return (
    process.env.NEON_DATABASE_URL ??
    process.env.DATABASE_URL ??
    process.env.POSTGRES_URL ??
    process.env.HSA_DATABASE_URL
  );
}

export function isContributorProfileStorageConfigured(): boolean {
  return Boolean(databaseUrl());
}

function pool(): Pool {
  const connectionString = databaseUrl();
  if (!connectionString) {
    throw new Error('contributor profile storage is not configured');
  }
  if (!globalThis.twogContributorProfilePool) {
    globalThis.twogContributorProfilePool = new Pool({
      connectionString,
      max: 3,
      ssl: { rejectUnauthorized: false },
    });
  }
  return globalThis.twogContributorProfilePool;
}

interface CapsuleRow {
  proof_capsule_id: string;
  candidate_id: string;
  capsule_type: string;
  title: string;
  status: string;
  submitted_at: Date | string;
  reviewed_at: Date | string | null;
}

interface RewardEventRow {
  score: number | null;
  candidate_id: string | null;
  outcome_bucket: string | null;
  rationale: string | null;
  payload: { metadata?: Record<string, unknown> } | null;
}

function toISO(value: Date | string | null): string | null {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString();
  return String(value);
}

function isoOrEmpty(value: Date | string): string {
  return toISO(value) ?? '';
}

// Exported so callers like the leaderboard aggregator use the same tier
// rules as the per-contributor profile page. Mirrors
// ``hsa_research.ingestion_bridge.contributor_profiles.compute_tier``.
export function computeContributorTier(args: {
  acceptedCount: number;
  routedCount: number;
  distinctCandidates: number;
  capsuleTypeCounts: Map<string, number>;
  proofPoints: number;
}): ContributorTier {
  const positiveTotal = args.acceptedCount + args.routedCount;
  const has = (...types: string[]): boolean => types.some((t) => (args.capsuleTypeCounts.get(t) ?? 0) > 0);
  if (positiveTotal >= 20 && args.proofPoints >= 1500) return 'proof_partner';
  if (positiveTotal >= 10) return 'trusted_reviewer';
  if (has('validation_proposal') && positiveTotal >= 2) return 'validation_contributor';
  if (has('docking_replication', 'md_review') && positiveTotal >= 2) return 'replication_contributor';
  if (positiveTotal >= 5 && args.distinctCandidates >= 2) return 'record_builder';
  if (has('citation_repair') && positiveTotal >= 3) return 'citation_repairer';
  if (positiveTotal >= 1) return 'scout';
  return 'observer';
}

// Backwards-compatible alias used in this module.
const pickTier = computeContributorTier;

export async function getContributorProfileByHandle(
  rawHandle: string
): Promise<ContributorProfile> {
  const handle = (rawHandle ?? '').trim();
  if (!handle) {
    throw new Error('handle is required');
  }
  const capsules = await pool().query<CapsuleRow & { contributor: Record<string, unknown> | null }>(
    `
      select
        proof_capsule_id::text as proof_capsule_id,
        candidate_id,
        capsule_type,
        title,
        contributor,
        status,
        submitted_at,
        reviewed_at
      from proof_capsules
      where contributor ->> 'handle' = $1
        and status in ('accepted', 'routed_to_validation', 'routed_to_compute_review')
      order by reviewed_at desc nulls last, submitted_at desc
      limit 100
    `,
    [handle]
  );
  // NOTE: reward_events stores candidate_id inside the payload jsonb, not as
  // a column. Use the jsonb accessor so the SELECT matches the Postgres
  // schema in postgres_store.py.
  const rewards = await pool().query<RewardEventRow>(
    `
      select
        score,
        (payload ->> 'candidate_id') as candidate_id,
        (payload ->> 'outcome_bucket') as outcome_bucket,
        (payload ->> 'rationale') as rationale,
        payload
      from reward_events
      where event_source = 'proof_capsule_review'
        and (payload -> 'metadata' ->> 'contributor_handle') = $1
      order by created_at desc
      limit 500
    `,
    [handle]
  );

  const accepted: ContributorProfile['accepted_capsules'] = [];
  const routed: ContributorProfile['routed_capsules'] = [];
  const capsuleTypeCounts = new Map<string, number>();
  const candidateIds = new Set<string>();
  let displayName: string | null = null;
  let affiliation: string | null = null;
  let kind: string | null = null;

  for (const row of capsules.rows) {
    const contributor = (row.contributor ?? {}) as Record<string, unknown>;
    const rowOut = {
      proof_capsule_id: row.proof_capsule_id,
      candidate_id: row.candidate_id,
      capsule_type: row.capsule_type,
      title: row.title,
      status: row.status,
      submitted_at: isoOrEmpty(row.submitted_at),
      reviewed_at: toISO(row.reviewed_at),
      status_url: `/api/proof-capsules/${encodeURIComponent(row.proof_capsule_id)}`,
    };
    if (row.status === 'accepted') accepted.push(rowOut);
    else routed.push(rowOut);
    candidateIds.add(row.candidate_id);
    capsuleTypeCounts.set(row.capsule_type, (capsuleTypeCounts.get(row.capsule_type) ?? 0) + 1);
    if (!displayName && typeof contributor.name === 'string') displayName = contributor.name;
    if (!affiliation && typeof contributor.affiliation === 'string') affiliation = contributor.affiliation;
    if (!kind && typeof contributor.kind === 'string') kind = contributor.kind;
  }

  let proofPointsTotal = 0;
  let rewardEventCount = 0;
  const outcomeCounts: Record<string, number> = {};
  let strongest: ContributorProfile['strongest_accepted_work'] = null;
  for (const event of rewards.rows) {
    const score = typeof event.score === 'number' ? event.score : 0;
    proofPointsTotal += score;
    rewardEventCount += 1;
    if (event.outcome_bucket) {
      outcomeCounts[event.outcome_bucket] = (outcomeCounts[event.outcome_bucket] ?? 0) + 1;
    }
    const meta = (event.payload?.metadata ?? {}) as Record<string, unknown>;
    if (!strongest || score > strongest.score) {
      strongest = {
        score,
        proof_capsule_id: typeof meta.proof_capsule_id === 'string' ? meta.proof_capsule_id : null,
        capsule_type: typeof meta.capsule_type === 'string' ? meta.capsule_type : null,
        candidate_id: event.candidate_id,
        rationale: event.rationale,
      };
    }
  }
  const proofPoints = Math.round(proofPointsTotal * 100);
  const tier = pickTier({
    acceptedCount: accepted.length,
    routedCount: routed.length,
    distinctCandidates: candidateIds.size,
    capsuleTypeCounts,
    proofPoints,
  });

  return {
    schema_version: 'twog-contributor-profile-v1',
    handle,
    display_name: displayName,
    affiliation,
    kind: kind ?? 'human',
    tier,
    proof_points: proofPoints,
    summary: {
      accepted_capsule_count: accepted.length,
      routed_capsule_count: routed.length,
      candidate_count: candidateIds.size,
      reward_event_count: rewardEventCount,
      outcome_counts: outcomeCounts,
    },
    capsule_type_counts: Object.fromEntries(capsuleTypeCounts),
    candidates: [...candidateIds].sort(),
    strongest_accepted_work: strongest,
    accepted_capsules: accepted,
    routed_capsules: routed,
    profile_url: `/contributors/${encodeURIComponent(handle)}`,
    errors: [],
  };
}
