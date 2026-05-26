/**
 * Proof Network leaderboard.
 *
 * Aggregates reward events from ``event_source='proof_capsule_review'``,
 * joins to ``proof_capsules`` for accepted/routed counts and distinct
 * candidate counts, and ranks contributors by total proof points.
 *
 * Tier is computed in TS via the same rules as
 * ``hsa_research.ingestion_bridge.contributor_profiles.compute_tier`` so
 * the leaderboard and the contributor profile page agree on tier for the
 * same handle.
 *
 * Time windows are applied on reward_events.created_at so a "last 30
 * days" view ranks by recent activity rather than cumulative volume.
 */

import { Pool } from 'pg';
import {
  ContributorTier,
  computeContributorTier,
  tierLabel as contributorTierLabel,
} from '@/lib/contributor-profiles';

export type LeaderboardWindow = 'all_time' | 'last_30_days' | 'last_7_days';

export interface LeaderboardEntry {
  rank: number;
  handle: string;
  display_name: string | null;
  affiliation: string | null;
  kind: string;
  tier: ContributorTier;
  tier_label: string;
  proof_points: number;
  accepted_capsule_count: number;
  routed_capsule_count: number;
  candidate_count: number;
  capsule_type_counts: Record<string, number>;
  // Top capsule type by count, for the specialty chip on each row.
  // Null when the contributor has no accepted/routed capsules yet.
  primary_specialty: string | null;
  first_event_at: string | null;
  last_event_at: string | null;
  profile_url: string;
  // Median weighted-rubric score across this contributor's
  // operator-grade reviews (llm_evaluator rows excluded). Mirrors the
  // Python compute_capsule_reward_from_rubric formula. null when no
  // operator-grade reviews exist yet. Surfaces quality next to volume.
  median_rubric_score: number | null;
}

export interface LeaderboardNetworkTotals {
  contributors_ranked: number;
  total_proof_points: number;
  total_accepted_capsules: number;
  total_routed_capsules: number;
  candidates_touched: number;
}

export interface RisingEntry {
  handle: string;
  display_name: string | null;
  tier: ContributorTier;
  tier_label: string;
  proof_points: number;
  first_event_at: string | null;
  reason: 'first_proof_points' | 'returning_contributor';
  profile_url: string;
}

export interface LeaderboardResult {
  schema_version: 'twog-leaderboard-v1';
  window: LeaderboardWindow;
  total_contributors: number;
  generated_at: string;
  network_totals: LeaderboardNetworkTotals;
  entries: LeaderboardEntry[];
  rising: RisingEntry[];
}

const WINDOW_HOURS: Record<Exclude<LeaderboardWindow, 'all_time'>, number> = {
  last_30_days: 30 * 24,
  last_7_days: 7 * 24,
};

declare global {
  var twogLeaderboardPool: Pool | undefined;
}

function databaseUrl(): string | undefined {
  return (
    process.env.NEON_DATABASE_URL ??
    process.env.DATABASE_URL ??
    process.env.POSTGRES_URL ??
    process.env.HSA_DATABASE_URL
  );
}

export function isLeaderboardStorageConfigured(): boolean {
  return Boolean(databaseUrl());
}

function pool(): Pool {
  const connectionString = databaseUrl();
  if (!connectionString) {
    throw new Error('leaderboard storage is not configured');
  }
  if (!globalThis.twogLeaderboardPool) {
    globalThis.twogLeaderboardPool = new Pool({
      connectionString,
      max: 2,
      ssl: { rejectUnauthorized: false },
    });
  }
  return globalThis.twogLeaderboardPool;
}

// Tier picker imported from contributor-profiles so both surfaces use the
// same rules — drift would mean a contributor's tier differs between
// their profile page and their leaderboard row.
const pickTier = computeContributorTier;

interface RawRewardRow {
  handle: string;
  total_score: number | string | null;
  event_count: number | string | null;
  first_event_at: Date | string;
  last_event_at: Date | string;
}

interface RawCapsuleRow {
  handle: string;
  capsule_type: string;
  status: string;
  candidate_id: string;
  name: string | null;
  affiliation: string | null;
  kind: string | null;
}

function asISO(value: Date | string | null): string | null {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString();
  return String(value);
}

function asNumber(value: unknown): number {
  if (value === null || value === undefined) return 0;
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? n : 0;
}

/**
 * Load all reward events (filtered by window) grouped by contributor
 * handle, plus the accepted/routed capsule rows needed to compute tier.
 * Returns the joined dataset; the aggregator does the rest in TS so
 * future tweaks (e.g. ranking by recent activity, multi-key sorts) stay
 * in one place.
 */
async function loadLeaderboardRows(window: LeaderboardWindow): Promise<{
  rewardRows: RawRewardRow[];
  capsuleRows: RawCapsuleRow[];
  medianRows: Array<{ handle: string; median_score: string | null }>;
}> {
  const params: unknown[] = [];
  let timeFilter = '';
  if (window !== 'all_time') {
    const sinceMs = Date.now() - WINDOW_HOURS[window] * 60 * 60 * 1000;
    params.push(new Date(sinceMs));
    timeFilter = `and created_at >= $${params.length}`;
  }
  const rewardSql = `
    select
      (payload -> 'metadata' ->> 'contributor_handle') as handle,
      sum(score) as total_score,
      count(*) as event_count,
      min(created_at) as first_event_at,
      max(created_at) as last_event_at
    from reward_events
    where event_source = 'proof_capsule_review'
      and (payload -> 'metadata' ->> 'contributor_handle') is not null
      ${timeFilter}
    group by handle
    order by total_score desc nulls last, last_event_at desc
    limit 200
  `;
  const rewardResult = await pool().query<RawRewardRow>(rewardSql, params);

  if (rewardResult.rows.length === 0) {
    return { rewardRows: [], capsuleRows: [], medianRows: [] };
  }
  const handles = rewardResult.rows.map((row) => row.handle);
  const capsuleResult = await pool().query<RawCapsuleRow>(
    `
      select
        (contributor ->> 'handle') as handle,
        capsule_type,
        status,
        candidate_id,
        (contributor ->> 'name') as name,
        (contributor ->> 'affiliation') as affiliation,
        (contributor ->> 'kind') as kind
      from proof_capsules
      where (contributor ->> 'handle') = any($1)
        and status in ('accepted', 'routed_to_validation', 'routed_to_compute_review')
    `,
    [handles],
  );
  // Median weighted-rubric score per contributor, computed inline in
  // SQL so we don't have to pull every review back to the app. Mirrors
  // proof_capsules.compute_capsule_reward_from_rubric weights.
  // llm_evaluator rows are advisory; they don't count toward quality.
  const medianResult = await pool().query<{ handle: string; median_score: string | null }>(
    `
      select
        (c.contributor ->> 'handle') as handle,
        percentile_cont(0.5) within group (order by
          0.25 * coalesce(r.scientific_usefulness, 0) +
          0.20 * coalesce(r.provenance_strength, 0) +
          0.20 * coalesce(r.actionability, 0) +
          0.10 * coalesce(r.reproducibility, 0) +
          0.10 * coalesce(r.novelty, 0) +
          0.10 * coalesce(r.downstream_impact, 0) +
          0.05 * coalesce(r.clarity, 0)
        ) as median_score
      from proof_capsule_reviews r
      join proof_capsules c on c.proof_capsule_id = r.proof_capsule_id
      where (c.contributor ->> 'handle') = any($1)
        and r.reviewer_type != 'llm_evaluator'
      group by handle
    `,
    [handles],
  );
  return {
    rewardRows: rewardResult.rows,
    capsuleRows: capsuleResult.rows,
    medianRows: medianResult.rows,
  };
}

interface ContributorAgg {
  handle: string;
  display_name: string | null;
  affiliation: string | null;
  kind: string | null;
  total_score: number;
  event_count: number;
  first_event_at: string | null;
  last_event_at: string | null;
  accepted_count: number;
  routed_count: number;
  candidate_ids: Set<string>;
  capsule_type_counts: Map<string, number>;
  median_rubric_score: number | null;
}

function aggregateLeaderboard(
  rewardRows: RawRewardRow[],
  capsuleRows: RawCapsuleRow[],
  medianRows: Array<{ handle: string; median_score: string | null }> = [],
): ContributorAgg[] {
  const medianByHandle = new Map<string, number | null>();
  for (const row of medianRows) {
    const value = row.median_score === null ? null : Number(row.median_score);
    medianByHandle.set(row.handle, value);
  }
  const aggs = new Map<string, ContributorAgg>();
  for (const row of rewardRows) {
    aggs.set(row.handle, {
      handle: row.handle,
      display_name: null,
      affiliation: null,
      kind: null,
      total_score: asNumber(row.total_score),
      event_count: asNumber(row.event_count),
      first_event_at: asISO(row.first_event_at),
      last_event_at: asISO(row.last_event_at),
      accepted_count: 0,
      routed_count: 0,
      candidate_ids: new Set(),
      capsule_type_counts: new Map(),
      median_rubric_score: medianByHandle.get(row.handle) ?? null,
    });
  }
  for (const row of capsuleRows) {
    const agg = aggs.get(row.handle);
    if (!agg) continue;
    if (row.status === 'accepted') agg.accepted_count += 1;
    else agg.routed_count += 1;
    agg.candidate_ids.add(row.candidate_id);
    agg.capsule_type_counts.set(
      row.capsule_type,
      (agg.capsule_type_counts.get(row.capsule_type) ?? 0) + 1,
    );
    if (!agg.display_name && row.name) agg.display_name = row.name;
    if (!agg.affiliation && row.affiliation) agg.affiliation = row.affiliation;
    if (!agg.kind && row.kind) agg.kind = row.kind;
  }
  return [...aggs.values()];
}

export async function getLeaderboard(
  window: LeaderboardWindow = 'all_time',
  options: { limit?: number; risingLimit?: number } = {},
): Promise<LeaderboardResult> {
  const limit = Math.max(1, Math.min(Math.trunc(options.limit ?? 25), 200));
  const risingLimit = Math.max(0, Math.min(Math.trunc(options.risingLimit ?? 5), 25));

  const { rewardRows, capsuleRows, medianRows } = await loadLeaderboardRows(window);
  const aggs = aggregateLeaderboard(rewardRows, capsuleRows, medianRows);

  const sortedAggs = aggs.sort((a, b) => {
    if (b.total_score !== a.total_score) return b.total_score - a.total_score;
    return (b.last_event_at ?? '').localeCompare(a.last_event_at ?? '');
  });

  const allEntries: LeaderboardEntry[] = sortedAggs.map((agg, index) => {
    const proofPoints = Math.round(agg.total_score * 100);
    const tier = pickTier({
      acceptedCount: agg.accepted_count,
      routedCount: agg.routed_count,
      distinctCandidates: agg.candidate_ids.size,
      capsuleTypeCounts: agg.capsule_type_counts,
      proofPoints,
    });
    let primarySpecialty: string | null = null;
    let primarySpecialtyCount = 0;
    for (const [type, count] of agg.capsule_type_counts.entries()) {
      if (count > primarySpecialtyCount) {
        primarySpecialty = type;
        primarySpecialtyCount = count;
      }
    }
    return {
      rank: index + 1,
      handle: agg.handle,
      display_name: agg.display_name,
      affiliation: agg.affiliation,
      kind: agg.kind ?? 'human',
      tier,
      tier_label: contributorTierLabel(tier),
      proof_points: proofPoints,
      accepted_capsule_count: agg.accepted_count,
      routed_capsule_count: agg.routed_count,
      candidate_count: agg.candidate_ids.size,
      capsule_type_counts: Object.fromEntries(agg.capsule_type_counts),
      primary_specialty: primarySpecialty,
      first_event_at: agg.first_event_at,
      last_event_at: agg.last_event_at,
      profile_url: `/contributors/${encodeURIComponent(agg.handle)}`,
      median_rubric_score: agg.median_rubric_score,
    };
  });

  // Network totals are computed across the *full* aggregation, not the
  // limit-truncated entries returned to the caller — the hero shows the
  // network's overall state regardless of how many rows the page renders.
  const candidatesTouched = new Set<string>();
  let totalProofPoints = 0;
  let totalAccepted = 0;
  let totalRouted = 0;
  for (const agg of sortedAggs) {
    totalProofPoints += Math.round(agg.total_score * 100);
    totalAccepted += agg.accepted_count;
    totalRouted += agg.routed_count;
    for (const cid of agg.candidate_ids) {
      candidatesTouched.add(cid);
    }
  }
  const networkTotals: LeaderboardNetworkTotals = {
    contributors_ranked: sortedAggs.length,
    total_proof_points: totalProofPoints,
    total_accepted_capsules: totalAccepted,
    total_routed_capsules: totalRouted,
    candidates_touched: candidatesTouched.size,
  };

  const entries = allEntries;

  // "Rising": contributors whose *first* reward event lands in the last 7
  // days. Cheap heuristic that surfaces newcomers without needing
  // historical tier-transition tracking.
  const risingCutoffMs = Date.now() - 7 * 24 * 60 * 60 * 1000;
  const rising: RisingEntry[] = entries
    .filter((entry) => {
      if (!entry.first_event_at) return false;
      const t = new Date(entry.first_event_at).getTime();
      return Number.isFinite(t) && t >= risingCutoffMs;
    })
    .slice(0, risingLimit)
    .map((entry) => ({
      handle: entry.handle,
      display_name: entry.display_name,
      tier: entry.tier,
      tier_label: entry.tier_label,
      proof_points: entry.proof_points,
      first_event_at: entry.first_event_at,
      reason: 'first_proof_points' as const,
      profile_url: entry.profile_url,
    }));

  return {
    schema_version: 'twog-leaderboard-v1',
    window,
    total_contributors: entries.length,
    generated_at: new Date().toISOString(),
    network_totals: networkTotals,
    entries: entries.slice(0, limit),
    rising,
  };
}
