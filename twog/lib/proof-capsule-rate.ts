/**
 * Per-handle Proof Capsule submission rate limiter.
 *
 * Hour-bucketed sliding window. The submit path calls
 * ``checkSubmissionRate(handle)`` *before* doing any expensive work, and
 * ``recordSubmissionRate(handle)`` *after* a successful submission. We
 * intentionally do not record on rejected submissions so a misbehaving
 * agent can stop sending and recover within one window.
 *
 * Defaults: 60 submissions per handle per hour. Override via the
 * ``TWOG_PROOF_CAPSULE_RATE_LIMIT_PER_HOUR`` env var on the server.
 */

import { Pool } from 'pg';

const DEFAULT_RATE_LIMIT_PER_HOUR = 60;
const WINDOW_HOURS = 1;

declare global {
  var twogProofCapsuleRatePool: Pool | undefined;
}

function databaseUrl(): string | undefined {
  return (
    process.env.NEON_DATABASE_URL ??
    process.env.DATABASE_URL ??
    process.env.POSTGRES_URL ??
    process.env.HSA_DATABASE_URL
  );
}

function pool(): Pool {
  const connectionString = databaseUrl();
  if (!connectionString) {
    throw new Error('proof capsule rate storage is not configured');
  }
  if (!globalThis.twogProofCapsuleRatePool) {
    globalThis.twogProofCapsuleRatePool = new Pool({
      connectionString,
      max: 2,
      ssl: { rejectUnauthorized: false },
    });
  }
  return globalThis.twogProofCapsuleRatePool;
}

export function rateLimitPerHour(): number {
  const raw = process.env.TWOG_PROOF_CAPSULE_RATE_LIMIT_PER_HOUR;
  if (!raw) return DEFAULT_RATE_LIMIT_PER_HOUR;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return DEFAULT_RATE_LIMIT_PER_HOUR;
  }
  return Math.floor(parsed);
}

function currentBucketStart(now: Date = new Date()): Date {
  return new Date(
    Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate(),
      now.getUTCHours(),
      0,
      0,
      0,
    ),
  );
}

function windowStart(now: Date = new Date()): Date {
  return new Date(now.getTime() - WINDOW_HOURS * 60 * 60 * 1000);
}

export async function ensureProofCapsuleRateSchema(): Promise<void> {
  await pool().query(`
    create table if not exists proof_capsule_submission_rate (
      handle text not null,
      window_start timestamptz not null,
      submission_count integer not null default 0,
      primary key (handle, window_start)
    );
    create index if not exists proof_capsule_submission_rate_handle_window_idx
      on proof_capsule_submission_rate (handle, window_start desc);
  `);
}

export interface RateCheckResult {
  allowed: boolean;
  current: number;
  limit: number;
}

/**
 * Read-only check: how many submissions has this handle made in the
 * trailing window? Returns ``allowed=false`` if a new submission would
 * exceed the limit. Side-effect-free — call ``recordSubmissionRate``
 * after the submission succeeds to advance the counter.
 */
export async function checkSubmissionRate(handle: string): Promise<RateCheckResult> {
  await ensureProofCapsuleRateSchema();
  const limit = rateLimitPerHour();
  const since = windowStart();
  const result = await pool().query<{ total: number }>(
    `select coalesce(sum(submission_count), 0)::int as total
     from proof_capsule_submission_rate
     where handle = $1 and window_start >= $2`,
    [handle, since],
  );
  const current = Number(result.rows[0]?.total ?? 0);
  return { allowed: current < limit, current, limit };
}

/**
 * Record one accepted submission for ``handle`` in the current hour
 * bucket. Idempotent at the bucket-row level via ON CONFLICT.
 */
export async function recordSubmissionRate(handle: string): Promise<void> {
  await ensureProofCapsuleRateSchema();
  const bucket = currentBucketStart();
  await pool().query(
    `insert into proof_capsule_submission_rate (handle, window_start, submission_count)
     values ($1, $2, 1)
     on conflict (handle, window_start) do update
       set submission_count = proof_capsule_submission_rate.submission_count + 1`,
    [handle, bucket],
  );
}
