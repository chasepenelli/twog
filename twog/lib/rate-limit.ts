// Per-IP sliding-window rate limiting for the PUBLIC write routes (suggest / contribute). This is the
// safety prerequisite for opening intake to the world: it caps how many packets one source can push into
// the Neon queue in a window, so the operator triage board isn't buried in spam.
//
// Design notes:
// - Neon-backed (not in-memory) so the limit holds across Vercel's many short-lived serverless instances,
//   which don't share memory. One small append-only table, `public_rate_events`, self-bootstraps.
// - FAIL-OPEN: if Neon is unreachable/unconfigured, we allow the request rather than 500 a legit user.
//   Rate limiting is best-effort spam control, not an auth boundary — availability wins on a DB hiccup.
// - We never store raw IPs. The client IP is hashed (sha256 + salt) before it touches the DB.

import { createHash } from 'crypto';
import { neonRows } from './neon';

const SALT = process.env.RATE_LIMIT_SALT ?? 'twog-public-intake-v1';

// Bootstrap the table at most once per serverless instance (not once per request).
let ensured: Promise<void> | null = null;
function ensureTable(): Promise<void> {
  if (!ensured) {
    ensured = (async () => {
      await neonRows(
        `create table if not exists public_rate_events (
           id bigserial primary key,
           scope text not null,
           ip_hash text not null,
           created_at timestamptz not null default now()
         )`,
      );
      await neonRows(
        `create index if not exists idx_rate_events_scope_ip_time
           on public_rate_events (scope, ip_hash, created_at)`,
      );
    })().catch(() => {
      // let a failed bootstrap retry on the next call rather than caching the failure
      ensured = null;
    });
  }
  return ensured ?? Promise.resolve();
}

/** Resolve the client IP from the most trustworthy header available, or null if none resolves.
 *  Priority: `x-vercel-forwarded-for` (Vercel sets this and STRIPS any inbound `x-vercel-*` header, so a
 *  client cannot spoof it) → `x-real-ip` → the LAST hop of `x-forwarded-for` (a client can prepend spoofed
 *  values to the FRONT of that list, but not past the proxy closest to us). Returns null when nothing
 *  resolves so the caller can fail open instead of herding every header-less request into one shared,
 *  lockable bucket. */
export function clientIp(request: Request): string | null {
  const lastHop = (h: string | null): string =>
    h ? (h.split(',').map((s) => s.trim()).filter(Boolean).pop() ?? '') : '';
  const ip =
    lastHop(request.headers.get('x-vercel-forwarded-for')) ||
    (request.headers.get('x-real-ip') ?? '').trim() ||
    lastHop(request.headers.get('x-forwarded-for'));
  return ip || null;
}

/** Hash so we never persist a raw address. */
export function ipHashOf(ip: string): string {
  return createHash('sha256').update(`${SALT}:${ip}`).digest('hex').slice(0, 32);
}

export type RateResult = { ok: boolean; retryAfterSec: number; remaining: number };

/**
 * Record this attempt and decide whether it's within the per-IP window budget.
 * `limit` = max requests allowed per `windowSec` seconds for one (scope, ip).
 * Returns ok:false with a Retry-After hint when the caller is over budget.
 */
export async function rateLimit(opts: {
  scope: string;
  request: Request;
  limit: number;
  windowSec: number;
}): Promise<RateResult> {
  const { scope, request, limit, windowSec } = opts;

  // No resolvable IP → fail open. On Vercel these headers are always present for external traffic; this
  // only spares off-Vercel/local callers from sharing (and DoS-ing) a single 'unknown' bucket.
  const ip = clientIp(request);
  if (!ip) return { ok: true, retryAfterSec: 0, remaining: limit };
  const ipHash = ipHashOf(ip);
  await ensureTable();

  // One statement: append the attempt, then count the PRIOR events in the window. A data-modifying CTE's
  // insert is NOT visible to the main SELECT's snapshot, so `n` is the count *before* this attempt; the
  // attempt-including count is n+1. Blocked when n+1 > limit  ⇔  n >= limit.
  const rows = await neonRows<{ n: number }>(
    `with ins as (
       insert into public_rate_events (scope, ip_hash) values ($1, $2) returning 1
     )
     select count(*)::int as n
       from public_rate_events
      where scope = $1 and ip_hash = $2
        and created_at > now() - ($3 || ' seconds')::interval`,
    [scope, ipHash, String(windowSec)],
  );

  // Opportunistic prune (~2% of calls) so the append-only table can't grow without bound. Rows older than
  // an hour are far outside any window. Best-effort; neonRows swallows failure.
  if (Math.random() < 0.02) {
    void neonRows(`delete from public_rate_events where created_at < now() - interval '1 hour'`);
  }

  // rows === [] means Neon is unconfigured or the query failed (neonRows swallows) → fail open.
  if (!rows.length) return { ok: true, retryAfterSec: 0, remaining: limit };

  const prior = rows[0].n ?? 0;
  const used = prior + 1;
  const ok = used <= limit;
  return { ok, retryAfterSec: ok ? 0 : windowSec, remaining: Math.max(0, limit - used) };
}
