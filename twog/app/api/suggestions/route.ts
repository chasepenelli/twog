// POST /api/suggestions — the public "suggest an experiment" intake (name a drug / target / question).
// Stores to Neon (public_suggestions) when configured. Graceful: never throws; always returns quickly.

import { NextResponse } from 'next/server';
import { neonRows, isNeonConfigured } from '@/lib/neon';
import { rateLimit } from '@/lib/rate-limit';

export const runtime = 'nodejs';

const MAX_BODY_BYTES = 8_000;

export async function POST(request: Request) {
  // Cheap header guard before we read any body.
  if (Number(request.headers.get('content-length') ?? 0) > MAX_BODY_BYTES) {
    return NextResponse.json({ ok: false, error: 'too_large' }, { status: 413 });
  }

  // Per-IP throttle FIRST — before parsing — so garbage/too-short spam is counted too. Best-effort
  // (fails open on a DB hiccup or when no client IP resolves).
  const rl = await rateLimit({ scope: 'suggest', request, limit: 5, windowSec: 600 });
  if (!rl.ok) {
    return NextResponse.json(
      { ok: false, error: 'rate_limited', retry_after: rl.retryAfterSec },
      { status: 429, headers: { 'Retry-After': String(rl.retryAfterSec) } },
    );
  }

  let idea = '';
  let email = '';
  try {
    const body = (await request.json()) as { idea?: string; email?: string };
    idea = String(body.idea ?? '').trim().slice(0, 2000);
    email = String(body.email ?? '').trim().toLowerCase().slice(0, 200);
  } catch {
    /* bad body */
  }
  if (idea.length < 4) {
    return NextResponse.json({ ok: false, error: 'too_short' }, { status: 400 });
  }

  let id: string | null = null;
  if (isNeonConfigured()) {
    await neonRows(
      `create table if not exists public_suggestions (
         id uuid primary key default gen_random_uuid(),
         idea text not null,
         email text,
         created_at timestamptz not null default now()
       )`,
    );
    const rows = await neonRows<{ id: string }>(
      `insert into public_suggestions (idea, email) values ($1, $2) returning id`,
      [idea, email || null],
    );
    id = rows[0]?.id ?? null;
  }
  return NextResponse.json({ ok: true, id });
}
