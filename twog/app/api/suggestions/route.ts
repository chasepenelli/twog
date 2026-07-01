// POST /api/suggestions — the public "suggest an experiment" intake (name a drug / target / question).
// Stores to Neon (public_suggestions) when configured. Graceful: never throws; always returns quickly.

import { NextResponse } from 'next/server';
import { neonRows, isNeonConfigured } from '@/lib/neon';

export async function POST(request: Request) {
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

  if (isNeonConfigured()) {
    await neonRows(
      `create table if not exists public_suggestions (
         id uuid primary key default gen_random_uuid(),
         idea text not null,
         email text,
         created_at timestamptz not null default now()
       )`,
    );
    await neonRows(`insert into public_suggestions (idea, email) values ($1, $2)`, [idea, email || null]);
  }
  return NextResponse.json({ ok: true });
}
