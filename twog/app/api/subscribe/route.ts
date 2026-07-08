// POST /api/subscribe — the newsletter / early-access email capture. Stores the email in Neon
// (email_signups) when configured and sets the twog_email cookie so the deep experiment tier unlocks.
// Honest: this is a mailing list + early-access wall, not a login. Graceful: the cookie unlocks even if
// the DB write can't happen, so the reveal never depends on storage.

import { NextResponse } from 'next/server';
import { cookies } from 'next/headers';
import { neonRows, isNeonConfigured } from '@/lib/neon';
import { EMAIL_COOKIE } from '@/lib/gate';

const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;

export async function POST(request: Request) {
  let email = '';
  let source = 'unknown';
  try {
    const body = (await request.json()) as { email?: string; source?: string };
    email = String(body.email ?? '').trim().toLowerCase();
    source = String(body.source ?? 'unknown').slice(0, 64);
  } catch {
    /* bad body */
  }
  if (!EMAIL_RE.test(email)) {
    return NextResponse.json({ ok: false, error: 'invalid_email' }, { status: 400 });
  }

  if (isNeonConfigured()) {
    // create-if-missing + upsert; neonRows never throws, so a DB hiccup won't block the unlock
    await neonRows(
      `create table if not exists email_signups (
         email text primary key,
         source text,
         created_at timestamptz not null default now()
       )`,
    );
    await neonRows(
      `insert into email_signups (email, source) values ($1, $2) on conflict (email) do nothing`,
      [email, source],
    );
  }

  const jar = await cookies();
  jar.set(EMAIL_COOKIE, email, {
    httpOnly: false,
    sameSite: 'lax',
    path: '/',
    maxAge: 60 * 60 * 24 * 365,
  });
  return NextResponse.json({ ok: true });
}
