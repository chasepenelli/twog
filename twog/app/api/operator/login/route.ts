import { NextResponse } from 'next/server';
import { cookies } from 'next/headers';
import { OPERATOR_COOKIE, isOperatorConfigured, tokenMatches } from '@/lib/operator-gate';

export const runtime = 'nodejs';

const MAX_AGE = 60 * 60 * 24 * 30; // 30 days

export async function POST(request: Request) {
  if (!isOperatorConfigured()) {
    return NextResponse.json(
      { error: 'operator_not_configured', message: 'Operator access is not set up (TWOG_OPERATOR_TOKEN unset).' },
      { status: 503 },
    );
  }
  let token: string | undefined;
  try {
    const body = await request.json();
    token = typeof body?.token === 'string' ? body.token : undefined;
  } catch {
    /* fall through to 400 */
  }
  if (!token) {
    return NextResponse.json({ error: 'missing_token' }, { status: 400 });
  }
  if (!tokenMatches(token)) {
    return NextResponse.json({ error: 'invalid_token' }, { status: 401 });
  }
  const jar = await cookies();
  jar.set(OPERATOR_COOKIE, token, {
    httpOnly: true,
    secure: true,
    sameSite: 'lax',
    path: '/',
    maxAge: MAX_AGE,
  });
  return NextResponse.json({ ok: true });
}
