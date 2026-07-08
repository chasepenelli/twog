import { NextResponse } from 'next/server';
import { cookies } from 'next/headers';
import { OPERATOR_COOKIE } from '@/lib/operator-gate';

export const runtime = 'nodejs';

export async function POST() {
  const jar = await cookies();
  jar.delete(OPERATOR_COOKIE);
  return NextResponse.json({ ok: true });
}
