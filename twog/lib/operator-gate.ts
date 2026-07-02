// Stopgap operator auth for the triage board. A single shared passphrase (TWOG_OPERATOR_TOKEN) compared
// constant-time against an httpOnly cookie. This is TEMPORARY — the canonical operator surface is the
// WorkOS-gated web/ console; this ships the twog board now. Gate BOTH the page and every operator API.

import { cookies } from 'next/headers';
import { timingSafeEqual } from 'node:crypto';

export const OPERATOR_COOKIE = 'twog_operator';

export function operatorToken(): string | undefined {
  const t = process.env.TWOG_OPERATOR_TOKEN;
  return t && t.length > 0 ? t : undefined;
}

export function isOperatorConfigured(): boolean {
  return Boolean(operatorToken());
}

/** Constant-time string compare that also resists length leaks. */
export function tokenMatches(candidate: string | undefined): boolean {
  const token = operatorToken();
  if (!token || !candidate) return false;
  const a = Buffer.from(candidate);
  const b = Buffer.from(token);
  if (a.length !== b.length) {
    // still burn a compare against a fixed-length buffer to avoid trivial timing signal
    timingSafeEqual(b, b);
    return false;
  }
  return timingSafeEqual(a, b);
}

/** True when the current request carries a valid operator cookie. Server-only (reads cookies()). */
export async function isOperator(): Promise<boolean> {
  const jar = await cookies();
  return tokenMatches(jar.get(OPERATOR_COOKIE)?.value);
}
