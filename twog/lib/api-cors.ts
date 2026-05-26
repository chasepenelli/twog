/**
 * CORS helpers for public Proof Network read endpoints.
 *
 * Read endpoints are deliberately open: an agent on any origin should be
 * able to `fetch('https://twog.bio/api/work-packets')` without a
 * server-side proxy. Write endpoints (POST /api/proof-capsules) stay
 * same-origin for now; agents that POST should do so directly over
 * server-to-server HTTP without CORS in the path.
 *
 * Cache headers stay alongside CORS so the read story is one place:
 * pass them straight to NextResponse.json's `{ headers }`.
 */

export const PUBLIC_READ_CACHE_CONTROL = 's-maxage=60, stale-while-revalidate';

export const PUBLIC_READ_HEADERS: Record<string, string> = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Accept',
  'Access-Control-Max-Age': '600',
  Vary: 'Origin',
  'Cache-Control': PUBLIC_READ_CACHE_CONTROL,
};

/**
 * Build a header object for a read response. Optionally override the
 * default cache control (e.g. for endpoints with tighter freshness
 * requirements).
 */
export function publicReadHeaders(overrides?: { cacheControl?: string }): Record<string, string> {
  return {
    ...PUBLIC_READ_HEADERS,
    ...(overrides?.cacheControl
      ? { 'Cache-Control': overrides.cacheControl }
      : {}),
  };
}

/**
 * Shared OPTIONS handler for public read endpoints. Returns 204 with the
 * CORS headers so a browser pre-flight succeeds before the real GET.
 */
export function publicOptionsResponse(): Response {
  return new Response(null, { status: 204, headers: PUBLIC_READ_HEADERS });
}
