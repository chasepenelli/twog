// Shared read-only Neon access for the public surfaces (runs / evidence / provenance). Mirrors the pool
// config in lib/candidate-contributions.ts but exposes a GRACEFUL query helper: it never throws — on
// missing config or a DB hiccup it returns [] so pages fall back to an empty/teaser state instead of 500ing.
// NOTE: needs NEON_DATABASE_URL set in the environment (the engine's Neon). In the twog Vercel project
// that var is currently empty — set it to the real connection string for live data in prod.

import { Pool } from 'pg';

declare global {
  // eslint-disable-next-line no-var
  var twogNeonReadPool: Pool | undefined;
}

function databaseUrl(): string | undefined {
  return (
    process.env.NEON_DATABASE_URL ??
    process.env.DATABASE_URL ??
    process.env.POSTGRES_URL ??
    process.env.HSA_DATABASE_URL
  );
}

export function isNeonConfigured(): boolean {
  return Boolean(databaseUrl());
}

function neonPool(): Pool {
  const connectionString = databaseUrl()!;
  if (!globalThis.twogNeonReadPool) {
    globalThis.twogNeonReadPool = new Pool({ connectionString, max: 3, ssl: { rejectUnauthorized: false } });
  }
  return globalThis.twogNeonReadPool;
}

/** Run a read query. NEVER throws: returns [] when Neon isn't configured or the query fails. */
export async function neonRows<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<T[]> {
  if (!isNeonConfigured()) return [];
  try {
    const res = await neonPool().query(sql, params);
    return res.rows as T[];
  } catch {
    return [];
  }
}

/** Run a write/query that MUST surface errors (throws on misconfig/failure). Use for operator writes
 *  where the caller needs to distinguish success from failure (unlike neonRows, which swallows). */
export async function neonWrite<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<T[]> {
  if (!isNeonConfigured()) throw new Error('neon_not_configured');
  const res = await neonPool().query(sql, params);
  return res.rows as T[];
}
