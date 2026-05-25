#!/usr/bin/env node
/**
 * Apply Proof Network migrations to a Neon/Postgres database.
 *
 * Applies four idempotent migrations in order:
 *   db/migrations/008_work_packets.sql
 *   db/migrations/009_proof_capsules.sql
 *   twog/db/migrations/003_work_packets.sql
 *   twog/db/migrations/004_proof_capsules.sql
 *
 * All four are CREATE TABLE IF NOT EXISTS + CREATE INDEX IF NOT EXISTS,
 * so reruns are safe. Use this against a Neon staging branch first.
 *
 * Reads the connection string from (in order):
 *   NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, HSA_DATABASE_URL
 *
 * Usage:
 *   HSA_DATABASE_URL='<staging-url>' node scripts/apply-proof-network-migrations.mjs
 */

import { readFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import process from 'node:process';
import pg from 'pg';

const { Pool } = pg;

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, '..');

const connectionString =
  process.env.NEON_DATABASE_URL ??
  process.env.DATABASE_URL ??
  process.env.POSTGRES_URL ??
  process.env.HSA_DATABASE_URL;

if (!connectionString) {
  console.error(
    'Missing connection string. Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL.',
  );
  process.exit(1);
}

const migrations = [
  join(repoRoot, 'db', 'migrations', '008_work_packets.sql'),
  join(repoRoot, 'db', 'migrations', '009_proof_capsules.sql'),
  join(repoRoot, 'twog', 'db', 'migrations', '003_work_packets.sql'),
  join(repoRoot, 'twog', 'db', 'migrations', '004_proof_capsules.sql'),
];

// Print the host (no credentials) so the operator sees which DB is being touched.
try {
  const u = new URL(connectionString.replace('postgres://', 'http://').replace('postgresql://', 'http://'));
  console.log(`Target: ${u.hostname}${u.pathname}`);
} catch {
  console.log('Target: <unparseable url, proceeding anyway>');
}

const pool = new Pool({
  connectionString,
  max: 1,
  ssl: { rejectUnauthorized: false },
});

let failed = false;
try {
  for (const path of migrations) {
    const sql = await readFile(path, 'utf8');
    process.stdout.write(`Applying ${path.replace(repoRoot + '/', '')} … `);
    try {
      await pool.query(sql);
      console.log('ok');
    } catch (error) {
      failed = true;
      console.log('FAILED');
      console.error(error.message);
      break;
    }
  }

  if (!failed) {
    const verify = await pool.query(`
      select table_name
      from information_schema.tables
      where table_schema = 'public'
        and table_name in ('work_packets', 'proof_capsules', 'proof_capsule_reviews')
      order by table_name
    `);
    console.log('\nTables now present:');
    for (const row of verify.rows) {
      console.log(`  - ${row.table_name}`);
    }
  }
} finally {
  await pool.end();
  if (failed) process.exit(1);
}
