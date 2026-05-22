import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import process from 'node:process';
import pg from 'pg';

const { Pool } = pg;

const connectionString =
  process.env.NEON_DATABASE_URL ??
  process.env.DATABASE_URL ??
  process.env.POSTGRES_URL ??
  process.env.HSA_DATABASE_URL;

if (!connectionString) {
  console.error('Missing Neon/Postgres URL. Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL.');
  process.exit(1);
}

const pool = new Pool({
  connectionString,
  max: 1,
  ssl: { rejectUnauthorized: false },
});

const migrationFiles = [
  '001_candidate_contribution_intake.sql',
  '002_candidate_contribution_proof_network.sql',
];

try {
  for (const file of migrationFiles) {
    const sql = await readFile(join(process.cwd(), 'db', 'migrations', file), 'utf8');
    await pool.query(sql);
    console.log(`Applied ${file}`);
  }
} finally {
  await pool.end();
}
