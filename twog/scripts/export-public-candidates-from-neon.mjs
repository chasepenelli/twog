import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import process from 'node:process';
import pg from 'pg';

const { Pool } = pg;

const connectionString =
  process.env.NEON_DATABASE_URL ??
  process.env.DATABASE_URL ??
  process.env.POSTGRES_URL ??
  process.env.HSA_DATABASE_URL;

const output = process.env.TWOG_PUBLIC_CANDIDATES_OUT ?? join(process.cwd(), 'data', 'public-candidates.json');
const includeVisibility = (process.env.TWOG_PUBLIC_CANDIDATE_VISIBILITY ?? 'draft_public,public')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const mergeExisting = process.env.TWOG_PUBLIC_CANDIDATE_MERGE_EXISTING !== 'false';

if (!connectionString) {
  console.error('Missing Neon/Postgres URL. Set NEON_DATABASE_URL, DATABASE_URL, POSTGRES_URL, or HSA_DATABASE_URL.');
  process.exit(1);
}

function normalizePayload(value) {
  if (!value) return null;
  if (typeof value === 'string') return JSON.parse(value);
  return value;
}

async function readExistingDataset(path) {
  try {
    const raw = await readFile(path, 'utf8');
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed.candidates) ? parsed.candidates : [];
  } catch (error) {
    if (error?.code === 'ENOENT') return [];
    throw error;
  }
}

const pool = new Pool({
  connectionString,
  max: 1,
  ssl: { rejectUnauthorized: false },
});

try {
  const params = [];
  let where = '';
  if (includeVisibility.length > 0) {
    params.push(includeVisibility);
    where = 'where visibility = any($1::text[])';
  }

  const candidateRows = await pool.query(
    `
      select candidate_id, latest_snapshot_id, priority_score, updated_at, payload
      from public_candidates
      ${where}
      order by priority_score desc, updated_at desc, candidate_id asc
    `,
    params
  );

  const candidates = [];
  for (const row of candidateRows.rows) {
    const candidate = normalizePayload(row.payload);
    if (!candidate?.candidate_id) continue;

    let snapshotRow = null;
    const latestSnapshotId = row.latest_snapshot_id ?? candidate.latest_snapshot_id;
    if (latestSnapshotId) {
      const latest = await pool.query(
        'select payload from public_candidate_snapshots where snapshot_id = $1 limit 1',
        [String(latestSnapshotId)]
      );
      snapshotRow = latest.rows[0] ?? null;
    }

    if (!snapshotRow) {
      const latest = await pool.query(
        `
          select payload
          from public_candidate_snapshots
          where candidate_id = $1
          order by snapshot_version desc, created_at desc
          limit 1
        `,
        [candidate.candidate_id]
      );
      snapshotRow = latest.rows[0] ?? null;
    }

    const events = await pool.query(
      `
        select payload
        from public_candidate_decision_events
        where candidate_id = $1
        order by occurred_at desc
        limit 100
      `,
      [candidate.candidate_id]
    );

    candidates.push({
      candidate,
      latest_snapshot: normalizePayload(snapshotRow?.payload),
      decision_events: events.rows.map((eventRow) => normalizePayload(eventRow.payload)).filter(Boolean),
    });
  }

  if (mergeExisting) {
    const seen = new Set(candidates.map((entry) => entry.candidate?.candidate_id).filter(Boolean));
    for (const existing of await readExistingDataset(output)) {
      const candidateId = existing?.candidate?.candidate_id;
      if (!candidateId || seen.has(candidateId)) continue;
      candidates.push(existing);
      seen.add(candidateId);
    }
  }

  const payload = {
    generatedAt: new Date().toISOString(),
    source: 'neon:public_candidates',
    sync: {
      visibility: includeVisibility,
      mergeExisting,
      exportedCount: candidateRows.rows.length,
      totalCount: candidates.length,
    },
    candidates,
  };

  await mkdir(dirname(output), { recursive: true });
  await writeFile(output, `${JSON.stringify(payload, null, 2)}\n`);
  console.log(`Wrote ${candidates.length} public candidate(s) to ${output}`);
  console.log(`Exported ${candidateRows.rows.length} candidate(s) from Neon.`);
} finally {
  await pool.end();
}
