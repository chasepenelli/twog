// Seed the Research Director's frontier proposals into public_candidates so they become first-class,
// browsable candidates on /candidates (the frontier roster). Grounded, honest provenance: each is marked
// public_status='frontier_proposed', candidate_kind='frontier_<modality>', source='research_director',
// carrying the cited capsule ids + rationale. NO compute is run here — these are proposals until tested.
//
//   node scripts/seed-frontier-roster.mjs [run_tag]
//
// Idempotent (upsert on candidate_id). Reads research_director_decisions (action_kind='propose_frontier').

import fs from 'node:fs';
import process from 'node:process';
import { createHash } from 'node:crypto';
import pg from 'pg';

const runTag = process.argv[2] || 'director-frontier-2026-07-06';

let url = process.env.NEON_DATABASE_URL || process.env.DATABASE_URL;
if (!url) {
  for (const f of ['.env.local', '.env']) {
    try { for (const l of fs.readFileSync(f, 'utf8').split('\n')) { const m = l.match(/^\s*(NEON_DATABASE_URL|DATABASE_URL)\s*=\s*"?([^"\n]+)"?/); if (m && m[2].startsWith('postgres')) { url = m[2]; break; } } } catch { /* */ }
    if (url) break;
  }
}
const pool = new pg.Pool({ connectionString: url, max: 3, ssl: { rejectUnauthorized: false } });

const decisions = (await pool.query(
  `select candidate_id, title, target, modality, testable_now, decision, rationale, confidence, next_lane,
     est_cost_usd, cited_capsule_ids
   from research_director_decisions
   where run_tag = $1 and action_kind = 'propose_frontier'
   order by priority asc`, [runTag],
)).rows;

let upserted = 0;
for (const d of decisions) {
  const payload = {
    title: d.title,
    targets: d.target ? [d.target] : [],
    biomarkers: [],
    modality: d.modality,
    testable_now: d.testable_now,
    summary: d.decision,
    rationale: d.rationale,
    evidence_refs: Array.isArray(d.cited_capsule_ids) ? d.cited_capsule_ids : [],
    validation_ready: false,
    public_status: 'frontier_proposed',
    candidate_kind: `frontier_${d.modality || 'modality'}`,
    source: 'research_director',
    run_tag: runTag,
    next_lane: d.next_lane || null,
    est_cost_usd: d.est_cost_usd ?? null,
  };
  const contentHash = createHash('sha256').update(JSON.stringify(payload)).digest('hex');
  await pool.query(
    `insert into public_candidates
       (candidate_id, candidate_kind, public_status, visibility, content_hash, priority_score, title, payload, created_at, updated_at)
     values ($1, $2, 'frontier_proposed', 'private', $3, $4, $5, $6::jsonb, now(), now())
     on conflict (candidate_id) do update set
       candidate_kind = excluded.candidate_kind, public_status = excluded.public_status,
       content_hash = excluded.content_hash, priority_score = excluded.priority_score,
       title = excluded.title, payload = excluded.payload, updated_at = now()`,
    [d.candidate_id, payload.candidate_kind, contentHash, typeof d.confidence === 'number' ? d.confidence : null, d.title, JSON.stringify(payload)],
  );
  upserted++;
}

console.log(`seeded ${upserted} frontier proposals into public_candidates (run_tag=${runTag})`);
const total = (await pool.query(`select count(*)::int n from public_candidates where public_status='frontier_proposed'`)).rows[0].n;
console.log(`total frontier_proposed candidates now: ${total}`);
await pool.end();
