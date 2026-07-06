// Dump the real proof ledger (roster + capsules + runs) to /tmp/ledger.json for a Research Director pass.
// Step 1 of the director pipeline:  dump-ledger  →  (frontier reasoning: the research-director agent)  →
// persist-director-decisions.  Read-only. Env: NEON_DATABASE_URL (or .env.local fallback), OUT (path).
//
//   node scripts/dump-ledger.mjs

import fs from 'node:fs';
import process from 'node:process';
import pg from 'pg';

const OUT = process.env.OUT || '/tmp/ledger.json';
const PUBLIC = ['submitted', 'accepted', 'promoted'];
const ALL_LANES = ['docking', 'cofolding', 'md', 'omics'];

let url = process.env.NEON_DATABASE_URL || process.env.DATABASE_URL;
if (!url) {
  for (const f of ['.env.local', '.env']) {
    try { for (const l of fs.readFileSync(f, 'utf8').split('\n')) { const m = l.match(/^\s*(NEON_DATABASE_URL|DATABASE_URL)\s*=\s*"?([^"\n]+)"?/); if (m && m[2].startsWith('postgres')) { url = m[2]; break; } } } catch { /* */ }
    if (url) break;
  }
}
const pool = new pg.Pool({ connectionString: url, max: 2, ssl: { rejectUnauthorized: false } });

const cands = (await pool.query(
  `select candidate_id, title, public_status, payload->'targets' as targets,
     (payload->>'validation_ready')::text as validation_ready, priority_score
   from public_candidates`,
)).rows;
const caps = (await pool.query(
  `select capsule_id::text, candidate_id, payload->'payload'->>'signal' as signal,
     payload->'payload'->>'validation_type' as lane, payload->'summary'->>'finding' as finding
   from proof_capsules where status = any($1)`, [PUBLIC],
)).rows;

const byC = {};
for (const c of caps) (byC[c.candidate_id] = byC[c.candidate_id] || []).push(c);
const verdict = (cs) =>
  cs.some((x) => x.signal === 'refutes') ? 'ruled-out' : cs.some((x) => x.signal === 'supports') ? 'still-standing' : 'needs-more';

const ledger = cands.map((c) => {
  const cs = byC[c.candidate_id] || [];
  const tested = [...new Set(cs.map((x) => x.lane).filter(Boolean))];
  return {
    candidate_id: c.candidate_id, title: c.title, targets: c.targets, public_status: c.public_status,
    validation_ready: c.validation_ready, priority_score: c.priority_score,
    verdict: cs.length ? verdict(cs) : 'needs-more',
    tested_lanes: tested, untested_lanes: ALL_LANES.filter((l) => !tested.includes(l)),
    capsules: cs.map((x) => ({ capsule_id: x.capsule_id, signal: x.signal, lane: x.lane, finding: (x.finding || '').slice(0, 200) })),
  };
});
const runs = (await pool.query(
  `select payload->>'manifest_id' as manifest_id, payload->>'title' as title,
     payload->'output_refs'->'rollup'->'leading_hypothesis_status' as rollup
   from run_manifests where manifest_type='falsification_campaign' order by created_at desc limit 5`,
)).rows;

fs.writeFileSync(OUT, JSON.stringify({ candidate_count: ledger.length, capsule_count: caps.length, ledger, runs }, null, 1));
const vc = {}; ledger.forEach((x) => (vc[x.verdict] = (vc[x.verdict] || 0) + 1));
console.log(`wrote ${OUT}: ${ledger.length} candidates, ${caps.length} capsules, verdicts ${JSON.stringify(vc)}`);
await pool.end();
