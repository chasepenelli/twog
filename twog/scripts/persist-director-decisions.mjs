// Persist a Research Director pass into Neon research_director_decisions, with a HARD grounding check:
// every cited_capsule_id is validated against real proof_capsules before insert (invalid ids are
// dropped; a decision left with zero valid citations that isn't a genuinely new proposal is skipped).
//
//   node scripts/persist-director-decisions.mjs /tmp/director-out.json
//
// Input JSON: { run_tag, model_profile?, decisions:[...], rejected?:[candidate_id,...] }

import fs from 'node:fs';
import process from 'node:process';
import pg from 'pg';

const inFile = process.argv[2] || '/tmp/director-out.json';
const doc = JSON.parse(fs.readFileSync(inFile, 'utf8'));
const runTag = doc.run_tag || `director-${Date.now()}`;
const modelProfile = doc.model_profile || 'frontier-agent';
const rejected = new Set(doc.rejected || []);

let url = process.env.NEON_DATABASE_URL || process.env.DATABASE_URL;
if (!url) {
  for (const f of ['.env.local', '.env']) {
    try { for (const l of fs.readFileSync(f, 'utf8').split('\n')) { const m = l.match(/^\s*(NEON_DATABASE_URL|DATABASE_URL)\s*=\s*"?([^"\n]+)"?/); if (m && m[2].startsWith('postgres')) { url = m[2]; break; } } } catch { /* */ }
    if (url) break;
  }
}
const pool = new pg.Pool({ connectionString: url, max: 3, ssl: { rejectUnauthorized: false } });

const realCapsules = new Set(
  (await pool.query(`select capsule_id::text from proof_capsules`)).rows.map((r) => r.capsule_id),
);

let inserted = 0, dropped = 0, ungrounded = 0;
for (const d of doc.decisions || []) {
  if (rejected.has(d.candidate_id)) { dropped++; continue; }
  const cited = (d.cited_capsule_ids || []).filter((c) => realCapsules.has(c));
  // deprioritize + testable-target proposals should cite at least one real capsule; a genuinely new
  // frontier idea may legitimately have few, but require >=1 valid citation to keep the feed honest.
  if (cited.length === 0) { ungrounded++; continue; }
  await pool.query(
    `insert into research_director_decisions
       (run_tag, candidate_id, title, target, action_kind, modality, testable_now, decision, rationale,
        confidence, priority, current_verdict, next_lane, est_cost_usd, cited_capsule_ids, model_profile)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16)`,
    [
      runTag, d.candidate_id ?? null, d.title ?? null, d.target ?? null, d.action_kind,
      d.modality || null, typeof d.testable_now === 'boolean' ? d.testable_now : null,
      d.decision, d.rationale, typeof d.confidence === 'number' ? d.confidence : null,
      Number.isInteger(d.priority) ? d.priority : null, d.current_verdict ?? null,
      d.next_lane || null, typeof d.est_cost_usd === 'number' ? d.est_cost_usd : null,
      JSON.stringify(cited), modelProfile,
    ],
  );
  inserted++;
}

console.log(`run_tag=${runTag}`);
console.log(`inserted=${inserted}  dropped_by_critic=${dropped}  dropped_ungrounded=${ungrounded}`);
await pool.end();
