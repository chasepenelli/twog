// Smoke test: the Contribute lane (public intake) + the operator triage board, end to end.
//
//   node scripts/smoke-contribution-lane.mjs
//
// Env:
//   BASE_URL             target origin (default http://localhost:3001; e.g. https://twog.bio)
//   TWOG_OPERATOR_TOKEN  operator passphrase — required to exercise the operator/triage cases
//   NEON_DATABASE_URL    (or DATABASE_URL / .env.local fallback) — used to seed + verify + clean up
//   SMOKE_CANDIDATE      live candidate_id to attach test rows to (default carvedilol-vegfr2)
//
// It seeds test rows DIRECTLY into Neon (so it's independent of the TWOG_CONTRIBUTIONS_OPEN pause flag),
// drives the real HTTP operator API + gate, verifies the persisted state matches the engine's triage
// contract, and deletes everything it created (rows are tagged by a unique per-run contact). Exit 1 on
// any failed assertion, 0 on all-pass — safe to wire into CI.

import fs from 'node:fs';
import process from 'node:process';
import { randomUUID } from 'node:crypto';
import pg from 'pg';

const BASE = (process.env.BASE_URL || 'http://localhost:3001').replace(/\/$/, '');
const TOKEN = process.env.TWOG_OPERATOR_TOKEN || '';
const CAND = process.env.SMOKE_CANDIDATE || 'carvedilol-vegfr2';
const RUN = randomUUID().slice(0, 8);
const TAG = `smoke-${RUN}@example.com`; // unique contact → precise cleanup

let NEON = process.env.NEON_DATABASE_URL || process.env.DATABASE_URL || process.env.POSTGRES_URL || process.env.HSA_DATABASE_URL;
if (!NEON) {
  for (const f of ['.env.local', '.env']) {
    try {
      for (const line of fs.readFileSync(f, 'utf8').split('\n')) {
        const m = line.match(/^\s*(NEON_DATABASE_URL|DATABASE_URL)\s*=\s*"?([^"\n]+)"?/);
        if (m && m[2].startsWith('postgres')) { NEON = m[2]; break; }
      }
    } catch { /* ignore */ }
    if (NEON) break;
  }
}
if (!NEON) { console.error('Missing Neon URL (NEON_DATABASE_URL). Cannot seed/verify.'); process.exit(2); }

const pool = new pg.Pool({ connectionString: NEON, max: 3, ssl: { rejectUnauthorized: false } });

let pass = 0;
const fails = [];
const ok = (name, cond, extra = '') => {
  if (cond) { pass++; console.log(`  ✓ ${name}`); }
  else { fails.push(name); console.log(`  ✗ ${name}${extra ? `  (${extra})` : ''}`); }
};

async function req(method, path, { body, cookie } = {}) {
  const headers = { 'Content-Type': 'application/json' };
  if (cookie) headers.Cookie = cookie;
  const r = await fetch(`${BASE}${path}`, { method, headers, body: body ? JSON.stringify(body) : undefined });
  let json;
  try { json = await r.clone().json(); } catch { /* non-json */ }
  const setCookie = typeof r.headers.getSetCookie === 'function' ? r.headers.getSetCookie() : [r.headers.get('set-cookie')].filter(Boolean);
  return { status: r.status, json, setCookie };
}

async function seed({ type = 'evidence', relation = 'supports', action = 'evidence_review', title = 'Smoke contribution' } = {}) {
  const packet = {
    title,
    summary: 'A seeded smoke-test contribution exercising the operator triage board.',
    claim_or_question: 'Does the triage transition persist correctly end to end?',
    requested_system_action: action,
  };
  const r = await pool.query(
    `insert into candidate_contribution_intake
       (contribution_id, candidate_id, source_payload_url, status, contribution_type,
        relation_to_current_record, requested_system_action, contributor, evidence, artifacts, packet)
     values (gen_random_uuid(), $1, $2, 'queued_for_intake', $3, $4, $5, $6::jsonb, '[]'::jsonb, '[]'::jsonb, $7::jsonb)
     returning contribution_id::text`,
    [CAND, `/api/public-candidates/${CAND}`, type, relation, action, JSON.stringify({ contact: TAG, name: 'Smoke' }), JSON.stringify(packet)],
  );
  return r.rows[0].contribution_id;
}
const stateOf = async (id) => (await pool.query(
  'select status, promoted_queue_id, review_notes, reviewed_at from candidate_contribution_intake where contribution_id::text = $1', [id],
)).rows[0];

async function main() {
  console.log(`\nsmoke: contribution lane + triage  —  BASE=${BASE}  candidate=${CAND}  run=${RUN}\n`);

  // 1) Intake API — the repoint (live candidate no longer 404s) + gate behaviour
  console.log('intake API:');
  const status = await req('GET', `/api/public-candidates/${CAND}/contributions`);
  ok('live candidate status → 200 (repoint, was 404)', status.status === 200, `got ${status.status}`);
  const paused = status.json?.intake_paused !== false; // default to "paused" if unknown
  const bogus = await req('GET', `/api/public-candidates/__nope__/contributions`);
  ok('unknown candidate → 404', bogus.status === 404, `got ${bogus.status}`);
  const post = await req('POST', `/api/public-candidates/${CAND}/contributions`, {
    body: { contribution_type: 'evidence', contributor: { contact: TAG }, title: 'Smoke intake', summary: 'A sufficiently long summary for validation here.', claim_or_question: 'Does public intake accept this?', relation_to_current_record: 'supports', requested_system_action: 'evidence_review', evidence: [], artifacts: [] },
  });
  if (paused) ok('POST intake while paused → 503', post.status === 503, `got ${post.status}`);
  else {
    ok('POST intake while open → 202', post.status === 202, `got ${post.status}`);
    const bad = await req('POST', `/api/public-candidates/${CAND}/contributions`, {
      body: { contribution_type: 'evidence', contributor: { contact: TAG }, title: 'x', summary: 'short', claim_or_question: 'no', relation_to_current_record: 'supports', requested_system_action: 'evidence_review' },
    });
    ok('POST invalid packet → 400', bad.status === 400, `got ${bad.status}`);
  }

  // 2) Operator gate
  console.log('\noperator gate:');
  const gateSeed = await seed();
  const noCookie = await req('PATCH', `/api/operator/contributions/${gateSeed}`, { body: { action: 'start_triage' } });
  ok('PATCH without operator cookie → 401', noCookie.status === 401, `got ${noCookie.status}`);
  const wrong = await req('POST', '/api/operator/login', { body: { token: 'definitely-not-the-token' } });
  ok('login wrong token → 401 (or 503 if unconfigured)', wrong.status === 401 || wrong.status === 503, `got ${wrong.status}`);

  let cookie = null;
  if (!TOKEN) {
    console.log('  (no TWOG_OPERATOR_TOKEN in env — skipping the authenticated triage cases)');
  } else {
    const login = await req('POST', '/api/operator/login', { body: { token: TOKEN } });
    ok('login correct token → 200', login.status === 200, `got ${login.status}`);
    const raw = (login.setCookie || []).join(';');
    ok('login sets httpOnly operator cookie', /twog_operator=/.test(raw) && /httponly/i.test(raw), raw.slice(0, 60));
    if (login.status === 200) cookie = `twog_operator=${TOKEN}`;
  }

  // 3) Triage transitions (need the cookie) — verify against the engine contract
  if (cookie) {
    console.log('\ntriage transitions:');
    const badAction = await req('PATCH', `/api/operator/contributions/${gateSeed}`, { cookie, body: { action: 'nuke_it' } });
    ok('invalid action → 400', badAction.status === 400, `got ${badAction.status}`);
    const notFound = await req('PATCH', '/api/operator/contributions/00000000-0000-0000-0000-000000000000', { cookie, body: { action: 'reject' } });
    ok('nonexistent contribution → 404', notFound.status === 404, `got ${notFound.status}`);

    // start_triage → triage_in_progress, reviewed_at stays null
    await req('PATCH', `/api/operator/contributions/${gateSeed}`, { cookie, body: { action: 'start_triage', operator: 'SMOKE', review_notes: 'looking now' } });
    let s = await stateOf(gateSeed);
    ok('start_triage → triage_in_progress', s.status === 'triage_in_progress', s.status);
    ok('start_triage keeps reviewed_at null', s.reviewed_at === null, String(s.reviewed_at));

    // accept_for_evidence_review → status + queue tag + reviewed_at + 2nd note line
    await req('PATCH', `/api/operator/contributions/${gateSeed}`, { cookie, body: { action: 'accept_for_evidence_review', operator: 'SMOKE', review_notes: 'evidence worth reviewing' } });
    s = await stateOf(gateSeed);
    ok('accept → accepted_for_evidence_review', s.status === 'accepted_for_evidence_review', s.status);
    ok('mints promoted_queue_id :evidence_review', s.promoted_queue_id === `candidate_contribution:${gateSeed}:evidence_review`, s.promoted_queue_id);
    ok('sets reviewed_at on acceptance', !!s.reviewed_at);
    ok('review_notes appended (2 lines)', (s.review_notes || '').split('\n').length === 2, JSON.stringify(s.review_notes));
    ok('review-note format matches engine', /\] TRIAGE SMOKE: accept_for_evidence_review - evidence worth reviewing/.test(s.review_notes || ''));

    // compute route
    const cmp = await seed({ action: 'docking_or_md_review', title: 'Smoke compute' });
    await req('PATCH', `/api/operator/contributions/${cmp}`, { cookie, body: { action: 'accept_for_compute_review' } });
    ok('accept_for_compute_review → :compute_review queue', (await stateOf(cmp)).promoted_queue_id === `candidate_contribution:${cmp}:compute_review`);

    // validation route
    const val = await seed({ action: 'validation_packet', title: 'Smoke validation' });
    await req('PATCH', `/api/operator/contributions/${val}`, { cookie, body: { action: 'accept_for_validation_queue' } });
    ok('accept_for_validation_queue → :validation_queue queue', (await stateOf(val)).promoted_queue_id === `candidate_contribution:${val}:validation_queue`);

    // reject → no queue id
    const rej = await seed({ title: 'Smoke reject' });
    await req('PATCH', `/api/operator/contributions/${rej}`, { cookie, body: { action: 'reject', review_notes: 'off topic' } });
    const rs = await stateOf(rej);
    ok('reject → rejected, no promoted_queue_id', rs.status === 'rejected' && rs.promoted_queue_id === null, `${rs.status}/${rs.promoted_queue_id}`);

    // needs-more-info + archive
    const nmi = await seed({ title: 'Smoke needs-info' });
    await req('PATCH', `/api/operator/contributions/${nmi}`, { cookie, body: { action: 'request_more_information' } });
    ok('request_more_information → needs_more_information', (await stateOf(nmi)).status === 'needs_more_information');
    const arc = await seed({ title: 'Smoke archive' });
    await req('PATCH', `/api/operator/contributions/${arc}`, { cookie, body: { action: 'archive' } });
    ok('archive → archived', (await stateOf(arc)).status === 'archived');
  }

  // 4) Cleanup — remove every row this run created
  const del = await pool.query(`delete from candidate_contribution_intake where contributor->>'contact' = $1`, [TAG]);
  console.log(`\ncleanup: removed ${del.rowCount} smoke row(s) (tag ${TAG})`);
}

main()
  .then(async () => {
    await pool.end();
    console.log(`\nRESULT: ${pass} passed, ${fails.length} failed`);
    if (fails.length) { console.log('FAILED: ' + fails.join(', ')); process.exit(1); }
    console.log('SMOKE PASS ✓\n');
  })
  .catch(async (e) => {
    console.error('\nSMOKE ERROR:', e?.message || e);
    try { await pool.query(`delete from candidate_contribution_intake where contributor->>'contact' = $1`, [TAG]); } catch { /* best effort */ }
    try { await pool.end(); } catch { /* ignore */ }
    process.exit(1);
  });
