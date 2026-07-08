// Live-Neon reader for public candidates (the "ideas": drug × target). Reads public_candidates directly
// (top-level columns + payload jsonb) so /candidates shares the SAME candidate_id namespace as the
// evidence/runs surfaces (carvedilol-vegfr2 …) and cross-links join with no mapping. The autonomous
// roster is lean, so this stays lean and only surfaces narrative fields when they're actually populated.
// Graceful: returns []/null on any Neon failure (never throws).

import { neonRows } from './neon';
import { publicQuestion, verdictFromCapsules } from './verdict';
import type { CandidateCore, CandidateDetail, CandidateSummary, Verdict } from './types/public-detail';
import type { PublicCandidateDetail } from './public-candidates';

const PUBLIC_STATUSES = ['submitted', 'accepted', 'promoted'];
const VERDICT_ORDER: Record<Verdict, number> = { 'still-standing': 0, 'needs-more': 1, 'ruled-out': 2 };

// Keep demo/auto duplicates + non-oncology negative-control fixtures out of the PUBLIC grid (they still
// resolve on a direct /candidates/[id] link). e.g. alpelisib-pi3ka-demo/-auto, aspirin/ethanol-*-auto.
const FIXTURE_CONTROLS = new Set(['aspirin', 'ethanol']);
function isFixtureCandidate(id: string): boolean {
  if (/-(demo|auto)$/i.test(id)) return true;
  const head = id.split('-')[0].toLowerCase();
  return FIXTURE_CONTROLS.has(head);
}

type Row = {
  candidate_id: string;
  display_id: string | null;
  title: string | null;
  public_status: string | null;
  content_hash: string | null;
  priority_score: number | null;
  candidate_kind: string | null;
  updated_at: unknown;
  payload: Record<string, any> | null;
};

const strArr = (v: unknown): string[] => (Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string' && x.length > 0) : []);
const nonEmpty = (v: unknown): string | undefined => (typeof v === 'string' && v.trim() ? v.trim() : undefined);

function isoDay(v: unknown): string | null {
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  if (typeof v === 'string' && v.length >= 10) return v.slice(0, 10);
  return null;
}

function core(row: Row): CandidateCore {
  const p = row.payload ?? {};
  const title = row.title ?? p.title ?? '';
  return {
    candidate_id: row.candidate_id,
    display_id: row.display_id ?? (typeof p.display_id === 'string' ? p.display_id : null),
    title,
    question: publicQuestion(title, row.candidate_id),
    targets: strArr(p.targets),
    biomarkers: strArr(p.biomarkers),
    public_status: row.public_status ?? p.public_status ?? null,
    validation_ready: Boolean(p.validation_ready),
    priority_score: typeof row.priority_score === 'number' ? row.priority_score : null,
    kind: row.candidate_kind ?? p.candidate_kind ?? null,
  };
}

const SELECT = `select candidate_id, display_id, title, public_status, content_hash, priority_score, candidate_kind, updated_at, payload from public_candidates`;

/** Every public idea, with a verdict derived from its capsule signals. Sorted still-standing → needs-more → ruled-out, then priority. */
export async function listCandidates(limit = 200): Promise<CandidateSummary[]> {
  const [rows, sigRows] = await Promise.all([
    neonRows<Row>(`${SELECT} order by priority_score desc nulls last, updated_at desc limit $1`, [limit]),
    neonRows<{ candidate_id: string; caps: { signal: string | null; held: boolean }[] }>(
      // Carry the confound-gate block alongside the signal so a gate-blocked "supports" (unauditable)
      // doesn't count as still-standing — mirrors verdictFromCapsules + shapeCapsule.
      `select candidate_id, jsonb_agg(jsonb_build_object(
                'signal', payload->'payload'->>'signal',
                'held', coalesce(payload->'metadata'->'confound_gate'->>'status','') = 'blocked'
                        or coalesce(payload->'metadata'->'confound_gate'->>'verdict','') = 'unauditable'
              )) as caps
         from proof_capsules where status = any($1) group by candidate_id`,
      [PUBLIC_STATUSES],
    ),
  ]);
  const verdictByCand = new Map<string, Verdict>();
  for (const s of sigRows) {
    verdictByCand.set(s.candidate_id, verdictFromCapsules(s.caps ?? []));
  }
  return rows
    .filter((row) => !isFixtureCandidate(row.candidate_id))
    .map((row) => ({ ...core(row), verdict: verdictByCand.get(row.candidate_id) ?? ('needs-more' as Verdict) }))
    .sort((a, b) => VERDICT_ORDER[a.verdict] - VERDICT_ORDER[b.verdict] || (b.priority_score ?? 0) - (a.priority_score ?? 0));
}

/** Adapt a lean live candidate into the legacy PublicCandidateDetail shape so the contribution-intake
 *  helpers (createCandidateContribution / buildPublicEvidenceBundle / publicCandidateAuditTrail) can
 *  validate + build against live Neon candidates. The autonomous roster has no snapshot/decisions, so
 *  those degrade to empty — the intake only needs candidate_id + content_hash. */
export function asPublicCandidateDetail(c: CandidateDetail): PublicCandidateDetail {
  return {
    candidate: {
      candidate_id: c.candidate_id,
      display_id: c.display_id ?? undefined,
      title: c.title,
      summary: c.summary,
      public_status: c.public_status ?? undefined,
      candidate_kind: c.kind ?? undefined,
      targets: c.targets,
      biomarkers: c.biomarkers,
      evidence_refs: c.evidence_refs,
      priority_score: c.priority_score ?? undefined,
      content_hash: c.content_hash ?? undefined,
      updated_at: c.updated_at ?? undefined,
      rationale_md: c.rationale_md,
      metadata: {},
    },
    latest_snapshot: null,
    decision_events: [],
    run_manifest: null,
  };
}

/** One idea by candidate_id (or display_id), case-insensitive. Lean detail; narrative fields only if populated. */
export async function getCandidate(idOrDisplay: string): Promise<CandidateDetail | null> {
  const rows = await neonRows<Row>(
    `${SELECT} where lower(candidate_id) = lower($1) or lower(coalesce(display_id, '')) = lower($1) limit 1`,
    [idOrDisplay],
  );
  const row = rows[0];
  if (!row) return null;
  const p = row.payload ?? {};
  return {
    ...core(row),
    evidence_refs: strArr(p.evidence_refs),
    content_hash: row.content_hash ?? p.content_hash ?? null,
    updated_at: isoDay(row.updated_at),
    summary: nonEmpty(p.summary),
    mechanism: nonEmpty(p.mechanism),
    rationale_md: nonEmpty(p.rationale_md),
  };
}
