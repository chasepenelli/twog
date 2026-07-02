// Live-Neon reader for public candidates (the "ideas": drug × target). Reads public_candidates directly
// (top-level columns + payload jsonb) so /candidates shares the SAME candidate_id namespace as the
// evidence/runs surfaces (carvedilol-vegfr2 …) and cross-links join with no mapping. The autonomous
// roster is lean, so this stays lean and only surfaces narrative fields when they're actually populated.
// Graceful: returns []/null on any Neon failure (never throws).

import { neonRows } from './neon';
import { publicQuestion, verdictFromCapsules } from './verdict';
import type { CandidateCore, CandidateDetail, CandidateSummary, Verdict } from './types/public-detail';

const PUBLIC_STATUSES = ['submitted', 'accepted', 'promoted'];
const VERDICT_ORDER: Record<Verdict, number> = { 'still-standing': 0, 'needs-more': 1, 'ruled-out': 2 };

type Row = {
  candidate_id: string;
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

const SELECT = `select candidate_id, title, public_status, content_hash, priority_score, candidate_kind, updated_at, payload from public_candidates`;

/** Every public idea, with a verdict derived from its capsule signals. Sorted still-standing → needs-more → ruled-out, then priority. */
export async function listCandidates(limit = 200): Promise<CandidateSummary[]> {
  const [rows, sigRows] = await Promise.all([
    neonRows<Row>(`${SELECT} order by priority_score desc nulls last, updated_at desc limit $1`, [limit]),
    neonRows<{ candidate_id: string; signals: (string | null)[] }>(
      `select candidate_id, jsonb_agg(payload->'payload'->>'signal') as signals
         from proof_capsules where status = any($1) group by candidate_id`,
      [PUBLIC_STATUSES],
    ),
  ]);
  const verdictByCand = new Map<string, Verdict>();
  for (const s of sigRows) {
    verdictByCand.set(s.candidate_id, verdictFromCapsules((s.signals ?? []).map((signal) => ({ signal }))));
  }
  return rows
    .map((row) => ({ ...core(row), verdict: verdictByCand.get(row.candidate_id) ?? ('needs-more' as Verdict) }))
    .sort((a, b) => VERDICT_ORDER[a.verdict] - VERDICT_ORDER[b.verdict] || (b.priority_score ?? 0) - (a.priority_score ?? 0));
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
