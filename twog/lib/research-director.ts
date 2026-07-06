// Live-Neon reader for the Research Director's decisions (research_director_decisions). Each row is one
// grounded, capsule-cited choice from a director reasoning pass over the real proof ledger. Graceful:
// []/null on any Neon failure. The engine never acts on these automatically — it's a transparent,
// prioritized recommendation surface (the "watch the director reason" feed).

import { neonRows } from './neon';

export interface DirectorDecision {
  decision_id: string;
  run_tag: string;
  candidate_id: string | null;
  title: string | null;
  target: string | null;
  action_kind: string; // propose_frontier | deprioritize | next_test | escalate | needs_input
  modality: string | null; // frontier modality (stapled_peptide, degrader, base_edit, car_t, mrna_vaccine, adc, …)
  testable_now: boolean | null; // fits the current compute lanes?
  decision: string;
  rationale: string;
  confidence: number | null;
  priority: number | null;
  current_verdict: string | null; // still-standing | ruled-out | needs-more
  next_lane: string | null;
  est_cost_usd: number | null;
  cited_capsule_ids: string[];
  created_at: string | null;
}

type Raw = Omit<DirectorDecision, 'cited_capsule_ids' | 'created_at'> & {
  cited_capsule_ids: unknown;
  created_at: unknown;
};

const iso = (v: unknown): string | null =>
  v instanceof Date ? v.toISOString() : typeof v === 'string' ? v : null;
const strArr = (v: unknown): string[] =>
  Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string') : [];

function shape(r: Raw): DirectorDecision {
  return {
    decision_id: r.decision_id,
    run_tag: r.run_tag,
    candidate_id: r.candidate_id,
    title: r.title,
    target: r.target,
    action_kind: r.action_kind,
    modality: r.modality ?? null,
    testable_now: typeof r.testable_now === 'boolean' ? r.testable_now : null,
    decision: r.decision,
    rationale: r.rationale,
    confidence: typeof r.confidence === 'number' ? r.confidence : null,
    priority: typeof r.priority === 'number' ? r.priority : null,
    current_verdict: r.current_verdict,
    next_lane: r.next_lane || null,
    est_cost_usd: typeof r.est_cost_usd === 'number' ? r.est_cost_usd : null,
    cited_capsule_ids: strArr(r.cited_capsule_ids),
    created_at: iso(r.created_at),
  };
}

const SELECT = `select decision_id::text, run_tag, candidate_id, title, target, action_kind, modality,
  testable_now, decision, rationale, confidence, priority, current_verdict, next_lane, est_cost_usd,
  cited_capsule_ids, created_at
  from research_director_decisions`;

/** The most-recent director pass's tag (null if none yet). */
export async function latestRunTag(): Promise<string | null> {
  const r = await neonRows<{ run_tag: string }>(
    `select run_tag from research_director_decisions order by created_at desc limit 1`,
  );
  return r[0]?.run_tag ?? null;
}

/** Decisions for a pass (defaults to the latest), ordered by priority. */
export async function listDecisions(runTag?: string, limit = 200): Promise<DirectorDecision[]> {
  const tag = runTag ?? (await latestRunTag());
  if (!tag) return [];
  const rows = await neonRows<Raw>(
    `${SELECT} where run_tag = $1 order by priority asc nulls last, created_at desc limit $2`,
    [tag, limit],
  );
  return rows.map(shape);
}

export interface DirectorSummary {
  run_tag: string | null;
  ran_at: string | null;
  total: number;
  frontier: number; // propose_frontier decisions (novel-modality bets)
  testable_now: number; // frontier bets that fit the current compute lanes
  deprioritize: number; // conventional repurposing to move past
  est_next_cost_usd: number; // total est cost of the testable-now next tests
}

export function summarize(decisions: DirectorDecision[]): DirectorSummary {
  const frontier = decisions.filter((d) => d.action_kind === 'propose_frontier');
  const est = frontier
    .filter((d) => d.testable_now)
    .reduce((sum, d) => sum + (d.est_cost_usd ?? 0), 0);
  return {
    run_tag: decisions[0]?.run_tag ?? null,
    ran_at: decisions.map((d) => d.created_at).filter(Boolean).sort().pop() ?? null,
    total: decisions.length,
    frontier: frontier.length,
    testable_now: frontier.filter((d) => d.testable_now).length,
    deprioritize: decisions.filter((d) => d.action_kind === 'deprioritize').length,
    est_next_cost_usd: Math.round(est * 100) / 100,
  };
}
