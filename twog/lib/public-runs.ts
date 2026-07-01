// Live-Neon reader for falsification-campaign runs. Reads run_manifests.payload.output_refs.{rollup,rows}
// (cost fields intentionally dropped — never surfaced publicly). Graceful: returns []/null on any failure.

import { neonRows } from './neon';
import type { RunManifest, RunRollup, RunRow } from './types/public-detail';

function shapeRun(payload: Record<string, any>): RunManifest {
  const out = payload.output_refs ?? {};
  const r = out.rollup ?? {};
  const rollup: RunRollup = {
    any_promoted: Boolean(r.any_promoted),
    candidates_selected: Number(r.candidates_selected ?? 0),
    candidates_processed: Number(r.candidates_processed ?? 0),
    terminal_reasons: r.terminal_reasons ?? {},
    leading_hypothesis_status: r.leading_hypothesis_status ?? {},
  };
  const rows: RunRow[] = (out.rows ?? []).map((row: Record<string, any>) => ({
    candidate_id: row.candidate_id ?? '',
    promoted: Boolean(row.promoted),
    rounds_run: Number(row.rounds_run ?? 0),
    capsule_ids: row.capsule_ids ?? [],
    compute_job_ids: row.compute_job_ids ?? [],
    terminal_reason: row.terminal_reason ?? '',
    leading_hypothesis_status: row.leading_hypothesis_status ?? '',
  }));
  return {
    manifest_id: payload.manifest_id ?? '',
    runner_kind: payload.metadata?.runner_kind ?? 'modal',
    title: payload.title ?? 'Falsification campaign',
    ran_at: typeof payload.created_at === 'string' ? payload.created_at.slice(0, 10) : null,
    rollup,
    rows,
  };
}

export async function listRuns(limit = 50): Promise<RunManifest[]> {
  const rows = await neonRows<{ payload: Record<string, any> }>(
    `select payload from run_manifests
       where manifest_type = 'falsification_campaign'
       order by created_at desc
       limit $1`,
    [limit],
  );
  return rows.map((r) => shapeRun(r.payload));
}

export async function getRun(manifestId: string): Promise<RunManifest | null> {
  const rows = await neonRows<{ payload: Record<string, any> }>(
    `select payload from run_manifests
       where manifest_id = $1 and manifest_type = 'falsification_campaign'
       limit 1`,
    [manifestId],
  );
  return rows[0] ? shapeRun(rows[0].payload) : null;
}
