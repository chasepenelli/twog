// Operator triage over Neon candidate_contribution_intake. A faithful TS port of the engine's
// src/hsa_research/ingestion_bridge/candidate_contribution_intake.py so the twog board and the
// engine/Dagster path write identical shapes (same statuses, same promoted_queue_id format, same
// review-note append). Reads are graceful (neonRows → []); the triage write surfaces errors (neonWrite).

import { neonRows, neonWrite } from './neon';

export const INTAKE_STATUSES = [
  'queued_for_intake',
  'triage_in_progress',
  'needs_more_information',
  'rejected',
  'accepted_for_evidence_review',
  'accepted_for_validation_queue',
  'accepted_for_compute_review',
  'archived',
] as const;

export const PENDING_INTAKE_STATUSES = [
  'queued_for_intake',
  'triage_in_progress',
  'needs_more_information',
] as const;

export const TRIAGE_ACTIONS = [
  'start_triage',
  'request_more_information',
  'reject',
  'accept_for_evidence_review',
  'accept_for_validation_queue',
  'accept_for_compute_review',
  'archive',
] as const;
export type TriageAction = (typeof TRIAGE_ACTIONS)[number];

export const TRIAGE_ACTION_STATUSES: Record<TriageAction, string> = {
  start_triage: 'triage_in_progress',
  request_more_information: 'needs_more_information',
  reject: 'rejected',
  accept_for_evidence_review: 'accepted_for_evidence_review',
  accept_for_validation_queue: 'accepted_for_validation_queue',
  accept_for_compute_review: 'accepted_for_compute_review',
  archive: 'archived',
};

const ACCEPTANCE_ACTIONS = new Set<TriageAction>([
  'accept_for_evidence_review',
  'accept_for_validation_queue',
  'accept_for_compute_review',
]);

export function isTriageAction(v: unknown): v is TriageAction {
  return typeof v === 'string' && (TRIAGE_ACTIONS as readonly string[]).includes(v);
}

/** Mirror of engine _recommended_route: contributor's requested action + current status → suggested route. */
export function recommendedRoute(requestedAction: string, status: string): { route: string; reason: string } {
  if (!(PENDING_INTAKE_STATUSES as readonly string[]).includes(status)) {
    return { route: 'already_triaged', reason: 'This contribution is not in a pending intake state.' };
  }
  switch (requestedAction) {
    case 'evidence_review':
      return { route: 'accepted_for_evidence_review', reason: 'Queue for citation/provenance review before synthesis.' };
    case 'citation_repair':
      return { route: 'accepted_for_evidence_review', reason: 'Route to citation repair and duplicate/source validation.' };
    case 'validation_packet':
      return { route: 'accepted_for_validation_queue', reason: 'Route to validation planning after evidence sufficiency check.' };
    case 'omics_readout':
      return { route: 'accepted_for_validation_queue', reason: 'Route to omics evidence packet review before validation.' };
    case 'docking_or_md_review':
      return { route: 'accepted_for_compute_review', reason: 'Route to compute review; keep expert gate before live compute.' };
    case 'no_action':
      return { route: 'archive_or_note', reason: 'No system action requested; keep as intake note unless operator promotes it.' };
    default:
      return { route: 'needs_human_review', reason: 'Requested action is missing or not recognized.' };
  }
}

/** Map a recommended route back to the triage action a UI should pre-select. */
export function actionForRoute(route: string): TriageAction {
  if (route === 'accepted_for_evidence_review') return 'accept_for_evidence_review';
  if (route === 'accepted_for_validation_queue') return 'accept_for_validation_queue';
  if (route === 'accepted_for_compute_review') return 'accept_for_compute_review';
  return 'start_triage';
}

function promotedQueueId(contributionId: string, action: TriageAction): string | null {
  if (!ACCEPTANCE_ACTIONS.has(action)) return null;
  const route = action.replace(/^accept_for_/, '');
  return `candidate_contribution:${contributionId}:${route}`;
}

/** One review-note line, matching the engine's format. The append itself happens atomically in SQL. */
function reviewNoteEntry(operator: string, action: string, note: string | undefined, iso: string): string {
  const clean = (note ?? '').trim() || 'No operator note supplied.';
  return `[${iso}] TRIAGE ${operator || 'unknown'}: ${action} - ${clean}`;
}

export interface ContributionRow {
  contribution_id: string;
  candidate_id: string | null;
  display_id: string | null;
  status: string;
  contribution_type: string | null;
  relation_to_current_record: string | null;
  requested_system_action: string | null;
  recommended_route: string;
  route_reason: string;
  contact: string | null;
  contributor_name: string | null;
  title: string | null;
  summary: string | null;
  claim: string | null;
  conflicts: string | null;
  evidence: Array<{ title?: string; url?: string; notes?: string }>;
  evidence_count: number;
  artifact_count: number;
  review_notes: string | null;
  promoted_queue_id: string | null;
  created_at: string | null;
  updated_at: string | null;
  reviewed_at: string | null;
  packet?: Record<string, any> | null;
}

type Raw = {
  contribution_id: string;
  candidate_id: string | null;
  display_id: string | null;
  status: string;
  contribution_type: string | null;
  relation_to_current_record: string | null;
  requested_system_action: string | null;
  contributor: Record<string, any> | null;
  evidence: any[] | null;
  artifacts: any[] | null;
  packet: Record<string, any> | null;
  review_notes: string | null;
  promoted_queue_id: string | null;
  created_at: unknown;
  updated_at: unknown;
  reviewed_at: unknown;
};

const iso = (v: unknown): string | null =>
  v instanceof Date ? v.toISOString() : typeof v === 'string' ? v : null;

function shape(r: Raw, includePacket = false): ContributionRow {
  const requested = r.requested_system_action ?? '';
  const rec = recommendedRoute(requested, r.status);
  const contributor = r.contributor ?? {};
  const packet = r.packet ?? {};
  const str = (v: unknown): string | null => (typeof v === 'string' && v.trim() ? v.trim() : null);
  const evidence = Array.isArray(r.evidence) ? r.evidence : Array.isArray(packet.evidence) ? packet.evidence : [];
  return {
    contribution_id: r.contribution_id,
    candidate_id: r.candidate_id,
    display_id: r.display_id,
    status: r.status,
    contribution_type: r.contribution_type,
    relation_to_current_record: r.relation_to_current_record,
    requested_system_action: requested || null,
    recommended_route: rec.route,
    route_reason: rec.reason,
    contact: typeof contributor.contact === 'string' ? contributor.contact : null,
    contributor_name: typeof contributor.name === 'string' ? contributor.name : null,
    title: str(packet.title),
    summary: str(packet.summary),
    claim: str(packet.claim_or_question),
    conflicts: str(packet.conflicts_or_limitations),
    evidence: evidence.slice(0, 25),
    evidence_count: Array.isArray(r.evidence) ? r.evidence.length : 0,
    artifact_count: Array.isArray(r.artifacts) ? r.artifacts.length : 0,
    review_notes: r.review_notes,
    promoted_queue_id: r.promoted_queue_id,
    created_at: iso(r.created_at),
    updated_at: iso(r.updated_at),
    reviewed_at: iso(r.reviewed_at),
    ...(includePacket ? { packet: r.packet ?? null } : {}),
  };
}

const SELECT = `select contribution_id::text, candidate_id, display_id, status, contribution_type,
  relation_to_current_record, requested_system_action, contributor, evidence, artifacts, packet,
  review_notes, promoted_queue_id, created_at, updated_at, reviewed_at
  from candidate_contribution_intake`;

function normalizeStatuses(statuses?: string[]): string[] {
  const valid = (statuses ?? []).filter((s) => (INTAKE_STATUSES as readonly string[]).includes(s));
  return valid.length ? valid : [...PENDING_INTAKE_STATUSES];
}

export interface TriageSummary {
  row_count: number;
  queued_for_intake: number;
  triage_in_progress: number;
  needs_more_information: number;
  actionable_count: number;
}

export async function listContributions(opts: { statuses?: string[]; limit?: number } = {}): Promise<{ rows: ContributionRow[]; summary: TriageSummary }> {
  const statuses = normalizeStatuses(opts.statuses);
  const limit = Math.max(1, Math.min(opts.limit ?? 100, 500));
  const raw = await neonRows<Raw>(`${SELECT} where status = any($1) order by created_at desc limit $2`, [statuses, limit]);
  const rows = raw.map((r) => shape(r));
  const count = (s: string) => rows.filter((r) => r.status === s).length;
  const summary: TriageSummary = {
    row_count: rows.length,
    queued_for_intake: count('queued_for_intake'),
    triage_in_progress: count('triage_in_progress'),
    needs_more_information: count('needs_more_information'),
    actionable_count: rows.filter(
      (r) => (PENDING_INTAKE_STATUSES as readonly string[]).includes(r.status) && r.requested_system_action !== 'no_action',
    ).length,
  };
  return { rows, summary };
}

export async function getContribution(id: string): Promise<ContributionRow | null> {
  const raw = await neonRows<Raw>(`${SELECT} where contribution_id::text = $1 limit 1`, [id]);
  return raw[0] ? shape(raw[0], true) : null;
}

export interface TriageResult {
  ok: boolean;
  contribution_id: string;
  old_status?: string;
  new_status?: string;
  promoted_queue_id?: string | null;
  error?: string;
}

/** Apply one operator triage decision. Faithful to engine triage_candidate_contributions (single row). */
export async function triageContribution(args: {
  contributionId: string;
  action: TriageAction;
  operator?: string;
  reviewNotes?: string;
  nowIso: string;
}): Promise<TriageResult> {
  const { contributionId, action, nowIso } = args;
  const operator = (args.operator ?? '').trim() || 'unknown';
  const targetStatus = TRIAGE_ACTION_STATUSES[action];
  const computedQueue = promotedQueueId(contributionId, action);
  const reviewedAt = targetStatus === 'triage_in_progress' ? null : nowIso;
  const entry = reviewNoteEntry(operator, action, args.reviewNotes, nowIso);

  // Single atomic statement: the note is appended against the row's CURRENT value (t.review_notes,
  // re-evaluated under the row lock), so a concurrent triage can't drop a note; promoted_queue_id is
  // preserved if already set. Runs through the THROWING neonWrite so a Neon outage is a real error
  // (500), not a masked 404. prev.old_status is a best-effort snapshot for the response only.
  const updated = await neonWrite<{ contribution_id: string; old_status: string; new_status: string; promoted_queue_id: string | null }>(
    `with prev as (
       select contribution_id, status as old_status
       from candidate_contribution_intake where contribution_id::text = $1
     )
     update candidate_contribution_intake t
        set status = $2,
            review_notes = case when coalesce(trim(t.review_notes), '') = '' then $3
                                else t.review_notes || E'\n' || $3 end,
            promoted_queue_id = coalesce(t.promoted_queue_id, $4),
            updated_at = $5,
            reviewed_at = $6
       from prev
      where t.contribution_id = prev.contribution_id
      returning t.contribution_id::text, prev.old_status, t.status as new_status, t.promoted_queue_id`,
    [contributionId, targetStatus, entry, computedQueue, nowIso, reviewedAt],
  );
  if (!updated[0]) return { ok: false, contribution_id: contributionId, error: 'not_found' };
  return {
    ok: true,
    contribution_id: contributionId,
    old_status: updated[0].old_status,
    new_status: updated[0].new_status,
    promoted_queue_id: updated[0].promoted_queue_id,
  };
}
