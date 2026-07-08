// Live-Neon reader for proof capsules (evidence) + provenance. Reads proof_capsules.payload
// (the ProofCapsuleRecord: summary + inner payload with signal/metrics/validation_type +
// falsification_preregistration + content_hash + signature). Graceful: []/null on failure.

import { neonRows } from './neon';
import type { Capsule, Provenance } from './types/public-detail';

const PUBLIC_STATUSES = ['submitted', 'accepted', 'promoted'];

const LANE_METHOD: Record<string, string> = {
  docking: 'Dock the drug into the target’s binding pocket and score the pose.',
  cofolding: 'Co-fold the target with the ligand to test complex formation + affinity.',
  md: 'Run molecular dynamics to see whether the pose holds once the atoms move.',
  omics: 'Check what real patient tumors actually show for this axis.',
};

// Cross-species honesty: every runnable structure lane (docking / co-folding) ran against a HUMAN
// ortholog — the redock-verified library is human PDBs, and co-folding pulls the human Swiss-Prot
// sequence — while candidates target the canine protein. Surface that substitution as a limitation so
// evidence never reads as if it ran on the canine target. The engine now writes this into NEW capsules
// directly (service.py); this covers historical capsules at display time without rewriting the record
// or invalidating any content hash. Idempotent via the "Cross-species inference only" marker.
const HUMAN_STRUCT: Record<string, string> = {
  pik3ca: 'human 4JPS (PI3Kα / p110α)',
  pi3ka: 'human 4JPS (PI3Kα / p110α)',
  kdr: 'human 3VHE (VEGFR2 / KDR)',
  vegfr2: 'human 3VHE (VEGFR2 / KDR)',
};
function crossSpeciesNote(validationType: string | null, candidateId: string): string | null {
  if (validationType !== 'docking' && validationType !== 'cofolding') return null;
  const key = candidateId.replace(/-(auto|demo|crux)$/i, '').split('-').pop() ?? '';
  const struct = HUMAN_STRUCT[key] ?? 'a human ortholog structure';
  const verb = validationType === 'docking' ? 'Docked against' : 'Co-folded against';
  return (
    `${verb} ${struct} — a human structure standing in for the canine ortholog; sequence/structure ` +
    `differences at the binding site are not accounted for. Cross-species inference only.`
  );
}

type Row = { capsule_id: string; candidate_id: string; status: string; payload: Record<string, any> };

function shapeCapsule(row: Row): Capsule {
  const p = row.payload ?? {};
  const inner = p.payload ?? {};
  const summary = p.summary ?? {};
  const validation_type = inner.validation_type ?? null;
  return {
    capsule_id: row.capsule_id,
    candidate_id: row.candidate_id,
    status: row.status,
    signal: inner.signal ?? null,
    validation_type,
    claim: summary.title ?? 'Falsification test',
    method: (validation_type && LANE_METHOD[validation_type]) || 'A real pre-registered test on the engine.',
    readout: summary.finding ?? '',
    why_it_matters: summary.why_it_matters ?? '',
    limitations: (() => {
      const base = [...(summary.limitations ?? p.limitations ?? [])];
      const note = crossSpeciesNote(validation_type, row.candidate_id);
      if (note && !base.some((l) => /Cross-species inference only/i.test(String(l)))) base.push(note);
      return base;
    })(),
    confidence: typeof inner.confidence === 'number' ? inner.confidence : null,
    content_hash: p.content_hash ?? null,
    preregistration: inner.falsification_preregistration ?? null,
    metrics: inner.metrics ?? null,
    lineage_index: typeof p.lineage_index === 'number' ? p.lineage_index : null,
    submitted_by: p.submitted_by ?? null,
    produced_by: p.producer?.name ?? p.producer?.producer_type ?? null,
  };
}

export async function listCapsules(limit = 200): Promise<Capsule[]> {
  const rows = await neonRows<Row>(
    `select capsule_id, candidate_id, status, payload from proof_capsules
       where status = any($1) order by updated_at desc limit $2`,
    [PUBLIC_STATUSES, limit],
  );
  return rows.map(shapeCapsule);
}

export async function getCapsule(capsuleId: string): Promise<Capsule | null> {
  const rows = await neonRows<Row>(
    `select capsule_id, candidate_id, status, payload from proof_capsules where capsule_id = $1 limit 1`,
    [capsuleId],
  );
  return rows[0] ? shapeCapsule(rows[0]) : null;
}

export async function getCapsulesForCandidate(candidateId: string): Promise<Capsule[]> {
  const rows = await neonRows<Row>(
    `select capsule_id, candidate_id, status, payload from proof_capsules
       where candidate_id = $1 and status = any($2) order by updated_at asc`,
    [candidateId, PUBLIC_STATUSES],
  );
  return rows.map(shapeCapsule);
}

export async function getProvenance(capsuleId: string): Promise<Provenance | null> {
  const rows = await neonRows<{ payload: Record<string, any> }>(
    `select payload from proof_capsules where capsule_id = $1 limit 1`,
    [capsuleId],
  );
  const p = rows[0]?.payload;
  if (!p) return null;
  const sig = p.signature ?? null;
  return {
    capsule_id: capsuleId,
    content_hash: p.content_hash ?? null,
    signed: Boolean(sig),
    signature: sig?.signature ?? null,
    signer: sig?.signer ?? sig?.public_key ?? null,
    signature_valid: null, // v1: report signed vs custodial; independent re-verify is a follow-up
    lineage_index: typeof p.lineage_index === 'number' ? p.lineage_index : null,
  };
}
