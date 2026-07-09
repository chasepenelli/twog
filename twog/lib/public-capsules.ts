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

// Cross-species disclosure: every runnable structure lane (docking / co-folding) runs against a HUMAN
// ortholog structure while candidates target the canine protein. Rather than hand-wave it, we quantify
// the fidelity: the canine PIK3CA and KDR orthologs were aligned to human and the binding pockets are
// SEQUENCE-IDENTICAL, so the human structures are justified proxies (not unverified substitutions). This
// renders as a public "cross-species caveat" on the evidence page; it's a dedicated Capsule field.
type Fidelity = { struct: string; note: string };
const CANINE_FIDELITY: Record<string, Fidelity> = {
  pik3ca: {
    struct: 'human 4JPS (PI3Kα / p110α)',
    note:
      'the canine PIK3CA ortholog (UniProt A0A5F4C2B1 / RefSeq XP_545208.2, taxid 9615) is 99.8% ' +
      'identical to human overall and 100% identical at every alpelisib/ATP-pocket residue — and a ' +
      'redock-verified canine receptor (AlphaFold AF-A0A5F4C2B1, alpelisib redock 0.62 Å) is now in the library',
  },
  kdr: {
    struct: 'human 3VHE (VEGFR2 / KDR)',
    note:
      'the canine KDR/VEGFR2 ortholog (RefSeq NP_001041489.1 / UniProt A0A8I3NL83, taxid 9615) is 93% ' +
      'identical to human overall, 97% across the kinase domain, and 100% identical at every ATP/TKI-pocket residue',
  },
};
const FIDELITY_ALIAS: Record<string, string> = { pi3ka: 'pik3ca', vegfr2: 'kdr' };
function crossSpeciesNote(validationType: string | null, candidateId: string): string | null {
  if (validationType !== 'docking' && validationType !== 'cofolding') return null;
  const raw = candidateId.replace(/-(auto|demo|crux)$/i, '').split('-').pop() ?? '';
  const key = FIDELITY_ALIAS[raw] ?? raw;
  const verb = validationType === 'docking' ? 'Docked against' : 'Co-folded against';
  const f = CANINE_FIDELITY[key];
  if (!f) {
    // Unknown target: keep the honest, un-quantified caveat rather than overclaim conservation.
    return (
      `${verb} a human ortholog structure standing in for the canine target — cross-species inference; ` +
      `binding-site conservation for this target has not yet been quantified.`
    );
  }
  return (
    `${verb} ${f.struct}. ${f.note[0].toUpperCase()}${f.note.slice(1)} — so the human structure is a ` +
    `justified cross-species proxy (the binding pocket is sequence-identical), not an unverified substitution.`
  );
}

type Row = { capsule_id: string; candidate_id: string; status: string; payload: Record<string, any> };

function shapeCapsule(row: Row): Capsule {
  const p = row.payload ?? {};
  const inner = p.payload ?? {};
  const summary = p.summary ?? {};
  const validation_type = inner.validation_type ?? null;
  // Confound gate: a capsule the engine could not accept (missing controls) is stamped
  // metadata.confound_gate.status='blocked' / verdict='unauditable'. It is NOT confirmed evidence.
  const gate = p.metadata?.confound_gate ?? null;
  const held = gate?.status === 'blocked' || gate?.verdict === 'unauditable';
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
    limitations: summary.limitations ?? p.limitations ?? [],
    // A dedicated, PUBLIC (ungated) caveat for structure lanes — see crossSpeciesNote. Kept out of the
    // member-gated `limitations` list so every visitor sees it.
    cross_species: crossSpeciesNote(validation_type, row.candidate_id),
    held,
    held_reason: held ? (gate?.reason ?? 'Held at the confound gate pending controls.') : null,
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
