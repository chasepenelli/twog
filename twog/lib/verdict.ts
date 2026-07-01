// Verdict vocabulary + helpers shared by the runs/evidence surfaces (ported from the /web app).
// The ceiling is "still standing" — nothing is ever "proven".

import type { Verdict } from './types/public-detail';

export const VERDICT_META: Record<Verdict, { label: string; tone: 'ok' | 'ko' | 'run'; gloss: string }> = {
  'still-standing': {
    label: 'still standing',
    tone: 'ok',
    gloss: "Survived every test the engine could run. Nothing here is proven — that's the point.",
  },
  'ruled-out': {
    label: 'ruled out',
    tone: 'ko',
    gloss: 'A pre-registered test disproved it. Being wrong fast, on purpose, is a win.',
  },
  'needs-more': {
    label: 'needs more testing',
    tone: 'run',
    gloss: 'Tested as far as its inputs allowed — no verdict yet.',
  },
};

/** Verdict → ProofStamp verdict (survived | killed | supports | running). */
export const STAMP_FOR: Record<Verdict, 'survived' | 'killed' | 'running'> = {
  'still-standing': 'survived',
  'ruled-out': 'killed',
  'needs-more': 'running',
};

export function verdictFromSignal(signal: string | null | undefined): Verdict {
  if (signal === 'refutes') return 'ruled-out';
  if (signal === 'supports') return 'still-standing';
  return 'needs-more';
}

export function verdictFromCapsules(caps: { signal: string | null }[]): Verdict {
  if (caps.some((c) => c.signal === 'refutes')) return 'ruled-out';
  if (caps.some((c) => c.signal === 'supports')) return 'still-standing';
  return 'needs-more';
}

/** campaign leading_hypothesis_status key → label/tone */
export const STATUS_META: Record<string, { label: string; tone: 'ok' | 'ko' | 'run' }> = {
  standing: { label: 'still standing', tone: 'ok' },
  refuted: { label: 'ruled out', tone: 'ko' },
  underpowered: { label: 'needs more testing', tone: 'run' },
};

/** "carvedilol-vegfr2" → "carvedilol × VEGFR2" (last token is the target). */
export function prettyCandidate(id: string): string {
  if (!id) return id;
  const parts = id.replace(/-(auto|demo|crux)$/i, '').split('-');
  if (parts.length < 2) return id;
  const target = parts.pop()!;
  return `${parts.join('-')} × ${target.toUpperCase()}`;
}

/** A plain-language question for a candidate/capsule (best-effort). */
export function publicQuestion(claim: string | null | undefined, candidateId: string): string {
  const c = (claim ?? '').replace(/^(Falsify:\s*)+/i, '').trim();
  if (c) return c.charAt(0).toUpperCase() + c.slice(1);
  return `Does ${prettyCandidate(candidateId)} hold up?`;
}
