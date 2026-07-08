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

// Slug target token → display gene. Only these count as a real "× TARGET"; anything else is not forced
// into drug×target form (avoids "pik3ca × IMMUNOSUPPRESSION" and the invalid gene "PI3KA").
const TARGET_DISPLAY: Record<string, string> = {
  pi3ka: 'PIK3CA', pik3ca: 'PIK3CA', kdr: 'KDR', vegfr2: 'VEGFR2', mtor: 'MTOR',
};

/** "carvedilol-vegfr2" → "carvedilol × VEGFR2"; "alpelisib-pi3ka" → "alpelisib × PIK3CA".
 *  Non-drug×target slugs (e.g. "pik3ca-immunosuppression-crux") become a readable label, not a fake target. */
export function prettyCandidate(id: string): string {
  if (!id) return id;
  const cleaned = id.replace(/-(auto|demo|crux)$/i, '');
  const parts = cleaned.split('-');
  const last = parts[parts.length - 1]?.toLowerCase() ?? '';
  if (parts.length >= 2 && TARGET_DISPLAY[last]) {
    return `${parts.slice(0, -1).join('-')} × ${TARGET_DISPLAY[last]}`;
  }
  return cleaned.replace(/-/g, ' ');
}

/** A plain-language question for a candidate/capsule (best-effort). */
export function publicQuestion(claim: string | null | undefined, candidateId: string): string {
  const c = (claim ?? '').replace(/^(Falsify:\s*)+/i, '').trim();
  if (c) return c.charAt(0).toUpperCase() + c.slice(1);
  return `Does ${prettyCandidate(candidateId)} hold up?`;
}
