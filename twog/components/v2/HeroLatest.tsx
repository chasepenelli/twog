'use client';

// HeroLatest — the hero HUD's live "last result" pill. Polls /api/ledger and shows the most recent
// DECIDED candidate + its verdict stamp. Falls back to the canonical run (a4c2036) with no JS / while
// loading / if the engine is unreachable — so the HUD is never empty and never fabricates.

import { useEffect, useState } from 'react';
import { ProofStamp } from './ProofStamp';
import type { LedgerApiResponse, Verdict } from './ledgerEvents';

/** "carvedilol-vegfr2" → "carvedilol × VEGFR2" (best-effort; last token is the target). */
function prettyExp(id: string): string {
  if (!id || id === 'engine') return id || 'a candidate';
  const parts = id.replace(/-(auto|demo|crux)$/i, '').split('-');
  if (parts.length < 2) return id;
  const target = parts.pop()!;
  return `${parts.join('-')} × ${target.toUpperCase()}`;
}

export function HeroLatest() {
  const [latest, setLatest] = useState<{ exp: string; verdict: Verdict } | null>(null);

  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const res = await fetch('/api/ledger', { cache: 'no-store' });
        const data = (await res.json()) as LedgerApiResponse;
        if (!alive) return;
        const decide = data.events.find(
          (e) => e.type === 'decide' && e.verdict && e.verdict !== 'running',
        );
        if (decide?.verdict) setLatest({ exp: decide.exp, verdict: decide.verdict });
      } catch {
        /* keep the a4c2036 fallback */
      }
    };
    load();
    const iv = window.setInterval(load, 15_000);
    return () => {
      alive = false;
      window.clearInterval(iv);
    };
  }, []);

  if (!latest) return <>Run a4c2036</>;
  return (
    <span className="v4-latest">
      <span className="v4-latest__k">Latest</span>
      <span className="v4-latest__exp">{prettyExp(latest.exp)}</span>
      <ProofStamp verdict={latest.verdict} />
    </span>
  );
}
