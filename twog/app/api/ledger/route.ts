// GET /api/ledger — the live engine activity ledger.
// Proxies the Python engine API (GET {ENGINE_API_BASE}/public/activity) and maps its raw event shapes
// into the LedgerEvent shape the LiveLedger renders, in the engine's own voice (raw titles like
// "capsule · refutes · cofolding" read as a data dump — we narrate them). On upstream failure the
// client falls back to the honest a4c2036 replay. Next 16: fetch is cached by default, so no-store is
// REQUIRED or the ledger freezes; no-store also makes this route dynamic (no `export const dynamic`).

import { NextResponse } from 'next/server';
import type { LedgerEvent, PythonActivityEvent, Verdict } from '@/components/v2/ledgerEvents';

const ENGINE = process.env.ENGINE_API_BASE ?? 'http://localhost:8000';

/** "carvedilol-vegfr2" → "carvedilol × VEGFR2" (best-effort; last token is the target). */
function pretty(id?: string): string {
  if (!id) return 'a candidate';
  const parts = id.replace(/-(auto|demo|crux)$/i, '').split('-');
  if (parts.length < 2) return id;
  const target = parts.pop()!;
  return `${parts.join('-')} × ${target.toUpperCase()}`;
}

function toLedgerEvent(e: PythonActivityEvent, i: number): LedgerEvent | null {
  const ts = e.occurred_at;
  const exp = e.candidate_id ?? 'engine';
  const id = `${e.capsule_id ?? e.occurred_at}-${i}`;

  switch (e.type) {
    case 'campaign':
      return {
        id, ts, type: 'propose', exp,
        summary: 'Opened a falsification campaign — picking the fights most likely to kill an idea.',
        detail: { note: e.title },
      };
    case 'agent':
      return {
        id, ts, type: 'pre-register', exp,
        summary: 'Planned the test most likely to KILL the candidate — not the one most likely to flatter it.',
        detail: { note: 'Pre-registered kill-criterion, locked before any compute ran.' },
      };
    case 'compute': {
      const lane = e.lane ?? 'compute';
      const running = e.status === 'submitted' || e.status === 'running';
      return {
        id, ts, type: 'dock', exp,
        verdict: running ? 'running' : undefined,
        summary: running ? `Running the ${lane} lane on a Modal GPU…` : `Ran real compute — ${lane} lane on Modal.`,
        detail: { note: e.candidate_id ? `${pretty(e.candidate_id)} · ${lane}` : lane },
      };
    }
    case 'capsule': {
      const verdict: Verdict | undefined =
        e.signal === 'refutes' ? 'killed' : e.signal === 'supports' ? 'supports' : undefined;
      const summary =
        e.signal === 'refutes'
          ? `Result: ${pretty(e.candidate_id)} did NOT survive its test — good, one less dead end.`
          : e.signal === 'supports'
          ? `Result: ${pretty(e.candidate_id)} survived falsification — logged, and a human decides what's real.`
          : `Result posted for ${pretty(e.candidate_id)}.`;
      return {
        id, ts, type: 'decide', exp, verdict, summary,
        detail: { note: 'Nothing auto-promotes — a human holds the final call.', link: '/candidates' },
      };
    }
    default:
      return null;
  }
}

export async function GET() {
  try {
    const res = await fetch(`${ENGINE}/public/activity`, { cache: 'no-store' });
    if (!res.ok) throw new Error(`engine ${res.status}`);
    const data = (await res.json()) as {
      events?: PythonActivityEvent[];
      idle?: boolean;
      running_jobs?: number;
      last_event_at?: string;
    };
    const events = (data.events ?? [])
      .map(toLedgerEvent)
      .filter((e): e is LedgerEvent => e !== null);

    return NextResponse.json(
      {
        events,
        idle: data.idle ?? true,
        running_jobs: data.running_jobs ?? 0,
        last_event_at: data.last_event_at ?? null,
        source: 'live' as const,
      },
      { headers: { 'Cache-Control': 'no-store' } },
    );
  } catch {
    // Engine unreachable → client shows the honest replay fallback.
    return NextResponse.json(
      { events: [], idle: true, running_jobs: 0, last_event_at: null, source: 'unreachable' as const },
      { headers: { 'Cache-Control': 'no-store' } },
    );
  }
}
