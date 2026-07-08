// Live Ledger event model + seed data.
//
// HONESTY RULE (the brand): the ledger never fabricates activity. The primary stream is a REPLAY of
// the one real autonomous run we have on record — alpelisib × PI3Kα (run a4c2036): the loop fetched its
// own inputs, docked on a Modal A100 at −9.8 kcal/mol, passed its gates, and the result "supports" the
// hypothesis but was NOT promoted. The single `example: true` row is clearly labelled EXAMPLE and shows
// what a *killed* result looks like — it is not presented as something that happened.
//
// COST is deliberately NOT modelled here — run cost is backend-only and never shown on the public ledger.
//
// In P2 these same shapes come from `GET /api/ledger/stream` (SSE) over real proof-capsule rows.

export type LedgerEventType =
  | 'propose'
  | 'pre-register'
  | 'resolve'
  | 'dock'
  | 'gate'
  | 'decide';

export type Verdict = 'survived' | 'killed' | 'supports' | 'running';

export interface LedgerEvent {
  id: string;
  ts: string; // ISO
  type: LedgerEventType;
  exp: string; // experiment / candidate code
  summary: string; // the engine's own voice
  verdict?: Verdict;
  detail?: { hash?: string; metric?: string; note?: string; link?: string };
  example?: true; // clearly-labelled illustration, never presented as real history
}

// The real run, in order. Timestamps are illustrative-of-cadence; the events are real.
export const REAL_RUN: LedgerEvent[] = [
  {
    id: 'a4c2036-1',
    ts: '2026-04-21T09:31:02Z',
    type: 'propose',
    exp: 'EXP-a4c2036',
    summary: 'Proposed the test most likely to KILL candidate alpelisib × PI3Kα — not the one most likely to flatter it.',
    detail: { note: 'Falsification first: pick a fight with the leading hypothesis.' },
  },
  {
    id: 'a4c2036-2',
    ts: '2026-04-21T09:31:05Z',
    type: 'pre-register',
    exp: 'EXP-a4c2036',
    summary: 'Locked the pass/fail bar 🔒 — hashed before any compute ran, so the goalposts can’t move.',
    detail: { hash: 'sha256:a4c2036…', note: 'Pre-registered kill-criterion.' },
  },
  {
    id: 'a4c2036-3',
    ts: '2026-04-21T09:31:11Z',
    type: 'resolve',
    exp: 'EXP-a4c2036',
    summary: 'Fetched my own inputs — PI3Kα structure + alpelisib SMILES. No hand-curated files.',
    detail: { note: 'PubChem ligand + RCSB receptor, resolved autonomously.' },
  },
  {
    id: 'a4c2036-4',
    ts: '2026-04-21T09:32:40Z',
    type: 'dock',
    exp: 'EXP-a4c2036',
    summary: 'Docking on a Modal A100 GPU…',
    verdict: 'running',
    detail: { metric: '−9.8 kcal/mol · CNN pose 0.99', note: 'Real gnina docking.' },
  },
  {
    id: 'a4c2036-5',
    ts: '2026-04-21T09:33:02Z',
    type: 'gate',
    exp: 'EXP-a4c2036',
    summary: 'Confound + provenance gates: passed ✓ — the result isn’t an artifact and the run is who it says it is.',
    detail: { note: 'Both pre-gates must pass before a result can be accepted.' },
  },
  {
    id: 'a4c2036-6',
    ts: '2026-04-21T09:33:05Z',
    type: 'decide',
    exp: 'EXP-a4c2036',
    summary: 'Result: supports the idea — but NOT promoted. A human decides what’s real.',
    verdict: 'supports',
    detail: { metric: '−9.8 kcal/mol', note: 'The hypothesis survived its own falsification attempt.', link: '/candidates/alpelisib-pi3ka' },
  },
];

// Clearly-labelled illustration of a *killed* outcome (the win we celebrate). Never shown as real.
export const EXAMPLE_KILLED: LedgerEvent = {
  id: 'example-killed',
  ts: '2026-04-21T09:18:00Z',
  type: 'decide',
  exp: 'EXP-example',
  summary: 'Result: the idea did NOT survive. Good — one less dead end.',
  verdict: 'killed',
  example: true,
  detail: { note: 'A falsified hypothesis is a win. This is what that looks like.' },
};

// Monochrome spines (white/black system) — hierarchy by gray shade, meaning carried by the TYPE label
// + verdict stamp, not hue. A killed decision is overridden to pure black in the component.
export const SPINE_COLOR: Record<LedgerEventType, string> = {
  propose: '#0a0a0a',
  'pre-register': '#0a0a0a',
  resolve: '#b4b4b4',
  dock: '#6b6b6b',
  gate: '#8e8e8e',
  decide: '#0a0a0a',
};

export const TYPE_LABEL: Record<LedgerEventType, string> = {
  propose: 'PROPOSE',
  'pre-register': 'LOCK',
  resolve: 'FETCH',
  dock: 'DOCK',
  gate: 'GATE',
  decide: 'DECIDE',
};

// ── Live wiring (P2) ────────────────────────────────────────────────────────
// Shape of the real engine feed (Python API GET /public/activity). The /api/ledger route maps these
// into LedgerEvent (engine voice), and LiveLedger polls it. REAL_RUN above stays as the honest
// offline fallback (badged REPLAY) — never dead code.
export interface PythonActivityEvent {
  type: 'campaign' | 'agent' | 'capsule' | 'compute';
  occurred_at: string; // ISO
  status: string;
  title: string;
  signal?: 'refutes' | 'supports' | 'neutral';
  candidate_id?: string;
  capsule_id?: string;
  lane?: string;
}

export interface LedgerApiResponse {
  events: LedgerEvent[];
  idle: boolean;
  running_jobs: number;
  last_event_at?: string | null;
  source: 'live' | 'unreachable';
}
