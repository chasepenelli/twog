'use client';

// The Live Ledger — the homepage centerpiece. Public, on-paper (NOT the dark dev terminal): a stream of
// the engine doing science — propose → lock → fetch → dock → gate → decide. No cost is ever shown
// (backend-only).
//
// P2 (now): polls GET /api/ledger (a proxy of the real engine feed) and shows LIVE / IDLE state with
// real recent events. If the engine is unreachable it falls back to an honest REPLAY of the one real
// run on record (a4c2036), clearly badged REPLAY — never fabricated. Respects prefers-reduced-motion.

import { useEffect, useRef, useState } from 'react';
import { ProofStamp } from './ProofStamp';
import {
  EXAMPLE_KILLED,
  REAL_RUN,
  SPINE_COLOR,
  TYPE_LABEL,
  type LedgerApiResponse,
  type LedgerEvent,
} from './ledgerEvents';

const POLL_MS = 10_000;
const MAX_ROWS = 8;

function prefersReducedMotion(): boolean {
  return typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
}

function clockTime(iso: string): string {
  return new Date(iso).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
}

function agoLabel(iso: string | null): string {
  if (!iso) return '';
  const s = Math.max(0, Math.floor((Date.now() - new Date(iso).getTime()) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}

function Row({ event, isNew, index }: { event: LedgerEvent; isNew: boolean; index: number }) {
  const [open, setOpen] = useState(false);
  const killed = event.verdict === 'killed';
  const spine = killed ? '#0a0a0a' : SPINE_COLOR[event.type];
  const hasDetail = Boolean(event.detail && (event.detail.hash || event.detail.metric || event.detail.note));

  return (
    <li
      className={`v2-row${isNew ? ' v2-row--new' : ''}${open ? ' v2-row--open' : ''}`}
      data-type={event.type}
      style={{ '--spine': spine, '--i': index } as React.CSSProperties}
    >
      <button
        type="button"
        className="v2-row__head"
        onClick={() => hasDetail && setOpen((o) => !o)}
        aria-expanded={hasDetail ? open : undefined}
        disabled={!hasDetail}
      >
        <span className="v2-row__spine" aria-hidden />
        <span className="v2-row__type">{TYPE_LABEL[event.type]}</span>
        <time className="v2-row__time" dateTime={event.ts}>{clockTime(event.ts)}</time>
        <span className="v2-row__claim">
          {event.summary}
          {event.example && <span className="v2-row__example">EXAMPLE</span>}
        </span>
        {event.verdict && event.verdict !== 'running' && (
          <ProofStamp verdict={event.verdict} />
        )}
        {event.verdict === 'running' && <span className="v2-row__running" aria-hidden />}
      </button>
      {hasDetail && (
        <div className="v2-row__detail" data-open={open}>
          <div className="v2-row__detail-inner">
            {event.detail?.hash && (
              <p className="v2-row__hash"><span aria-hidden>🔒</span> locked before the run · <code>{event.detail.hash}</code></p>
            )}
            {event.detail?.metric && <p><strong>readout</strong> {event.detail.metric}</p>}
            {event.detail?.note && <p className="v2-row__note">{event.detail.note}</p>}
            {event.detail?.link && <a className="v2-row__open" href={event.detail.link}>Open the full record →</a>}
          </div>
        </div>
      )}
    </li>
  );
}

export default function LiveLedger() {
  // ── live poll of the real engine feed ──────────────────────────────────────
  const [live, setLive] = useState<LedgerEvent[] | null>(null); // null = still loading first poll
  const [idle, setIdle] = useState(true);
  const [running, setRunning] = useState(0);
  const [lastEventAt, setLastEventAt] = useState<string | null>(null);
  const [flashId, setFlashId] = useState<string | null>(null);
  const prevHead = useRef<string | null>(null);

  useEffect(() => {
    let alive = true;
    const reduce = prefersReducedMotion();
    const load = async () => {
      try {
        const res = await fetch('/api/ledger', { cache: 'no-store' });
        const data = (await res.json()) as LedgerApiResponse;
        if (!alive) return;
        if (data.source === 'unreachable' || data.events.length === 0) {
          setLive([]); // → honest replay fallback
          return;
        }
        setLive(data.events);
        setIdle(data.idle);
        setRunning(data.running_jobs);
        setLastEventAt(data.last_event_at ?? null);
        const head = data.events[0]?.id ?? null;
        if (!reduce && prevHead.current && head && head !== prevHead.current) {
          setFlashId(head);
          window.setTimeout(() => { if (alive) setFlashId(null); }, 640);
        }
        prevHead.current = head;
      } catch {
        if (alive) setLive([]);
      }
    };
    load();
    const iv = window.setInterval(load, POLL_MS);
    return () => { alive = false; window.clearInterval(iv); };
  }, []);

  const isLive = live !== null && live.length > 0;

  // ── replay fallback (only when the engine feed is unavailable/empty) ────────
  const [shown, setShown] = useState(0);
  const [paused, setPaused] = useState(false);
  const [replayDone, setReplayDone] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (isLive || live === null) return; // wait for first poll; run replay only in the empty/unreachable branch
    if (prefersReducedMotion()) { setShown(REAL_RUN.length); setReplayDone(true); return; }
    if (paused || shown >= REAL_RUN.length) {
      if (shown >= REAL_RUN.length) setReplayDone(true);
      return;
    }
    timer.current = setTimeout(() => setShown((n) => n + 1), shown === 0 ? 400 : 1100);
    return () => { if (timer.current) clearTimeout(timer.current); };
  }, [isLive, live, shown, paused]);

  const replay = () => { setReplayDone(false); setShown(0); };

  // ── staggered cascade the first time the ledger scrolls into view ───────────
  const sectionRef = useRef<HTMLElement>(null);
  const [inView, setInView] = useState(false);
  useEffect(() => {
    const el = sectionRef.current;
    if (!el) return;
    if (prefersReducedMotion()) { setInView(true); return; }
    const io = new IntersectionObserver(
      (entries) => { if (entries[0]?.isIntersecting) { setInView(true); io.disconnect(); } },
      { threshold: 0.15 },
    );
    io.observe(el);
    return () => io.disconnect();
  }, []);

  // ── derive what to render ───────────────────────────────────────────────────
  let rows: LedgerEvent[];
  let badge: 'LIVE' | 'IDLE' | 'REPLAY';
  let pulsing: boolean;
  let foot: React.ReactNode;
  let showReplayBtn = false;

  if (isLive) {
    rows = live!.slice(0, MAX_ROWS); // already newest-first from the engine
    badge = running > 0 ? 'LIVE' : 'IDLE';
    pulsing = running > 0;
    foot = running > 0
      ? <><code>{running}</code> job{running === 1 ? '' : 's'} running · the engine runs only when a test is worth it</>
      : <>last event {agoLabel(lastEventAt)} · the engine runs only when a test is worth it</>;
  } else {
    const streamed = REAL_RUN.slice(0, shown);
    rows = [...streamed].reverse();
    badge = 'REPLAY';
    pulsing = !replayDone && shown < REAL_RUN.length;
    showReplayBtn = replayDone;
    foot = <>real run <code>a4c2036</code> · {pulsing ? 'replaying' : 'the engine runs only when a test is worth it'}</>;
  }

  return (
    <section
      ref={sectionRef}
      className={`v2-ledger${inView ? ' v2-ledger--in' : ''}`}
      aria-label="TWOG live ledger"
      onMouseEnter={() => setPaused(true)}
      onMouseLeave={() => setPaused(false)}
    >
      <header className="v2-ledger__head">
        <span className={`v2-status ${pulsing ? 'v2-status--live' : 'v2-status--idle'}`}>
          <span className="v2-status__dot" aria-hidden />
          {badge}
        </span>
        <span className="v2-ledger__title">the engine, step by step</span>
        {showReplayBtn && (
          <button type="button" className="v2-ledger__replay" onClick={replay}>↻ replay run</button>
        )}
      </header>

      <ol className="v2-ledger__rows">
        {rows.map((event, i) => (
          <Row
            key={event.id}
            event={event}
            index={i}
            isNew={isLive ? event.id === flashId : i === 0 && pulsing}
          />
        ))}
        {!isLive && replayDone && <Row event={EXAMPLE_KILLED} isNew={false} index={rows.length} />}
      </ol>

      <footer className="v2-ledger__foot">{foot}</footer>
    </section>
  );
}
