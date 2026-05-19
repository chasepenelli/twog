'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

type Proposal = {
  paper_id: number;
  pmid: string | null;
  doi: string | null;
  current_track: string | null;
  proposed_track: string | null;
  title: string;
  journal: string | null;
  year: number | null;
  abstract_preview: string | null;
  n_signals: number;
  signal_flags: string[];
  sim: number | null;
  rationale: string | null;
};

type Decision = 'accept' | 'reject' | 'skip';

type DecisionRecord = {
  paper_id: number;
  decision: Decision;
  reviewed_at: string;
};

type StoredState = {
  version: 1;
  decisions: Record<string, DecisionRecord>;
};

const STORAGE_KEY = 'twog.breed_audit.decisions.v1';

const TRACK_COLORS: Record<string, string> = {
  treatment: '#ef4444',
  early_detection: '#3b82f6',
  supplements: '#10b981',
  breed_screening: '#f59e0b',
  cross_cutting: '#8b5cf6',
  untagged: '#9ca3af',
};

const TRACK_LABEL: Record<string, string> = {
  treatment: 'treatment',
  early_detection: 'early detection',
  supplements: 'supplements',
  breed_screening: 'breed screening',
  cross_cutting: 'cross cutting',
  untagged: 'untagged',
};

function loadStored(): StoredState {
  if (typeof window === 'undefined') return { version: 1, decisions: {} };
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return { version: 1, decisions: {} };
    const parsed = JSON.parse(raw) as StoredState;
    if (parsed && parsed.version === 1 && typeof parsed.decisions === 'object') {
      return parsed;
    }
  } catch {
    /* swallow */
  }
  return { version: 1, decisions: {} };
}

function saveStored(state: StoredState) {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {
    /* quota etc. — non-fatal */
  }
}

function TrackChip({ track }: { track: string | null }) {
  if (!track) return null;
  const color = TRACK_COLORS[track] ?? '#9ca3af';
  return (
    <span
      className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full border bg-white text-[0.62rem] font-mono uppercase tracking-[0.18em]"
      style={{ borderColor: color, color }}
    >
      <span
        className="inline-block w-1.5 h-1.5 rounded-full"
        style={{ background: color }}
      />
      {TRACK_LABEL[track] ?? track}
    </span>
  );
}

function SignalChip({ flag }: { flag: string }) {
  return (
    <span
      className="inline-flex items-center px-2 py-0.5 rounded-full text-[0.62rem] font-mono"
      style={{
        background: 'rgba(34, 197, 94, 0.08)',
        color: '#15803d',
        border: '1px solid rgba(34, 197, 94, 0.28)',
      }}
    >
      {flag}
    </span>
  );
}

export default function BreedAuditReviewer() {
  const [proposals, setProposals] = useState<Proposal[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [decisions, setDecisions] = useState<Record<string, DecisionRecord>>({});
  const [index, setIndex] = useState(0);
  const [showReviewed, setShowReviewed] = useState(false);
  const [hydrated, setHydrated] = useState(false);
  const cardRef = useRef<HTMLDivElement | null>(null);

  // Load proposals.
  useEffect(() => {
    let cancelled = false;
    fetch('/data/breed_audit_proposals.json', { cache: 'no-store' })
      .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data: Proposal[]) => {
        if (cancelled) return;
        setProposals(data);
      })
      .catch(err => {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : 'failed to load');
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Load persisted decisions once we're in the browser.
  useEffect(() => {
    const stored = loadStored();
    setDecisions(stored.decisions);
    setHydrated(true);
  }, []);

  // Persist on change (only after initial hydrate, so we don't clobber storage).
  useEffect(() => {
    if (!hydrated) return;
    saveStored({ version: 1, decisions });
  }, [decisions, hydrated]);

  const visible = useMemo(() => {
    if (!proposals) return [];
    if (showReviewed) return proposals;
    return proposals.filter(p => !decisions[String(p.paper_id)]);
  }, [proposals, decisions, showReviewed]);

  // Clamp index whenever the visible list changes.
  useEffect(() => {
    if (visible.length === 0) {
      setIndex(0);
      return;
    }
    setIndex(i => Math.min(i, visible.length - 1));
  }, [visible.length]);

  const current = visible[index] ?? null;

  const recordDecision = useCallback(
    (decision: Decision) => {
      if (!current) return;
      const now = new Date().toISOString();
      setDecisions(prev => ({
        ...prev,
        [String(current.paper_id)]: {
          paper_id: current.paper_id,
          decision,
          reviewed_at: now,
        },
      }));
      // If we're hiding reviewed, the list will shrink under us; stay put so
      // the next unreviewed paper slides into this index. If we're showing
      // reviewed, advance.
      if (showReviewed) {
        setIndex(i => Math.min(i + 1, visible.length - 1));
      }
    },
    [current, showReviewed, visible.length],
  );

  const goto = useCallback(
    (delta: number) => {
      setIndex(i => {
        const n = visible.length;
        if (n === 0) return 0;
        return Math.max(0, Math.min(n - 1, i + delta));
      });
    },
    [visible.length],
  );

  // Keyboard shortcuts.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const tag = (e.target as HTMLElement | null)?.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA') return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      const k = e.key.toLowerCase();
      if (k === 'y') {
        e.preventDefault();
        recordDecision('accept');
      } else if (k === 'n') {
        e.preventDefault();
        recordDecision('reject');
      } else if (k === 's') {
        e.preventDefault();
        recordDecision('skip');
      } else if (e.key === 'ArrowLeft') {
        e.preventDefault();
        goto(-1);
      } else if (e.key === 'ArrowRight') {
        e.preventDefault();
        goto(1);
      }
    }
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [recordDecision, goto]);

  const totals = useMemo(() => {
    const values = Object.values(decisions);
    const accepted = values.filter(d => d.decision === 'accept').length;
    const rejected = values.filter(d => d.decision === 'reject').length;
    const skipped = values.filter(d => d.decision === 'skip').length;
    return {
      total: proposals?.length ?? 0,
      reviewed: values.length,
      accepted,
      rejected,
      skipped,
    };
  }, [decisions, proposals]);

  function exportDecisions() {
    const payload = Object.values(decisions).sort(
      (a, b) => a.paper_id - b.paper_id,
    );
    const blob = new Blob([JSON.stringify(payload, null, 2)], {
      type: 'application/json',
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'breed_audit_decisions.json';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  function resetOne(paper_id: number) {
    setDecisions(prev => {
      const next = { ...prev };
      delete next[String(paper_id)];
      return next;
    });
  }

  if (error) {
    return (
      <div className="rounded-2xl border border-[var(--gray-200)] p-8 bg-[#fdfcfa] text-[0.85rem] text-[var(--gray-500)]">
        Could not load proposals: {error}
      </div>
    );
  }

  if (!proposals) {
    return (
      <div className="rounded-2xl border border-[var(--gray-200)] p-8 bg-[#fdfcfa] text-[0.85rem] font-mono text-[var(--gray-400)]">
        Loading triage queue...
      </div>
    );
  }

  const pct = totals.total === 0 ? 0 : (totals.reviewed / totals.total) * 100;
  const existingDecision = current
    ? decisions[String(current.paper_id)]
    : undefined;

  return (
    <div className="space-y-5">
      {/* Progress bar */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center gap-3 text-[0.6rem] font-mono uppercase tracking-[0.28em] text-[var(--gray-400)]">
          <span>Queue</span>
          <span className="text-[var(--gray-300)]">·</span>
          <span>
            {Math.min(index + 1, Math.max(visible.length, 1))} / {visible.length || 0}
            {!showReviewed && proposals.length !== visible.length
              ? ` (of ${proposals.length})`
              : ''}
          </span>
        </div>
        <div className="flex items-center gap-3 flex-1 max-w-[360px]">
          <div className="relative h-[6px] flex-1 rounded-full bg-[var(--gray-100)] overflow-hidden">
            <div
              className="absolute inset-y-0 left-0 rounded-full transition-all"
              style={{ width: `${pct}%`, background: '#22C55E' }}
            />
          </div>
          <span className="text-[0.6rem] font-mono tracking-[0.2em] text-[var(--gray-400)]">
            {totals.reviewed}/{totals.total}
          </span>
        </div>
      </div>

      {/* Toggle + export row */}
      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-[var(--gray-200)] pt-4">
        <label className="flex items-center gap-2 text-[0.68rem] font-mono uppercase tracking-[0.2em] text-[var(--gray-500)] cursor-pointer select-none">
          <input
            type="checkbox"
            checked={showReviewed}
            onChange={e => setShowReviewed(e.target.checked)}
            className="accent-[#22C55E] h-3.5 w-3.5"
          />
          Show already-reviewed
        </label>
        <div className="flex items-center gap-4 text-[0.68rem] font-mono text-[var(--gray-500)]">
          <span>
            <span style={{ color: '#15803d' }}>{totals.accepted}</span> accept
          </span>
          <span>
            <span style={{ color: '#b91c1c' }}>{totals.rejected}</span> reject
          </span>
          <span>
            <span style={{ color: '#6b7280' }}>{totals.skipped}</span> skip
          </span>
        </div>
      </div>

      {/* Card */}
      {current ? (
        <div
          ref={cardRef}
          className="rounded-2xl border border-[var(--gray-200)] p-7 sm:p-9"
          style={{ background: '#fdfcfa' }}
        >
          {/* meta line */}
          <div className="flex flex-wrap items-center gap-2 mb-5 text-[0.6rem] font-mono uppercase tracking-[0.22em] text-[var(--gray-400)]">
            <span>#{current.paper_id}</span>
            {current.year != null && (
              <>
                <span className="text-[var(--gray-300)]">·</span>
                <span>{current.year}</span>
              </>
            )}
            {current.journal && (
              <>
                <span className="text-[var(--gray-300)]">·</span>
                <span className="normal-case tracking-normal text-[0.7rem] text-[var(--gray-500)]">
                  {current.journal}
                </span>
              </>
            )}
          </div>

          {/* Title */}
          <h2
            className="font-bold leading-[1.08] tracking-tight mb-5"
            style={{
              fontFamily: 'var(--font-crimson), Georgia, serif',
              fontSize: 'clamp(1.5rem, 2.6vw, 2rem)',
            }}
          >
            {current.title}
          </h2>

          {/* Track transition */}
          <div className="flex items-center flex-wrap gap-2 mb-5 text-[0.7rem] font-mono">
            <TrackChip track={current.current_track} />
            <span className="text-[var(--gray-400)]">to</span>
            <TrackChip track={current.proposed_track} />
            {current.sim != null && (
              <span className="ml-2 text-[0.62rem] font-mono uppercase tracking-[0.2em] text-[var(--gray-400)]">
                sim = {current.sim.toFixed(2)}
              </span>
            )}
          </div>

          {/* Signal flags */}
          {current.signal_flags.length > 0 && (
            <div className="mb-5">
              <p className="text-[0.58rem] font-mono uppercase tracking-[0.28em] text-[var(--gray-400)] mb-2">
                Signals that fired
              </p>
              <div className="flex flex-wrap gap-1.5">
                {current.signal_flags.map(f => (
                  <SignalChip key={f} flag={f} />
                ))}
              </div>
            </div>
          )}

          {/* Rationale */}
          {current.rationale && (
            <div className="mb-5">
              <p className="text-[0.58rem] font-mono uppercase tracking-[0.28em] text-[var(--gray-400)] mb-2">
                Why the machine flagged it
              </p>
              <p
                className="text-[0.95rem] leading-[1.6] text-[var(--gray-600)] italic"
                style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
              >
                {current.rationale}
              </p>
            </div>
          )}

          {/* Abstract */}
          {current.abstract_preview && (
            <div className="mb-6">
              <p className="text-[0.58rem] font-mono uppercase tracking-[0.28em] text-[var(--gray-400)] mb-2">
                Abstract preview
              </p>
              <p
                className="text-[0.98rem] leading-[1.75] text-[var(--gray-600)]"
                style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
              >
                {current.abstract_preview}
              </p>
            </div>
          )}

          {/* PMID / DOI links */}
          {(current.pmid || current.doi) && (
            <div className="flex flex-wrap gap-4 text-[0.68rem] font-mono text-[var(--gray-500)] mb-6">
              {current.pmid && (
                <a
                  href={`https://pubmed.ncbi.nlm.nih.gov/${current.pmid}/`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="underline decoration-[var(--gray-300)] hover:decoration-[var(--foreground)]"
                >
                  PMID {current.pmid}
                </a>
              )}
              {current.doi && (
                <a
                  href={`https://doi.org/${current.doi}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="underline decoration-[var(--gray-300)] hover:decoration-[var(--foreground)]"
                >
                  doi:{current.doi}
                </a>
              )}
            </div>
          )}

          {/* Existing decision (when showReviewed) */}
          {existingDecision && (
            <div className="mb-5 flex items-center gap-3 text-[0.68rem] font-mono uppercase tracking-[0.2em]">
              <span className="text-[var(--gray-400)]">Already</span>
              <span
                style={{
                  color:
                    existingDecision.decision === 'accept'
                      ? '#15803d'
                      : existingDecision.decision === 'reject'
                      ? '#b91c1c'
                      : '#6b7280',
                }}
              >
                {existingDecision.decision}ed
              </span>
              <button
                onClick={() => resetOne(current.paper_id)}
                className="text-[var(--gray-400)] underline decoration-[var(--gray-300)] hover:text-[var(--foreground)]"
              >
                undo
              </button>
            </div>
          )}

          {/* Action row */}
          <div className="flex flex-wrap items-center justify-between gap-3 pt-5 border-t border-[var(--gray-200)]">
            <div className="flex flex-wrap items-center gap-2">
              <button
                onClick={() => recordDecision('accept')}
                className="px-4 py-2 rounded-full text-[0.72rem] font-mono uppercase tracking-[0.2em] border transition-colors"
                style={{
                  borderColor: '#15803d',
                  color: '#15803d',
                  background:
                    existingDecision?.decision === 'accept'
                      ? 'rgba(34, 197, 94, 0.12)'
                      : 'white',
                }}
              >
                Accept <span className="opacity-60">(Y)</span>
              </button>
              <button
                onClick={() => recordDecision('reject')}
                className="px-4 py-2 rounded-full text-[0.72rem] font-mono uppercase tracking-[0.2em] border transition-colors"
                style={{
                  borderColor: '#b91c1c',
                  color: '#b91c1c',
                  background:
                    existingDecision?.decision === 'reject'
                      ? 'rgba(185, 28, 28, 0.08)'
                      : 'white',
                }}
              >
                Reject <span className="opacity-60">(N)</span>
              </button>
              <button
                onClick={() => recordDecision('skip')}
                className="px-4 py-2 rounded-full text-[0.72rem] font-mono uppercase tracking-[0.2em] border transition-colors"
                style={{
                  borderColor: 'var(--gray-300)',
                  color: 'var(--gray-500)',
                  background:
                    existingDecision?.decision === 'skip'
                      ? 'var(--gray-100)'
                      : 'white',
                }}
              >
                Skip <span className="opacity-60">(S)</span>
              </button>
            </div>
            <div className="flex items-center gap-1.5">
              <button
                onClick={() => goto(-1)}
                disabled={index === 0}
                aria-label="Previous"
                className="w-9 h-9 rounded-full border border-[var(--gray-200)] flex items-center justify-center text-[var(--gray-500)] hover:border-[var(--gray-400)] disabled:opacity-30 disabled:cursor-not-allowed"
              >
                {'\u2190'}
              </button>
              <button
                onClick={() => goto(1)}
                disabled={index >= visible.length - 1}
                aria-label="Next"
                className="w-9 h-9 rounded-full border border-[var(--gray-200)] flex items-center justify-center text-[var(--gray-500)] hover:border-[var(--gray-400)] disabled:opacity-30 disabled:cursor-not-allowed"
              >
                {'\u2192'}
              </button>
            </div>
          </div>
        </div>
      ) : (
        <div
          className="rounded-2xl border border-[var(--gray-200)] p-12 text-center"
          style={{ background: '#fdfcfa' }}
        >
          <p
            className="text-[1.3rem] mb-3"
            style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
          >
            {totals.reviewed >= totals.total && totals.total > 0
              ? 'Every proposal has a decision.'
              : 'Nothing left in the visible queue.'}
          </p>
          <p className="text-[0.82rem] font-mono uppercase tracking-[0.2em] text-[var(--gray-400)]">
            {totals.accepted} accepted · {totals.rejected} rejected · {totals.skipped} skipped
          </p>
          {!showReviewed && totals.reviewed > 0 && (
            <button
              onClick={() => setShowReviewed(true)}
              className="mt-6 text-[0.72rem] font-mono uppercase tracking-[0.2em] text-[var(--gray-500)] underline decoration-[var(--gray-300)] hover:text-[var(--foreground)]"
            >
              Review your calls
            </button>
          )}
        </div>
      )}

      {/* Export row */}
      <div className="flex flex-wrap items-center justify-between gap-3 pt-2">
        <p className="text-[0.65rem] font-mono uppercase tracking-[0.25em] text-[var(--gray-400)]">
          Y accept · N reject · S skip · {'\u2190'}/{'\u2192'} nav
        </p>
        <button
          onClick={exportDecisions}
          disabled={totals.reviewed === 0}
          className="px-5 py-2 rounded-full text-[0.72rem] font-mono uppercase tracking-[0.22em] border border-[var(--foreground)] text-[var(--foreground)] hover:bg-[var(--foreground)] hover:text-white transition-colors disabled:opacity-30 disabled:cursor-not-allowed"
        >
          Export decisions
        </button>
      </div>
    </div>
  );
}
