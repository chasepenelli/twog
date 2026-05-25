'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import {
  AcceptedCapsuleCardData,
  contributorLabel,
  formatPublicDateShort,
  formatVerdict,
} from './AcceptedCapsuleCard';
import { formatPacketType } from './WorkPacketCard';

interface WireEntry {
  proof_capsule_id: string;
  candidate_id: string;
  capsule_type: string;
  title: string;
  contributor: AcceptedCapsuleCardData['contributor'];
  status: string;
  submitted_at: string;
  reviewed_at: string | null;
  status_url: string;
}

interface Annotated extends WireEntry {
  highlight: 'new' | 'status_changed' | null;
  previousStatus: string | null;
  highlightUntil: number | null;
}

interface Props {
  initialEntries: WireEntry[];
  candidateLabels?: Record<string, string>;
  pollIntervalMs?: number;
  highlightMs?: number;
}

const DEFAULT_POLL_MS = 8_000;
const DEFAULT_HIGHLIGHT_MS = 6_000;

function entryKey(entry: WireEntry): string {
  return `${entry.proof_capsule_id}::${entry.status}`;
}

function relativeTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  const diffMs = Date.now() - date.getTime();
  const minutes = Math.round(diffMs / 60000);
  if (minutes < 1) return 'just now';
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.round(hours / 24);
  if (days < 30) return `${days}d ago`;
  return formatPublicDateShort(value);
}

function timestampFor(entry: WireEntry): string {
  return entry.reviewed_at ?? entry.submitted_at;
}

function annotate(
  current: WireEntry[],
  previousById: Map<string, WireEntry>,
  highlightUntil: number,
): Annotated[] {
  return current.map((entry) => {
    const prev = previousById.get(entry.proof_capsule_id);
    if (!prev) {
      return {
        ...entry,
        highlight: 'new' as const,
        previousStatus: null,
        highlightUntil,
      };
    }
    if (prev.status !== entry.status) {
      return {
        ...entry,
        highlight: 'status_changed' as const,
        previousStatus: prev.status,
        highlightUntil,
      };
    }
    return {
      ...entry,
      highlight: null,
      previousStatus: null,
      highlightUntil: null,
    };
  });
}

interface FeedResponse {
  recent_capsules?: WireEntry[];
  accepted_capsules?: WireEntry[];
  error?: string;
}

export function LiveActivityWire({
  initialEntries,
  candidateLabels,
  pollIntervalMs = DEFAULT_POLL_MS,
  highlightMs = DEFAULT_HIGHLIGHT_MS,
}: Props) {
  const [entries, setEntries] = useState<Annotated[]>(() =>
    initialEntries.map((entry) => ({
      ...entry,
      highlight: null,
      previousStatus: null,
      highlightUntil: null,
    })),
  );
  const [lastUpdateAt, setLastUpdateAt] = useState<number>(() => Date.now());
  const [error, setError] = useState<string | null>(null);
  const [isFresh, setIsFresh] = useState<boolean>(false);
  const previousRef = useRef<Map<string, WireEntry>>(
    new Map(initialEntries.map((entry) => [entry.proof_capsule_id, entry])),
  );

  const tick = useCallback(async () => {
    try {
      const response = await fetch('/api/network/feed', { cache: 'no-store' });
      if (!response.ok) {
        setError(`feed returned ${response.status}`);
        return;
      }
      const body = (await response.json()) as FeedResponse;
      const next = body.recent_capsules ?? [];
      const previous = previousRef.current;
      const highlightUntil = Date.now() + highlightMs;
      const annotated = annotate(next, previous, highlightUntil);
      const previouslyKnown = new Set(previous.keys());
      const hasChange = annotated.some(
        (entry) => entry.highlight !== null || !previouslyKnown.has(entry.proof_capsule_id),
      );
      setEntries(annotated);
      setError(null);
      setLastUpdateAt(Date.now());
      if (hasChange) {
        setIsFresh(true);
        window.setTimeout(() => setIsFresh(false), highlightMs);
      }
      previousRef.current = new Map(next.map((entry) => [entry.proof_capsule_id, entry]));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, [highlightMs]);

  useEffect(() => {
    const id = window.setInterval(tick, pollIntervalMs);
    return () => window.clearInterval(id);
  }, [tick, pollIntervalMs]);

  // Re-render every second so relative timestamps stay current.
  const [, forceTick] = useState(0);
  useEffect(() => {
    const id = window.setInterval(() => forceTick((n) => n + 1), 1000);
    return () => window.clearInterval(id);
  }, []);

  const updatedSecondsAgo = Math.max(0, Math.round((Date.now() - lastUpdateAt) / 1000));

  if (entries.length === 0) {
    return (
      <div className="activity-wire-empty">
        <span className="lab-label">Live activity</span>
        <p>No capsule activity yet. Submissions will stream in here as work checks in.</p>
        <LiveIndicator updatedSecondsAgo={updatedSecondsAgo} error={error} fresh={isFresh} />
      </div>
    );
  }

  return (
    <div className={`activity-wire${isFresh ? ' fresh' : ''}`}>
      <div className="activity-wire-head">
        <span className="lab-label">Live activity</span>
        <LiveIndicator updatedSecondsAgo={updatedSecondsAgo} error={error} fresh={isFresh} />
      </div>
      <ol className="activity-wire-rail">
        {entries.map((entry) => {
          const candidateLabel = candidateLabels?.[entry.candidate_id] ?? entry.candidate_id;
          const stillHot =
            entry.highlight !== null &&
            entry.highlightUntil !== null &&
            entry.highlightUntil > Date.now();
          const highlightClass = stillHot
            ? entry.highlight === 'new'
              ? ' wire-new'
              : ' wire-status-changed'
            : '';
          return (
            <li
              className={`activity-wire-row activity-status-${entry.status}${highlightClass}`}
              key={entry.proof_capsule_id}
            >
              <div className="activity-wire-time" title={timestampFor(entry)}>
                {relativeTimestamp(timestampFor(entry))}
              </div>
              <div className="activity-wire-body">
                <div className="activity-wire-line">
                  {stillHot && entry.highlight === 'new' ? (
                    <span className="activity-wire-badge wire-badge-new">new</span>
                  ) : null}
                  {stillHot && entry.highlight === 'status_changed' ? (
                    <span className="activity-wire-badge wire-badge-changed">
                      {entry.previousStatus} → {entry.status}
                    </span>
                  ) : null}
                  <span className="activity-wire-verdict">{formatVerdict(entry.status)}</span>
                  <span className="activity-wire-type">{formatPacketType(entry.capsule_type)}</span>
                </div>
                <Link
                  className="activity-wire-title"
                  href={`/capsules/${encodeURIComponent(entry.proof_capsule_id)}`}
                >
                  {entry.title}
                </Link>
                <div className="activity-wire-meta">
                  {entry.contributor.handle ? (
                    <Link href={`/contributors/${encodeURIComponent(entry.contributor.handle)}`}>
                      {contributorLabel(entry.contributor)}
                    </Link>
                  ) : (
                    <span>{contributorLabel(entry.contributor)}</span>
                  )}
                  <span>·</span>
                  <Link href={`/candidates/${encodeURIComponent(entry.candidate_id)}`}>
                    {candidateLabel}
                  </Link>
                </div>
              </div>
            </li>
          );
        })}
      </ol>
    </div>
  );
}

interface IndicatorProps {
  updatedSecondsAgo: number;
  error: string | null;
  fresh: boolean;
}

function LiveIndicator({ updatedSecondsAgo, error, fresh }: IndicatorProps) {
  if (error) {
    return (
      <span className="activity-wire-indicator state-error" title={error}>
        <span className="activity-wire-dot" />
        offline · retrying
      </span>
    );
  }
  const stateClass = fresh ? 'state-fresh' : 'state-live';
  const label = fresh ? 'updating' : updatedSecondsAgo <= 1 ? 'live' : `live · ${updatedSecondsAgo}s`;
  return (
    <span className={`activity-wire-indicator ${stateClass}`}>
      <span className="activity-wire-dot" />
      {label}
    </span>
  );
}
