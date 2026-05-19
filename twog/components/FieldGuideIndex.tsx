'use client';

import { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';

const TRACK_COLORS: Record<string, string> = {
  treatment: '#ef4444',
  early_detection: '#3b82f6',
  supplements: '#10b981',
  breed_screening: '#f59e0b',
  splenic_hsa: '#06b6d4',
  cross_cutting: '#8b5cf6',
  untagged: '#9ca3af',
};

const TRACK_ORDER = [
  'treatment',
  'early_detection',
  'supplements',
  'breed_screening',
  'splenic_hsa',
  'cross_cutting',
  'untagged',
];

const TRACK_LABELS: Record<string, string> = {
  treatment: 'Treatment',
  early_detection: 'Early detection',
  supplements: 'Supplements',
  breed_screening: 'Breed risk',
  splenic_hsa: 'Splenic HSA',
  cross_cutting: 'General biology',
  untagged: 'Unsorted',
};

type TopPaper = {
  id: number;
  pmid: string | null;
  doi: string | null;
  title: string;
  journal: string | null;
  year: number | null;
  citation_count: number;
  starter: boolean;
  score: number;
};

type Neighbor = {
  cluster_id: number;
  name: string;
  similarity: number;
};

export type FieldGuideEntry = {
  cluster_id: number;
  kind: 'cluster' | 'subcluster';
  parent_cluster: number | null;
  name: string;
  one_line: string;
  synthesis_md: string;
  gap_line: string;
  vitals: {
    n: number;
    median_year: number;
    pct_embedded: number;
    pct_abstract: number;
    track_mix: Record<string, number>;
  };
  neighbors: Neighbor[];
  top_papers: TopPaper[];
};

type FieldGuideData = {
  generated_at: number;
  n_entries: number;
  n_clusters: number;
  n_subclusters: number;
  parent_cluster_for_subclusters: number;
  entries: FieldGuideEntry[];
};

function dominantTrack(mix: Record<string, number>): string {
  const entries = Object.entries(mix);
  if (entries.length === 0) return 'untagged';
  entries.sort((a, b) => b[1] - a[1]);
  return entries[0][0];
}

function TrackMixBar({ mix }: { mix: Record<string, number> }) {
  const segments = TRACK_ORDER.map(track => ({
    track,
    share: mix[track] || 0,
  })).filter(s => s.share > 0);

  return (
    <div className="w-full h-[4px] rounded-full overflow-hidden bg-[var(--gray-100)] flex">
      {segments.map(s => (
        <span
          key={s.track}
          style={{
            width: `${s.share * 100}%`,
            background: TRACK_COLORS[s.track] || '#9ca3af',
          }}
          title={`${TRACK_LABELS[s.track] || s.track} — ${Math.round(s.share * 100)}%`}
        />
      ))}
    </div>
  );
}

export default function FieldGuideIndex() {
  const [data, setData] = useState<FieldGuideData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [trackFilter, setTrackFilter] = useState<string>('all');
  const [sortBy, setSortBy] = useState<'size' | 'alpha'>('size');

  useEffect(() => {
    let alive = true;
    fetch('/data/field_guide_v2.json')
      .then(r => r.json())
      .then(d => {
        if (alive) setData(d);
      })
      .catch(e => {
        if (alive) setError(String(e));
      });
    return () => {
      alive = false;
    };
  }, []);

  const topLevel = useMemo(() => {
    if (!data) return [];
    const list = data.entries.filter(e => e.kind === 'cluster');
    const filtered = list.filter(e => {
      if (trackFilter === 'all') return true;
      return dominantTrack(e.vitals.track_mix) === trackFilter;
    });
    if (sortBy === 'alpha') {
      filtered.sort((a, b) => a.name.localeCompare(b.name));
    } else {
      filtered.sort((a, b) => b.vitals.n - a.vitals.n);
    }
    return filtered;
  }, [data, trackFilter, sortBy]);

  const availableTracks = useMemo(() => {
    if (!data) return [] as string[];
    const s = new Set<string>();
    data.entries
      .filter(e => e.kind === 'cluster')
      .forEach(e => s.add(dominantTrack(e.vitals.track_mix)));
    return TRACK_ORDER.filter(t => s.has(t));
  }, [data]);

  if (error) {
    return (
      <p className="text-[0.75rem] font-mono text-red-600">
        Failed to load field guide: {error}
      </p>
    );
  }

  if (!data) {
    return (
      <p className="text-[0.72rem] font-mono text-[var(--gray-400)] animate-pulse">
        loading the field guide…
      </p>
    );
  }

  return (
    <div>
      {/* Filter row */}
      <div className="flex flex-wrap items-center gap-2.5 mb-8">
        <span className="text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)] mr-1">
          Filter
        </span>
        <button
          onClick={() => setTrackFilter('all')}
          className="flex items-center gap-1.5 px-2.5 py-1 rounded-full border text-[0.72rem] transition-all"
          style={{
            borderColor:
              trackFilter === 'all' ? 'var(--foreground)' : 'var(--gray-200)',
            background:
              trackFilter === 'all' ? 'var(--gray-100)' : 'transparent',
            color:
              trackFilter === 'all' ? 'var(--foreground)' : 'var(--gray-400)',
          }}
        >
          All tracks
        </button>
        {availableTracks.map(track => {
          const active = trackFilter === track;
          return (
            <button
              key={track}
              onClick={() => setTrackFilter(active ? 'all' : track)}
              className="flex items-center gap-1.5 px-2.5 py-1 rounded-full border text-[0.72rem] transition-all"
              style={{
                borderColor: active ? TRACK_COLORS[track] : 'var(--gray-200)',
                background: active
                  ? `${TRACK_COLORS[track]}15`
                  : 'transparent',
                color: active ? TRACK_COLORS[track] : 'var(--gray-400)',
              }}
            >
              <span
                className="inline-block w-2 h-2 rounded-full"
                style={{ background: TRACK_COLORS[track] }}
              />
              {TRACK_LABELS[track] || track}
            </button>
          );
        })}

        <span className="flex-1" />

        <span className="text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)] mr-1">
          Sort
        </span>
        <div className="inline-flex rounded-full border border-[var(--gray-200)] overflow-hidden">
          <button
            onClick={() => setSortBy('size')}
            className="px-3 py-1 text-[0.72rem] font-mono transition-colors"
            style={{
              background: sortBy === 'size' ? 'var(--foreground)' : 'white',
              color: sortBy === 'size' ? 'white' : 'var(--gray-400)',
            }}
          >
            size
          </button>
          <button
            onClick={() => setSortBy('alpha')}
            className="px-3 py-1 text-[0.72rem] font-mono transition-colors border-l border-[var(--gray-200)]"
            style={{
              background: sortBy === 'alpha' ? 'var(--foreground)' : 'white',
              color: sortBy === 'alpha' ? 'white' : 'var(--gray-400)',
            }}
          >
            A–Z
          </button>
        </div>
      </div>

      {/* Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
        {topLevel.map(entry => {
          const track = dominantTrack(entry.vitals.track_mix);
          const color = TRACK_COLORS[track] || '#9ca3af';
          return (
            <Link
              key={entry.cluster_id}
              href={`/research/field-guide/${entry.cluster_id}`}
              className="group flex flex-col p-5 border border-[var(--gray-200)] rounded-2xl bg-white hover:border-[var(--gray-400)] hover:shadow-sm transition-all"
            >
              <div className="flex items-center gap-2 mb-3 text-[0.58rem] font-mono uppercase tracking-[0.25em] text-[var(--gray-400)]">
                <span
                  className="inline-block w-2 h-2 rounded-full"
                  style={{ background: color }}
                />
                <span>{TRACK_LABELS[track] || track}</span>
                <span className="text-[var(--gray-300)]">·</span>
                <span>No. {String(entry.cluster_id).padStart(2, '0')}</span>
              </div>

              <h3
                className="text-[1.15rem] font-semibold leading-[1.25] tracking-tight mb-2 text-[var(--foreground)] group-hover:underline"
                style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
              >
                {entry.name}
              </h3>

              <p
                className="text-[0.92rem] leading-[1.5] italic text-[var(--gray-500)] mb-4 flex-1"
                style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
              >
                {entry.one_line}
              </p>

              <div className="flex items-center gap-3 text-[0.6rem] font-mono text-[var(--gray-400)] mb-2.5">
                <span>
                  <strong className="text-[var(--foreground)] font-semibold">
                    {entry.vitals.n.toLocaleString()}
                  </strong>{' '}
                  papers
                </span>
                <span className="text-[var(--gray-300)]">·</span>
                <span>median {entry.vitals.median_year}</span>
                {entry.cluster_id ===
                  data.parent_cluster_for_subclusters && (
                  <>
                    <span className="text-[var(--gray-300)]">·</span>
                    <span className="text-[var(--foreground)]">
                      {data.n_subclusters} sub-topics
                    </span>
                  </>
                )}
              </div>

              <TrackMixBar mix={entry.vitals.track_mix} />
            </Link>
          );
        })}
      </div>

      {topLevel.length === 0 && (
        <p className="text-[0.8rem] text-[var(--gray-400)] text-center py-12">
          No clusters match that filter.
        </p>
      )}
    </div>
  );
}
