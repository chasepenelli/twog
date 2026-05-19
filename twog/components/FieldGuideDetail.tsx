'use client';

import { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import FieldGuideSynthesis from './FieldGuideSynthesis';
import type { FieldGuideEntry } from './FieldGuideIndex';

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

type FieldGuideData = {
  generated_at: number;
  n_entries: number;
  n_clusters: number;
  n_subclusters: number;
  parent_cluster_for_subclusters: number;
  entries: FieldGuideEntry[];
};

interface Props {
  clusterParam: string;
}

function dominantTrack(mix: Record<string, number>): string {
  const entries = Object.entries(mix);
  if (entries.length === 0) return 'untagged';
  entries.sort((a, b) => b[1] - a[1]);
  return entries[0][0];
}

// URL scheme:
//   /research/field-guide/<N>             → top-level cluster N (0-49)
//   /research/field-guide/25-sub-<N>      → sub-cluster of C25 (0-33)
function resolveEntry(
  data: FieldGuideData,
  param: string,
): FieldGuideEntry | null {
  const subMatch = param.match(/^(\d+)-sub-(\d+)$/);
  if (subMatch) {
    const parent = parseInt(subMatch[1], 10);
    const sub = parseInt(subMatch[2], 10);
    return (
      data.entries.find(
        e =>
          e.kind === 'subcluster' &&
          e.parent_cluster === parent &&
          e.cluster_id === sub,
      ) || null
    );
  }
  const idx = parseInt(param, 10);
  if (Number.isNaN(idx)) return null;
  return (
    data.entries.find(e => e.kind === 'cluster' && e.cluster_id === idx) ||
    null
  );
}

function TrackMixStack({ mix }: { mix: Record<string, number> }) {
  const segments = TRACK_ORDER.map(track => ({
    track,
    share: mix[track] || 0,
  })).filter(s => s.share > 0.001);
  return (
    <div>
      <div className="w-full h-[10px] rounded-full overflow-hidden bg-[var(--gray-100)] flex">
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
      <div className="flex flex-wrap gap-x-4 gap-y-1 mt-3 text-[0.66rem] font-mono text-[var(--gray-500)]">
        {segments.map(s => (
          <span key={s.track} className="inline-flex items-center gap-1.5">
            <span
              className="inline-block w-2 h-2 rounded-full"
              style={{ background: TRACK_COLORS[s.track] }}
            />
            {TRACK_LABELS[s.track] || s.track} ·{' '}
            {Math.round(s.share * 100)}%
          </span>
        ))}
      </div>
    </div>
  );
}

function ScoreChip({ score }: { score: number }) {
  if (!score || !Number.isFinite(score)) return null;
  return (
    <span className="inline-flex items-center text-[0.58rem] font-mono uppercase tracking-[0.15em] px-2 py-[2px] rounded-full border border-[var(--gray-200)] text-[var(--gray-500)] bg-[var(--gray-100)]">
      score {score.toFixed(2)}
    </span>
  );
}

function PaperCard({
  paper,
  starter,
}: {
  paper: FieldGuideEntry['top_papers'][number];
  starter: boolean;
}) {
  const pubmedUrl = paper.pmid
    ? `https://pubmed.ncbi.nlm.nih.gov/${paper.pmid}/`
    : null;
  const doiUrl = paper.doi ? `https://doi.org/${paper.doi}` : null;
  // Titles occasionally carry HTML like <i>...</i>; strip them for display.
  const cleanTitle = paper.title.replace(/<[^>]+>/g, '');

  return (
    <article
      className={`p-5 border rounded-2xl bg-white ${
        starter
          ? 'border-[var(--gray-300)]'
          : 'border-[var(--gray-200)]'
      }`}
    >
      <h4
        className={`font-semibold leading-[1.3] mb-2 ${starter ? 'text-[1.1rem]' : 'text-[0.98rem]'}`}
        style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
      >
        {cleanTitle}
      </h4>
      <p className="text-[0.65rem] font-mono text-[var(--gray-400)] mb-3">
        {paper.journal ? `${paper.journal} · ` : ''}
        {paper.year ?? 'n.d.'}
        {typeof paper.citation_count === 'number'
          ? ` · ${paper.citation_count} citations`
          : ''}
      </p>
      <div className="flex flex-wrap gap-2 items-center">
        {pubmedUrl && (
          <a
            href={pubmedUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="text-[0.66rem] font-mono px-2.5 py-1 rounded-full border border-[var(--gray-200)] hover:border-[var(--foreground)] hover:bg-[var(--gray-100)] transition-colors"
          >
            PubMed
          </a>
        )}
        {doiUrl && (
          <a
            href={doiUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="text-[0.66rem] font-mono px-2.5 py-1 rounded-full border border-[var(--gray-200)] hover:border-[var(--foreground)] hover:bg-[var(--gray-100)] transition-colors"
          >
            DOI
          </a>
        )}
        <ScoreChip score={paper.score} />
      </div>
    </article>
  );
}

export default function FieldGuideDetail({ clusterParam }: Props) {
  const [data, setData] = useState<FieldGuideData | null>(null);
  const [error, setError] = useState<string | null>(null);

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

  const entry = useMemo(() => {
    if (!data) return null;
    return resolveEntry(data, clusterParam);
  }, [data, clusterParam]);

  const parentEntry = useMemo(() => {
    if (!data || !entry || entry.kind !== 'subcluster') return null;
    return (
      data.entries.find(
        e => e.kind === 'cluster' && e.cluster_id === entry.parent_cluster,
      ) || null
    );
  }, [data, entry]);

  const subChildren = useMemo(() => {
    if (!data || !entry) return [];
    if (entry.kind !== 'cluster') return [];
    return data.entries
      .filter(
        e =>
          e.kind === 'subcluster' && e.parent_cluster === entry.cluster_id,
      )
      .sort((a, b) => b.vitals.n - a.vitals.n);
  }, [data, entry]);

  if (error) {
    return (
      <main className="min-h-screen pt-24 pb-24 px-6 max-w-[960px] mx-auto">
        <p className="text-[0.75rem] font-mono text-red-600">
          Failed to load field guide: {error}
        </p>
      </main>
    );
  }

  if (!data) {
    return (
      <main className="min-h-screen pt-24 pb-24 px-6 max-w-[960px] mx-auto">
        <p className="text-[0.72rem] font-mono text-[var(--gray-400)] animate-pulse">
          loading…
        </p>
      </main>
    );
  }

  if (!entry) {
    return (
      <main className="min-h-screen pt-24 pb-24 px-6 max-w-[960px] mx-auto">
        <p className="text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)] mb-4">
          Field Guide
        </p>
        <h1
          className="text-[2rem] font-bold mb-3"
          style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
        >
          Neighborhood not found.
        </h1>
        <p className="text-[0.95rem] text-[var(--gray-500)] mb-6">
          We couldn't find a cluster at <code>{clusterParam}</code>.
        </p>
        <Link
          href="/research/field-guide"
          className="text-[0.75rem] font-mono underline text-[var(--gray-500)] hover:text-[var(--foreground)]"
        >
          ← Back to the field guide
        </Link>
      </main>
    );
  }

  const track = dominantTrack(entry.vitals.track_mix);
  const trackColor = TRACK_COLORS[track] || '#9ca3af';
  const trackLabel = TRACK_LABELS[track] || track;

  const starters = entry.top_papers.filter(p => p.starter).slice(0, 3);
  const shelf = entry.top_papers.filter(p => !p.starter);

  const isC25 =
    entry.kind === 'cluster' &&
    entry.cluster_id === data.parent_cluster_for_subclusters;

  return (
    <main className="min-h-screen pt-24 pb-24 px-4 sm:px-6 lg:px-10 max-w-[960px] mx-auto">
      {/* Breadcrumb */}
      <nav className="flex flex-wrap items-center gap-2 mb-6 text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)]">
        <Link
          href="/research/field-guide"
          className="hover:text-[var(--foreground)]"
        >
          Field Guide
        </Link>
        <span className="text-[var(--gray-300)]">/</span>
        {parentEntry ? (
          <>
            <Link
              href={`/research/field-guide/${parentEntry.cluster_id}`}
              className="hover:text-[var(--foreground)]"
            >
              {parentEntry.name}
            </Link>
            <span className="text-[var(--gray-300)]">/</span>
          </>
        ) : null}
        <span className="text-[var(--gray-500)] normal-case tracking-normal font-mono">
          {entry.name}
        </span>
      </nav>

      {/* Masthead */}
      <header className="mb-10 max-w-[760px]">
        <div className="flex items-center gap-3 mb-4 text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)]">
          <span
            className="inline-block w-2.5 h-2.5 rounded-full"
            style={{ background: trackColor }}
          />
          <span>{trackLabel}</span>
          <span className="text-[var(--gray-300)]">·</span>
          <span>
            {entry.kind === 'subcluster'
              ? `Sub-topic of C${entry.parent_cluster}`
              : `Cluster No. ${String(entry.cluster_id).padStart(2, '0')}`}
          </span>
        </div>
        <h1
          className="font-bold leading-[0.98] tracking-tight mb-5"
          style={{
            fontFamily: 'var(--font-crimson), Georgia, serif',
            fontSize: 'clamp(2rem, 4.4vw, 3.1rem)',
          }}
        >
          {entry.name}
        </h1>
        <p
          className="text-[1.1rem] leading-[1.55] italic text-[var(--gray-500)]"
          style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
        >
          {entry.one_line}
        </p>
      </header>

      {/* Vitals strip */}
      <section className="mb-8 grid grid-cols-2 sm:grid-cols-4 gap-px bg-[var(--gray-200)] rounded-2xl overflow-hidden border border-[var(--gray-200)]">
        <div className="bg-white py-5 px-4 text-center">
          <div className="text-[1.5rem] font-bold font-mono leading-none">
            {entry.vitals.n.toLocaleString()}
          </div>
          <div className="text-[0.55rem] uppercase tracking-[0.2em] text-[var(--gray-400)] mt-2">
            Papers
          </div>
        </div>
        <div className="bg-white py-5 px-4 text-center">
          <div className="text-[1.5rem] font-bold font-mono leading-none">
            {entry.vitals.median_year}
          </div>
          <div className="text-[0.55rem] uppercase tracking-[0.2em] text-[var(--gray-400)] mt-2">
            Median year
          </div>
        </div>
        <div className="bg-white py-5 px-4 text-center">
          <div className="text-[1.5rem] font-bold font-mono leading-none">
            {Math.round(entry.vitals.pct_abstract)}%
          </div>
          <div className="text-[0.55rem] uppercase tracking-[0.2em] text-[var(--gray-400)] mt-2">
            Have abstracts
          </div>
        </div>
        <div className="bg-white py-5 px-4 text-center">
          <div
            className="text-[1rem] font-bold leading-tight"
            style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
          >
            {trackLabel}
          </div>
          <div className="text-[0.55rem] uppercase tracking-[0.2em] text-[var(--gray-400)] mt-2">
            Top track
          </div>
        </div>
      </section>

      {/* Track mix bar */}
      <section className="mb-12">
        <p className="text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)] mb-3">
          Track mix
        </p>
        <TrackMixStack mix={entry.vitals.track_mix} />
      </section>

      {/* Synthesis */}
      <section className="mb-12 max-w-[720px]">
        <p className="text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)] mb-4">
          What we've read
        </p>
        <FieldGuideSynthesis markdown={entry.synthesis_md} />
      </section>

      {/* Gap */}
      <section
        className="mb-14 max-w-[720px] px-5 py-4 border-l-4 rounded-r-xl"
        style={{
          borderColor: '#f59e0b',
          background: 'rgba(245, 158, 11, 0.06)',
        }}
      >
        <p className="text-[0.58rem] font-mono uppercase tracking-[0.3em] text-[#b45309] mb-2">
          The gap
        </p>
        <p
          className="text-[1rem] leading-[1.6] italic text-[var(--gray-600)]"
          style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
        >
          {entry.gap_line}
        </p>
      </section>

      {/* Starter papers */}
      {starters.length > 0 && (
        <section className="mb-12">
          <p className="text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)] mb-2">
            Start here
          </p>
          <p
            className="text-[0.9rem] italic text-[var(--gray-500)] mb-5"
            style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
          >
            {starters.length === 3
              ? '3 papers to read first.'
              : `${starters.length} papers to read first.`}
          </p>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {starters.map(p => (
              <PaperCard key={p.id} paper={p} starter />
            ))}
          </div>
        </section>
      )}

      {/* Shelf */}
      {shelf.length > 0 && (
        <section className="mb-14">
          <p className="text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)] mb-5">
            Also on the shelf
          </p>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {shelf.map(p => (
              <PaperCard key={p.id} paper={p} starter={false} />
            ))}
          </div>
        </section>
      )}

      {/* C25 sub-topics */}
      {isC25 && subChildren.length > 0 && (
        <section className="mb-14">
          <p className="text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)] mb-2">
            Inside this neighborhood
          </p>
          <h2
            className="text-[1.6rem] font-bold leading-tight mb-5"
            style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
          >
            {subChildren.length} sub-topics.
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {subChildren.map(sub => {
              const subTrack = dominantTrack(sub.vitals.track_mix);
              const subColor = TRACK_COLORS[subTrack] || '#9ca3af';
              return (
                <Link
                  key={sub.cluster_id}
                  href={`/research/field-guide/${entry.cluster_id}-sub-${sub.cluster_id}`}
                  className="group flex flex-col p-4 border border-[var(--gray-200)] rounded-xl bg-white hover:border-[var(--gray-400)] hover:shadow-sm transition-all"
                >
                  <div className="flex items-center gap-2 mb-2 text-[0.55rem] font-mono uppercase tracking-[0.25em] text-[var(--gray-400)]">
                    <span
                      className="inline-block w-1.5 h-1.5 rounded-full"
                      style={{ background: subColor }}
                    />
                    <span>{TRACK_LABELS[subTrack] || subTrack}</span>
                  </div>
                  <h3
                    className="text-[0.98rem] font-semibold leading-[1.3] mb-1.5 group-hover:underline"
                    style={{
                      fontFamily: 'var(--font-crimson), Georgia, serif',
                    }}
                  >
                    {sub.name}
                  </h3>
                  <p
                    className="text-[0.8rem] italic text-[var(--gray-500)] leading-[1.45] mb-2 flex-1"
                    style={{
                      fontFamily: 'var(--font-crimson), Georgia, serif',
                    }}
                  >
                    {sub.one_line}
                  </p>
                  <p className="text-[0.6rem] font-mono text-[var(--gray-400)]">
                    {sub.vitals.n} papers · median {sub.vitals.median_year}
                  </p>
                </Link>
              );
            })}
          </div>
        </section>
      )}

      {/* Neighbors */}
      {entry.neighbors.length > 0 && (
        <section className="mb-14">
          <p className="text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)] mb-5">
            Nearby neighborhoods
          </p>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {entry.neighbors.map(n => (
              <Link
                key={n.cluster_id}
                href={`/research/field-guide/${n.cluster_id}`}
                className="group flex flex-col p-4 border border-[var(--gray-200)] rounded-xl bg-white hover:border-[var(--gray-400)] transition-all"
              >
                <p className="text-[0.58rem] font-mono uppercase tracking-[0.25em] text-[var(--gray-400)] mb-2">
                  similarity {n.similarity.toFixed(2)}
                </p>
                <h4
                  className="text-[0.98rem] font-semibold leading-[1.3] group-hover:underline"
                  style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
                >
                  {n.name}
                </h4>
              </Link>
            ))}
          </div>
        </section>
      )}

      {/* Sign-off */}
      <section className="max-w-[720px] border-t border-[var(--gray-200)] pt-6 flex items-center justify-between text-[0.68rem] font-mono text-[var(--gray-400)]">
        <span>— TWOG Research</span>
        <span>twog.bio · for Graffiti</span>
      </section>
    </main>
  );
}
