'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import Image from 'next/image';
import { useSupabase } from '@/hooks/useSupabase';

type HubStats = {
  papers: number | null;
  embedded: number | null;
  clusters: number;
  tracks: number;
  deepDives: number;
};

const TRACK_COLORS: { [k: string]: string } = {
  treatment: '#ef4444',
  early_detection: '#3b82f6',
  supplements: '#10b981',
  breed_screening: '#f59e0b',
  splenic_hsa: '#06b6d4',
  cross_cutting: '#8b5cf6',
  untagged: '#9ca3af',
};

const FIELD_GUIDE_TEASERS = [
  { name: 'Cutaneous angiosarcoma treatment outcomes', n: 1438, track: 'treatment' },
  { name: 'TME immune checkpoint resistance mechanisms', n: 683, track: 'treatment' },
  { name: 'Tea polyphenols cancer prevention', n: 295, track: 'supplements' },
];

function useHubStats(): HubStats {
  const sb = useSupabase();
  const [stats, setStats] = useState<HubStats>({
    papers: null,
    embedded: null,
    clusters: 50,
    tracks: 6,
    deepDives: 25,
  });

  useEffect(() => {
    if (!sb) return;
    (async () => {
      const [total, embedded] = await Promise.all([
        sb.from('papers').select('id', { count: 'exact', head: true }),
        sb.from('papers').select('id', { count: 'exact', head: true }).not('embedding', 'is', null),
      ]);
      setStats(s => ({
        ...s,
        papers: total.count ?? null,
        embedded: embedded.count ?? null,
      }));
    })();
  }, [sb]);

  return stats;
}

function StatCell({ value, label }: { value: string | number | null; label: string }) {
  return (
    <div className="flex flex-col items-start gap-1">
      <span
        className="leading-none font-bold"
        style={{ fontFamily: 'var(--font-crimson), Georgia, serif', fontSize: 'clamp(1.6rem, 3vw, 2.2rem)' }}
      >
        {value === null ? '—' : typeof value === 'number' ? value.toLocaleString() : value}
      </span>
      <span className="text-[0.6rem] font-mono uppercase tracking-[0.18em] text-[var(--gray-400)]">
        {label}
      </span>
    </div>
  );
}

export default function ResearchPage() {
  const stats = useHubStats();

  return (
    <main className="min-h-screen pt-24 pb-16 px-4 sm:px-6 lg:px-10 max-w-[1280px] mx-auto">
      {/* Masthead */}
      <header className="mb-8 max-w-[860px]">
        <div className="flex items-center gap-3 mb-4 text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)]">
          <span>Research</span>
          <span className="text-[var(--gray-300)]">·</span>
          <span>TWOG Corpus</span>
          <span className="text-[var(--gray-300)]">·</span>
          <span>2026-04-21</span>
        </div>
        <h1
          className="font-bold leading-[0.95] tracking-tight mb-4"
          style={{ fontFamily: 'var(--font-crimson), Georgia, serif', fontSize: 'clamp(2.2rem, 5vw, 3.4rem)' }}
        >
          The research, mapped.
        </h1>
        <p
          className="italic text-[1.05rem] leading-[1.55] text-[var(--gray-500)] max-w-[720px]"
          style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
        >
          Everything TWOG&apos;s pipeline has read about canine hemangiosarcoma — sixteen thousand papers,
          fifty topic neighborhoods, six research tracks.
        </p>
      </header>

      {/* Stats strip */}
      <section className="mb-10 border-y border-[var(--gray-200)] py-5">
        <div className="grid grid-cols-2 md:grid-cols-5 gap-6">
          <StatCell value={stats.papers} label="papers" />
          <StatCell value={stats.embedded} label="embedded" />
          <StatCell value={stats.clusters} label="clusters" />
          <StatCell value={stats.tracks} label="tracks" />
          <StatCell value={stats.deepDives} label="deep-dives" />
        </div>
      </section>

      {/* Feature cards */}
      <section className="grid md:grid-cols-2 gap-5 mb-10">
        {/* Topic Map */}
        <Link
          href="/research/topic-map"
          className="group block border border-[var(--gray-200)] rounded-2xl overflow-hidden bg-white hover:shadow-lg hover:-translate-y-[1px] transition-all"
        >
          <div className="relative w-full h-[280px] bg-[#fdfcfa] overflow-hidden">
            <Image
              src="/images/topic_map_thumb.png"
              alt="Topic map preview"
              fill
              sizes="(max-width: 768px) 100vw, 600px"
              className="object-cover object-center group-hover:scale-[1.02] transition-transform duration-500"
              priority
            />
          </div>
          <div className="p-6">
            <p className="text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)] mb-2">
              Topic map
            </p>
            <h2
              className="text-[1.4rem] font-bold leading-tight mb-2"
              style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
            >
              Every paper, one dot.
            </h2>
            <p className="text-[0.85rem] text-[var(--gray-500)] leading-relaxed">
              A UMAP projection of every embedded paper in the corpus. HDBSCAN finds the dense
              neighborhoods; color tells you the research track. Hover to read, click to jump to
              PubMed.
            </p>
          </div>
        </Link>

        {/* Field Guide */}
        <Link
          href="/research/field-guide"
          className="group block border border-[var(--gray-200)] rounded-2xl overflow-hidden bg-white hover:shadow-lg hover:-translate-y-[1px] transition-all"
        >
          <div className="relative w-full h-[280px] bg-[#fdfcfa] overflow-hidden p-5 flex flex-col justify-center gap-2.5">
            {FIELD_GUIDE_TEASERS.map((t, i) => (
              <div
                key={t.name}
                className="border border-[var(--gray-200)] rounded-xl bg-white px-4 py-3 shadow-sm"
                style={{
                  marginLeft: `${i * 8}px`,
                  marginRight: `${(FIELD_GUIDE_TEASERS.length - 1 - i) * 8}px`,
                }}
              >
                <div className="flex items-center gap-2">
                  <span
                    className="inline-block w-2 h-2 rounded-full shrink-0"
                    style={{ background: TRACK_COLORS[t.track] }}
                  />
                  <p className="text-[0.82rem] font-semibold leading-tight truncate">{t.name}</p>
                </div>
                <p className="text-[0.62rem] font-mono text-[var(--gray-400)] mt-1 pl-4">
                  {t.n.toLocaleString()} papers
                </p>
              </div>
            ))}
          </div>
          <div className="p-6">
            <p className="text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)] mb-2">
              Field guide
            </p>
            <h2
              className="text-[1.4rem] font-bold leading-tight mb-2"
              style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
            >
              Fifty neighborhoods, one page each.
            </h2>
            <p className="text-[0.85rem] text-[var(--gray-500)] leading-relaxed">
              A research-neighborhood-at-a-time guide to the corpus. Each page shows the vitals,
              the top papers, a synthesis paragraph with citations, and what&apos;s missing.
            </p>
          </div>
        </Link>
      </section>

    </main>
  );
}
