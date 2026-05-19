import type { Metadata } from 'next';
import CorpusTopicMap from '@/components/CorpusTopicMap';

export const metadata: Metadata = {
  title: 'Corpus topic map — TWOG',
  description:
    'Every embedded paper in the TWOG corpus, projected into 2D. Color is research track. Hover a dot for the paper, click to open PubMed.',
};

const COLORS: { color: string; label: string }[] = [
  { color: '#ef4444', label: 'Treatments' },
  { color: '#3b82f6', label: 'Early detection' },
  { color: '#10b981', label: 'Supplements' },
  { color: '#f59e0b', label: 'Breed risk' },
  { color: '#06b6d4', label: 'Splenic HSA' },
  { color: '#8b5cf6', label: 'General biology' },
  { color: '#9ca3af', label: 'Untagged' },
];

export default function Page() {
  return (
    <main className="min-h-screen pt-24 pb-16 px-4 sm:px-6 lg:px-10 max-w-[1400px] mx-auto">
      <header className="mb-6 max-w-[820px]">
        <p className="text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)] mb-3">
          Corpus topic map · 2026-04-20
        </p>
        <h1
          className="font-bold leading-[0.95] tracking-tight mb-3"
          style={{ fontFamily: 'var(--font-crimson), Georgia, serif', fontSize: 'clamp(1.8rem, 4vw, 2.6rem)' }}
        >
          Every embedded paper in the corpus.
        </h1>
      </header>

      <section className="mb-4">
        <div className="flex flex-wrap gap-2">
          {COLORS.map(t => (
            <div
              key={t.label}
              className="flex items-center gap-2 px-3 py-1.5 border border-[var(--gray-200)] rounded-full bg-white"
            >
              <span className="inline-block w-2.5 h-2.5 rounded-full" style={{ background: t.color }} />
              <span className="text-[0.78rem] font-semibold">{t.label}</span>
            </div>
          ))}
        </div>
      </section>

      <section>
        <CorpusTopicMap />
      </section>
    </main>
  );
}
