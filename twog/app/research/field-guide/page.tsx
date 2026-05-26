import type { Metadata } from 'next';
import FieldGuideIndex from '@/components/FieldGuideIndex';

export const metadata: Metadata = {
  title: 'The Field Guide — TWOG',
  description:
    'Fifty research neighborhoods, one page each. Every cluster on the TWOG topic map gets its own page — a 120-word synthesis, the three papers to read first, and the gap that still needs filling.',
};

export default function FieldGuidePage() {
  return (
    <main className="min-h-screen pt-24 pb-24 px-4 sm:px-6 lg:px-10 max-w-[1200px] mx-auto">
      {/* ── MASTHEAD ── */}
      <header className="mb-12 max-w-[760px]">
        <div className="flex items-center gap-3 mb-6 text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)]">
          <span>The Field Guide</span>
          <span className="text-[var(--gray-300)]">·</span>
          <span>Corpus Review</span>
          <span className="text-[var(--gray-300)]">·</span>
          <span>April 2026</span>
        </div>
        <h1
          className="font-bold leading-[0.95] tracking-tight mb-5"
          style={{
            fontFamily: 'var(--font-crimson), Georgia, serif',
            fontSize: 'clamp(2.2rem, 5vw, 3.6rem)',
          }}
        >
          Fifty research neighborhoods, one page each.
        </h1>
        <p
          className="text-[1.15rem] leading-[1.55] text-[var(--gray-500)] italic"
          style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
        >
          Every research cluster on the topic map gets its own page. Start
          here, or drop in on any neighborhood that interests you.
        </p>
      </header>

      {/* ── LEAD ── */}
      <section
        className="mb-12 max-w-[720px] space-y-5"
        style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
      >
        <p className="text-[1.02rem] leading-[1.75] text-[var(--gray-500)]">
          Each neighborhood below is a cluster of papers that the pipeline
          found sitting near each other — papers about the same thing, even
          when nobody used the same words for it. We wrote a short synthesis
          for every one, picked three papers to read first, and named the
          gap we still can&apos;t see into.
        </p>
      </section>

      {/* ── GRID ── */}
      <section className="mb-16">
        <FieldGuideIndex />
      </section>

      {/* ── SIGN-OFF ── */}
      <section className="max-w-[720px] border-t border-[var(--gray-200)] pt-6 flex items-center justify-between text-[0.68rem] font-mono text-[var(--gray-400)]">
        <span>— TWOG Research</span>
        <span>twog.bio · for Graffiti</span>
      </section>
    </main>
  );
}
