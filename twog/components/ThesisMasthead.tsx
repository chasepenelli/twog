'use client';

import ScrollReveal from '@/components/ScrollReveal';
import SplitTextReveal from '@/components/SplitTextReveal';
import type { ParsedThesis } from '@/lib/parseThesis';

interface ThesisMastheadProps {
  thesis: ParsedThesis;
  papersTotal?: number;
  moleculesTotal?: number;
}

export default function ThesisMasthead({ thesis, papersTotal, moleculesTotal }: ThesisMastheadProps) {
  return (
    <section className="pt-32 pb-6 px-6">
      <div className="max-w-5xl mx-auto">
        {/* Masthead */}
        <div className="flex items-baseline justify-between border-b-2 border-[var(--foreground)] pb-3 mb-4">
          <SplitTextReveal
            as="h1"
            className="text-[6vw] md:text-[4vw] lg:text-[3vw] leading-[0.9] font-bold"
            stagger={0.02}
            duration={0.8}
          >
            The TWOG Review
          </SplitTextReveal>

          <ScrollReveal delay={0.5}>
            <span className="text-[0.55rem] text-[var(--gray-400)] italic hidden md:inline"
              style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}>
              For Graffiti
            </span>
          </ScrollReveal>
        </div>

        {/* Edition line */}
        <ScrollReveal delay={0.3}>
          <div className="flex flex-wrap items-center gap-x-6 gap-y-1 text-[0.5rem] mono uppercase tracking-[0.15em] text-[var(--gray-400)]">
            {thesis.dateRange && <span>{thesis.dateRange}</span>}
            {(papersTotal || thesis.stats.papers > 0) && (
              <span>{(papersTotal || thesis.stats.papers).toLocaleString()} papers</span>
            )}
            {(moleculesTotal || thesis.stats.molecules > 0) && (
              <span>{(moleculesTotal || thesis.stats.molecules).toLocaleString()} molecules</span>
            )}
            {thesis.stats.cycles > 0 && (
              <span>{thesis.stats.cycles.toLocaleString()} cycles</span>
            )}
          </div>
        </ScrollReveal>

        {/* Thin rule */}
        <div className="border-b border-[var(--gray-200)] mt-4" />
      </div>
    </section>
  );
}
