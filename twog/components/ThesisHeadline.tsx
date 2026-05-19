'use client';

import ScrollReveal from '@/components/ScrollReveal';
import Molecule3D from '@/components/Molecule3D';
import ConfidenceMeter from '@/components/ConfidenceMeter';
import type { ParsedThesis } from '@/lib/parseThesis';
import type { TopCompound } from '@/hooks/useTopCompounds';

interface ThesisHeadlineProps {
  thesis: ParsedThesis;
  lead: TopCompound | null;
}

export default function ThesisHeadline({ thesis, lead }: ThesisHeadlineProps) {
  /* Use the first section's summary as the lede */
  const weekInReview = thesis.sections.find((s) => s.number === 1);
  const lede = weekInReview?.summary || '';

  /* Extract a punchy headline from the thesis title or first section */
  const headline = thesis.headline.replace(/^HSA AutoResearch\s*[—–-]\s*/i, '').trim();

  return (
    <section className="py-12 md:py-16 px-6">
      <div className="max-w-5xl mx-auto">
        <div className="flex flex-col lg:flex-row gap-10 lg:gap-16">
          {/* Headline + Lede */}
          <div className="flex-1">
            <ScrollReveal>
              <h2 className="text-[5vw] md:text-[3.5vw] lg:text-[2.5vw] leading-[1.05] font-bold mb-6">
                {headline}
              </h2>
            </ScrollReveal>

            <ScrollReveal delay={0.2}>
              <p className="editorial-body text-[1.1rem] leading-[1.9] text-[var(--gray-500)]">
                {lede}
              </p>
            </ScrollReveal>

            {/* Pull quote from week in review */}
            {weekInReview?.pullQuote && (
              <ScrollReveal delay={0.35}>
                <blockquote className="pull-quote mt-6">
                  {weekInReview.pullQuote}
                </blockquote>
              </ScrollReveal>
            )}
          </div>

          {/* Sidebar: Lead compound + confidence */}
          {lead && (
            <ScrollReveal delay={0.15} className="shrink-0 w-full lg:w-[240px]">
              <div className="flex flex-row lg:flex-col items-center lg:items-start gap-6">
                <Molecule3D compoundName={lead.name} size={180} />
                <div>
                  <span className="block text-[0.5rem] uppercase tracking-[0.2em] text-[var(--gray-400)] mb-1">
                    Lead Candidate
                  </span>
                  <span className="block text-[1.2rem] font-bold mono leading-none mb-1">
                    {lead.name}
                  </span>
                  <span className="block text-[0.65rem] mono text-[var(--gray-400)] mb-4">
                    {lead.target_gene} · {Number(lead.composite_score).toFixed(4)}
                  </span>

                  {thesis.confidence && (
                    <ConfidenceMeter score={thesis.confidence.score} />
                  )}
                </div>
              </div>
            </ScrollReveal>
          )}
        </div>
      </div>
    </section>
  );
}
