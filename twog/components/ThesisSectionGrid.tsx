'use client';

import { useState } from 'react';
import ScrollReveal from '@/components/ScrollReveal';
import SectionDivider from '@/components/SectionDivider';
import ThesisSectionCard from '@/components/ThesisSectionCard';
import type { ThesisSection } from '@/lib/parseThesis';

interface ThesisSectionGridProps {
  sections: ThesisSection[];
}

export default function ThesisSectionGrid({ sections }: ThesisSectionGridProps) {
  const [expanded, setExpanded] = useState<Set<number>>(new Set());

  function toggle(num: number) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(num)) next.delete(num);
      else next.add(num);
      return next;
    });
  }

  function toggleAll() {
    if (expanded.size === sections.length) {
      setExpanded(new Set());
    } else {
      setExpanded(new Set(sections.map((s) => s.number)));
    }
  }

  const allExpanded = expanded.size === sections.length;

  // Split into rows: first 2 large, next 3 medium, last 2
  const row1 = sections.slice(0, 2);
  const row2 = sections.slice(2, 5);
  const row3 = sections.slice(5);

  return (
    <section className="py-12 md:py-16 px-6">
      <div className="max-w-5xl mx-auto">
        <SectionDivider />

        {/* Controls */}
        <ScrollReveal>
          <div className="flex items-center justify-between mb-8">
            <span className="text-[0.55rem] uppercase tracking-[0.25em] text-[var(--gray-400)]">
              This Week&apos;s Review
            </span>
            <button
              onClick={toggleAll}
              className="text-[0.55rem] uppercase tracking-[0.15em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors"
            >
              {allExpanded ? 'Collapse all' : 'Read full review'}
            </button>
          </div>
        </ScrollReveal>

        {/* Row 1: 2 large cards */}
        {row1.length > 0 && (
          <ScrollReveal>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
              {row1.map((s) => (
                <ThesisSectionCard
                  key={s.number}
                  section={s}
                  expanded={expanded.has(s.number)}
                  onToggle={() => toggle(s.number)}
                />
              ))}
            </div>
          </ScrollReveal>
        )}

        {/* Row 2: 3 medium cards */}
        {row2.length > 0 && (
          <ScrollReveal delay={0.1}>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
              {row2.map((s) => (
                <ThesisSectionCard
                  key={s.number}
                  section={s}
                  expanded={expanded.has(s.number)}
                  onToggle={() => toggle(s.number)}
                />
              ))}
            </div>
          </ScrollReveal>
        )}

        {/* Row 3: remaining cards */}
        {row3.length > 0 && (
          <ScrollReveal delay={0.2}>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {row3.map((s) => (
                <ThesisSectionCard
                  key={s.number}
                  section={s}
                  expanded={expanded.has(s.number)}
                  onToggle={() => toggle(s.number)}
                />
              ))}
            </div>
          </ScrollReveal>
        )}
      </div>
    </section>
  );
}
