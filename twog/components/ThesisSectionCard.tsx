'use client';

import { useRef, useEffect } from 'react';
import { gsap } from 'gsap';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import PullQuote from '@/components/PullQuote';
import type { ThesisSection } from '@/lib/parseThesis';

interface ThesisSectionCardProps {
  section: ThesisSection;
  expanded: boolean;
  onToggle: () => void;
}

/** Section title labels for editorial flair */
const SECTION_LABELS: Record<string, string> = {
  'week-in-review': 'The Week',
  'lead-compound-deep-dive': 'Lead Compound',
  'target-biology-update': 'Target Biology',
  'combination-strategy': 'Combinations',
  'gaps-and-opportunities': 'Gaps',
  'gaps-opportunities': 'Gaps',
  'next-weeks-priorities': 'Priorities',
  'confidence-assessment': 'Confidence',
};

export default function ThesisSectionCard({ section, expanded, onToggle }: ThesisSectionCardProps) {
  const contentRef = useRef<HTMLDivElement>(null);
  const label = SECTION_LABELS[section.slug] || section.title;

  useEffect(() => {
    if (!contentRef.current) return;
    if (expanded) {
      gsap.fromTo(
        contentRef.current,
        { height: 0, opacity: 0 },
        { height: 'auto', opacity: 1, duration: 0.5, ease: 'power2.out' }
      );
    } else {
      gsap.to(contentRef.current, {
        height: 0, opacity: 0, duration: 0.3, ease: 'power2.in',
      });
    }
  }, [expanded]);

  return (
    <div className="border border-[var(--gray-200)] p-5 md:p-6 transition-shadow hover:shadow-md">
      {/* Header — always visible */}
      <div className="flex items-start gap-3 mb-3">
        <span className="text-[0.55rem] mono font-bold text-[var(--gray-300)] leading-none pt-1">
          {String(section.number).padStart(2, '0')}
        </span>
        <div className="flex-1">
          <h3 className="text-[0.7rem] uppercase tracking-[0.2em] font-bold mb-2">
            {label}
          </h3>
          <p className="editorial-body text-[0.85rem] leading-[1.7] text-[var(--gray-500)]">
            {section.summary}
          </p>
        </div>
      </div>

      {/* Pull quote preview */}
      {!expanded && section.pullQuote && (
        <PullQuote text={section.pullQuote} />
      )}

      {/* Toggle */}
      <button
        onClick={onToggle}
        className="text-[0.6rem] uppercase tracking-[0.15em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors mt-3"
      >
        {expanded ? 'Collapse' : 'Continue reading'} &rarr;
      </button>

      {/* Expanded content */}
      <div ref={contentRef} className="overflow-hidden" style={{ height: 0, opacity: 0 }}>
        <div className="pt-6 border-t border-[var(--gray-100)] mt-4">
          <div className="editorial-body digest-article">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>
              {section.rawMarkdown}
            </ReactMarkdown>
          </div>
        </div>
      </div>
    </div>
  );
}
