'use client';

import { useState } from 'react';
import type { Hypothesis } from '@/hooks/useResearchData';

interface HypothesisCardProps {
  hypothesis: Hypothesis;
}

const riskColors: Record<string, string> = {
  high: 'text-red-500',
  medium: 'text-amber-500',
  low: 'text-green-500',
};

export default function HypothesisCard({ hypothesis }: HypothesisCardProps) {
  const [expanded, setExpanded] = useState(false);
  const risk = hypothesis.risk_level?.toLowerCase() ?? 'medium';

  return (
    <div className="border border-[var(--gray-200)] p-6 transition-all duration-200 hover:border-[var(--gray-400)]">
      {/* Top row */}
      <div className="flex items-center justify-between mb-4">
        <span className={`text-[0.5rem] uppercase tracking-[0.15em] font-bold ${riskColors[risk] ?? 'text-[var(--gray-400)]'}`}>
          {risk} risk
        </span>
        <span className="text-[0.5rem] uppercase tracking-[0.1em] text-[var(--gray-400)]">
          Novelty <span className="mono font-bold text-[var(--foreground)]">{hypothesis.novelty_score}/10</span>
        </span>
      </div>

      {/* Hypothesis text */}
      <p className="text-[0.8rem] font-bold leading-[1.5] mb-4 text-[var(--foreground)] normal-case tracking-normal">
        {hypothesis.hypothesis_text}
      </p>

      {/* Stats */}
      <div className="flex gap-6 mb-4">
        <span className="text-[0.5rem] uppercase tracking-[0.1em] text-[var(--gray-400)]">
          Confidence <span className="mono font-bold text-[var(--foreground)]">{hypothesis.confidence}</span>
        </span>
        <span className="text-[0.5rem] uppercase tracking-[0.1em] text-[var(--gray-400)]">
          Feasibility <span className="mono font-bold text-[var(--foreground)]">{hypothesis.feasibility_score}/10</span>
        </span>
      </div>

      {/* Pathway tags */}
      {hypothesis.pathways_involved && hypothesis.pathways_involved.length > 0 && (
        <div className="flex flex-wrap gap-1.5 mb-4">
          {hypothesis.pathways_involved.map((p) => (
            <span key={p} className="text-[0.45rem] uppercase tracking-[0.08em] px-2 py-0.5 border border-[var(--gray-200)] text-[var(--gray-400)]">
              {p}
            </span>
          ))}
        </div>
      )}

      {/* Expand toggle */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="text-[0.55rem] uppercase tracking-[0.1em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors"
      >
        {expanded ? 'Collapse' : 'Read rationale'} &rarr;
      </button>

      {/* Expanded content */}
      {expanded && (
        <div className="mt-4 pt-4 border-t border-[var(--gray-100)]">
          <p className="text-[0.75rem] text-[var(--gray-500)] leading-relaxed mb-4">
            {hypothesis.rationale}
          </p>
          {hypothesis.actionable_steps && (
            <>
              <span className="block text-[0.5rem] uppercase tracking-[0.15em] text-[var(--gray-400)] mb-2">
                Actionable Steps
              </span>
              <p className="text-[0.7rem] text-[var(--gray-500)] leading-relaxed">
                {hypothesis.actionable_steps}
              </p>
            </>
          )}
        </div>
      )}
    </div>
  );
}
