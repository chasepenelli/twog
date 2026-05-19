'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import BreedAuditReviewer from '@/components/BreedAuditReviewer';

type DecisionRecord = {
  paper_id: number;
  decision: 'accept' | 'reject' | 'skip';
  reviewed_at: string;
};

type StoredState = {
  version: 1;
  decisions: Record<string, DecisionRecord>;
};

const STORAGE_KEY = 'twog.breed_audit.decisions.v1';
const TOTAL = 44;

function readDecisions(): DecisionRecord[] {
  if (typeof window === 'undefined') return [];
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as StoredState;
    if (parsed && parsed.version === 1) return Object.values(parsed.decisions);
  } catch {
    /* swallow */
  }
  return [];
}

function useDecisionCounts() {
  const [counts, setCounts] = useState({ reviewed: 0, accepted: 0, rejected: 0, skipped: 0 });

  useEffect(() => {
    function refresh() {
      const values = readDecisions();
      setCounts({
        reviewed: values.length,
        accepted: values.filter(v => v.decision === 'accept').length,
        rejected: values.filter(v => v.decision === 'reject').length,
        skipped: values.filter(v => v.decision === 'skip').length,
      });
    }
    refresh();
    // Cross-tab + same-tab storage events (BreedAuditReviewer writes on each change).
    window.addEventListener('storage', refresh);
    const id = window.setInterval(refresh, 500);
    return () => {
      window.removeEventListener('storage', refresh);
      window.clearInterval(id);
    };
  }, []);

  return counts;
}

export default function BreedAuditPage() {
  const counts = useDecisionCounts();

  return (
    <main className="min-h-screen pt-24 pb-24 px-4 sm:px-6 lg:px-10 max-w-[960px] mx-auto">
      {/* ── MASTHEAD ── */}
      <header className="mb-10 max-w-[760px]">
        <div className="flex items-center gap-3 mb-6 text-[0.6rem] font-mono uppercase tracking-[0.3em] text-[var(--gray-400)]">
          <span>Corpus Review</span>
          <span className="text-[var(--gray-300)]">·</span>
          <span>Triage</span>
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
          Forty-four papers that might be in the wrong bucket.
        </h1>

        <p
          className="text-[1.1rem] leading-[1.6] text-[var(--gray-500)] italic mb-5"
          style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
        >
          The ground-truth audit flagged these forty-four papers as candidates for
          reassignment to the breed-screening track. They look like breed papers
          by every cheap signal we have — a breed name in the title, epidemiology
          language, and high cosine similarity to the breed-screening centroid —
          but they currently live somewhere else in the corpus. You decide which
          ones actually move.
        </p>

        <div className="flex flex-wrap items-center gap-3 text-[0.68rem] font-mono uppercase tracking-[0.22em] text-[var(--gray-400)]">
          <span>
            {counts.reviewed} of {TOTAL} reviewed
          </span>
          <span className="text-[var(--gray-300)]">·</span>
          <span style={{ color: '#15803d' }}>{counts.accepted} accepted</span>
          <span className="text-[var(--gray-300)]">·</span>
          <span style={{ color: '#b91c1c' }}>{counts.rejected} rejected</span>
          {counts.skipped > 0 && (
            <>
              <span className="text-[var(--gray-300)]">·</span>
              <span>{counts.skipped} skipped</span>
            </>
          )}
        </div>
      </header>

      {/* ── LEAD ── */}
      <section
        className="mb-10 max-w-[720px] space-y-4"
        style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
      >
        <p className="text-[1.02rem] leading-[1.75] text-[var(--gray-500)]">
          For each card below: read the title, check the signal flags, skim the
          abstract. Accept if the paper is really about which dogs get the
          disease. Reject if the breed language is incidental and the paper
          belongs in treatment, detection, or supplements. Skip the ones that
          need a closer look later.
        </p>
        <p className="text-[0.92rem] leading-[1.65] text-[var(--gray-400)]">
          Decisions save to your browser as you go. When you are done, hit
          export and send the JSON file to the pipeline.
        </p>
      </section>

      {/* ── REVIEWER ── */}
      <section className="mb-16">
        <BreedAuditReviewer />
      </section>

      {/* ── SIGN-OFF ── */}
      <section className="max-w-[720px] border-t border-[var(--gray-200)] pt-6 flex items-center justify-between text-[0.68rem] font-mono text-[var(--gray-400)]">
        <Link href="/research" className="hover:text-[var(--foreground)]">
          {'\u2190'} back to research
        </Link>
        <span>twog.bio · for Graffiti</span>
      </section>
    </main>
  );
}
