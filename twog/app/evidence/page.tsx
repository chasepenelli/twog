import Link from 'next/link';
import { connection } from 'next/server';
import '../v4/v4.css';
import '../v2/v2.css';
import '../detail.css';
import { ProofStamp } from '@/components/v2/ProofStamp';
import { listCapsules } from '@/lib/public-capsules';
import { verdictFromCapsules, VERDICT_META, STAMP_FOR, prettyCandidate } from '@/lib/verdict';
import type { Verdict } from '@/lib/types/public-detail';

export const metadata = {
  title: 'Evidence — TWOG',
  description: 'Every idea the engine has tested, and how it’s holding up.',
};

const ORDER: Record<Verdict, number> = { 'still-standing': 0, 'needs-more': 1, 'ruled-out': 2 };

export default async function EvidencePage() {
  await connection(); // Next 16: read live per request (pg reads aren't auto-detected as dynamic)
  const caps = await listCapsules(200);

  // group by candidate — each card is one IDEA, not one test receipt
  const byCand = new Map<string, typeof caps>();
  for (const c of caps) {
    const arr = byCand.get(c.candidate_id) ?? [];
    arr.push(c);
    byCand.set(c.candidate_id, arr);
  }
  const cards = [...byCand.entries()]
    .map(([cid, list]) => ({ cid, list, verdict: verdictFromCapsules(list), rep: list[0] }))
    .sort((a, b) => ORDER[a.verdict] - ORDER[b.verdict]);

  return (
    <div className="v4-shell">
      <div className="v4-grain" />
      <div className="v4-detail">
        <nav className="v4-dnav">
          <Link href="/" className="v4-dnav__home">twog</Link>
          <Link href="/runs">Runs</Link>
          <span className="v4-dnav__spacer" />
          <Link href="/involved">Get involved</Link>
        </nav>

        <p className="v4-kick">Evidence</p>
        <h1 className="v4-dh1">Every idea, and how it’s holding up.</h1>
        <p className="v4-lead">
          Each is a real drug against a real target in the cancer dogs and people share. The engine tests
          each one to try to prove it wrong — and keeps only what survives. An idea <strong>ruled out</strong>{' '}
          is a result, not a failure.
        </p>

        {cards.length ? (
          <div className="v4-cards">
            {cards.map(({ cid, list, verdict, rep }) => {
              const v = VERDICT_META[verdict];
              const lanes = [...new Set(list.map((c) => c.validation_type).filter(Boolean))];
              const stripe = v.tone === 'ok' ? 'var(--bone)' : v.tone === 'ko' ? 'var(--kill)' : 'var(--line-2)';
              return (
                <Link
                  key={cid}
                  href={`/evidence/${rep.capsule_id}`}
                  className="v4-card v4-card--stripe"
                  style={{ ['--spine' as string]: stripe } as React.CSSProperties}
                >
                  <div className="v4-card__row">
                    <span className="v4-card__title">{prettyCandidate(cid)}</span>
                    <ProofStamp verdict={STAMP_FOR[verdict]} />
                  </div>
                  <div className="v4-card__meta">tested via {lanes.join(' · ') || '—'}</div>
                </Link>
              );
            })}
          </div>
        ) : (
          <p className="v4-lead v4-muted" style={{ marginTop: '2rem' }}>
            No evidence to show yet — the engine posts results here as it runs.
          </p>
        )}
      </div>
    </div>
  );
}
