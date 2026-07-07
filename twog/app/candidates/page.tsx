import Link from 'next/link';
import { connection } from 'next/server';
import '../v4/v4.css';
import '../v2/v2.css';
import '../detail.css';
import { ProofStamp } from '@/components/v2/ProofStamp';
import { SiteNav } from '@/components/v4/SiteNav';
import { listCandidates } from '@/lib/public-candidates-live';
import { VERDICT_META, STAMP_FOR, prettyCandidate } from '@/lib/verdict';

export const metadata = {
  title: 'Candidates — TWOG',
  description: 'Every idea the engine is testing — each a real drug against a real target, and where it stands.',
};

export default async function CandidatesPage() {
  await connection(); // Next 16: read live per request (pg reads aren't auto-detected as dynamic)
  const candidates = await listCandidates(200);

  return (
    <div className="v4-shell">
      <div className="v4-grain" />
      <div className="v4-detail">
        <SiteNav />

        <p className="v4-kick">Candidates</p>
        <h1 className="v4-dh1">Every idea we’re putting to the test.</h1>
        <p className="v4-lead">
          Each candidate is a real drug against a real target in the cancer dogs and people share. The
          engine tries to <strong>disprove</strong> each one with a pre-registered kill-test — and keeps
          only what survives. Open any idea to see its evidence and the run that tested it.
        </p>

        {candidates.length ? (
          <div className="v4-cards">
            {candidates.map((c) => {
              const v = VERDICT_META[c.verdict];
              const stripe = v.tone === 'ok' ? 'var(--bone)' : v.tone === 'ko' ? 'var(--kill)' : 'var(--line-2)';
              return (
                <Link
                  key={c.candidate_id}
                  href={`/candidates/${c.candidate_id}`}
                  className="v4-card v4-card--stripe"
                  style={{ ['--spine' as string]: stripe } as React.CSSProperties}
                >
                  <div className="v4-card__row">
                    <span className="v4-card__title">
                      {c.title && !/^falsify/i.test(c.title) ? c.title : prettyCandidate(c.candidate_id)}
                    </span>
                    <ProofStamp verdict={STAMP_FOR[c.verdict]} />
                  </div>
                  <div className="v4-card__meta">
                    {c.targets.length ? `target ${c.targets.join(' · ')}` : 'target —'} · {v.label}
                  </div>
                </Link>
              );
            })}
          </div>
        ) : (
          <p className="v4-lead v4-muted" style={{ marginTop: '2rem' }}>
            No candidates to show yet — ideas appear here as the engine generates and tests them.
          </p>
        )}
      </div>
    </div>
  );
}
