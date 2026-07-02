import Link from 'next/link';
import { notFound } from 'next/navigation';
import { connection } from 'next/server';
import '../../v4/v4.css';
import '../../v2/v2.css';
import '../../detail.css';
import { ProofStamp } from '@/components/v2/ProofStamp';
import { getCandidate } from '@/lib/public-candidates-live';
import { getCapsulesForCandidate } from '@/lib/public-capsules';
import { findRunForCandidate } from '@/lib/public-runs';
import { verdictFromCapsules, VERDICT_META, STAMP_FOR, prettyCandidate } from '@/lib/verdict';

// Lean, live candidate record (an "idea": drug × target) read from Neon public_candidates. Shares the
// candidate_id namespace with evidence + runs, so the cross-links below resolve directly. Narrative
// sections render only when the payload actually carries them.

export async function generateMetadata({ params }: { params: Promise<{ candidateId: string }> }) {
  const { candidateId } = await params;
  const c = await getCandidate(candidateId);
  return {
    title: c ? `${prettyCandidate(c.candidate_id)} — TWOG` : 'TWOG Candidate',
    description: c?.question ?? 'A real drug against a real target — tested in the open.',
  };
}

export default async function CandidateDetailPage({ params }: { params: Promise<{ candidateId: string }> }) {
  await connection(); // Next 16: read live per request
  const { candidateId } = await params;

  const candidate = await getCandidate(candidateId);
  if (!candidate) notFound();

  const [capsules, run] = await Promise.all([
    getCapsulesForCandidate(candidate.candidate_id),
    findRunForCandidate(candidate.candidate_id),
  ]);
  const verdict = verdictFromCapsules(capsules);
  const meta = VERDICT_META[verdict];
  const firstCapsule = capsules[0]?.capsule_id;

  return (
    <div className="v4-shell">
      <div className="v4-grain" />
      <div className="v4-detail">
        <nav className="v4-dnav">
          <Link href="/candidates" className="v4-dnav__home">Candidates</Link>
          <Link href="/evidence">Evidence</Link>
          <Link href="/runs">Runs</Link>
          <span className="v4-dnav__spacer" />
          <Link href="/involved">Get involved</Link>
        </nav>

        <div style={{ display: 'flex', gap: '0.7rem', alignItems: 'center', flexWrap: 'wrap' }}>
          <ProofStamp verdict={STAMP_FOR[verdict]} />
          <span className="v4-chip">{prettyCandidate(candidate.candidate_id)}</span>
        </div>
        <h1 className="v4-dh1">{candidate.question}</h1>

        <section className="v4-sec2">
          <div className="v4-dh2">What we’re testing</div>
          <div className="v4-prov__badges">
            {candidate.targets.map((t) => <span key={t} className="v4-chip">target · {t}</span>)}
            {candidate.biomarkers.map((b) => <span key={b} className="v4-chip">{b}</span>)}
            {!candidate.targets.length && !candidate.biomarkers.length ? <span className="v4-chip">—</span> : null}
          </div>
          {candidate.summary ? <p className="v4-lead">{candidate.summary}</p> : null}
          {candidate.mechanism ? (
            <p className="v4-note"><strong style={{ color: 'var(--bone)' }}>Mechanism.</strong> {candidate.mechanism}</p>
          ) : null}
        </section>

        <section className="v4-sec2">
          <div className="v4-dh2">Where it stands — {meta.label}</div>
          <p className="v4-lead">{meta.gloss}</p>
          <p className="v4-note">
            Status on the engine: <strong style={{ color: 'var(--bone)' }}>{(candidate.public_status ?? 'open').replace(/_/g, ' ')}</strong>
            {candidate.validation_ready ? ' · validation-ready' : ''}. Nothing here is promoted automatically —
            a human holds the final call.
          </p>
        </section>

        <section className="v4-sec2">
          <div className="v4-dh2">Follow the trail</div>
          <div className="v4-cards">
            {firstCapsule ? (
              <Link href={`/evidence/${firstCapsule}`} className="v4-card">
                <div className="v4-card__row">
                  <span className="v4-card__title">See the evidence →</span>
                  <span className="v4-badge v4-badge--ok">{capsules.length} test{capsules.length === 1 ? '' : 's'}</span>
                </div>
                <div className="v4-card__meta">the pre-registered tests, verdict, and signed provenance</div>
              </Link>
            ) : (
              <div className="v4-card" style={{ opacity: 0.6 }}>
                <div className="v4-card__row"><span className="v4-card__title">Evidence pending</span></div>
                <div className="v4-card__meta">no published capsule for this idea yet</div>
              </div>
            )}
            {run ? (
              <Link href={`/runs/${run.manifest_id}`} className="v4-card">
                <div className="v4-card__row"><span className="v4-card__title">View the run →</span></div>
                <div className="v4-card__meta">the campaign that tested this idea — {run.title}</div>
              </Link>
            ) : null}
          </div>
        </section>

        {candidate.evidence_refs.length ? (
          <section className="v4-sec2">
            <div className="v4-dh2">Grounded in</div>
            <div className="v4-prov__badges">
              {candidate.evidence_refs.map((r) => <span key={r} className="v4-chip">{r.replace(/^curate:/, '')}</span>)}
            </div>
          </section>
        ) : null}

        <section className="v4-sec2">
          <div className="v4-dh2">How we know it’s real</div>
          <div className="v4-prov__badges">
            {candidate.content_hash ? (
              <span className="v4-chip">content-addressed · {candidate.content_hash.slice(0, 16)}…</span>
            ) : null}
            {candidate.updated_at ? <span className="v4-chip">updated {candidate.updated_at}</span> : null}
          </div>
          <p className="v4-note">
            Every idea and every result is hashed so anyone can re-derive it. Want a drug or target put to the
            test? <Link href="/involved#suggest" style={{ color: 'var(--bone)' }}>Suggest one →</Link>
          </p>
        </section>
      </div>
    </div>
  );
}
