import Link from 'next/link';
import { notFound } from 'next/navigation';
import '../../v4/v4.css';
import '../../v2/v2.css';
import '../../detail.css';
import { ProofStamp } from '@/components/v2/ProofStamp';
import { SiteNav } from '@/components/v4/SiteNav';
import { LaneTrack } from '@/components/v4/LaneTrack';
import { EmailGate } from '@/components/v4/EmailGate';
import { MoonshotRubricView } from './_components/MoonshotRubricView';
import { ProvenancePanel } from './_components/ProvenancePanel';
import { getCapsule, getCapsulesForCandidate, getProvenance } from '@/lib/public-capsules';
import { findRunForCandidate } from '@/lib/public-runs';
import { verdictFromCapsules, VERDICT_META, STAMP_FOR, prettyCandidate, publicQuestion } from '@/lib/verdict';
import { getViewer } from '@/lib/gate';

/* Two-tier on ONE route:
   - PUBLIC teaser (always): the question, the test, how far it's been checked, where it stands, and how
     we know it's real.
   - DEEP tier (email-gated): the pre-registered kill-criterion + measured metrics + provenance. Gated by
     getViewer() (twog_email cookie); ?view=public previews the teaser-only state. */

export default async function CapsuleDetail({
  params,
  searchParams,
}: {
  params: Promise<{ capsuleId: string }>;
  searchParams: Promise<{ view?: string }>;
}) {
  const { capsuleId } = await params;
  const { view } = await searchParams;

  const cap = await getCapsule(capsuleId);
  if (!cap) notFound();

  const [siblings, prov, viewer, run] = await Promise.all([
    getCapsulesForCandidate(cap.candidate_id),
    getProvenance(capsuleId),
    getViewer(view),
    findRunForCandidate(cap.candidate_id),
  ]);
  const track = siblings.length ? siblings : [cap];
  const verdict = verdictFromCapsules(track);
  const meta = VERDICT_META[verdict];
  const question = publicQuestion(cap.claim, cap.candidate_id);
  const isMember = viewer.tier === 'member';

  return (
    <div className="v4-shell">
      <div className="v4-grain" />
      <div className="v4-detail">
        <SiteNav back={{ href: '/evidence', label: 'Evidence' }} />

        <div style={{ display: 'flex', gap: '0.7rem', alignItems: 'center', flexWrap: 'wrap' }}>
          <ProofStamp verdict={STAMP_FOR[verdict]} />
          <Link href={`/candidates/${cap.candidate_id}`} className="v4-chip">{prettyCandidate(cap.candidate_id)} →</Link>
          {run ? <Link href={`/runs/${run.manifest_id}`} className="v4-chip">from this run →</Link> : null}
        </div>
        <h1 className="v4-dh1">We asked: {question}</h1>

        <section className="v4-sec2">
          <div className="v4-dh2">The test</div>
          <p className="v4-lead">{cap.method}</p>
          {cap.cross_species ? (
            <p className="v4-note">
              <strong style={{ color: 'var(--bone)' }}>Cross-species caveat.</strong> {cap.cross_species}
            </p>
          ) : null}
        </section>

        <section className="v4-sec2">
          <div className="v4-dh2">How far we’ve checked</div>
          <LaneTrack capsules={track} />
          <p className="v4-note">
            Pre-registered tests, each locked to a kill-criterion <em>before</em> it ran. Any single one
            failing rules the whole idea out.
          </p>
        </section>

        <section className="v4-sec2">
          <div className="v4-dh2">Where it stands — {meta.label}</div>
          <p className="v4-lead">{meta.gloss}</p>
          {cap.readout ? (
            <p className="v4-note"><strong style={{ color: 'var(--bone)' }}>Readout.</strong> {cap.readout}</p>
          ) : null}
          {typeof cap.confidence === 'number' ? (
            <p className="v4-note">
              Confidence so far: {Math.round(cap.confidence * 100)}% — never proof.
              {verdict === 'still-standing' && cap.confidence < 0.5
                ? ' It survived the cheap lane(s) run so far; confidence stays low until deeper lanes run.'
                : ''}
            </p>
          ) : null}
        </section>

        <section className="v4-sec2">
          <div className="v4-dh2">How we know it’s real</div>
          <div className="v4-prov__badges">
            {cap.content_hash ? (
              <span className="v4-chip">content-addressed · {cap.content_hash.replace('sha256:', '').slice(0, 16)}…</span>
            ) : null}
            {prov?.signed ? <span className="v4-badge v4-badge--ok">✓ signed</span> : <span className="v4-chip">custodial</span>}
          </div>
          <p className="v4-note">
            Every result is hashed so anyone can re-derive it. Clearing these checks is necessary, not
            sufficient — a human still holds the final call, and nothing is auto-promoted.
          </p>
        </section>

        <section className="v4-sec2">
          <div className="v4-dh2">The full experiment record</div>
          {isMember ? (
            <>
              <MoonshotRubricView capsule={cap} />
              <div style={{ height: '1.4rem' }} />
              <ProvenancePanel prov={prov} capsule={cap} />
            </>
          ) : (
            <EmailGate source={`evidence:${cap.candidate_id}`} />
          )}
        </section>
      </div>
    </div>
  );
}
