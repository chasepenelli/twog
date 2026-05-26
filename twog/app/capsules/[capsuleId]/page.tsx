import Link from 'next/link';
import { notFound } from 'next/navigation';
import {
  getLatestPublicReview,
  getProofCapsule,
  isProofCapsuleStorageConfigured,
} from '@/lib/proof-capsules';
import { getCandidate } from '@/lib/public-candidates';
import { formatPacketType } from '@/components/proof-network/WorkPacketCard';
import {
  contributorLabel,
  formatPublicDateShort,
  formatVerdict,
} from '@/components/proof-network/AcceptedCapsuleCard';
import styles from './capsule.module.css';

export const dynamic = 'force-dynamic';
export const revalidate = 30;

interface PageProps {
  params: Promise<{ capsuleId: string }>;
}

export async function generateMetadata({ params }: PageProps) {
  const { capsuleId } = await params;
  if (!isProofCapsuleStorageConfigured()) {
    return {
      title: 'Proof capsule — TWOG Proof Network',
    };
  }
  try {
    const capsule = await getProofCapsule(capsuleId);
    if (!capsule) {
      return { title: 'Capsule not found — TWOG Proof Network' };
    }
    return {
      title: `${capsule.title} — TWOG Proof Network`,
      description: capsule.analysis_summary.slice(0, 220),
    };
  } catch {
    return { title: 'Proof capsule — TWOG Proof Network' };
  }
}

function shortContentHash(value: string | null | undefined): string {
  if (!value) return 'pending';
  return value.slice(0, 14);
}

export default async function ProofCapsulePage({ params }: PageProps) {
  const { capsuleId } = await params;

  if (!isProofCapsuleStorageConfigured()) {
    notFound();
  }

  let capsule;
  try {
    capsule = await getProofCapsule(capsuleId);
  } catch (error) {
    console.error('failed to load proof capsule', error);
    notFound();
  }
  if (!capsule) {
    notFound();
  }

  const candidate = getCandidate(capsule.candidate_id);
  const candidateDisplayId =
    candidate?.candidate.display_id ?? candidate?.candidate.candidate_id ?? capsule.candidate_id;

  // Latest operator-grade review (if any). LLM-evaluator recommendations
  // are intentionally excluded — they're advisory, not authoritative.
  const latestReview = await getLatestPublicReview(capsule.proof_capsule_id);

  const rawUrl = `/api/proof-capsules/${encodeURIComponent(capsule.proof_capsule_id)}`;
  const verdictTone =
    capsule.status === 'accepted' ||
    capsule.status === 'routed_to_validation' ||
    capsule.status === 'routed_to_compute_review'
      ? styles.verdictAccept
      : capsule.status === 'rejected'
        ? styles.verdictReject
        : styles.verdictPending;

  return (
    <div className={`site-shell page-shell ${styles.shell}`}>
      <section className="network-hero">
        <div className="network-hero-copy">
          <p className="section-kicker">
            TWOG / Proof Network / Capsule / {formatPacketType(capsule.capsule_type)}
          </p>
          <h1>{capsule.title}</h1>
          <div className={styles.heroChips}>
            <span className="accepted-capsule-type">{formatPacketType(capsule.capsule_type)}</span>
            {capsule.contributor.handle ? (
              <Link
                href={`/contributors/${encodeURIComponent(capsule.contributor.handle)}`}
                className={styles.contributorLink}
              >
                {contributorLabel(capsule.contributor)}
              </Link>
            ) : (
              <span className={styles.contributorPlain}>
                {contributorLabel(capsule.contributor)}
              </span>
            )}
            <span className={`${styles.verdictChip} ${verdictTone}`}>
              {formatVerdict(capsule.status)}
            </span>
            <code className={styles.inlineCodeStrong}>
              {shortContentHash(capsule.content_hash)}
            </code>
          </div>
        </div>
      </section>

      <section className={styles.timelinePanel}>
        <div className={styles.sectionHeading}>
          <p className="section-kicker">Timeline</p>
          <h2>What happened to this capsule.</h2>
        </div>
        <ol className={styles.timeline}>
          <li>
            <span className={styles.timelineLabel}>Submitted</span>
            <span className={styles.timelineValue}>
              {formatPublicDateShort(capsule.submitted_at)}
            </span>
          </li>
          {capsule.reviewed_at ? (
            <li>
              <span className={styles.timelineLabel}>Reviewed</span>
              <span className={styles.timelineValue}>
                {formatPublicDateShort(capsule.reviewed_at)}
              </span>
            </li>
          ) : (
            <li>
              <span className={styles.timelineLabel}>Reviewed</span>
              <span className={styles.timelinePending}>pending operator pickup</span>
            </li>
          )}
          <li>
            <span className={styles.timelineLabel}>Current status</span>
            <span className={`${styles.verdictChip} ${verdictTone}`}>
              {formatVerdict(capsule.status)}
            </span>
          </li>
        </ol>
      </section>

      {latestReview ? (
        <section className={styles.timelinePanel}>
          <div className={styles.sectionHeading}>
            <p className="section-kicker">Reviewer rubric</p>
            <h2>How this capsule was scored.</h2>
            <p className={styles.reviewerHint}>
              {latestReview.reward_score !== null ? (
                <>
                  Earned <strong>{Math.round(latestReview.reward_score * 100)} proof points</strong>
                  {' '}from this review ({formatVerdict(latestReview.verdict)}).
                </>
              ) : (
                <>Reviewed but no rubric scores were recorded.</>
              )}
            </p>
          </div>
          <ul className={styles.rubricList}>
            {[
              ['scientific_usefulness', 'Scientific usefulness'],
              ['provenance_strength', 'Provenance strength'],
              ['actionability', 'Actionability'],
              ['reproducibility', 'Reproducibility'],
              ['novelty', 'Novelty'],
              ['downstream_impact', 'Downstream impact'],
              ['clarity', 'Clarity'],
            ].map(([key, label]) => {
              const value = latestReview.rubric[key as keyof typeof latestReview.rubric];
              const pct = value === null ? 0 : Math.round(value * 100);
              return (
                <li key={key} className={styles.rubricRow}>
                  <span className={styles.rubricLabel}>{label}</span>
                  <div className={styles.rubricBarOuter}>
                    <div
                      className={styles.rubricBarInner}
                      style={{ width: `${value === null ? 0 : pct}%` }}
                      aria-label={`${label} ${value === null ? 'not scored' : pct + ' percent'}`}
                    />
                  </div>
                  <span className={styles.rubricValue}>
                    {value === null ? '—' : value.toFixed(2)}
                  </span>
                </li>
              );
            })}
          </ul>
        </section>
      ) : null}

      <section className={styles.workSection}>
        <div className={styles.sectionHeading}>
          <p className="section-kicker">The work</p>
          <h2>What the contributor submitted.</h2>
        </div>
        <div className={styles.workCard}>
          <span className="lab-label">Analysis summary</span>
          <p className={styles.workProse}>{capsule.analysis_summary}</p>
        </div>
        {capsule.findings ? (
          <div className={styles.workCard}>
            <span className="lab-label">Findings</span>
            <p className={styles.workProse}>{capsule.findings}</p>
          </div>
        ) : null}
        {capsule.limitations ? (
          <div className={styles.workCard}>
            <span className="lab-label">Limitations</span>
            <p className={styles.workProse}>{capsule.limitations}</p>
          </div>
        ) : null}
        {capsule.method_refs.length > 0 ? (
          <div className={styles.workCard}>
            <span className="lab-label">Methods referenced</span>
            <ul className={styles.chipList}>
              {capsule.method_refs.map((ref) => (
                <li key={ref}>
                  <span className={styles.methodChip}>{ref}</span>
                </li>
              ))}
            </ul>
          </div>
        ) : null}
      </section>

      <section className={styles.manifestSection}>
        <div className={styles.sectionHeading}>
          <p className="section-kicker">Artifact manifest</p>
          <h2>What got attached.</h2>
        </div>
        {capsule.artifact_manifest.length === 0 ? (
          <p className={styles.emptyNote}>No artifacts attached to this capsule.</p>
        ) : (
          <ul className={styles.manifestList}>
            {capsule.artifact_manifest.map((artifact) => (
              <li className={styles.manifestRow} key={`${artifact.label}::${artifact.content_hash}`}>
                <div className={styles.manifestMain}>
                  {artifact.url ? (
                    <a
                      href={artifact.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className={styles.manifestLink}
                    >
                      {artifact.label}
                    </a>
                  ) : (
                    <span className={styles.manifestLabel}>{artifact.label}</span>
                  )}
                  {artifact.method_or_tool ? (
                    <span className={styles.manifestMeta}>{artifact.method_or_tool}</span>
                  ) : null}
                </div>
                <code className={styles.inlineCode}>{shortContentHash(artifact.content_hash)}</code>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className={styles.verdictSection}>
        <div className={styles.sectionHeading}>
          <p className="section-kicker">Verdict / reward</p>
          <h2>What review saw.</h2>
        </div>
        <div className={styles.verdictRow}>
          <span className={`${styles.verdictChip} ${verdictTone}`}>
            {formatVerdict(capsule.status)}
          </span>
          <span className={styles.reviewCount}>
            {capsule.review_count === 1
              ? '1 review event'
              : `${capsule.review_count} review events`}
          </span>
          <Link href={rawUrl} className={styles.receiptLink}>
            Raw JSON status receipt →
          </Link>
        </div>
      </section>

      {capsule.quality_flags.length > 0 ? (
        <section className={styles.qualitySection}>
          <div className={styles.sectionHeading}>
            <p className="section-kicker">Quality flags</p>
            <h2>What the auto-screener noticed.</h2>
            <p className={styles.sectionLead}>
              Hints from the public fields — not rejections. Operators use these
              to prioritize the review queue; contributors can self-correct
              before resubmitting.
            </p>
          </div>
          <ul className={styles.qualityList}>
            {capsule.quality_flags.map((flag) => (
              <li key={flag}>
                <span className={styles.qualityChip}>{flag.replace(/_/g, ' ')}</span>
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      <section className={styles.provenanceSection}>
        <div className={styles.sectionHeading}>
          <p className="section-kicker">Provenance</p>
          <h2>What was signed.</h2>
        </div>
        <dl className={styles.provenanceDl}>
          <div>
            <dt>Content hash</dt>
            <dd>
              <code className={styles.inlineCodeBlock}>{capsule.content_hash}</code>
            </dd>
          </div>
          {capsule.candidate_snapshot_hash ? (
            <div>
              <dt>Candidate snapshot hash</dt>
              <dd>
                <code className={styles.inlineCodeBlock}>{capsule.candidate_snapshot_hash}</code>
              </dd>
            </div>
          ) : null}
          {capsule.evidence_bundle_hash ? (
            <div>
              <dt>Evidence bundle hash</dt>
              <dd>
                <code className={styles.inlineCodeBlock}>{capsule.evidence_bundle_hash}</code>
              </dd>
            </div>
          ) : null}
          {capsule.notebook_ref ? (
            <div>
              <dt>Notebook ref</dt>
              <dd>{capsule.notebook_ref}</dd>
            </div>
          ) : null}
        </dl>
      </section>

      <section className="network-hero">
        <div className="network-hero-copy">
          <p className="section-kicker">Where to next</p>
          <h2>See the candidate, the contributor, or the raw receipt.</h2>
          <div className="network-hero-actions">
            <Link
              href={`/candidates/${encodeURIComponent(capsule.candidate_id)}`}
              className="network-cta primary"
            >
              View candidate ({candidateDisplayId})
            </Link>
            {capsule.contributor.handle ? (
              <Link
                href={`/contributors/${encodeURIComponent(capsule.contributor.handle)}`}
                className="network-cta"
              >
                View contributor
              </Link>
            ) : null}
            <Link href={rawUrl} className="network-cta">
              Receipt JSON
            </Link>
          </div>
        </div>
      </section>
    </div>
  );
}
