import Link from 'next/link';
import { notFound } from 'next/navigation';
import {
  getContributorProfileByHandle,
  isContributorProfileStorageConfigured,
  tierLabel,
} from '@/lib/contributor-profiles';
import { publicCandidates } from '@/lib/public-candidates';
import {
  AcceptedCapsuleCardData,
  formatPublicDateShort,
  formatVerdict,
} from '@/components/proof-network/AcceptedCapsuleCard';
import { formatPacketType } from '@/components/proof-network/WorkPacketCard';

export const dynamic = 'force-dynamic';
export const revalidate = 60;

interface PageProps {
  params: Promise<{ handle: string }>;
}

export async function generateMetadata({ params }: PageProps) {
  const { handle } = await params;
  const decoded = decodeURIComponent(handle ?? '');
  return {
    title: `${decoded} — Proof Portfolio · TWOG`,
    description: `Accepted proof capsules and proof points for ${decoded} on the TWOG Proof Network.`,
  };
}

function candidateLabelMap(): Record<string, string> {
  const map: Record<string, string> = {};
  publicCandidates.forEach((detail) => {
    map[detail.candidate.candidate_id] = detail.candidate.display_id ?? detail.candidate.candidate_id;
  });
  return map;
}

export default async function ContributorProfilePage({ params }: PageProps) {
  const { handle } = await params;
  const decoded = decodeURIComponent(handle ?? '').trim();
  if (!decoded) notFound();

  const labels = candidateLabelMap();

  if (!isContributorProfileStorageConfigured()) {
    return (
      <div className="site-shell page-shell contributor-profile-shell">
        <section className="contributor-hero">
          <p className="section-kicker">Contributor / {decoded}</p>
          <h1>{decoded}</h1>
          <p className="contributor-storage-empty">
            The hosted Neon/Postgres database is not configured for this environment.
            Once it is connected, this page will derive the contributor profile from
            accepted proof capsules and reward events keyed by handle.
          </p>
        </section>
      </div>
    );
  }

  const profile = await getContributorProfileByHandle(decoded);
  const hasWork = profile.summary.accepted_capsule_count + profile.summary.routed_capsule_count > 0;
  const acceptedCardsData: AcceptedCapsuleCardData[] = profile.accepted_capsules
    .concat(profile.routed_capsules)
    .map((row) => ({
      proof_capsule_id: row.proof_capsule_id,
      work_packet_id: null,
      candidate_id: row.candidate_id,
      capsule_type: row.capsule_type,
      title: row.title,
      contributor: {
        kind: profile.kind,
        name: profile.display_name,
        handle: profile.handle,
        affiliation: profile.affiliation,
      },
      analysis_summary: '',
      findings: '',
      artifact_manifest: [],
      status: row.status,
      submitted_at: row.submitted_at,
      reviewed_at: row.reviewed_at,
      status_url: row.status_url ?? `/api/proof-capsules/${encodeURIComponent(row.proof_capsule_id)}`,
    }));

  return (
    <div className="site-shell page-shell contributor-profile-shell">
      <section className="contributor-hero">
        <div className="contributor-hero-copy">
          <p className="section-kicker">Contributor / {profile.kind}</p>
          <h1>{profile.display_name ?? profile.handle}</h1>
          <p className="contributor-hero-meta">
            {profile.handle}
            {profile.affiliation ? <> · {profile.affiliation}</> : null}
          </p>
        </div>
        <aside className="contributor-hero-stats" aria-label="Proof portfolio summary">
          <div>
            <span className="lab-label">Tier</span>
            <strong>{tierLabel(profile.tier)}</strong>
          </div>
          <div>
            <span className="lab-label">Proof points</span>
            <strong>{profile.proof_points}</strong>
          </div>
          <div>
            <span className="lab-label">Accepted</span>
            <strong>{profile.summary.accepted_capsule_count}</strong>
          </div>
          <div>
            <span className="lab-label">Routed</span>
            <strong>{profile.summary.routed_capsule_count}</strong>
          </div>
          <div>
            <span className="lab-label">Candidates</span>
            <strong>{profile.summary.candidate_count}</strong>
          </div>
          <div>
            <span className="lab-label">Reward events</span>
            <strong>{profile.summary.reward_event_count}</strong>
          </div>
        </aside>
      </section>

      {!hasWork ? (
        <section className="contributor-empty">
          <p className="section-kicker">No accepted work yet</p>
          <p>
            This handle has no accepted proof capsules on TWOG. Submit a capsule against
            a candidate record and, once it passes review, it will appear here with a
            content-hashed receipt.
          </p>
          <Link href="/network" className="network-cta primary">
            See open packets
          </Link>
        </section>
      ) : null}

      {profile.strongest_accepted_work ? (
        <section className="contributor-strongest">
          <div className="network-section-heading">
            <p className="section-kicker">Strongest accepted work</p>
            <h2>What landed hardest</h2>
            <p>
              Reviewers scored this capsule highest across the rubric dimensions.
              Strongest doesn&apos;t mean biggest — it means the work was most
              defensible.
            </p>
          </div>
          <article className="contributor-strongest-card">
            <header>
              <span className="accepted-capsule-type">
                {profile.strongest_accepted_work.capsule_type
                  ? formatPacketType(profile.strongest_accepted_work.capsule_type)
                  : 'capsule'}
              </span>
              <span className="accepted-capsule-verdict">accepted · {Math.round(profile.strongest_accepted_work.score * 100)}</span>
            </header>
            {profile.strongest_accepted_work.candidate_id ? (
              <Link
                className="accepted-capsule-candidate"
                href={`/candidates/${encodeURIComponent(profile.strongest_accepted_work.candidate_id)}`}
              >
                {labels[profile.strongest_accepted_work.candidate_id] ?? profile.strongest_accepted_work.candidate_id}
              </Link>
            ) : null}
            {profile.strongest_accepted_work.rationale ? (
              <p className="contributor-strongest-rationale">
                “{profile.strongest_accepted_work.rationale}”
              </p>
            ) : null}
            {profile.strongest_accepted_work.proof_capsule_id ? (
              <Link
                className="accepted-capsule-receipt"
                href={`/capsules/${encodeURIComponent(profile.strongest_accepted_work.proof_capsule_id)}`}
              >
                Receipt →
              </Link>
            ) : null}
          </article>
        </section>
      ) : null}

      {acceptedCardsData.length > 0 ? (
        <section className="contributor-portfolio">
          <div className="network-section-heading">
            <p className="section-kicker">Proof portfolio</p>
            <h2>All accepted and routed capsules</h2>
            <p>
              Every accepted capsule is a public receipt: a piece of work that survived
              the operator review gate and improved a TWOG candidate record.
            </p>
          </div>
          <ul className="contributor-portfolio-list">
            {acceptedCardsData.map((entry) => (
              <li className="contributor-portfolio-row" key={entry.proof_capsule_id}>
                <div className="contributor-portfolio-row-meta">
                  <span className="accepted-capsule-type">{formatPacketType(entry.capsule_type)}</span>
                  <span className="accepted-capsule-verdict">{formatVerdict(entry.status)}</span>
                  <span className="contributor-portfolio-row-date">
                    {formatPublicDateShort(entry.reviewed_at ?? entry.submitted_at)}
                  </span>
                </div>
                <div className="contributor-portfolio-row-body">
                  <Link
                    className="contributor-portfolio-row-title"
                    href={`/capsules/${encodeURIComponent(entry.proof_capsule_id)}`}
                  >
                    {entry.title}
                  </Link>
                  <Link
                    className="accepted-capsule-candidate"
                    href={`/candidates/${encodeURIComponent(entry.candidate_id)}`}
                  >
                    {labels[entry.candidate_id] ?? entry.candidate_id}
                  </Link>
                </div>
              </li>
            ))}
          </ul>
        </section>
      ) : null}
    </div>
  );
}
