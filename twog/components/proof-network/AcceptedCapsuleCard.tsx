import Link from 'next/link';
import { formatPacketType } from './WorkPacketCard';

export interface AcceptedCapsuleCardData {
  proof_capsule_id: string;
  work_packet_id: string | null;
  candidate_id: string;
  capsule_type: string;
  title: string;
  contributor: {
    kind: string;
    name: string | null;
    handle: string | null;
    affiliation: string | null;
  };
  analysis_summary: string;
  findings: string;
  artifact_manifest: Array<{
    label: string;
    url: string | null;
    content_hash: string;
    mime_type: string | null;
  }>;
  status: string;
  submitted_at: string;
  reviewed_at: string | null;
  status_url: string;
}

const CAPSULE_VERDICT_LABELS: Record<string, string> = {
  accepted: 'Accepted',
  routed_to_validation: 'Routed → validation',
  routed_to_compute_review: 'Routed → compute review',
  submitted: 'In intake',
  in_review: 'In review',
  needs_changes: 'Needs changes',
  rejected: 'Rejected',
  archived: 'Archived',
};

export function formatVerdict(status: string): string {
  return CAPSULE_VERDICT_LABELS[status] ?? status.replace(/_/g, ' ');
}

export function formatPublicDateShort(value: string | null): string {
  if (!value) return '—';
  try {
    return new Date(value).toLocaleDateString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  } catch {
    return value;
  }
}

export function contributorLabel(contributor: AcceptedCapsuleCardData['contributor']): string {
  const name = contributor.name ?? contributor.handle;
  const tag =
    contributor.kind === 'agent'
      ? ' · agent'
      : contributor.kind === 'team'
        ? ' · team'
        : contributor.kind === 'lab'
          ? ' · lab'
          : contributor.kind === 'company'
            ? ' · company'
            : '';
  if (name) {
    return `${name}${tag}`;
  }
  return contributor.kind;
}

interface Props {
  capsule: AcceptedCapsuleCardData;
  showCandidate?: boolean;
  candidateLabel?: string;
}

export function AcceptedCapsuleCard({ capsule, showCandidate = false, candidateLabel }: Props) {
  return (
    <article className="accepted-capsule-card">
      <header className="accepted-capsule-head">
        <span className="accepted-capsule-type">{formatPacketType(capsule.capsule_type)}</span>
        <span className="accepted-capsule-verdict">{formatVerdict(capsule.status)}</span>
      </header>
      {showCandidate ? (
        <Link
          className="accepted-capsule-candidate"
          href={`/candidates/${encodeURIComponent(capsule.candidate_id)}`}
        >
          {candidateLabel ?? capsule.candidate_id}
        </Link>
      ) : null}
      <h3>{capsule.title}</h3>
      <div className="accepted-capsule-meta">
        {capsule.contributor.handle ? (
          <Link
            className="accepted-capsule-contributor"
            href={`/contributors/${encodeURIComponent(capsule.contributor.handle)}`}
          >
            {contributorLabel(capsule.contributor)}
          </Link>
        ) : (
          <span>{contributorLabel(capsule.contributor)}</span>
        )}
        <span>·</span>
        <span>{formatPublicDateShort(capsule.reviewed_at ?? capsule.submitted_at)}</span>
      </div>
      <p className="accepted-capsule-summary">{capsule.analysis_summary}</p>
      {capsule.findings ? (
        <p className="accepted-capsule-findings">{capsule.findings}</p>
      ) : null}
      {capsule.artifact_manifest.length > 0 ? (
        <div className="accepted-capsule-artifacts">
          <span className="lab-label">Artifacts</span>
          <ul>
            {capsule.artifact_manifest.slice(0, 4).map((artifact) => (
              <li key={artifact.content_hash}>
                {artifact.url ? (
                  <a href={artifact.url} target="_blank" rel="noopener noreferrer">
                    {artifact.label}
                  </a>
                ) : (
                  artifact.label
                )}
                <code>{artifact.content_hash.slice(0, 14)}</code>
              </li>
            ))}
          </ul>
        </div>
      ) : null}
      <footer className="accepted-capsule-foot">
        <Link
          className="accepted-capsule-receipt"
          href={`/capsules/${encodeURIComponent(capsule.proof_capsule_id)}`}
        >
          Receipt →
        </Link>
      </footer>
    </article>
  );
}
