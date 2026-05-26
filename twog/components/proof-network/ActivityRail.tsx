import Link from 'next/link';
import {
  AcceptedCapsuleCardData,
  contributorLabel,
  formatPublicDateShort,
  formatVerdict,
} from './AcceptedCapsuleCard';
import { formatPacketType } from './WorkPacketCard';

interface ActivityRailEntry {
  proof_capsule_id: string;
  candidate_id: string;
  capsule_type: string;
  title: string;
  contributor: AcceptedCapsuleCardData['contributor'];
  status: string;
  submitted_at: string;
  reviewed_at: string | null;
  status_url: string;
}

interface Props {
  entries: ActivityRailEntry[];
  candidateLabels?: Record<string, string>;
}

function relativeTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  const diffMs = Date.now() - date.getTime();
  const minutes = Math.round(diffMs / 60000);
  if (minutes < 1) return 'just now';
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.round(hours / 24);
  if (days < 30) return `${days}d ago`;
  return formatPublicDateShort(value);
}

function entryTimestamp(entry: ActivityRailEntry): string {
  return entry.reviewed_at ?? entry.submitted_at;
}

export function ActivityRail({ entries, candidateLabels }: Props) {
  if (entries.length === 0) {
    return (
      <div className="activity-rail-empty">
        <span className="lab-label">Activity</span>
        <p>No capsule activity yet. Submissions will stream in here as work checks in.</p>
      </div>
    );
  }
  return (
    <ol className="activity-rail">
      {entries.map((entry) => {
        const candidateLabel = candidateLabels?.[entry.candidate_id] ?? entry.candidate_id;
        return (
          <li className={`activity-rail-row activity-status-${entry.status}`} key={entry.proof_capsule_id}>
            <div className="activity-rail-time" title={entryTimestamp(entry)}>
              {relativeTimestamp(entryTimestamp(entry))}
            </div>
            <div className="activity-rail-body">
              <div className="activity-rail-line">
                <span className="activity-rail-verdict">{formatVerdict(entry.status)}</span>
                <span className="activity-rail-type">{formatPacketType(entry.capsule_type)}</span>
              </div>
              <a className="activity-rail-title" href={entry.status_url}>
                {entry.title}
              </a>
              <div className="activity-rail-meta">
                <span>{contributorLabel(entry.contributor)}</span>
                <span>·</span>
                <Link href={`/candidates/${encodeURIComponent(entry.candidate_id)}`}>
                  {candidateLabel}
                </Link>
              </div>
            </div>
          </li>
        );
      })}
    </ol>
  );
}
