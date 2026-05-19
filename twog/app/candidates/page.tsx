import Link from 'next/link';
import { formatPublicDate, publicCandidates, readableKind, shortHash } from '@/lib/public-candidates';

export const metadata = {
  title: 'Candidates — TWOG',
  description: 'Inspectable TWOG public candidate records.',
};

export default function CandidatesPage() {
  return (
    <div className="site-shell page-shell">
      <section className="page-hero">
        <p className="section-kicker">Public proof records</p>
        <h1>Candidates</h1>
        <p>
          Each candidate page is a citeable research artifact: rationale, source audit,
          decision history, known gaps, and the method version used to publish it.
        </p>
      </section>

      <section className="record-list">
        {publicCandidates.length === 0 ? (
          <article className="proof-card">
            <span>No public candidates exported yet</span>
            <p>Run the candidate sync script after the Command Center is serving public candidate records.</p>
          </article>
        ) : (
          publicCandidates.map(({ candidate, latest_snapshot }) => (
            <Link
              href={`/candidates/${candidate.display_id?.toLowerCase() ?? candidate.candidate_id}`}
              className="candidate-row"
              key={candidate.candidate_id}
            >
              <div>
                <p className="row-kicker">{candidate.display_id ?? candidate.candidate_id}</p>
                <h2>{candidate.title}</h2>
                <p>{candidate.summary}</p>
              </div>
              <div className="row-meta">
                <span>{readableKind(candidate.public_status)}</span>
                <span>{candidate.targets?.join(' / ')}</span>
                <span>hash {shortHash(candidate.content_hash ?? latest_snapshot?.content_hash)}</span>
                <span>{formatPublicDate(candidate.updated_at)}</span>
              </div>
            </Link>
          ))
        )}
      </section>
    </div>
  );
}
