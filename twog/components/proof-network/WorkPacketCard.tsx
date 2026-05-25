import Link from 'next/link';

export interface WorkPacketCardData {
  work_packet_id: string;
  candidate_id: string;
  packet_type: string;
  title: string;
  question: string;
  why_it_matters: string;
  acceptance_criteria: string[];
  reward_hint: string;
  difficulty: string;
  notebook_recommended: boolean;
  url: string;
  checkout_url: string;
}

const PACKET_TYPE_LABELS: Record<string, string> = {
  citation_repair: 'Citation repair',
  claim_critique: 'Claim critique',
  evidence_addition: 'Evidence addition',
  omics_note: 'Omics note',
  docking_replication: 'Docking replication',
  md_review: 'MD review',
  validation_proposal: 'Validation proposal',
  demotion_case: 'Demotion case',
  methods_review: 'Methods review',
};

const DIFFICULTY_LABELS: Record<string, string> = {
  light: 'Light',
  moderate: 'Moderate',
  heavy: 'Heavy',
};

export function formatPacketType(type: string): string {
  return PACKET_TYPE_LABELS[type] ?? type.replace(/_/g, ' ');
}

interface Props {
  packet: WorkPacketCardData;
  // When true the card surfaces the candidate it belongs to (used on
  // /network where packets span multiple candidates). On the candidate
  // workbench the candidate identity is already in the page header.
  showCandidate?: boolean;
  candidateLabel?: string;
}

export function WorkPacketCard({ packet, showCandidate = false, candidateLabel }: Props) {
  return (
    <article className="work-packet-card">
      <header className="work-packet-card-head">
        <span className="work-packet-type">{formatPacketType(packet.packet_type)}</span>
        <span className={`work-packet-difficulty diff-${packet.difficulty}`}>
          {DIFFICULTY_LABELS[packet.difficulty] ?? packet.difficulty}
        </span>
      </header>
      {showCandidate ? (
        <Link
          className="work-packet-candidate"
          href={`/candidates/${encodeURIComponent(packet.candidate_id)}`}
        >
          {candidateLabel ?? packet.candidate_id}
        </Link>
      ) : null}
      <h3>{packet.title}</h3>
      <p className="work-packet-question">{packet.question}</p>
      {packet.why_it_matters ? (
        <p className="work-packet-why">{packet.why_it_matters}</p>
      ) : null}
      {packet.acceptance_criteria.length > 0 ? (
        <div className="work-packet-criteria">
          <span className="lab-label">What we look for</span>
          <ul>
            {packet.acceptance_criteria.slice(0, 3).map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </div>
      ) : null}
      <footer className="work-packet-card-foot">
        {packet.reward_hint ? (
          <span className="work-packet-reward">{packet.reward_hint.replace(/_/g, ' ')}</span>
        ) : null}
        {packet.notebook_recommended ? (
          <span className="work-packet-notebook">notebook recommended</span>
        ) : null}
        <Link
          className="work-packet-checkout"
          href={`/packets/${encodeURIComponent(packet.work_packet_id)}`}
        >
          Check out →
        </Link>
      </footer>
    </article>
  );
}
