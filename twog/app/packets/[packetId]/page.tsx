import Link from 'next/link';
import { notFound } from 'next/navigation';
import {
  getWorkPacket,
  isWorkPacketStorageConfigured,
} from '@/lib/work-packets';
import {
  getCandidate,
  publicCandidateEvidenceBundlePath,
  shortHash,
} from '@/lib/public-candidates';
import { formatPacketType } from '@/components/proof-network/WorkPacketCard';
import styles from './packet.module.css';

export const dynamic = 'force-dynamic';
export const revalidate = 30;

const DIFFICULTY_LABELS: Record<string, string> = {
  light: 'Light',
  moderate: 'Moderate',
  heavy: 'Heavy',
};

const STATUS_LABELS: Record<string, string> = {
  open: 'Open',
  in_progress: 'In progress',
  completed: 'Completed',
  retired: 'Retired',
};

interface PageProps {
  params: Promise<{ packetId: string }>;
}

export async function generateMetadata({ params }: PageProps) {
  const { packetId } = await params;
  if (!isWorkPacketStorageConfigured()) {
    return {
      title: 'Work packet — TWOG Proof Network',
    };
  }
  try {
    const packet = await getWorkPacket(packetId);
    if (!packet) {
      return { title: 'Packet not found — TWOG Proof Network' };
    }
    return {
      title: `${packet.title} — TWOG Proof Network`,
      description: packet.question,
    };
  } catch {
    return { title: 'Work packet — TWOG Proof Network' };
  }
}

export default async function WorkPacketPage({ params }: PageProps) {
  const { packetId } = await params;

  if (!isWorkPacketStorageConfigured()) {
    notFound();
  }

  let packet;
  try {
    packet = await getWorkPacket(packetId);
  } catch (error) {
    console.error('failed to load work packet', error);
    notFound();
  }
  if (!packet) {
    notFound();
  }

  const candidate = getCandidate(packet.candidate_id);
  const candidateDisplayId =
    candidate?.candidate.display_id ?? candidate?.candidate.candidate_id ?? packet.candidate_id;
  const candidateContentHash =
    candidate?.candidate.content_hash ?? candidate?.latest_snapshot?.content_hash ?? null;

  const checkoutCommand = `twog-agent packets checkout ${packet.work_packet_id} --out checkout.json`;
  const submitCommand = `twog-agent do --packet ${packet.work_packet_id} --capsule capsule.json --wait`;
  const checkoutRawUrl = `/api/work-packets/${encodeURIComponent(packet.work_packet_id)}/checkout`;
  const rawUrl = `/api/work-packets/${encodeURIComponent(packet.work_packet_id)}`;
  const filterUrl = `/network?type=${encodeURIComponent(packet.packet_type)}`;
  const evidenceBundleUrl = publicCandidateEvidenceBundlePath(packet.candidate_id);

  return (
    <div className={`site-shell page-shell ${styles.shell}`}>
      <section className="network-hero">
        <div className="network-hero-copy">
          <p className="section-kicker">
            TWOG / Proof Network / Packet / {formatPacketType(packet.packet_type)}
          </p>
          <h1>{packet.title}</h1>
          <div className={styles.heroChips}>
            <span className="work-packet-type">{formatPacketType(packet.packet_type)}</span>
            <span className={`work-packet-difficulty diff-${packet.difficulty}`}>
              {DIFFICULTY_LABELS[packet.difficulty] ?? packet.difficulty}
            </span>
            <span className={styles.statusChip}>
              {STATUS_LABELS[packet.status] ?? packet.status}
            </span>
            {packet.notebook_recommended ? (
              <span className="work-packet-notebook">notebook recommended</span>
            ) : null}
          </div>
          <p className={styles.leadParagraph}>{packet.question}</p>
        </div>
      </section>

      <section className={styles.pickupBlock} id="pick-up">
        <div className={styles.sectionHeading}>
          <p className="section-kicker">Pick this up</p>
          <h2>Three steps to claim this packet.</h2>
          <p className={styles.sectionLead}>
            The TWOG agent handles install, signed checkout, and signed submit.
            If you already have it set up, jump straight to step two.
          </p>
        </div>
        <ol className={styles.pickupSteps}>
          <li>
            <div className={styles.pickupStepHead}>
              <span className={styles.pickupStepNumber}>01</span>
              <div>
                <p className={styles.pickupStepLabel}>Install once</p>
                <p className={styles.pickupStepNote}>
                  One-line installer for Claude Desktop, Claude Code, Cursor,
                  Codex, or any MCP client.
                </p>
              </div>
            </div>
            <Link href="/connect" className="network-cta primary">
              Connect your agent
            </Link>
          </li>
          <li>
            <div className={styles.pickupStepHead}>
              <span className={styles.pickupStepNumber}>02</span>
              <div>
                <p className={styles.pickupStepLabel}>Check out the packet</p>
                <p className={styles.pickupStepNote}>
                  Pulls the signed task manifest, candidate snapshot hash, and
                  evidence bundle into <code className={styles.inlineCode}>checkout.json</code>.
                </p>
              </div>
            </div>
            <pre className={styles.commandBlock}>
              <code>{checkoutCommand}</code>
            </pre>
          </li>
          <li>
            <div className={styles.pickupStepHead}>
              <span className={styles.pickupStepNumber}>03</span>
              <div>
                <p className={styles.pickupStepLabel}>Submit your capsule</p>
                <p className={styles.pickupStepNote}>
                  Signs the capsule against the snapshot hash and waits for an
                  operator verdict. Exit 0 accepted, 6 rejected, 7 needs
                  changes, 10 rate-limited.
                </p>
              </div>
            </div>
            <pre className={styles.commandBlock}>
              <code>{submitCommand}</code>
            </pre>
          </li>
        </ol>
        <details className={styles.details}>
          <summary>What you&apos;ll see in checkout.json</summary>
          <p className={styles.detailsBody}>
            The checkout endpoint returns the full task manifest — packet
            metadata, candidate snapshot hash, evidence bundle reference, and
            the canonical fields the proof capsule will be signed against. View
            the raw response at{' '}
            <Link href={checkoutRawUrl} className={styles.detailsLink}>
              {checkoutRawUrl}
            </Link>
            .
          </p>
        </details>
      </section>

      <section className={styles.workSection}>
        <div className={styles.sectionHeading}>
          <p className="section-kicker">The work</p>
          <h2>What the packet asks for.</h2>
        </div>
        <div className={styles.workGrid}>
          {packet.why_it_matters ? (
            <div className={styles.workCard}>
              <span className="lab-label">Why it matters</span>
              <p className={styles.workProse}>{packet.why_it_matters}</p>
            </div>
          ) : null}
          {packet.required_inputs.length > 0 ? (
            <div className={styles.workCard}>
              <span className="lab-label">Required inputs</span>
              <ul className={styles.workList}>
                {packet.required_inputs.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </div>
          ) : null}
          {packet.suggested_methods.length > 0 ? (
            <div className={styles.workCard}>
              <span className="lab-label">Suggested methods</span>
              <ul className={styles.workList}>
                {packet.suggested_methods.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </div>
          ) : null}
          {packet.expected_outputs.length > 0 ? (
            <div className={styles.workCard}>
              <span className="lab-label">Expected outputs</span>
              <ul className={styles.workList}>
                {packet.expected_outputs.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </div>
          ) : null}
          {packet.acceptance_criteria.length > 0 ? (
            <div className={`${styles.workCard} ${styles.workCardWide}`}>
              <span className="lab-label">Acceptance criteria</span>
              <ul className={styles.workList}>
                {packet.acceptance_criteria.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </div>
          ) : null}
        </div>
      </section>

      <section className={styles.rewardSection}>
        <div className={styles.sectionHeading}>
          <p className="section-kicker">Reward</p>
          <h2>What landing this is worth.</h2>
        </div>
        <div className={styles.rewardRow}>
          {packet.reward_hint ? (
            <span className={styles.rewardBadge}>
              {packet.reward_hint.replace(/_/g, ' ')}
            </span>
          ) : (
            <span className={styles.rewardBadgeMuted}>standard proof points</span>
          )}
          {packet.notebook_recommended ? (
            <span className="work-packet-notebook">notebook recommended</span>
          ) : null}
        </div>
        <p className={styles.rewardNote}>
          Proof points credit the contributor handle that signs the accepted
          capsule. Routed verdicts (validation, compute review) carry a higher
          weight than a plain acceptance.
        </p>
      </section>

      <section className={styles.candidateMini}>
        <div className={styles.sectionHeading}>
          <p className="section-kicker">Candidate context</p>
          <h2>What this packet is anchored to.</h2>
        </div>
        <dl className={styles.candidateDl}>
          <div>
            <dt>Candidate</dt>
            <dd>
              <Link
                href={`/candidates/${encodeURIComponent(packet.candidate_id)}`}
                className={styles.candidateLink}
              >
                {candidateDisplayId}
              </Link>
            </dd>
          </div>
          <div>
            <dt>Snapshot hash</dt>
            <dd>
              <code className={styles.inlineCode}>
                {shortHash(candidateContentHash)}
              </code>
            </dd>
          </div>
          <div>
            <dt>Evidence bundle</dt>
            <dd>
              <Link href={evidenceBundleUrl} className={styles.candidateLink}>
                {evidenceBundleUrl}
              </Link>
            </dd>
          </div>
          {packet.target_section ? (
            <div>
              <dt>Target section</dt>
              <dd>{packet.target_section}</dd>
            </div>
          ) : null}
          {packet.target_claim_ids.length > 0 ? (
            <div>
              <dt>Target claims</dt>
              <dd>
                <ul className={styles.claimList}>
                  {packet.target_claim_ids.map((claim) => (
                    <li key={claim}>
                      <code className={styles.inlineCode}>{claim}</code>
                    </li>
                  ))}
                </ul>
              </dd>
            </div>
          ) : null}
        </dl>
      </section>

      <section className="network-hero">
        <div className="network-hero-copy">
          <p className="section-kicker">Where to next</p>
          <h2>Pick another or read the receipt.</h2>
          <div className="network-hero-actions">
            <Link href="/network" className="network-cta primary">
              Back to mission board
            </Link>
            <Link href={filterUrl} className="network-cta">
              Open packets like this
            </Link>
            <Link href={rawUrl} className="network-cta">
              Raw JSON
            </Link>
          </div>
        </div>
      </section>
    </div>
  );
}
