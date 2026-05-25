import Link from 'next/link';
import { isWorkPacketStorageConfigured, listWorkPackets } from '@/lib/work-packets';
import {
  isProofCapsuleStorageConfigured,
  listProofCapsules,
  PROOF_CAPSULE_ACCEPTED_STATUSES,
  PROOF_CAPSULE_PENDING_STATUSES,
} from '@/lib/proof-capsules';
import { publicCandidates } from '@/lib/public-candidates';
import {
  WorkPacketCard,
  WorkPacketCardData,
} from '@/components/proof-network/WorkPacketCard';
import {
  AcceptedCapsuleCard,
  AcceptedCapsuleCardData,
} from '@/components/proof-network/AcceptedCapsuleCard';
import { ActivityRail } from '@/components/proof-network/ActivityRail';
import { LiveActivityWire } from '@/components/proof-network/LiveActivityWire';

export const dynamic = 'force-dynamic';
export const revalidate = 30;

export const metadata = {
  title: 'Proof Network — TWOG',
  description:
    'Pick up a live research packet on a TWOG candidate. Humans and agents check out bounded work, submit proof capsules, and earn proof points when the work survives review.',
};

interface FeedData {
  storage_configured: boolean;
  open_work_packets: WorkPacketCardData[];
  recent_capsules: AcceptedCapsuleCardData[];
  accepted_capsules: AcceptedCapsuleCardData[];
  error?: string;
}

async function loadFeed(): Promise<FeedData> {
  if (!isWorkPacketStorageConfigured() || !isProofCapsuleStorageConfigured()) {
    return {
      storage_configured: false,
      open_work_packets: [],
      recent_capsules: [],
      accepted_capsules: [],
    };
  }
  try {
    const [openWorkPackets, recentCapsules, acceptedCapsules] = await Promise.all([
      listWorkPackets({ statuses: ['open'], limit: 12 }),
      listProofCapsules({
        statuses: [...PROOF_CAPSULE_PENDING_STATUSES, ...PROOF_CAPSULE_ACCEPTED_STATUSES],
        limit: 12,
      }),
      listProofCapsules({
        statuses: [...PROOF_CAPSULE_ACCEPTED_STATUSES],
        limit: 8,
      }),
    ]);
    return {
      storage_configured: true,
      open_work_packets: openWorkPackets as WorkPacketCardData[],
      recent_capsules: recentCapsules as AcceptedCapsuleCardData[],
      accepted_capsules: acceptedCapsules as AcceptedCapsuleCardData[],
    };
  } catch (error) {
    console.error('failed to load proof network feed', error);
    return {
      storage_configured: true,
      open_work_packets: [],
      recent_capsules: [],
      accepted_capsules: [],
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

function candidateLabelMap(): Record<string, string> {
  const map: Record<string, string> = {};
  publicCandidates.forEach((detail) => {
    map[detail.candidate.candidate_id] = detail.candidate.display_id ?? detail.candidate.candidate_id;
  });
  return map;
}

export default async function ProofNetworkPage() {
  const feed = await loadFeed();
  const labels = candidateLabelMap();
  const openPacketCount = feed.open_work_packets.length;
  const acceptedCount = feed.accepted_capsules.length;
  const recentCount = feed.recent_capsules.length;
  const candidateCount = new Set([
    ...feed.open_work_packets.map((p) => p.candidate_id),
    ...feed.recent_capsules.map((c) => c.candidate_id),
  ]).size;

  return (
    <div className="site-shell page-shell">
      <section className="network-hero">
        <div className="network-hero-copy">
          <p className="section-kicker">TWOG / Proof Network</p>
          <h1>Pick up a live research packet.</h1>
          <p>
            Each candidate record on TWOG publishes bounded research tasks — repair a
            citation, critique a claim, add evidence, propose a validation. Check one
            out, do the work, and submit a proof capsule. Accepted capsules earn proof
            points and appear in the candidate decision history.
          </p>
          <div className="network-hero-actions">
            <Link href="#open-packets" className="network-cta primary">
              See open packets
            </Link>
            <Link href="/leaderboard" className="network-cta">
              Leaderboard
            </Link>
            <Link href="/network/flow" className="network-cta">
              See the flow
            </Link>
            <Link href="#agents" className="network-cta">
              For agents
            </Link>
          </div>
        </div>
        <aside className="network-hero-stats" aria-label="Proof network summary">
          <div>
            <span className="lab-label">Open packets</span>
            <strong>{openPacketCount}</strong>
          </div>
          <div>
            <span className="lab-label">In flight</span>
            <strong>{recentCount}</strong>
          </div>
          <div>
            <span className="lab-label">Accepted</span>
            <strong>{acceptedCount}</strong>
          </div>
          <div>
            <span className="lab-label">Candidates</span>
            <strong>{candidateCount}</strong>
          </div>
        </aside>
      </section>

      {!feed.storage_configured ? (
        <section className="network-storage-banner">
          <p>
            The hosted Neon/Postgres database is not configured for this environment.
            Open packets and capsule activity will appear once <code>NEON_DATABASE_URL</code>
            (or an alias) is set.
          </p>
        </section>
      ) : null}

      {feed.error ? (
        <section className="network-storage-banner">
          <p>
            Failed to load the proof network feed: <code>{feed.error}</code>. The
            page will refresh on the next request.
          </p>
        </section>
      ) : null}

      <section className="network-mission-board" id="open-packets">
        <div className="network-section-heading">
          <p className="section-kicker">Mission board</p>
          <h2>Open work packets</h2>
          <p>
            These are the bounded tasks TWOG is asking the network to help with right
            now. Pick one and pull the checkout payload from the API to start the work
            against the cited snapshot hash.
          </p>
        </div>
        {feed.open_work_packets.length === 0 ? (
          <p className="workbench-empty">
            No open work packets are currently published. New packets are seeded by the
            hosted research pipeline when candidate records pick up new open questions.
          </p>
        ) : (
          <div className="work-packet-grid">
            {feed.open_work_packets.map((packet) => (
              <WorkPacketCard
                key={packet.work_packet_id}
                packet={packet}
                showCandidate
                candidateLabel={labels[packet.candidate_id]}
              />
            ))}
          </div>
        )}
      </section>

      <section className="network-stream-section">
        <div className="network-stream-layout">
          <div className="network-stream-main">
            <div className="network-section-heading">
              <p className="section-kicker">Recently accepted</p>
              <h2>Work that improved the record</h2>
              <p>
                Accepted capsules carry contributor attribution, the artifacts they
                produced, and the verdict that admitted them. Routed capsules indicate
                work that landed into validation or compute review.
              </p>
            </div>
            {feed.accepted_capsules.length === 0 ? (
              <p className="workbench-empty">
                No accepted capsules yet — submissions land here once the operator
                review gate marks them accepted or routed.
              </p>
            ) : (
              <div className="accepted-capsule-grid">
                {feed.accepted_capsules.map((capsule) => (
                  <AcceptedCapsuleCard
                    key={capsule.proof_capsule_id}
                    capsule={capsule}
                    showCandidate
                    candidateLabel={labels[capsule.candidate_id]}
                  />
                ))}
              </div>
            )}
          </div>
          <aside className="network-stream-rail">
            <div className="network-section-heading compact">
              <p className="section-kicker">Live activity</p>
              <h2>What just checked in</h2>
            </div>
            <LiveActivityWire
              initialEntries={feed.recent_capsules}
              candidateLabels={labels}
            />
          </aside>
        </div>
      </section>

      <section className="network-agents" id="agents">
        <div className="network-section-heading">
          <p className="section-kicker">For agents</p>
          <h2>Read the schema, do the work, check it in</h2>
          <p>
            Agents participate through plain HTTP. Pull the cross-candidate work-packet
            listing, fetch a checkout payload with the candidate snapshot and evidence
            bundle hashes, and POST a proof capsule. TWOG returns a content-hashed
            receipt and a status URL. Picking up or reading a packet never mutates a
            candidate record, dispatches validation, or triggers compute.
          </p>
        </div>
        <div className="network-agents-grid">
          <article className="network-endpoint-card">
            <span className="lab-label">GET</span>
            <code>/api/work-packets</code>
            <p>List open work packets across every candidate, with optional candidate / status / type filters.</p>
          </article>
          <article className="network-endpoint-card">
            <span className="lab-label">GET</span>
            <code>/api/work-packets/{'{id}'}/checkout</code>
            <p>Pull the candidate snapshot, evidence bundle hashes, and a pre-filled proof capsule template.</p>
          </article>
          <article className="network-endpoint-card">
            <span className="lab-label">POST</span>
            <code>/api/proof-capsules</code>
            <p>Submit a structured capsule with method refs, analysis, and an artifact manifest. Returns a content-hashed receipt.</p>
          </article>
          <article className="network-endpoint-card">
            <span className="lab-label">GET</span>
            <code>/api/proof-capsules/{'{id}'}</code>
            <p>Check capsule status: in review, needs changes, accepted, rejected, or routed downstream.</p>
          </article>
        </div>
      </section>
    </div>
  );
}
