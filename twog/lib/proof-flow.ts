import {
  isProofCapsuleStorageConfigured,
  listProofCapsules,
  PROOF_CAPSULE_STATUSES,
} from '@/lib/proof-capsules';
import {
  isWorkPacketStorageConfigured,
  listWorkPackets,
} from '@/lib/work-packets';

// Server-side aggregator for the /network/flow Sankey view.
//
// The shape is a snapshot, not a stream: every status bucket the public
// pipeline can report is counted once, with bounded `limit` reads against
// the public list endpoints. The diagram component does the layout.
//
// This module never mutates anything — it is read-only, like every other
// public-boundary read on the Proof Network.

export interface ProofFlowSnapshot {
  schema_version: 'twog-proof-flow-v1';
  generated_at: string;
  storage_configured: boolean;
  // Source bucket counts (work_packets table).
  work_packets_open: number;
  work_packets_in_progress: number;
  // Capsule status counts (proof_capsules table, across every status).
  capsules_submitted: number;
  capsules_in_review: number;
  capsules_needs_changes: number;
  capsules_accepted: number;
  capsules_routed_to_validation: number;
  capsules_routed_to_compute_review: number;
  capsules_rejected: number;
  capsules_archived: number;
  // Derived rollups.
  capsules_total: number;
  packets_total: number;
}

function databaseUrl(): string | undefined {
  return (
    process.env.NEON_DATABASE_URL ??
    process.env.DATABASE_URL ??
    process.env.POSTGRES_URL ??
    process.env.HSA_DATABASE_URL
  );
}

export function isProofFlowStorageConfigured(): boolean {
  return (
    Boolean(databaseUrl()) &&
    isWorkPacketStorageConfigured() &&
    isProofCapsuleStorageConfigured()
  );
}

// Bounded ceiling on each listing read. The public listing endpoints cap
// at 200, so we ask for 200 across the relevant buckets and count what
// comes back. The Sankey is a *qualitative* visualization — saturation
// past the cap is acceptable, and a status block visually saying "200+"
// is fine for v1. If totals routinely exceed this, swap to a dedicated
// COUNT(*) aggregation query.
const PAGE_SIZE = 200;

function emptySnapshot(): ProofFlowSnapshot {
  return {
    schema_version: 'twog-proof-flow-v1',
    generated_at: new Date().toISOString(),
    storage_configured: false,
    work_packets_open: 0,
    work_packets_in_progress: 0,
    capsules_submitted: 0,
    capsules_in_review: 0,
    capsules_needs_changes: 0,
    capsules_accepted: 0,
    capsules_routed_to_validation: 0,
    capsules_routed_to_compute_review: 0,
    capsules_rejected: 0,
    capsules_archived: 0,
    capsules_total: 0,
    packets_total: 0,
  };
}

export async function getProofFlowSnapshot(): Promise<ProofFlowSnapshot> {
  if (!isProofFlowStorageConfigured()) {
    return emptySnapshot();
  }

  // Two work-packet reads (one per source bucket) and one wide capsule
  // read that asks for every status. `listProofCapsules` defaults to the
  // pending statuses when called with no filter, so we explicitly pass
  // the full status list.
  const [openPackets, inProgressPackets, allCapsules] = await Promise.all([
    listWorkPackets({ statuses: ['open'], limit: PAGE_SIZE }),
    listWorkPackets({ statuses: ['in_progress'], limit: PAGE_SIZE }),
    listProofCapsules({
      statuses: [...PROOF_CAPSULE_STATUSES],
      limit: PAGE_SIZE,
    }),
  ]);

  const counts: Record<string, number> = {
    submitted: 0,
    in_review: 0,
    needs_changes: 0,
    accepted: 0,
    routed_to_validation: 0,
    routed_to_compute_review: 0,
    rejected: 0,
    archived: 0,
  };
  for (const capsule of allCapsules) {
    if (capsule.status in counts) {
      counts[capsule.status] += 1;
    }
  }

  const packetsTotal = openPackets.length + inProgressPackets.length;
  const capsulesTotal =
    counts.submitted +
    counts.in_review +
    counts.needs_changes +
    counts.accepted +
    counts.routed_to_validation +
    counts.routed_to_compute_review +
    counts.rejected +
    counts.archived;

  return {
    schema_version: 'twog-proof-flow-v1',
    generated_at: new Date().toISOString(),
    storage_configured: true,
    work_packets_open: openPackets.length,
    work_packets_in_progress: inProgressPackets.length,
    capsules_submitted: counts.submitted,
    capsules_in_review: counts.in_review,
    capsules_needs_changes: counts.needs_changes,
    capsules_accepted: counts.accepted,
    capsules_routed_to_validation: counts.routed_to_validation,
    capsules_routed_to_compute_review: counts.routed_to_compute_review,
    capsules_rejected: counts.rejected,
    capsules_archived: counts.archived,
    capsules_total: capsulesTotal,
    packets_total: packetsTotal,
  };
}
