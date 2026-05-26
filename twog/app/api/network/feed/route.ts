import { NextResponse } from 'next/server';
import {
  isWorkPacketStorageConfigured,
  listWorkPackets,
  WorkPacketError,
} from '@/lib/work-packets';
import {
  isProofCapsuleStorageConfigured,
  listProofCapsules,
  PROOF_CAPSULE_ACCEPTED_STATUSES,
  PROOF_CAPSULE_PENDING_STATUSES,
  ProofCapsuleError,
} from '@/lib/proof-capsules';
import { publicOptionsResponse, publicReadHeaders } from '@/lib/api-cors';

export const runtime = 'nodejs';

export const OPTIONS = async () => publicOptionsResponse();

// Mission-board feed combining cross-candidate open work packets with the
// stream of recent + accepted proof capsules. Designed for /network and for
// agents that want a single endpoint to "see what's happening." Sensitive
// contributor fields and operator notes never appear in this response.
export async function GET(request: Request) {
  const url = new URL(request.url);
  const packetLimit = Math.max(1, Math.min(Number(url.searchParams.get('packet_limit')) || 12, 50));
  const recentLimit = Math.max(1, Math.min(Number(url.searchParams.get('recent_limit')) || 12, 50));
  const acceptedLimit = Math.max(1, Math.min(Number(url.searchParams.get('accepted_limit')) || 8, 50));

  const storageConfigured =
    isWorkPacketStorageConfigured() && isProofCapsuleStorageConfigured();
  if (!storageConfigured) {
    return NextResponse.json(
      {
        schema_version: 'twog-network-feed-v1',
        storage_configured: false,
        message: 'Network feed requires Neon/Postgres storage.',
        open_work_packets: [],
        recent_capsules: [],
        accepted_capsules: [],
        summary: {
          open_packet_count: 0,
          recent_capsule_count: 0,
          accepted_capsule_count: 0,
        },
      },
      { status: 503 }
    );
  }

  try {
    const [openPackets, recentCapsules, acceptedCapsules] = await Promise.all([
      listWorkPackets({ statuses: ['open'], limit: packetLimit }),
      listProofCapsules({
        statuses: [...PROOF_CAPSULE_PENDING_STATUSES, ...PROOF_CAPSULE_ACCEPTED_STATUSES],
        limit: recentLimit,
      }),
      listProofCapsules({
        statuses: [...PROOF_CAPSULE_ACCEPTED_STATUSES],
        limit: acceptedLimit,
      }),
    ]);

    const candidateIds = new Set<string>();
    openPackets.forEach((packet) => candidateIds.add(packet.candidate_id));
    recentCapsules.forEach((capsule) => candidateIds.add(capsule.candidate_id));

    return NextResponse.json(
      {
        schema_version: 'twog-network-feed-v1',
        storage_configured: true,
        public_boundary:
          'Feed entries are intake-only. Picking up a packet, reading a capsule, or watching activity never mutates a candidate, dispatches validation, or triggers compute.',
        summary: {
          open_packet_count: openPackets.length,
          recent_capsule_count: recentCapsules.length,
          accepted_capsule_count: acceptedCapsules.length,
          candidate_count: candidateIds.size,
        },
        filters: {
          packet_limit: packetLimit,
          recent_limit: recentLimit,
          accepted_limit: acceptedLimit,
        },
        open_work_packets: openPackets,
        recent_capsules: recentCapsules,
        accepted_capsules: acceptedCapsules,
      },
      {
        headers: publicReadHeaders({ cacheControl: 's-maxage=30, stale-while-revalidate' }),
      }
    );
  } catch (error) {
    if (error instanceof WorkPacketError || error instanceof ProofCapsuleError) {
      return NextResponse.json(
        { error: error.code, message: error.message, details: error.details ?? [] },
        { status: error.status, headers: publicReadHeaders() }
      );
    }
    console.error('network feed failed', error);
    return NextResponse.json(
      { error: 'network_feed_failed' },
      { status: 500, headers: publicReadHeaders() }
    );
  }
}
