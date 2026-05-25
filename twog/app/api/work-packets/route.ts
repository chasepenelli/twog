import { NextResponse } from 'next/server';
import {
  isWorkPacketStorageConfigured,
  listWorkPackets,
  OPEN_WORK_PACKET_STATUSES,
  WORK_PACKET_TYPES,
  WorkPacketError,
} from '@/lib/work-packets';
import { publicOptionsResponse, publicReadHeaders } from '@/lib/api-cors';

export const runtime = 'nodejs';

export const OPTIONS = async () => publicOptionsResponse();

// Cross-candidate work packet listing. Designed for the /network mission
// board and for agents calling `twog.work_packets.list` via HTTP. Public
// boundary: listing only; no mutation, no compute dispatch, no validation
// dispatch.
export async function GET(request: Request) {
  if (!isWorkPacketStorageConfigured()) {
    return NextResponse.json(
      {
        error: 'work_packet_storage_not_configured',
        message: 'Work packet listing requires Neon/Postgres storage.',
      },
      { status: 503 }
    );
  }

  const url = new URL(request.url);
  const candidateIds = url.searchParams.getAll('candidate_id');
  const statuses = url.searchParams.getAll('status');
  const packetTypes = url.searchParams.getAll('packet_type');
  const limitParam = url.searchParams.get('limit');

  try {
    const packets = await listWorkPackets({
      candidate_ids: candidateIds,
      statuses: statuses.length > 0 ? statuses : [...OPEN_WORK_PACKET_STATUSES],
      packet_types: packetTypes,
      limit: limitParam ? Number(limitParam) : undefined,
    });

    return NextResponse.json(
      {
        schema_version: 'twog-work-packets-v1',
        filters: {
          candidate_ids: candidateIds,
          statuses: statuses.length > 0 ? statuses : [...OPEN_WORK_PACKET_STATUSES],
          packet_types: packetTypes,
        },
        allowed_packet_types: WORK_PACKET_TYPES,
        work_packet_count: packets.length,
        open_work_packets: packets.filter((p) => p.status === 'open').length,
        public_boundary:
          'Work packets are intake-only. Listing or fetching a packet does not mutate any candidate or dispatch downstream work.',
        work_packets: packets,
      },
      {
        headers: publicReadHeaders({ cacheControl: 's-maxage=60, stale-while-revalidate' }),
      }
    );
  } catch (error) {
    if (error instanceof WorkPacketError) {
      return NextResponse.json(
        { error: error.code, message: error.message, details: error.details ?? [] },
        { status: error.status, headers: publicReadHeaders() }
      );
    }
    console.error('cross-candidate work packets listing failed', error);
    return NextResponse.json(
      { error: 'work_packets_listing_failed' },
      { status: 500, headers: publicReadHeaders() }
    );
  }
}
