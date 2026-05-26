import { NextResponse } from 'next/server';
import { getCandidate } from '@/lib/public-candidates';
import {
  isWorkPacketStorageConfigured,
  listWorkPackets,
  OPEN_WORK_PACKET_STATUSES,
  WORK_PACKET_STATUSES,
  WORK_PACKET_TYPES,
  WorkPacketError,
} from '@/lib/work-packets';
import { publicOptionsResponse, publicReadHeaders } from '@/lib/api-cors';

export const runtime = 'nodejs';

export const OPTIONS = async () => publicOptionsResponse();

export async function GET(
  request: Request,
  { params }: { params: Promise<{ candidateId: string }> }
) {
  const { candidateId } = await params;
  const candidate = getCandidate(candidateId);
  if (!candidate) {
    return NextResponse.json(
      { error: 'public_candidate_not_found', candidateId },
      {
        status: 404,
        headers: { 'Cache-Control': 's-maxage=300, stale-while-revalidate' },
      }
    );
  }

  if (!isWorkPacketStorageConfigured()) {
    return NextResponse.json(
      {
        error: 'work_packet_storage_not_configured',
        candidate_id: candidate.candidate.candidate_id,
        message: 'Work packet listing requires Neon/Postgres storage.',
      },
      { status: 503 }
    );
  }

  const url = new URL(request.url);
  const statusParam = url.searchParams.getAll('status');
  const typeParam = url.searchParams.getAll('packet_type');
  const limitParam = url.searchParams.get('limit');
  const requestedStatuses =
    statusParam.length > 0 ? statusParam : ([...OPEN_WORK_PACKET_STATUSES] as string[]);

  try {
    const packets = await listWorkPackets({
      candidate_ids: [candidate.candidate.candidate_id],
      statuses: requestedStatuses,
      packet_types: typeParam,
      limit: limitParam ? Number(limitParam) : undefined,
    });

    return NextResponse.json(
      {
        schema_version: 'twog-work-packets-v1',
        candidate_id: candidate.candidate.candidate_id,
        display_id: candidate.candidate.display_id,
        filters: {
          statuses: requestedStatuses,
          packet_types: typeParam,
          limit: packets.length,
        },
        work_packet_count: packets.length,
        open_work_packets: packets.filter((p) => p.status === 'open').length,
        allowed_packet_types: WORK_PACKET_TYPES,
        allowed_statuses: WORK_PACKET_STATUSES,
        public_boundary:
          'Work packets are intake-only. Picking up a packet does not mutate the candidate, dispatch validation, or trigger compute.',
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
    console.error('work packets listing failed', error);
    return NextResponse.json(
      { error: 'work_packets_listing_failed' },
      { status: 500, headers: publicReadHeaders() }
    );
  }
}
