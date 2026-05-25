import { NextResponse } from 'next/server';
import {
  getWorkPacket,
  isWorkPacketStorageConfigured,
  WorkPacketError,
} from '@/lib/work-packets';
import { publicOptionsResponse, publicReadHeaders } from '@/lib/api-cors';

export const runtime = 'nodejs';

export const OPTIONS = async () => publicOptionsResponse();

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ workPacketId: string }> }
) {
  const { workPacketId } = await params;

  if (!isWorkPacketStorageConfigured()) {
    return NextResponse.json(
      {
        error: 'work_packet_storage_not_configured',
        work_packet_id: workPacketId,
        message: 'Work packet lookup requires Neon/Postgres storage.',
      },
      { status: 503 }
    );
  }

  try {
    const packet = await getWorkPacket(workPacketId);
    if (!packet) {
      return NextResponse.json(
        { error: 'work_packet_not_found', work_packet_id: workPacketId },
        { status: 404 }
      );
    }

    return NextResponse.json(
      {
        schema_version: 'twog-work-packet-v1',
        public_boundary:
          'Reading a work packet does not check it out, lock it, or mutate any candidate state. Submit a ProofCapsule to act on it.',
        work_packet: packet,
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
    console.error('work packet lookup failed', error);
    return NextResponse.json(
      { error: 'work_packet_lookup_failed' },
      { status: 500, headers: publicReadHeaders() }
    );
  }
}
