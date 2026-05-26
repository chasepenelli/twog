import { NextResponse } from 'next/server';
import {
  getProofFlowSnapshot,
  isProofFlowStorageConfigured,
} from '@/lib/proof-flow';
import { publicOptionsResponse, publicReadHeaders } from '@/lib/api-cors';

export const runtime = 'nodejs';

export const OPTIONS = async () => publicOptionsResponse();

export async function GET() {
  if (!isProofFlowStorageConfigured()) {
    return NextResponse.json(
      {
        schema_version: 'twog-proof-flow-v1',
        storage_configured: false,
        message: 'Proof flow snapshot requires Neon/Postgres storage.',
      },
      { status: 503, headers: publicReadHeaders() }
    );
  }
  try {
    const snapshot = await getProofFlowSnapshot();
    return NextResponse.json(snapshot, {
      headers: publicReadHeaders({
        cacheControl: 's-maxage=30, stale-while-revalidate',
      }),
    });
  } catch (error) {
    console.error('proof flow snapshot failed', error);
    return NextResponse.json(
      {
        error: 'proof_flow_snapshot_failed',
        message: error instanceof Error ? error.message : String(error),
      },
      { status: 500, headers: publicReadHeaders() }
    );
  }
}
