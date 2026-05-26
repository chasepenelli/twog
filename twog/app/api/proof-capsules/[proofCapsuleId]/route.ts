import { NextResponse } from 'next/server';
import {
  getProofCapsule,
  isProofCapsuleStorageConfigured,
  ProofCapsuleError,
} from '@/lib/proof-capsules';
import { publicOptionsResponse, publicReadHeaders } from '@/lib/api-cors';

export const runtime = 'nodejs';

export const OPTIONS = async () => publicOptionsResponse();

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ proofCapsuleId: string }> }
) {
  const { proofCapsuleId } = await params;

  if (!isProofCapsuleStorageConfigured()) {
    return NextResponse.json(
      {
        error: 'proof_capsule_storage_not_configured',
        proof_capsule_id: proofCapsuleId,
        message: 'Proof capsule status lookup requires Neon/Postgres storage.',
      },
      { status: 503 }
    );
  }

  try {
    const capsule = await getProofCapsule(proofCapsuleId);
    if (!capsule) {
      return NextResponse.json(
        { error: 'proof_capsule_not_found', proof_capsule_id: proofCapsuleId },
        { status: 404 }
      );
    }

    return NextResponse.json(
      {
        schema_version: 'twog-proof-capsule-status-v1',
        public_boundary:
          'Status lookup exposes the public capsule view only. Operator review notes, raw payload JSON, and sensitive contributor fields (contact, agent_id, website) are intentionally absent.',
        proof_capsule: capsule,
      },
      {
        headers: publicReadHeaders({ cacheControl: 's-maxage=30, stale-while-revalidate' }),
      }
    );
  } catch (error) {
    if (error instanceof ProofCapsuleError) {
      return NextResponse.json(
        { error: error.code, message: error.message, details: error.details ?? [] },
        { status: error.status, headers: publicReadHeaders() }
      );
    }
    console.error('proof capsule status lookup failed', error);
    return NextResponse.json(
      { error: 'proof_capsule_status_failed' },
      { status: 500, headers: publicReadHeaders() }
    );
  }
}
