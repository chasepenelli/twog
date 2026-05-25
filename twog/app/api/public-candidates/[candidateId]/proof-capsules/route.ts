import { NextResponse } from 'next/server';
import { getCandidate } from '@/lib/public-candidates';
import {
  isProofCapsuleStorageConfigured,
  listProofCapsulesForCandidate,
  PROOF_CAPSULE_ACCEPTED_STATUSES,
  PROOF_CAPSULE_PENDING_STATUSES,
  PROOF_CAPSULE_STATUSES,
  ProofCapsuleError,
} from '@/lib/proof-capsules';
import { publicOptionsResponse, publicReadHeaders } from '@/lib/api-cors';

export const runtime = 'nodejs';

export const OPTIONS = async () => publicOptionsResponse();

// Public listing of proof capsules attached to a candidate. Default filter
// returns accepted capsules so the candidate page can show "what work
// improved this record." Callers can request pending capsules to see
// in-flight work via ?status=submitted&status=in_review.
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

  if (!isProofCapsuleStorageConfigured()) {
    return NextResponse.json(
      {
        error: 'proof_capsule_storage_not_configured',
        candidate_id: candidate.candidate.candidate_id,
        message: 'Proof capsule listing requires Neon/Postgres storage.',
      },
      { status: 503 }
    );
  }

  const url = new URL(request.url);
  const requestedStatuses = url.searchParams.getAll('status');
  const allowed = new Set<string>(PROOF_CAPSULE_STATUSES);
  const statuses = (requestedStatuses.length > 0
    ? requestedStatuses
    : ([...PROOF_CAPSULE_ACCEPTED_STATUSES] as string[])
  ).filter((status) => allowed.has(status));
  const limitParam = url.searchParams.get('limit');

  try {
    const capsules = await listProofCapsulesForCandidate(candidate.candidate.candidate_id, {
      statuses,
      limit: limitParam ? Number(limitParam) : undefined,
    });

    return NextResponse.json(
      {
        schema_version: 'twog-candidate-proof-capsules-v1',
        candidate_id: candidate.candidate.candidate_id,
        display_id: candidate.candidate.display_id,
        filters: {
          statuses,
          limit: capsules.length,
        },
        pending_statuses: PROOF_CAPSULE_PENDING_STATUSES,
        accepted_statuses: PROOF_CAPSULE_ACCEPTED_STATUSES,
        proof_capsule_count: capsules.length,
        public_boundary:
          'Listing proof capsules does not mutate the candidate record or trigger downstream work. Sensitive contributor fields (contact, agent_id, website) are scrubbed from each capsule.',
        proof_capsules: capsules,
      },
      {
        headers: publicReadHeaders({ cacheControl: 's-maxage=60, stale-while-revalidate' }),
      }
    );
  } catch (error) {
    if (error instanceof ProofCapsuleError) {
      return NextResponse.json(
        { error: error.code, message: error.message, details: error.details ?? [] },
        { status: error.status, headers: publicReadHeaders() }
      );
    }
    console.error('candidate proof capsules listing failed', error);
    return NextResponse.json(
      { error: 'proof_capsules_listing_failed' },
      { status: 500, headers: publicReadHeaders() }
    );
  }
}
