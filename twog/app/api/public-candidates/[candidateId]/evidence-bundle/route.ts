import { NextResponse } from 'next/server';
import { buildPublicEvidenceBundle } from '@/lib/public-evidence-bundle';
import { getCandidate } from '@/lib/public-candidates';

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ candidateId: string }> }
) {
  const { candidateId } = await params;
  const candidate = getCandidate(candidateId);

  if (!candidate) {
    return NextResponse.json(
      {
        error: 'public_candidate_not_found',
        candidateId,
      },
      {
        status: 404,
        headers: {
          'Cache-Control': 's-maxage=300, stale-while-revalidate',
        },
      }
    );
  }

  const stableId = candidate.candidate.candidate_id;

  return NextResponse.json(buildPublicEvidenceBundle(candidate), {
    headers: {
      'Cache-Control': 's-maxage=300, stale-while-revalidate',
      'Content-Disposition': `inline; filename="${stableId}-evidence-bundle.json"`,
    },
  });
}
