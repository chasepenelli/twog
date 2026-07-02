import { NextResponse } from 'next/server';
import { buildPublicEvidenceBundle } from '@/lib/public-evidence-bundle';
import { getCandidate as getLiveCandidate, asPublicCandidateDetail } from '@/lib/public-candidates-live';

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ candidateId: string }> }
) {
  const { candidateId } = await params;
  const lean = await getLiveCandidate(candidateId);

  if (!lean) {
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

  const candidate = asPublicCandidateDetail(lean);
  const stableId = candidate.candidate.candidate_id;

  return NextResponse.json(buildPublicEvidenceBundle(candidate), {
    headers: {
      'Cache-Control': 's-maxage=300, stale-while-revalidate',
      'Content-Disposition': `inline; filename="${stableId}-evidence-bundle.json"`,
    },
  });
}
