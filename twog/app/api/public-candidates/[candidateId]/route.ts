import { NextResponse } from 'next/server';
import { getCandidate } from '@/lib/public-candidates-live';

// Live public-candidate payload (reads Neon public_candidates via the graceful reader).
export const runtime = 'nodejs';

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ candidateId: string }> }
) {
  const { candidateId } = await params;
  const candidate = await getCandidate(candidateId);

  if (!candidate) {
    return NextResponse.json(
      { error: 'public_candidate_not_found', candidateId },
      { status: 404, headers: { 'Cache-Control': 's-maxage=60, stale-while-revalidate' } }
    );
  }

  return NextResponse.json(candidate, {
    headers: {
      'Cache-Control': 's-maxage=60, stale-while-revalidate',
      'Content-Disposition': `inline; filename="${candidate.candidate_id}.json"`,
    },
  });
}
