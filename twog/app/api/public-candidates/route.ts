import { NextResponse } from 'next/server';
import { listCandidates } from '@/lib/public-candidates-live';

// Live public-candidate list (reads Neon public_candidates via the graceful reader). Lean shape, same
// candidate_id namespace as the evidence/runs surfaces.
export const runtime = 'nodejs';

export async function GET() {
  const candidates = await listCandidates(200);
  return NextResponse.json(
    {
      recordCount: candidates.length,
      candidates: candidates.map((candidate) => ({
        ...candidate,
        payload_url: `/api/public-candidates/${candidate.candidate_id}`,
      })),
    },
    { headers: { 'Cache-Control': 's-maxage=60, stale-while-revalidate' } },
  );
}
