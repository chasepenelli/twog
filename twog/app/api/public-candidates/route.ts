import { NextResponse } from 'next/server';
import { publicCandidateDataset, publicCandidatePayloadPath } from '@/lib/public-candidates';

export async function GET() {
  return NextResponse.json(
    {
      generatedAt: publicCandidateDataset.generatedAt,
      recordCount: publicCandidateDataset.candidates.length,
      candidates: publicCandidateDataset.candidates.map((candidate) => ({
        ...candidate,
        payload_url: publicCandidatePayloadPath(candidate.candidate.candidate_id),
      })),
    },
    {
      headers: {
        'Cache-Control': 's-maxage=300, stale-while-revalidate',
      },
    }
  );
}
