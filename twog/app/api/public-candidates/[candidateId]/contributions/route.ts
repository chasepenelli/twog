import { NextResponse } from 'next/server';
import {
  CandidateContributionError,
  createCandidateContribution,
  isCandidateContributionStorageConfigured,
  normalizeCandidateContributionPacket,
} from '@/lib/candidate-contributions';
import {
  getCandidate,
  publicCandidateEvidenceBundlePath,
  publicCandidatePayloadPath,
} from '@/lib/public-candidates';
import {
  CANDIDATE_CONTRIBUTIONS_PAUSED,
  CANDIDATE_CONTRIBUTIONS_PAUSED_MESSAGE,
} from '@/lib/public-contribution-status';

export const runtime = 'nodejs';

const MAX_BODY_BYTES = 250_000;

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
      { status: 404 }
    );
  }

  return NextResponse.json({
    endpoint: `/api/public-candidates/${candidate.candidate.candidate_id}/contributions`,
    method: 'POST',
    storage_configured: !CANDIDATE_CONTRIBUTIONS_PAUSED && isCandidateContributionStorageConfigured(),
    intake_paused: CANDIDATE_CONTRIBUTIONS_PAUSED,
    candidate_payload_url: publicCandidatePayloadPath(candidate.candidate.candidate_id),
    evidence_bundle_url: publicCandidateEvidenceBundlePath(candidate.candidate.candidate_id),
    status: CANDIDATE_CONTRIBUTIONS_PAUSED ? 'intake_paused' : 'intake_queue',
    description:
      CANDIDATE_CONTRIBUTIONS_PAUSED
        ? CANDIDATE_CONTRIBUTIONS_PAUSED_MESSAGE
        : 'Submit a bounded contribution packet for review. Accepted packets are queued for TWOG intake and do not directly change the public candidate record.',
    required_fields: [
      'contribution_type',
      'contributor.contact',
      'title',
      'summary',
      'claim_or_question',
      'relation_to_current_record',
      'requested_system_action',
    ],
  });
}

export async function POST(
  request: Request,
  { params }: { params: Promise<{ candidateId: string }> }
) {
  if (CANDIDATE_CONTRIBUTIONS_PAUSED) {
    return NextResponse.json(
      {
        error: 'candidate_contribution_intake_paused',
        message: CANDIDATE_CONTRIBUTIONS_PAUSED_MESSAGE,
      },
      { status: 503 }
    );
  }

  const contentLength = Number(request.headers.get('content-length') ?? 0);
  if (contentLength > MAX_BODY_BYTES) {
    return NextResponse.json(
      {
        error: 'candidate_contribution_too_large',
        max_bytes: MAX_BODY_BYTES,
      },
      { status: 413 }
    );
  }

  const { candidateId } = await params;
  const candidate = getCandidate(candidateId);

  if (!candidate) {
    return NextResponse.json(
      {
        error: 'public_candidate_not_found',
        candidateId,
      },
      { status: 404 }
    );
  }

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json(
      {
        error: 'invalid_json_body',
      },
      { status: 400 }
    );
  }

  try {
    const packet = normalizeCandidateContributionPacket(body);
    const record = await createCandidateContribution(candidate, packet);

    return NextResponse.json(
      {
        contribution_id: record.contribution_id,
        status: record.status,
        candidate_id: record.candidate_id,
        display_id: record.display_id,
        snapshot_content_hash: record.snapshot_content_hash,
        source_payload_url: record.source_payload_url,
        created_at: record.created_at,
        next_step:
          'Queued for TWOG intake: provenance review, citation repair/dedupe, and routing to evidence review, validation planning, or compute review.',
      },
      { status: 202 }
    );
  } catch (error) {
    if (error instanceof CandidateContributionError) {
      return NextResponse.json(
        {
          error: error.code,
          message: error.message,
          details: error.details ?? [],
        },
        { status: error.status }
      );
    }

    console.error('candidate contribution intake failed', error);
    return NextResponse.json(
      {
        error: 'candidate_contribution_intake_failed',
      },
      { status: 500 }
    );
  }
}
