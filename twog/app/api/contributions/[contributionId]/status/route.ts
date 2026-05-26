import { NextResponse } from 'next/server';
import {
  CandidateContributionError,
  contributionStatusPath,
  getCandidateContributionStatus,
  isCandidateContributionStorageConfigured,
} from '@/lib/candidate-contributions';

export const runtime = 'nodejs';

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ contributionId: string }> }
) {
  const { contributionId } = await params;

  if (!isCandidateContributionStorageConfigured()) {
    return NextResponse.json(
      {
        error: 'candidate_contribution_storage_not_configured',
        contribution_id: contributionId,
        message: 'Contribution status lookup requires Neon/Postgres intake storage.',
      },
      { status: 503 }
    );
  }

  try {
    const status = await getCandidateContributionStatus(contributionId);

    if (!status) {
      return NextResponse.json(
        {
          error: 'candidate_contribution_not_found',
          contribution_id: contributionId,
        },
        { status: 404 }
      );
    }

    return NextResponse.json({
      schema_version: 'twog-contribution-status-v1',
      contribution_id: status.contribution_id,
      candidate_id: status.candidate_id,
      display_id: status.display_id,
      snapshot_content_hash: status.snapshot_content_hash,
      contribution_content_hash: status.contribution_content_hash,
      source_payload_url: status.source_payload_url,
      status: status.status,
      contribution_type: status.contribution_type,
      relation_to_current_record: status.relation_to_current_record,
      requested_system_action: status.requested_system_action,
      promoted_queue_id: status.promoted_queue_id,
      created_at: status.created_at,
      updated_at: status.updated_at,
      reviewed_at: status.reviewed_at,
      status_url: contributionStatusPath(status.contribution_id),
      public_boundary:
        'Status lookup exposes compact receipt and routing state only. It does not expose private review notes, mutate candidates, or trigger validation or compute.',
    });
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

    console.error('candidate contribution status lookup failed', error);
    return NextResponse.json(
      {
        error: 'candidate_contribution_status_failed',
      },
      { status: 500 }
    );
  }
}
