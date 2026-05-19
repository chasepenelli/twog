import { NextResponse } from 'next/server';
import { getCandidate, publicCandidatePayloadPath, shortHash } from '@/lib/public-candidates';

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

  const record = candidate.candidate;
  const contentHash = record.content_hash ?? candidate.latest_snapshot?.content_hash ?? null;

  return NextResponse.json(
    {
      schema_version: 'twog-candidate-contribution-v1',
      candidate_id: record.candidate_id,
      display_id: record.display_id,
      candidate_payload_url: publicCandidatePayloadPath(record.candidate_id),
      contribution_submission_url: `/api/public-candidates/${record.candidate_id}/contributions`,
      snapshot_content_hash: contentHash,
      snapshot_short_hash: shortHash(contentHash),
      check_in_status: {
        live_submission_api: 'enabled_when_neon_storage_is_configured',
        current_path:
          'POST the completed contribution_packet JSON to contribution_submission_url. Email poppa@bradyandgraffiti.com if the storage endpoint is not configured.',
        intended_queue:
          'candidate_contribution_intake -> provenance review -> citation repair/dedupe -> validation or compute queue',
      },
      contribution_packet: {
        contribution_type:
          'evidence | critique | replication | artifact | validation_proposal | compute_result',
        contributor: {
          name: '',
          affiliation: '',
          contact: '',
        },
        title: '',
        summary: '',
        claim_or_question: '',
        relation_to_current_record:
          'supports | challenges | extends | corrects | requests_validation | requests_compute',
        evidence: [
          {
            title: '',
            doi: '',
            pmid: '',
            pmcid: '',
            url: '',
            source_type: 'paper | dataset | method | compute_artifact | clinical_record | other',
            supported_claim: '',
            notes: '',
          },
        ],
        artifacts: [
          {
            label: '',
            url: '',
            content_hash: '',
            method_or_tool: '',
            notes: '',
          },
        ],
        requested_system_action:
          'evidence_review | citation_repair | validation_packet | omics_readout | docking_or_md_review | no_action',
        conflicts_or_limitations: '',
      },
    },
    {
      headers: {
        'Cache-Control': 's-maxage=300, stale-while-revalidate',
        'Content-Disposition': `inline; filename="${record.candidate_id}-contribution-template.json"`,
      },
    }
  );
}
