import { NextResponse } from 'next/server';
import {
  getCandidate,
  publicCandidateEvidenceBundlePath,
  publicCandidatePayloadPath,
  shortHash,
} from '@/lib/public-candidates';
import {
  CANDIDATE_CONTRIBUTIONS_PAUSED,
  CANDIDATE_CONTRIBUTIONS_PAUSED_MESSAGE,
} from '@/lib/public-contribution-status';
import { CONTRIBUTION_TYPES, RECORD_RELATIONS, REQUESTED_ACTIONS } from '@/lib/candidate-contributions';

export const runtime = 'nodejs';

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
      evidence_bundle_url: publicCandidateEvidenceBundlePath(record.candidate_id),
      contribution_submission_url: `/api/public-candidates/${record.candidate_id}/contributions`,
      contribution_status_url_template: '/api/contributions/{contribution_id}/status',
      snapshot_content_hash: contentHash,
      snapshot_short_hash: shortHash(contentHash),
      proof_network_loop: [
        'Check out the candidate payload and evidence bundle.',
        'Do outside work against the cited snapshot hash.',
        'Check in a structured contribution packet with evidence, methods, artifacts, limitations, and requested action.',
        'TWOG reviews the packet through intake before any candidate record, validation queue, or compute lane changes.',
        'Accepted work can be attributed later in the public decision history.',
      ],
      check_in_status: {
        live_submission_api: CANDIDATE_CONTRIBUTIONS_PAUSED
          ? 'paused'
          : 'enabled_when_neon_storage_is_configured',
        current_path:
          CANDIDATE_CONTRIBUTIONS_PAUSED
            ? CANDIDATE_CONTRIBUTIONS_PAUSED_MESSAGE
            : 'POST the completed contribution_packet JSON to contribution_submission_url. Email poppa@bradyandgraffiti.com if the storage endpoint is not configured.',
        intended_queue:
          'candidate_contribution_intake -> provenance review -> citation repair/dedupe -> validation or compute queue',
        boundary:
          'Public submissions are intake packets only. They do not mutate candidate records, dispatch validation, or start GPU compute.',
      },
      contribution_packet: {
        contribution_type: CONTRIBUTION_TYPES.join(' | '),
        contributor: {
          name: '',
          handle: '',
          affiliation: '',
          contact: '',
        },
        title: '',
        summary: '',
        claim_or_question: '',
        targeted_claim_or_section: '',
        method_notes: '',
        relation_to_current_record: RECORD_RELATIONS.join(' | '),
        evidence_refs: [],
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
        artifact_refs: [],
        artifacts: [
          {
            label: '',
            url: '',
            content_hash: '',
            method_or_tool: '',
            input_snapshot_content_hash: contentHash,
            settings: {},
            container_or_software_versions: {},
            notes: '',
          },
        ],
        requested_system_action: REQUESTED_ACTIONS.join(' | '),
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
