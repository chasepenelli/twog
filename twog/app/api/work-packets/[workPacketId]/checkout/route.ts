import { NextResponse } from 'next/server';
import { getCandidate, publicCandidatePayloadPath, publicCandidateEvidenceBundlePath, shortHash } from '@/lib/public-candidates';
import { buildPublicEvidenceBundle } from '@/lib/public-evidence-bundle';
import {
  getWorkPacket,
  isWorkPacketStorageConfigured,
  WorkPacketError,
} from '@/lib/work-packets';
import {
  PROOF_CAPSULE_CONTRIBUTOR_KINDS,
  PROOF_CAPSULE_TYPES,
} from '@/lib/proof-capsules';
import { publicOptionsResponse, publicReadHeaders } from '@/lib/api-cors';

export const runtime = 'nodejs';

export const OPTIONS = async () => publicOptionsResponse();

// Checkout returns the work packet, the candidate snapshot/hash references,
// and a proof capsule template the contributor (or agent) can fill out.
// The endpoint is read-only: calling it does not lock the packet, does not
// mutate state, and does not commit the contributor to submitting.
export async function GET(
  _request: Request,
  { params }: { params: Promise<{ workPacketId: string }> }
) {
  const { workPacketId } = await params;

  if (!isWorkPacketStorageConfigured()) {
    return NextResponse.json(
      {
        error: 'work_packet_storage_not_configured',
        work_packet_id: workPacketId,
        message: 'Work packet checkout requires Neon/Postgres storage.',
      },
      { status: 503 }
    );
  }

  try {
    const packet = await getWorkPacket(workPacketId);
    if (!packet) {
      return NextResponse.json(
        { error: 'work_packet_not_found', work_packet_id: workPacketId },
        { status: 404 }
      );
    }

    const candidate = getCandidate(packet.candidate_id);
    if (!candidate) {
      return NextResponse.json(
        {
          error: 'public_candidate_not_found',
          work_packet_id: workPacketId,
          candidate_id: packet.candidate_id,
        },
        { status: 404 }
      );
    }

    const record = candidate.candidate;
    const candidateSnapshotHash =
      record.content_hash ?? candidate.latest_snapshot?.content_hash ?? null;
    const evidenceBundle = buildPublicEvidenceBundle(candidate);

    return NextResponse.json(
      {
        schema_version: 'twog-work-packet-checkout-v1',
        work_packet: packet,
        candidate: {
          candidate_id: record.candidate_id,
          display_id: record.display_id,
          title: record.title,
          public_status: record.public_status,
          payload_url: publicCandidatePayloadPath(record.candidate_id),
          evidence_bundle_url: publicCandidateEvidenceBundlePath(record.candidate_id),
          snapshot_content_hash: candidateSnapshotHash,
          snapshot_short_hash: shortHash(candidateSnapshotHash),
        },
        evidence_bundle_summary: {
          snapshot: evidenceBundle.snapshot,
          source_document_count: Array.isArray(evidenceBundle.source_documents)
            ? evidenceBundle.source_documents.length
            : 0,
          compute_job_count: evidenceBundle.run_manifest?.compute_job_ids?.length ?? 0,
        },
        proof_loop: [
          'Read the work packet question and the candidate evidence bundle.',
          'Do the bounded task: gather sources, run analysis, produce artifacts.',
          'Submit a ProofCapsule with content hashes for everything you produced.',
          'TWOG reviews the capsule through the operator gate before any candidate or queue state changes.',
          'Accepted capsules earn proof points and become part of the decision history.',
        ],
        public_boundary:
          'Checking out a work packet does not lock it, mutate the candidate, dispatch validation, or trigger compute. Only the operator review gate can affect downstream state.',
        proof_capsule_template: {
          schema_version: 'twog-proof-capsule-v1',
          submission_url: '/api/proof-capsules',
          status_url_template: '/api/proof-capsules/{proof_capsule_id}',
          candidate_id: record.candidate_id,
          work_packet_id: packet.work_packet_id,
          capsule_type: PROOF_CAPSULE_TYPES.join(' | '),
          contributor: {
            kind: PROOF_CAPSULE_CONTRIBUTOR_KINDS.join(' | '),
            name: '',
            handle: '',
            affiliation: '',
            contact: '',
            agent_id: '',
            website: '',
          },
          title: '',
          candidate_snapshot_hash: candidateSnapshotHash,
          evidence_bundle_hash: evidenceBundle.snapshot?.content_hash ?? candidateSnapshotHash,
          method_refs: [],
          notebook_ref: '',
          analysis_summary: '',
          findings: '',
          output_refs: [],
          artifact_manifest: [
            {
              label: '',
              url: '',
              content_hash: '',
              mime_type: '',
              size_bytes: 0,
              method_or_tool: '',
              container_or_software_versions: {},
              notes: '',
            },
          ],
          limitations: '',
          conflicts_or_disclosures: '',
          task_manifest: {},
          requested_review_route: '',
          signature: '',
        },
      },
      {
        headers: publicReadHeaders({ cacheControl: 's-maxage=60, stale-while-revalidate' }),
      }
    );
  } catch (error) {
    if (error instanceof WorkPacketError) {
      return NextResponse.json(
        { error: error.code, message: error.message, details: error.details ?? [] },
        { status: error.status, headers: publicReadHeaders() }
      );
    }
    console.error('work packet checkout failed', error);
    return NextResponse.json(
      { error: 'work_packet_checkout_failed' },
      { status: 500, headers: publicReadHeaders() }
    );
  }
}
