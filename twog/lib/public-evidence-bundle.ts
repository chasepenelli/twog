import {
  publicCandidateAuditTrail,
  publicCandidateEvidenceBundlePath,
  publicCandidatePayloadPath,
  type LiteratureRecord,
  type PublicCandidateDetail,
} from '@/lib/public-candidates';
import { CONTRIBUTION_TYPES, REQUESTED_ACTIONS } from '@/lib/candidate-contributions';

function topLevelComputeJobIds(candidate: PublicCandidateDetail): string[] {
  const snapshot = candidate.latest_snapshot as
    | (NonNullable<PublicCandidateDetail['latest_snapshot']> & { compute_job_ids?: string[] })
    | null
    | undefined;
  return snapshot?.compute_job_ids ?? [];
}

function sourceDocument(item: LiteratureRecord) {
  return {
    ref: item.ref,
    title: item.title,
    source_key: item.source_key,
    source_url: item.source_url,
    evidence_kind: item.evidence_kind,
    supported_claim: item.supports,
    identifiers: item.identifiers ?? {},
    published_at: item.published_at,
    publication_year: item.publication_year,
    resolved: item.resolved ?? false,
    public_access: {
      mirrored_full_text: false,
      source_url: item.source_url ?? null,
      note:
        'The public bundle exposes source identifiers and provenance pointers. It does not mirror copyrighted full text.',
    },
    provenance: {
      research_object_ids: item.provenance?.research_object_ids ?? [],
      chunk_ids: item.provenance?.chunk_ids ?? [],
      section_labels: item.section_labels ?? [],
      dedupe_keys: item.provenance?.dedupe_keys ?? [],
    },
    dedupe: item.dedupe ?? {},
  };
}

function chunkManifest(literature: LiteratureRecord[]) {
  return literature.flatMap((item) =>
    (item.provenance?.chunk_ids ?? []).map((chunkId) => ({
      chunk_id: chunkId,
      source_ref: item.ref,
      source_title: item.title,
      source_key: item.source_key,
      research_object_ids: item.provenance?.research_object_ids ?? [],
      section_labels: item.section_labels ?? [],
      public_text_included: false,
      access_note:
        'Chunk text is not included in public bundle v1. Use the source URL and identifiers for inspection, or check in a contribution packet that cites this chunk_id.',
    }))
  );
}

export function buildPublicEvidenceBundle(candidate: PublicCandidateDetail) {
  const record = candidate.candidate;
  const snapshot = candidate.latest_snapshot;
  const payload = snapshot?.payload;
  const literature = payload?.literature ?? [];
  const auditTrail = publicCandidateAuditTrail(candidate);
  const computeJobIds = Array.from(
    new Set([
      ...(payload?.linked_records?.compute_job_ids ?? []),
      ...topLevelComputeJobIds(candidate),
      ...auditTrail.compute_job_ids,
    ])
  );

  return {
    schema_version: 'twog-public-evidence-bundle-v1',
    candidate_id: record.candidate_id,
    display_id: record.display_id,
    title: record.title,
    public_status: record.public_status,
    snapshot: {
      snapshot_id: snapshot?.snapshot_id ?? null,
      content_hash: record.content_hash ?? snapshot?.content_hash ?? null,
      snapshot_version: snapshot?.snapshot_version ?? null,
      pipeline_version: payload?.reproducibility?.pipeline_version ?? snapshot?.pipeline_version ?? null,
      generated_at: payload?.reproducibility?.generated_at ?? snapshot?.created_at ?? null,
    },
    run_manifest: {
      trace_id: auditTrail.trace_id ?? null,
      run_manifest_id: auditTrail.run_manifest_id ?? null,
      dagster_run_id: auditTrail.dagster_run_id ?? null,
      commit_sha: auditTrail.commit_sha ?? null,
      compute_job_ids: computeJobIds,
      decision_count: auditTrail.decision_count,
      run_manifest_found: auditTrail.run_manifest_found,
      record: candidate.run_manifest ?? null,
      note:
        'This is the pipeline receipt for the public candidate snapshot. If trace_id, run_manifest_id, or record is null, the static export is not audit-ready and should be refreshed from Neon.',
    },
    checkout: {
      candidate_payload_url: publicCandidatePayloadPath(record.candidate_id),
      evidence_bundle_url: publicCandidateEvidenceBundlePath(record.candidate_id),
      contribution_template_url: `/api/public-candidates/${record.candidate_id}/contribution-template`,
      contribution_submission_url: `/api/public-candidates/${record.candidate_id}/contributions`,
      contribution_status_url_template: '/api/contributions/{contribution_id}/status',
      intended_use:
        'Use this bundle to inspect the record, cite source refs, reproduce public claims, attach outside evidence, or submit validation/compute work back to TWOG intake.',
      review_boundary:
        'Checking in work creates an intake packet only. It cannot mutate the public record, queue validation, or trigger GPU compute without TWOG review.',
    },
    claim_packet: {
      rationale: payload?.rationale ?? {},
      biology: payload?.biology ?? {},
      evidence: payload?.evidence ?? {},
      linked_records: payload?.linked_records ?? {},
    },
    source_documents: literature.map(sourceDocument),
    chunk_manifest: chunkManifest(literature),
    artifact_manifest: {
      artifacts: payload?.artifacts ?? [],
      note:
        'Artifacts are listed when the candidate has public-safe files such as poses, plots, notebooks, trajectories, or method outputs.',
    },
    open_questions: {
      next_experiments: payload?.evidence?.next_experiments ?? [],
      risks: payload?.evidence?.risks ?? record.risk_flags ?? [],
      blockers: [],
      limitations: [],
      note:
        'These are the safest public work targets: strengthen a claim, repair a citation, reproduce an artifact, or explain why the record should be demoted.',
    },
    compute_manifest: {
      status: computeJobIds.length > 0 ? 'compute_records_attached' : 'no_public_compute_runs_yet',
      compute_job_ids: computeJobIds,
      computational_evidence: payload?.computational_evidence ?? [],
      md_reproducibility_contract: {
        purpose:
          'When docking or MD evidence is attached, this is the minimum public shape needed for another reviewer to rerun or critique the job.',
        required_inputs: [
          'protein_pdb or protein_pdb_artifact_url',
          'compound_smiles or ligand_structure_artifact_url',
          'target_name',
          'compound_name',
          'protein_source',
          'ligand_source',
          'preparation_method',
        ],
        required_settings: [
          'simulation_steps',
          'temperature',
          'ph',
          'box_padding',
          'force_field',
          'solvent_model',
          'seeds',
          'container_image',
          'runpod_endpoint_or_worker',
        ],
        expected_artifacts: [
          'sanitized_protein_pdb',
          'ligand_sdf_or_mol',
          'receptor_pdbqt',
          'ligand_pdbqt',
          'pose_or_initial_complex',
          'trajectory_or_smoke_output',
          'stage_diagnostics',
          'stdout_stderr_tails',
          'artifact_hashes',
        ],
        gate:
          'Public submissions can request docking_or_md_review, but GPU work remains approval-first and ledgered inside TWOG.',
      },
    },
    contribution_packet_requirements: {
      accepted_types: CONTRIBUTION_TYPES,
      requested_actions: REQUESTED_ACTIONS,
      required_receipt_fields: [
        'content hash',
        'contributor identity',
        'targeted claim or section',
        'method notes',
        'evidence refs',
        'artifact refs',
        'conflicts or limitations',
        'requested action',
      ],
      compute_artifact_minimum_fields: [
        'method_or_tool',
        'input_snapshot_content_hash',
        'input_files_or_urls',
        'settings',
        'container_or_software_versions',
        'outputs',
        'artifact_hashes',
        'limitations',
      ],
    },
  };
}
