import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import process from 'node:process';

const input = process.env.TWOG_PUBLIC_CANDIDATES_IN ?? join(process.cwd(), 'data', 'public-candidates.json');

function firstString(...values) {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) return value;
  }
  return null;
}

function stringArray(value) {
  return Array.isArray(value) ? value.filter((item) => typeof item === 'string' && item.length > 0) : [];
}

const dataset = JSON.parse(await readFile(input, 'utf8'));
const candidates = Array.isArray(dataset.candidates) ? dataset.candidates : [];

const records = candidates.map((entry) => {
  const candidate = entry?.candidate ?? {};
  const snapshot = entry?.latest_snapshot ?? {};
  const payload = snapshot?.payload ?? {};
  const candidateMetadata = candidate.metadata ?? {};
  const snapshotMetadata = snapshot.metadata ?? {};
  const reproducibility = payload.reproducibility ?? {};
  const linkedRecords = payload.linked_records ?? {};
  const computeJobIds = Array.from(
    new Set([
      ...stringArray(snapshot.compute_job_ids),
      ...stringArray(snapshotMetadata.compute_job_ids),
      ...stringArray(candidateMetadata.compute_job_ids),
      ...stringArray(linkedRecords.compute_job_ids),
    ])
  );

  const runManifestId = firstString(
    snapshotMetadata.run_manifest_id,
    reproducibility.run_manifest_id,
    candidateMetadata.run_manifest_id
  );
  const traceId = firstString(snapshotMetadata.trace_id, reproducibility.trace_id, candidateMetadata.trace_id);

  return {
    candidate_id: candidate.candidate_id ?? null,
    display_id: candidate.display_id ?? null,
    snapshot_id: snapshot.snapshot_id ?? null,
    run_manifest_id: runManifestId,
    trace_id: traceId,
    dagster_run_id: firstString(
      snapshotMetadata.dagster_run_id,
      reproducibility.dagster_run_id,
      candidateMetadata.dagster_run_id
    ),
    commit_sha: firstString(snapshot.commit_sha, reproducibility.commit_sha),
    compute_job_count: computeJobIds.length,
    has_manifest: Boolean(runManifestId && traceId),
  };
});

const summary = {
  source: dataset.source ?? null,
  generated_at: dataset.generatedAt ?? null,
  total_candidates: records.length,
  candidates_with_manifest: records.filter((record) => record.has_manifest).length,
  candidates_with_dagster_run: records.filter((record) => record.dagster_run_id).length,
  candidates_with_compute: records.filter((record) => record.compute_job_count > 0).length,
  records,
};

console.log(JSON.stringify(summary, null, 2));

if (process.env.TWOG_REQUIRE_PUBLIC_CANDIDATE_MANIFESTS === 'true') {
  const missing = records.filter((record) => !record.has_manifest);
  if (missing.length > 0) {
    console.error(
      `Missing run_manifest_id and trace_id metadata for ${missing.length} candidate(s): ${missing
        .map((record) => record.candidate_id)
        .join(', ')}`
    );
    process.exit(1);
  }
}
