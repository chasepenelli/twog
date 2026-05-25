#!/usr/bin/env node
/**
 * Cross-language hash parity helper.
 *
 * Reads a single capsule submission body on stdin (JSON object), prints
 * the TS-computed content_hash on stdout. Used by
 * ``tests/test_twog_agent_cli.py`` to assert that the TS implementation
 * in ``twog/lib/proof-capsules.ts`` matches the Python implementations
 * in ``src/twog_agent/content_hash.py`` and
 * ``src/hsa_research/ingestion_bridge/contracts.py``.
 *
 * Drift between the three breaks signature verification on the server.
 */

import { _internalComputeContentHashForTest } from '../lib/proof-capsules.ts';

let raw = '';
for await (const chunk of process.stdin) {
  raw += chunk;
}
const body = JSON.parse(raw);
const candidateId = body.candidate_id ?? '';
const packet = {
  capsule_type: body.capsule_type ?? '',
  work_packet_id: body.work_packet_id ?? undefined,
  title: body.title ?? '',
  contributor: body.contributor ?? { kind: 'human', contact: '' },
  candidate_snapshot_hash: body.candidate_snapshot_hash ?? undefined,
  evidence_bundle_hash: body.evidence_bundle_hash ?? undefined,
  method_refs: body.method_refs ?? [],
  notebook_ref: body.notebook_ref ?? undefined,
  analysis_summary: body.analysis_summary ?? '',
  findings: body.findings ?? undefined,
  output_refs: body.output_refs ?? [],
  artifact_manifest: body.artifact_manifest ?? [],
  limitations: body.limitations ?? undefined,
};
process.stdout.write(_internalComputeContentHashForTest(packet, candidateId));
