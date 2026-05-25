/**
 * Content-quality smell flags for Proof Capsules.
 *
 * Pure-function rules applied at read time. Mirrors
 * ``src/hsa_research/ingestion_bridge/proof_capsules.py::compute_quality_flags``.
 * Drift between the two = inconsistent flags between the Python intake
 * report and the public API. The fix: edit both files together.
 *
 * Flags are *hints* for operators and contributors — never blockers.
 */

export const QUALITY_FLAG_THIN_ANALYSIS = 'thin_analysis';
export const QUALITY_FLAG_NO_SOURCE_TOKEN = 'no_source_token';
export const QUALITY_FLAG_REPETITIVE_TEXT = 'repetitive_text';
export const QUALITY_FLAG_MISSING_FINDINGS = 'missing_findings';
export const QUALITY_FLAG_NO_ARTIFACT_FOR_REPLICATION = 'no_artifact_for_replication';
export const QUALITY_FLAG_SHORT_CITATION_REPAIR = 'short_citation_repair';

const SUBSTANTIVE_TYPES_REQUIRING_FINDINGS = new Set([
  'claim_critique',
  'validation_proposal',
  'methods_review',
  'demotion_case',
]);

const REPLICATION_TYPES_REQUIRING_ARTIFACT = new Set([
  'docking_replication',
  'md_review',
]);

const SOURCE_TOKEN_PATTERNS = [
  'doi:',
  'doi.org',
  'pmid:',
  'pmcid:',
  'ncbi.nlm.nih',
  'pubmed',
  'europepmc',
  'biorxiv',
  'arxiv',
  '10.',
  'http://',
  'https://',
];

const THIN_ANALYSIS_MIN_CHARS = 80;
const THIN_ANALYSIS_MIN_WORDS = 12;
const REPETITION_MIN_LEN = 6;
const REPETITION_MIN_COUNT = 4;

function hasSourceToken(text: string): boolean {
  const lowered = text.toLowerCase();
  return SOURCE_TOKEN_PATTERNS.some((token) => lowered.includes(token));
}

function repetitionSmell(text: string): boolean {
  const tokens = text
    .toLowerCase()
    .split(/\s+/)
    .filter((token) => token.length >= REPETITION_MIN_LEN);
  if (tokens.length === 0) return false;
  const counts = new Map<string, number>();
  for (const token of tokens) {
    counts.set(token, (counts.get(token) ?? 0) + 1);
  }
  for (const count of counts.values()) {
    if (count >= REPETITION_MIN_COUNT) return true;
  }
  return false;
}

export interface QualityInputs {
  capsule_type: string;
  title: string;
  analysis_summary: string;
  findings: string;
  method_refs: string[];
  artifact_manifest: Array<{ content_hash: string; label: string }>;
}

export function computeQualityFlags(inputs: QualityInputs): string[] {
  const flags: string[] = [];
  const summary = (inputs.analysis_summary ?? '').trim();
  const finds = (inputs.findings ?? '').trim();
  const combined = [inputs.title ?? '', summary, finds, ...(inputs.method_refs ?? [])].join(' ');

  const wordCount = summary.split(/\s+/).filter(Boolean).length;
  if (summary.length < THIN_ANALYSIS_MIN_CHARS || wordCount < THIN_ANALYSIS_MIN_WORDS) {
    flags.push(QUALITY_FLAG_THIN_ANALYSIS);
  }

  if (!hasSourceToken(combined) && (inputs.artifact_manifest ?? []).length === 0) {
    flags.push(QUALITY_FLAG_NO_SOURCE_TOKEN);
  }

  if (repetitionSmell(summary) || repetitionSmell(finds)) {
    flags.push(QUALITY_FLAG_REPETITIVE_TEXT);
  }

  if (SUBSTANTIVE_TYPES_REQUIRING_FINDINGS.has(inputs.capsule_type) && finds.length === 0) {
    flags.push(QUALITY_FLAG_MISSING_FINDINGS);
  }

  if (
    REPLICATION_TYPES_REQUIRING_ARTIFACT.has(inputs.capsule_type) &&
    (inputs.artifact_manifest ?? []).length === 0
  ) {
    flags.push(QUALITY_FLAG_NO_ARTIFACT_FOR_REPLICATION);
  }

  if (inputs.capsule_type === 'citation_repair' && (summary.length < 120 || finds.length < 20)) {
    flags.push(QUALITY_FLAG_SHORT_CITATION_REPAIR);
  }

  return flags;
}
