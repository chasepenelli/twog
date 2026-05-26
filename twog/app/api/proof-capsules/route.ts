import { NextResponse } from 'next/server';
import { getCandidate } from '@/lib/public-candidates';
import {
  isProofCapsuleStorageConfigured,
  normalizeProofCapsuleSubmission,
  ProofCapsuleError,
  submitProofCapsule,
} from '@/lib/proof-capsules';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

// Hard cap on incoming submission size. Capsules carry text + an artifact
// manifest of hashes/labels only (TWOG never hosts artifact bodies), so a
// legitimate submission is comfortably under this ceiling. Above it, we
// reject 413 to keep abuse cheap on the server side.
const MAX_PROOF_CAPSULE_BODY_BYTES = 256 * 1024;

// POST a proof capsule packet. The candidate_id is resolved from the body
// (or its embedded work_packet → candidate link in a future iteration); for
// now we require the body to provide candidate_id explicitly.
export async function POST(request: Request) {
  if (!isProofCapsuleStorageConfigured()) {
    return NextResponse.json(
      {
        error: 'proof_capsule_storage_not_configured',
        message: 'Proof capsule submission requires Neon/Postgres storage.',
      },
      { status: 503 }
    );
  }

  // Header-level fast-reject before we buffer anything into memory.
  const contentLength = Number(request.headers.get('content-length') ?? '');
  if (Number.isFinite(contentLength) && contentLength > MAX_PROOF_CAPSULE_BODY_BYTES) {
    return NextResponse.json(
      {
        error: 'proof_capsule_submission_too_large',
        message: `Proof capsule submissions must be at most ${MAX_PROOF_CAPSULE_BODY_BYTES} bytes.`,
        details: [`Content-Length=${contentLength}`],
      },
      { status: 413 }
    );
  }

  // Buffer-level guard for cases where Content-Length is missing or lying.
  let rawText: string;
  try {
    rawText = await request.text();
  } catch {
    return NextResponse.json(
      {
        error: 'invalid_proof_capsule_submission',
        details: ['Request body could not be read.'],
      },
      { status: 400 }
    );
  }
  if (Buffer.byteLength(rawText, 'utf-8') > MAX_PROOF_CAPSULE_BODY_BYTES) {
    return NextResponse.json(
      {
        error: 'proof_capsule_submission_too_large',
        message: `Proof capsule submissions must be at most ${MAX_PROOF_CAPSULE_BODY_BYTES} bytes.`,
      },
      { status: 413 }
    );
  }

  let raw: unknown;
  try {
    raw = rawText.length > 0 ? JSON.parse(rawText) : {};
  } catch {
    return NextResponse.json(
      {
        error: 'invalid_proof_capsule_submission',
        details: ['Request body must be valid JSON.'],
      },
      { status: 400 }
    );
  }

  const body = (raw && typeof raw === 'object' ? (raw as Record<string, unknown>) : {});
  const candidateId =
    typeof body.candidate_id === 'string'
      ? body.candidate_id.trim()
      : typeof (body.proof_capsule as Record<string, unknown> | undefined)?.candidate_id === 'string'
        ? ((body.proof_capsule as Record<string, unknown>).candidate_id as string).trim()
        : '';

  if (!candidateId) {
    return NextResponse.json(
      {
        error: 'invalid_proof_capsule_submission',
        details: ['candidate_id is required at the top level of the request body.'],
      },
      { status: 400 }
    );
  }

  const candidate = getCandidate(candidateId);
  if (!candidate) {
    return NextResponse.json(
      { error: 'public_candidate_not_found', candidate_id: candidateId },
      { status: 404 }
    );
  }

  try {
    const packet = normalizeProofCapsuleSubmission(body);
    const capsule = await submitProofCapsule(candidate.candidate.candidate_id, packet);

    return NextResponse.json(
      {
        schema_version: 'twog-proof-capsule-receipt-v1',
        public_boundary:
          'Proof capsule submissions enter the review queue. They do not mutate candidate records, dispatch validation, or trigger compute.',
        proof_capsule: capsule,
      },
      { status: 201 }
    );
  } catch (error) {
    if (error instanceof ProofCapsuleError) {
      return NextResponse.json(
        { error: error.code, message: error.message, details: error.details ?? [] },
        { status: error.status }
      );
    }
    console.error('proof capsule submission failed', error);
    return NextResponse.json({ error: 'proof_capsule_submission_failed' }, { status: 500 });
  }
}
