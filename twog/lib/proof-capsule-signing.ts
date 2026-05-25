/**
 * Server-side verification of ed25519 capsule signatures.
 *
 * Capsule submissions may carry a `signature` field in the format
 * `ed25519:<pubkey-b64>:<sig-b64>`. The signature is over the
 * server-computed canonical `content_hash` (UTF-8 bytes of the hex
 * string, including the `sha256:` prefix). If a signature is present
 * and fails verification, the submission is rejected with a 400.
 *
 * The CLI's signing path (twog_agent.signing) signs the *client*'s
 * computed content_hash; the cross-language parity test
 * `test_twog_agent_client_hash_matches_server` guarantees that hash
 * equals what we compute here. If that test ever breaks, signature
 * verification on this path will also start failing — which is the
 * desired behavior (a broken hash contract is worse than a broken
 * signature).
 */

import { createPublicKey, verify } from 'node:crypto';

export interface SignatureVerificationResult {
  ok: boolean;
  reason?: string;
}

const ED25519_SPKI_PREFIX = Buffer.from('302a300506032b6570032100', 'hex');

/**
 * Build a SPKI-encoded public key from raw 32-byte ed25519 bytes.
 *
 * Node's `crypto.createPublicKey` accepts SPKI/DER. We do not depend on
 * a userland ed25519 library — the standard library handles it as long
 * as we frame the raw key bytes in the SPKI prefix above.
 */
function ed25519PublicKeyFromRaw(raw: Buffer): ReturnType<typeof createPublicKey> {
  if (raw.length !== 32) {
    throw new Error(`ed25519 public key must be 32 bytes (got ${raw.length})`);
  }
  const spki = Buffer.concat([ED25519_SPKI_PREFIX, raw]);
  return createPublicKey({ key: spki, format: 'der', type: 'spki' });
}

/**
 * Verify a capsule's signature packet against its content hash.
 *
 * Returns `{ ok: true }` when the signature is missing (signing is
 * optional) or when verification succeeds. Otherwise returns
 * `{ ok: false, reason }` so the caller can return a structured 400.
 */
export function verifyProofCapsuleSignature(
  signaturePacket: unknown,
  contentHash: string
): SignatureVerificationResult {
  if (signaturePacket === undefined || signaturePacket === null) {
    return { ok: true };
  }
  if (typeof signaturePacket !== 'string') {
    return { ok: false, reason: 'signature must be a string when present' };
  }
  const trimmed = signaturePacket.trim();
  if (trimmed.length === 0) {
    return { ok: true };
  }

  const parts = trimmed.split(':');
  if (parts.length !== 3) {
    return {
      ok: false,
      reason: 'signature must be in the form "<algorithm>:<pubkey-b64>:<sig-b64>"',
    };
  }
  const [algorithm, publicKeyB64, signatureB64] = parts;
  if (algorithm.toLowerCase() !== 'ed25519') {
    return { ok: false, reason: `unsupported signature algorithm: ${algorithm}` };
  }
  let publicKeyBytes: Buffer;
  let signatureBytes: Buffer;
  try {
    publicKeyBytes = Buffer.from(publicKeyB64, 'base64');
    signatureBytes = Buffer.from(signatureB64, 'base64');
  } catch {
    return { ok: false, reason: 'signature components must be valid base64' };
  }
  if (publicKeyBytes.length !== 32) {
    return { ok: false, reason: 'ed25519 public key must decode to 32 bytes' };
  }
  if (signatureBytes.length !== 64) {
    return { ok: false, reason: 'ed25519 signature must decode to 64 bytes' };
  }
  let keyObject;
  try {
    keyObject = ed25519PublicKeyFromRaw(publicKeyBytes);
  } catch (error) {
    return {
      ok: false,
      reason: `failed to construct public key: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
  // For ed25519, the `algorithm` argument to verify() must be null per Node docs.
  const messageBytes = Buffer.from(contentHash, 'utf-8');
  const verified = verify(null, messageBytes, keyObject, signatureBytes);
  if (!verified) {
    return { ok: false, reason: 'signature does not verify against the canonical content_hash' };
  }
  return { ok: true };
}
