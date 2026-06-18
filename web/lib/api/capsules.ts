/**
 * Capsules service: the operator WRITE GATE. Review submitted proof capsules and
 * accept or promote them. This is the terminal HUMAN gate — the platform never
 * auto-promotes. `accept_capsule` and `promote_candidate` are operator-only scopes.
 *
 * API-CLIENT owns lib/api/*.
 */

import type { ProofCapsule } from "@/lib/types/domain";
import { MOCK_CAPSULES } from "@/lib/mocks/fixtures";
import { ApiError, USE_MOCKS, mock, request } from "./config";

/** List capsules for the public EVIDENCE surface (presented; no auth). */
export async function list(signal?: AbortSignal): Promise<ProofCapsule[]> {
  if (USE_MOCKS) return mock(MOCK_CAPSULES);
  return request<ProofCapsule[]>("/public/capsules", { signal });
}

/** A single proof capsule — the auditable receipt (public, presented). */
export async function get(capsule_id: string, signal?: AbortSignal): Promise<ProofCapsule> {
  if (USE_MOCKS) return mock(findOrFirst(capsule_id));
  return request<ProofCapsule>(`/public/capsules/${capsule_id}`, { signal });
}

/** List only capsules still pending the operator's accept/promote decision. */
export async function listPending(
  signal?: AbortSignal,
): Promise<ProofCapsule[]> {
  if (USE_MOCKS) {
    return mock(MOCK_CAPSULES.filter((c) => c.status === "submitted"));
  }
  return request<ProofCapsule[]>("/capsules?status=submitted", { signal });
}

function findOrFirst(capsule_id: string): ProofCapsule {
  const found = MOCK_CAPSULES.find((c) => c.capsule_id === capsule_id);
  if (!found) throw new ApiError(`Capsule not found: ${capsule_id}`, 404);
  return found;
}

/**
 * Operator action (scope: accept_capsule): accept a reviewed capsule into the
 * evidence record. Accepting does NOT promote the underlying candidate.
 */
export async function accept(capsule_id: string): Promise<ProofCapsule> {
  if (USE_MOCKS) {
    return mock<ProofCapsule>({ ...findOrFirst(capsule_id), status: "accepted" });
  }
  return request<ProofCapsule>(`/capsules/${capsule_id}/accept`, {
    method: "POST",
  });
}

/**
 * Operator action (scope: promote_candidate): the terminal human gate. Promotes
 * the candidate backing this capsule. Nothing reaches here automatically.
 */
export async function promote(capsule_id: string): Promise<ProofCapsule> {
  if (USE_MOCKS) {
    return mock<ProofCapsule>({ ...findOrFirst(capsule_id), status: "promoted" });
  }
  return request<ProofCapsule>(`/capsules/${capsule_id}/promote`, {
    method: "POST",
  });
}
