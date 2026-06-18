/**
 * Collaborators service: applicant queue, approve/deny + scope assignment,
 * revoke. Mirrors the backend collaborator endpoints.
 *
 * API-CLIENT owns lib/api/*.
 */

import type { Collaborator, Scope } from "@/lib/types/domain";
import { MOCK_COLLABORATORS } from "@/lib/mocks/fixtures";
import { USE_MOCKS, mock, request } from "./config";

export interface ApplyInput {
  name: string;
  principal: string;
  /** Optional public key when the applicant intends to self-hold their signing key. */
  public_key?: string;
}

export interface ApproveInput {
  collaborator_id: string;
  /** Scopes the operator grants on approval. */
  scopes: Scope[];
}

/** List all collaborators (operator queue + active roster). */
export async function list(signal?: AbortSignal): Promise<Collaborator[]> {
  if (USE_MOCKS) return mock(MOCK_COLLABORATORS);
  return request<Collaborator[]>("/collaborators", { signal });
}

/** Submit a collaborator application. Lands as status: "pending". */
export async function apply(input: ApplyInput): Promise<Collaborator> {
  if (USE_MOCKS) {
    return mock<Collaborator>({
      collaborator_id: `col_pending_${Date.now()}`,
      principal: input.principal,
      name: input.name,
      role: "collaborator",
      status: "pending",
      scopes: [],
      public_key: input.public_key,
    });
  }
  return request<Collaborator>("/collaborators/apply", {
    method: "POST",
    body: input,
  });
}

/** Operator action: approve a pending applicant and set their scopes -> "active". */
export async function approve(input: ApproveInput): Promise<Collaborator> {
  if (USE_MOCKS) {
    const found =
      MOCK_COLLABORATORS.find((c) => c.collaborator_id === input.collaborator_id) ??
      MOCK_COLLABORATORS[3];
    return mock<Collaborator>({
      ...found,
      status: "active",
      scopes: input.scopes,
    });
  }
  return request<Collaborator>(
    `/collaborators/${input.collaborator_id}/approve`,
    { method: "POST", body: { scopes: input.scopes } },
  );
}

/** Operator action: deny a pending applicant -> "revoked". */
export async function deny(collaborator_id: string): Promise<Collaborator> {
  if (USE_MOCKS) {
    const found =
      MOCK_COLLABORATORS.find((c) => c.collaborator_id === collaborator_id) ??
      MOCK_COLLABORATORS[3];
    return mock<Collaborator>({ ...found, status: "revoked", scopes: [] });
  }
  return request<Collaborator>(`/collaborators/${collaborator_id}/deny`, {
    method: "POST",
  });
}

/** Operator action: revoke an active collaborator -> "revoked". */
export async function revoke(collaborator_id: string): Promise<Collaborator> {
  if (USE_MOCKS) {
    const found =
      MOCK_COLLABORATORS.find((c) => c.collaborator_id === collaborator_id) ??
      MOCK_COLLABORATORS[1];
    return mock<Collaborator>({ ...found, status: "revoked", scopes: [] });
  }
  return request<Collaborator>(`/collaborators/${collaborator_id}/revoke`, {
    method: "POST",
  });
}
