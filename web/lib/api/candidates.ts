/**
 * Candidates service: the library of hypotheses under test + detail lookup.
 *
 * API-CLIENT owns lib/api/*.
 */

import type { Candidate, MoonshotRubric } from "@/lib/types/domain";
import { MOCK_CANDIDATES, MOCK_RUBRICS } from "@/lib/mocks/fixtures";
import { ApiError, USE_MOCKS, mock, request } from "./config";

/** List all candidates (mix of validation_ready). */
export async function list(signal?: AbortSignal): Promise<Candidate[]> {
  if (USE_MOCKS) return mock(MOCK_CANDIDATES);
  return request<Candidate[]>("/public/candidates", { signal });
}

/** Fetch a single candidate by id. Throws ApiError(404) when not found. */
export async function get(
  candidate_id: string,
  signal?: AbortSignal,
): Promise<Candidate> {
  if (USE_MOCKS) {
    const found = MOCK_CANDIDATES.find((c) => c.candidate_id === candidate_id);
    if (!found) throw new ApiError(`Candidate not found: ${candidate_id}`, 404);
    return mock(found);
  }
  return request<Candidate>(`/public/candidates/${candidate_id}`, { signal });
}

/**
 * The pre-registered MoonshotRubric for a candidate — the "whole shabang" of what the moonshot must
 * show and how it will be tested. Returns null when the candidate carries no rubric (e.g. a
 * non-moonshot-grade candidate), so callers can simply skip the section rather than error.
 */
export async function rubric(
  candidate_id: string,
  signal?: AbortSignal,
): Promise<MoonshotRubric | null> {
  if (USE_MOCKS) return mock(MOCK_RUBRICS[candidate_id] ?? null);
  try {
    return await request<MoonshotRubric>(`/public/candidates/${candidate_id}/rubric`, { signal });
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) return null;
    throw err;
  }
}
