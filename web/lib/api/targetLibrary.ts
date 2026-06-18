/**
 * Verified-target catalog service. Entries carry a falsification-first verdict:
 * "verified" (survived structure + expression checks) or "refused" (rejected at
 * the gate — e.g. MTOR, refused on a bad pose).
 *
 * API-CLIENT owns lib/api/*.
 */

import type { TargetLibraryEntry } from "@/lib/types/domain";
import { MOCK_TARGET_LIBRARY } from "@/lib/mocks/fixtures";
import { USE_MOCKS, mock, request } from "./config";

export type { TargetLibraryEntry };

/** List verified-target catalog entries (verified + refused). */
export async function list(
  signal?: AbortSignal,
): Promise<TargetLibraryEntry[]> {
  if (USE_MOCKS) return mock(MOCK_TARGET_LIBRARY);
  return request<TargetLibraryEntry[]>("/target-library", { signal });
}
