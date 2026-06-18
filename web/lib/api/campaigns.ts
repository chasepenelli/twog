/**
 * Campaigns service: list falsification runs + fetch a single campaign report.
 * Every rollup carries the invariant any_promoted=false — nothing auto-promotes.
 *
 * API-CLIENT owns lib/api/*.
 */

import type { RunManifest } from "@/lib/types/domain";
import { MOCK_CAMPAIGNS } from "@/lib/mocks/fixtures";
import { ApiError, USE_MOCKS, mock, request } from "./config";

/** List all campaigns (RunManifests). */
export async function list(signal?: AbortSignal): Promise<RunManifest[]> {
  if (USE_MOCKS) return mock(MOCK_CAMPAIGNS);
  return request<RunManifest[]>("/public/campaigns", { signal });
}

/** Fetch a single campaign report by manifest id. Throws ApiError(404). */
export async function get(
  manifest_id: string,
  signal?: AbortSignal,
): Promise<RunManifest> {
  if (USE_MOCKS) {
    const found = MOCK_CAMPAIGNS.find((c) => c.manifest_id === manifest_id);
    if (!found) throw new ApiError(`Campaign not found: ${manifest_id}`, 404);
    return mock(found);
  }
  return request<RunManifest>(`/public/campaigns/${manifest_id}`, { signal });
}
