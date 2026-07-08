/**
 * Sandbox service: lease a workspace and receive the bundle that describes the
 * leased environment + the contribution modes it accepts.
 *
 * API-CLIENT owns lib/api/*.
 */

import type { SandboxBundle } from "@/lib/types/domain";
import { MOCK_SANDBOX_BUNDLE } from "@/lib/mocks/fixtures";
import { USE_MOCKS, mock, request } from "./config";

/** Open (lease) a sandbox workspace for the current collaborator. */
export async function open(signal?: AbortSignal): Promise<SandboxBundle> {
  if (USE_MOCKS) return mock(MOCK_SANDBOX_BUNDLE);
  return request<SandboxBundle>("/sandbox/open", { method: "POST", signal });
}
