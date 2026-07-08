/**
 * twog API client — single import surface for both surfaces (Contribute + Operate).
 *
 * Usage:
 *   import { api } from "@/lib/api";
 *   const candidates = await api.candidates.list();
 *   await api.capsules.promote(id); // operator-only; the terminal human gate
 *
 * Every function returns domain types from @/lib/types/domain and is backed by
 * realistic fixtures when NEXT_PUBLIC_USE_MOCKS is on (the v1 default). The real
 * fetch path is wired but stubbed (no live backend yet).
 *
 * API-CLIENT owns lib/api/*.
 */

import * as collaborators from "./collaborators";
import * as sandbox from "./sandbox";
import * as contributions from "./contributions";
import * as candidates from "./candidates";
import * as campaigns from "./campaigns";
import * as capsules from "./capsules";
import * as targetLibrary from "./targetLibrary";
import * as engine from "./engine";

export const api = {
  collaborators,
  sandbox,
  contributions,
  candidates,
  campaigns,
  capsules,
  targetLibrary,
  engine,
} as const;

// Re-export config + error type for callers that need them directly.
export { ApiError, USE_MOCKS, API_BASE_URL } from "./config";

// Namespaced module re-exports (for `import { candidates } from "@/lib/api"`).
export {
  collaborators,
  sandbox,
  contributions,
  candidates,
  campaigns,
  capsules,
  targetLibrary,
  engine,
};

// Input/utility types each module defines, surfaced for form components.
export type { ApplyInput, ApproveInput } from "./collaborators";
export type {
  SigningMode,
  SubmitInput,
  SubmitEvidenceCapsuleInput,
  SubmitCandidateProposalInput,
  Contribution,
} from "./contributions";
export type { TargetLibraryEntry } from "./targetLibrary";
