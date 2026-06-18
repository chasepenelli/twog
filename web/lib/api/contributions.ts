/**
 * Contributions service: submit a contribution (evidence_capsule or
 * candidate_proposal) and track one's own submissions through the confound +
 * provenance gates toward the operator decision.
 *
 * A contribution may be signed two ways:
 *  - custodial: the platform signs on the collaborator's behalf.
 *  - self_held: the collaborator signs with their own key (signature provided).
 *
 * API-CLIENT owns lib/api/*.
 */

import type {
  Candidate,
  CapsuleSignal,
  ContributionMode,
  ProofCapsule,
} from "@/lib/types/domain";
import { MOCK_CAPSULES, MOCK_CANDIDATES } from "@/lib/mocks/fixtures";
import { USE_MOCKS, mock, request } from "./config";

/** How a submitted artifact is signed. */
export type SigningMode = "custodial" | "self_held";

interface BaseSubmitInput {
  mode: ContributionMode;
  signing_mode: SigningMode;
  /** Required when signing_mode is "self_held". */
  signature?: string;
}

export interface SubmitEvidenceCapsuleInput extends BaseSubmitInput {
  mode: "evidence_capsule";
  candidate_id: string;
  signal: CapsuleSignal;
  validation_type: string;
}

export interface SubmitCandidateProposalInput extends BaseSubmitInput {
  mode: "candidate_proposal";
  title: string;
  targets: string[];
}

export type SubmitInput =
  | SubmitEvidenceCapsuleInput
  | SubmitCandidateProposalInput;

/** A submitted contribution may resolve to a capsule or a proposed candidate. */
export type Contribution = ProofCapsule | Candidate;

/** Submit a contribution from a leased sandbox. Lands in the admission gates. */
export async function submit(input: SubmitInput): Promise<Contribution> {
  if (USE_MOCKS) {
    if (input.mode === "evidence_capsule") {
      const capsule: ProofCapsule = {
        capsule_id: `cap_new_${Date.now()}`,
        candidate_id: input.candidate_id,
        signal: input.signal,
        validation_type: input.validation_type,
        status: "submitted",
        signature: input.signing_mode === "self_held" ? input.signature : undefined,
        confound_verdict: "pending",
        provenance_verdict: "pending",
      };
      return mock<Contribution>(capsule);
    }
    const candidate: Candidate = {
      candidate_id: `cand_new_${Date.now()}`,
      title: input.title,
      public_status: "proposed",
      validation_ready: false,
      evidence_refs: [],
      targets: input.targets,
    };
    return mock<Contribution>(candidate);
  }
  return request<Contribution>("/contributions", {
    method: "POST",
    body: input,
  });
}

/**
 * List the current collaborator's own submitted contributions, so the
 * Contribute surface can track them through the gates to the operator decision.
 */
export async function listMine(signal?: AbortSignal): Promise<ProofCapsule[]> {
  if (USE_MOCKS) {
    // The Kestrel/Marrow collaborators' submissions in the fixture set.
    const mine = MOCK_CAPSULES.filter((c) =>
      ["cap_kdr_supports", "cap_kdr_neutral", "cap_mtor_refutes"].includes(
        c.capsule_id,
      ),
    );
    return mock(mine);
  }
  return request<ProofCapsule[]>("/contributions/mine", { signal });
}

/** Convenience: candidates available to attach evidence to, for the submit form. */
export async function listCandidateTargets(
  signal?: AbortSignal,
): Promise<Candidate[]> {
  if (USE_MOCKS) return mock(MOCK_CANDIDATES);
  return request<Candidate[]>("/candidates", { signal });
}
