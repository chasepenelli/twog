/**
 * Mock collaborator lookup bridge (cross-module integration, wired by POLISH).
 *
 * `lib/auth/principal.ts` (AUTH) resolves the signed-in WorkOS email to a twog
 * Collaborator by dynamically importing `@/lib/mocks/collaborators` and probing
 * for one of: `mockCollaborators`, `collaborators`, `COLLABORATORS`, `default`.
 * The fixtures barrel (API-CLIENT) instead exports `MOCK_COLLABORATORS`, so the
 * import silently fell back to an empty list — meaning NO signed-in user ever
 * resolved a collaborator record (operators included) and every privileged
 * surface 403'd. This thin module re-exports the canonical fixtures under the
 * names AUTH probes for, closing that gap without editing either owner's file.
 *
 * The stable account→principal key is `auth_subject` (the WorkOS user id). Each
 * fixture gets a deterministic fake `auth_subject` so resolution works in mock
 * mode the same way it will against the real backend. For local demos where you
 * don't know your WorkOS user id, set NEXT_PUBLIC_DEV_OPERATOR_AUTH_SUBJECT to it
 * (binds the operator by id), or rely on the email DEV fallback
 * (NEXT_PUBLIC_DEV_OPERATOR_EMAIL, default the project owner's address) — AUTH's
 * resolver matches auth_subject first, then email.
 */
import type { Collaborator } from "@/lib/types/domain";
import { MOCK_COLLABORATORS } from "@/lib/mocks/fixtures";

/** Email used to bind the mock operator to a real AuthKit login during demos. */
const DEV_OPERATOR_EMAIL =
  process.env.NEXT_PUBLIC_DEV_OPERATOR_EMAIL ?? "chasepenelli@gmail.com";
/** Optional: bind the operator to a real WorkOS user id (the production-shaped key). */
const DEV_OPERATOR_AUTH_SUBJECT = process.env.NEXT_PUBLIC_DEV_OPERATOR_AUTH_SUBJECT ?? "";

/**
 * Fixtures augmented with a stable `auth_subject` (and a dev `email`/override on
 * the operator) so AuthKit logins resolve in mock mode by the same key the real
 * backend uses (the WorkOS user id).
 */
export const mockCollaborators: (Collaborator & { email?: string })[] =
  MOCK_COLLABORATORS.map((c) => {
    const withSubject: Collaborator & { email?: string } = {
      ...c,
      auth_subject: c.auth_subject ?? `workos_user_${c.principal}`,
    };
    if (c.collaborator_id === "col_operator_root") {
      withSubject.email = DEV_OPERATOR_EMAIL;
      if (DEV_OPERATOR_AUTH_SUBJECT) withSubject.auth_subject = DEV_OPERATOR_AUTH_SUBJECT;
    }
    return withSubject;
  });

// Aliases AUTH's loader also accepts, so the binding is robust to naming.
export const collaborators = mockCollaborators;
export const COLLABORATORS = mockCollaborators;
export default mockCollaborators;
