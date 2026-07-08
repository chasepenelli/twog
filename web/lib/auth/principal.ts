/**
 * Principal mapping: WorkOS user -> twog Collaborator.
 *
 * AUTH specialist owns lib/auth/*.
 *
 * In twog, identity and authorization are TWO different things. WorkOS AuthKit
 * answers "who is this human?" (a stable WorkOS user id + email). The backend
 * contract answers "what may this principal DO?" via a Collaborator record
 * (role + scopes + status). This module is the bridge.
 *
 * The stable link is the WorkOS user id (`auth_subject`), NOT the email — emails
 * can change or be reassigned, which would silently break or hijack the mapping.
 * For v1 (no live backend) we resolve against the mock fixtures by `auth_subject`,
 * with the authenticated email kept only as a DEV fallback (the demo shim). When
 * a real backend exists this becomes a `/me` fetch keyed on the WorkOS user id;
 * the SHAPE of `Principal` stays the same so callers don't change.
 */
import type {
  Collaborator,
  CollaboratorRole,
  Scope,
} from "@/lib/types/domain";

/**
 * The resolved identity used throughout the app. Combines the WorkOS-supplied
 * human identity with the twog Collaborator authorization record (if one
 * exists). A signed-in user with NO collaborator record is `known` but has an
 * empty scope set — they can apply, but can do nothing privileged.
 */
export interface Principal {
  /** WorkOS user id (stable auth subject). */
  userId: string;
  /** Authenticated email from WorkOS. */
  email: string;
  /** Display name (best-effort from WorkOS profile). */
  name: string;
  /** Matched twog Collaborator, or null if the email is not yet a collaborator. */
  collaborator: Collaborator | null;
  /** Convenience mirror of `collaborator.role`; defaults to "collaborator". */
  role: CollaboratorRole;
  /** Effective scopes (empty if no active collaborator record). */
  scopes: Scope[];
  /** True only when a matched collaborator is `active`. */
  isActive: boolean;
}

/** Minimal shape we need from a WorkOS user; avoids a hard dep on their types. */
export interface WorkOSUserLike {
  id: string;
  email: string;
  firstName?: string | null;
  lastName?: string | null;
}

/**
 * Best-effort import of the mock collaborator fixtures owned by API-CLIENT.
 * We support a few likely export names so the two work streams integrate
 * without a rigid contract on the fixture's symbol name. If none resolve we
 * fall back to an empty list (everyone is "known but unprivileged").
 */
async function loadMockCollaborators(): Promise<Collaborator[]> {
  try {
    const mod: Record<string, unknown> = await import("@/lib/mocks/collaborators");
    const candidates = [
      mod.mockCollaborators,
      mod.collaborators,
      mod.COLLABORATORS,
      mod.default,
    ];
    for (const c of candidates) {
      if (Array.isArray(c)) return c as Collaborator[];
    }
  } catch {
    // Fixtures not present yet (parallel build) — degrade gracefully.
  }
  return [];
}

/**
 * Resolve a Collaborator for an authenticated WorkOS user. Matches on the stable
 * `auth_subject` (WorkOS user id) FIRST; the email is only a DEV fallback for the
 * demo shim. v1 uses mock fixtures — swap the body for a `/me` backend fetch keyed
 * on `userId` when one exists (the `Principal` shape is unchanged so callers don't).
 */
export async function resolveCollaboratorByAuthSubject(
  userId: string,
  emailFallback?: string,
): Promise<Collaborator | null> {
  const all = await loadMockCollaborators();
  const id = (userId ?? "").trim();
  if (id) {
    const byId = all.find((c) => (c.auth_subject ?? "") === id);
    if (byId) return byId;
  }
  // DEV-ONLY fallback: bind by email (or principal===email) for local demos where
  // the WorkOS user id isn't yet wired onto a fixture. Never the primary key.
  const email = (emailFallback ?? "").trim().toLowerCase();
  if (!email) return null;
  return (
    all.find((c) => {
      const principal = (c.principal ?? "").toLowerCase();
      const emailField = ((c as unknown as { email?: string }).email ?? "").toLowerCase();
      return principal === email || emailField === email;
    }) ?? null
  );
}

/** Build a {@link Principal} from a WorkOS user + (optional) collaborator. */
export function toPrincipal(
  user: WorkOSUserLike,
  collaborator: Collaborator | null,
): Principal {
  const name =
    [user.firstName, user.lastName].filter(Boolean).join(" ").trim() ||
    collaborator?.name ||
    user.email;
  const isActive = collaborator?.status === "active";
  return {
    userId: user.id,
    email: user.email,
    name,
    collaborator,
    role: collaborator?.role ?? "collaborator",
    // Scopes are only effective for an ACTIVE collaborator. A pending or
    // revoked record grants nothing — falsification platform stays locked down.
    scopes: isActive ? collaborator?.scopes ?? [] : [],
    isActive,
  };
}

/** Does this principal hold the given scope (and is the collaborator active)? */
export function principalHasScope(p: Principal | null, scope: Scope): boolean {
  if (!p) return false;
  return p.scopes.includes(scope);
}
