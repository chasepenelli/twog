/**
 * Server-side auth helpers (Server Components, Route Handlers, Server Actions).
 *
 * AUTH specialist owns lib/auth/*.
 *
 * These wrap WorkOS AuthKit's `withAuth()` and layer twog's SCOPE model on top.
 * "Signed in" is necessary but never sufficient: every privileged surface must
 * additionally hold a scope. Operator-only scopes (accept_capsule,
 * promote_candidate) gate the terminal human write gate — nothing auto-promotes.
 */
import "server-only";
import { redirect } from "next/navigation";
import { withAuth, getSignInUrl } from "@workos-inc/authkit-nextjs";
import type { Scope } from "@/lib/types/domain";
import {
  type Principal,
  type WorkOSUserLike,
  resolveCollaboratorByAuthSubject,
  toPrincipal,
  principalHasScope,
} from "@/lib/auth/principal";

/** Error thrown when a guard fails and the caller wants to handle it (API routes). */
export class ScopeError extends Error {
  constructor(
    public readonly scope: Scope,
    public readonly reason: "unauthenticated" | "forbidden",
  ) {
    super(
      reason === "unauthenticated"
        ? "Not signed in."
        : `Missing required scope: ${scope}.`,
    );
    this.name = "ScopeError";
  }
}

/**
 * Resolve the current {@link Principal} on the server, or null if not signed in.
 * Combines the WorkOS session user with the twog collaborator record (mock
 * lookup in v1). Safe to call in any Server Component / Route Handler.
 */
export async function getPrincipal(): Promise<Principal | null> {
  // DEV-ONLY auth bypass (server-only env, OFF by default → never active in prod). Lets the app run
  // on mock data + render the authenticated UI without a WorkOS account (local dev + screenshots).
  // Resolves a synthetic operator via the same mock shim a real login would (by the dev operator email).
  if (process.env.DEV_AUTH_BYPASS === "1") {
    const email = process.env.NEXT_PUBLIC_DEV_OPERATOR_EMAIL ?? "chasepenelli@gmail.com";
    const collaborator = await resolveCollaboratorByAuthSubject("dev-bypass", email);
    return toPrincipal(
      { id: "dev-bypass", email, firstName: "Dev", lastName: "Operator" },
      collaborator,
    );
  }
  // Public Phase-1 launch: when WorkOS isn't configured, the AuthKit middleware is a pass-through, so
  // withAuth() has no context and would throw. Treat as unauthenticated — the public surfaces
  // (STATE / EVIDENCE / RUNS) need no principal; enforcement engages once WorkOS keys are set.
  if (!process.env.WORKOS_API_KEY) return null;
  const { user } = await withAuth();
  if (!user) return null;
  const userLike: WorkOSUserLike = {
    id: user.id,
    email: user.email,
    firstName: user.firstName,
    lastName: user.lastName,
  };
  // Map by the stable WorkOS user id (auth_subject); email is only a dev fallback.
  const collaborator = await resolveCollaboratorByAuthSubject(user.id, user.email);
  return toPrincipal(userLike, collaborator);
}

/**
 * Require a signed-in principal. Redirects to AuthKit sign-in if absent.
 * Use at the top of any authenticated Server Component / page.
 */
export async function requirePrincipal(): Promise<Principal> {
  const principal = await getPrincipal();
  if (!principal) {
    redirect(await getSignInUrl());
  }
  return principal;
}

/**
 * Require a specific scope. Redirects to sign-in when unauthenticated and to
 * `/forbidden` when signed in but lacking the scope. Returns the principal so
 * the page can render with it. This is the primary guard for Server Components.
 *
 * @example
 *   export default async function WriteGatePage() {
 *     const principal = await requireScope("accept_capsule");
 *     // ...render the operator write gate
 *   }
 */
export async function requireScope(scope: Scope): Promise<Principal> {
  const principal = await getPrincipal();
  if (!principal) {
    redirect(await getSignInUrl());
  }
  if (!principalHasScope(principal, scope)) {
    redirect(`/forbidden?scope=${encodeURIComponent(scope)}`);
  }
  return principal;
}

/**
 * Non-redirecting scope assertion for Route Handlers / Server Actions where you
 * want to return a structured error instead of navigating. Throws {@link ScopeError}.
 */
export async function assertScope(scope: Scope): Promise<Principal> {
  const principal = await getPrincipal();
  if (!principal) throw new ScopeError(scope, "unauthenticated");
  if (!principalHasScope(principal, scope)) {
    throw new ScopeError(scope, "forbidden");
  }
  return principal;
}
