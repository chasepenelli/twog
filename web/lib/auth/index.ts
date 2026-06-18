/**
 * Public auth surface barrel.
 *
 * AUTH specialist owns lib/auth/*. Other specialists import from "@/lib/auth".
 *
 *   Server (Server Components / Route Handlers / Server Actions):
 *     getPrincipal, requirePrincipal, requireScope, assertScope, ScopeError
 *     withScope (route-handler guard)
 *     signInAction, signUpAction, signOutAction
 *   Client:
 *     useSession() — needs <SessionProvider/> (POLISH wires it into layout)
 *   Types/helpers:
 *     Principal, principalHasScope, resolveCollaboratorByAuthSubject, toPrincipal
 *
 * NOTE: server.ts is server-only and actions.ts is "use server"; import those
 * from server contexts. use-session.ts is "use client". This barrel re-exports
 * types freely and runtime symbols from their respective environments — import
 * client/server pieces from their specific modules if a bundler complains.
 */
export type { Principal, WorkOSUserLike } from "@/lib/auth/principal";
export {
  principalHasScope,
  resolveCollaboratorByAuthSubject,
  toPrincipal,
} from "@/lib/auth/principal";

export type { SessionValue } from "@/lib/auth/session-context";
