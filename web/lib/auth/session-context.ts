/**
 * Client session context shape (shared by SessionProvider + useSession).
 *
 * AUTH specialist owns lib/auth/*.
 *
 * The twog Principal carries authorization (role + scopes) that WorkOS itself
 * does not know about, so we resolve it on the SERVER and pass it down. This
 * file holds only the context object + types so both the provider (a client
 * component) and the hook can import it without a cycle.
 */
"use client";

import { createContext } from "react";
import type { Scope } from "@/lib/types/domain";
import type { Principal } from "@/lib/auth/principal";

export interface SessionValue {
  /** Resolved twog principal, or null when signed out. */
  principal: Principal | null;
  /** Convenience: signed in at all? */
  isAuthenticated: boolean;
  /** Convenience: signed in AND an active collaborator? */
  isActive: boolean;
  /** Does the current principal hold this scope? */
  hasScope: (scope: Scope) => boolean;
  /** Is the current principal an operator? */
  isOperator: boolean;
}

export const SessionContext = createContext<SessionValue | undefined>(
  undefined,
);
