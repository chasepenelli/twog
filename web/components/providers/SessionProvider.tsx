/**
 * SessionProvider — wires WorkOS AuthKit's provider AND exposes the resolved
 * twog Principal (role + scopes) to client components.
 *
 * AUTH specialist owns components/providers/*. This component is EXPORTED for
 * POLISH to wire into app/layout.tsx; AUTH does NOT edit the layout itself.
 *
 * Usage (in app/layout.tsx, by POLISH):
 *   import { SessionProvider } from "@/components/providers/SessionProvider";
 *   import { getPrincipal } from "@/lib/auth/server";
 *   // ...
 *   const principal = await getPrincipal();
 *   return (
 *     <html><body>
 *       <SessionProvider principal={principal}>{children}</SessionProvider>
 *     </body></html>
 *   );
 *
 * The `principal` prop is resolved on the SERVER (so scopes are trustworthy)
 * and handed to the client. AuthKitProvider underneath keeps the WorkOS session
 * fresh and powers sign-out.
 */
"use client";

import { useMemo } from "react";
import { AuthKitProvider } from "@workos-inc/authkit-nextjs/components";
import type { Scope } from "@/lib/types/domain";
import type { Principal } from "@/lib/auth/principal";
import { SessionContext, type SessionValue } from "@/lib/auth/session-context";

export interface SessionProviderProps {
  /** Server-resolved principal (null when signed out). */
  principal: Principal | null;
  children: React.ReactNode;
}

export function SessionProvider({
  principal,
  children,
}: SessionProviderProps) {
  const value = useMemo<SessionValue>(() => {
    const scopes = new Set<Scope>(principal?.scopes ?? []);
    return {
      principal,
      isAuthenticated: principal != null,
      isActive: principal?.isActive ?? false,
      hasScope: (scope: Scope) => scopes.has(scope),
      isOperator: principal?.role === "operator",
    };
  }, [principal]);

  return (
    <AuthKitProvider>
      <SessionContext.Provider value={value}>
        {children}
      </SessionContext.Provider>
    </AuthKitProvider>
  );
}
