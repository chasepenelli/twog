/**
 * useSession() — client hook exposing the resolved twog Principal + scope checks.
 *
 * AUTH specialist owns lib/auth/*.
 *
 * Reads the context populated by <SessionProvider/>. Use this in client
 * components to drive scope-aware UI (e.g. show the operator nav only to
 * operators, disable the "accept capsule" button without `accept_capsule`).
 *
 * NOTE: client-side scope checks are for UX only. The authoritative gate is the
 * server (`requireScope` / `withScope`); never trust the client for the write gate.
 *
 * @example
 *   const { isOperator, hasScope } = useSession();
 *   if (hasScope("promote_candidate")) { ...show promote control... }
 */
"use client";

import { useContext } from "react";
import { SessionContext, type SessionValue } from "@/lib/auth/session-context";

export function useSession(): SessionValue {
  const ctx = useContext(SessionContext);
  if (ctx === undefined) {
    throw new Error(
      "useSession() must be used within <SessionProvider/>. " +
        "POLISH wires SessionProvider into app/layout.tsx.",
    );
  }
  return ctx;
}
