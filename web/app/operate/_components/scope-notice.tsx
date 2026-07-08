"use client";

/**
 * ScopeNotice — advisory banner shown when the signed-in principal lacks an
 * operator-only scope needed for an action on this page. Client-side scope
 * checks are UX-only; the authoritative gate is the server (withScope). We read
 * the session defensively so these pages still render in isolation (before
 * POLISH wires <SessionProvider/> into the layout) by treating an absent
 * provider as "unknown — show controls, let the server decide".
 *
 * OPERATE owns app/operate/*.
 */

import * as React from "react";
import { ShieldAlert } from "lucide-react";

import type { Scope } from "@/lib/types/domain";
import { SessionContext } from "@/lib/auth/session-context";

/**
 * Safe scope read. Returns `undefined` when no SessionProvider is mounted (so
 * callers can choose to fail-open for UX) and a boolean otherwise.
 */
export function useMaybeScope(scope: Scope): boolean | undefined {
  const ctx = React.useContext(SessionContext);
  if (ctx === undefined) return undefined;
  return ctx.hasScope(scope);
}

export function useMaybeOperator(): boolean | undefined {
  const ctx = React.useContext(SessionContext);
  if (ctx === undefined) return undefined;
  return ctx.isOperator;
}

export function ScopeNotice({
  scope,
  children,
}: {
  scope: Scope;
  children: React.ReactNode;
}) {
  const has = useMaybeScope(scope);
  // Only warn when we positively know the scope is missing.
  if (has !== false) return null;
  return (
    <div
      role="note"
      className="flex items-start gap-2 rounded-md border border-warning/40 bg-warning-subtle/50 px-3 py-2 text-sm text-warning"
    >
      <ShieldAlert className="mt-0.5 size-4 shrink-0" aria-hidden />
      <span>{children}</span>
    </div>
  );
}
