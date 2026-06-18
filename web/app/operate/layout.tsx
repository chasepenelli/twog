/**
 * Operator console layout. Wraps every /operate route with the section nav and
 * a framing note that the operator holds the terminal human gate. The global
 * shell + role-aware top nav is POLISH's concern; this is the operate surface's
 * own chrome.
 *
 * OPERATE owns app/operate/*.
 */

import * as React from "react";
import { redirect } from "next/navigation";

import { getPrincipal } from "@/lib/auth/server";
import { OperateNav } from "./_components/operate-nav";

export default async function OperateLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  // Server-side gate for the whole operator console. "Signed in" is never
  // sufficient in twog — the operator console (which fronts the terminal
  // accept/promote write gate) requires the operator role. Per-action operator
  // scopes are still asserted at the point of write; this segment guard turns
  // away any non-operator before the surface renders. Wired by POLISH; the
  // client-side ScopeNotice remains UX-only.
  const principal = await getPrincipal();
  if (!principal) redirect("/login");
  if (principal.role !== "operator") {
    redirect("/forbidden?scope=accept_capsule");
  }

  return (
    <div className="mx-auto w-full max-w-6xl px-4 py-6 sm:px-6 lg:py-8">
      <div className="mb-2 flex items-center gap-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">
        Operator console
      </div>
      <OperateNav />
      <main className="mt-6 space-y-6">{children}</main>
    </div>
  );
}
