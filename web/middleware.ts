/**
 * WorkOS AuthKit middleware.
 *
 * AUTH specialist owns this file.
 *
 * Auth runs in ENFORCING mode (`middlewareAuth.enabled: true`): every matched
 * route requires a signed-in user EXCEPT those in `unauthenticatedPaths` (the
 * public allowlist) — an unauthenticated request elsewhere is redirected to
 * AuthKit. Being signed in is necessary but NOT sufficient: access in twog is
 * gated by SCOPE, so per-surface guards (see `lib/auth/server.ts`) turn away a
 * logged-in collaborator who lacks, e.g., `accept_capsule` at the operator
 * write gate. Authentication here + scope checks per surface = defense in depth.
 */
import { NextResponse, type NextRequest } from "next/server";
import type { NextFetchEvent } from "next/server";
import { authkitMiddleware } from "@workos-inc/authkit-nextjs";

const authkit = authkitMiddleware({
  middlewareAuth: {
    enabled: true,
    // PUBLIC allowlist — reachable WITHOUT signing in. Every other matched route
    // requires authentication (then the per-surface guards enforce scope).
    unauthenticatedPaths: ["/", "/login", "/callback"],
  },
});

export default function middleware(req: NextRequest, ev: NextFetchEvent) {
  // DEV-ONLY bypass (server-only env, OFF by default). Skips WorkOS enforcement so the app renders on
  // mock data without an account. getPrincipal() returns a synthetic operator under the same flag.
  if (process.env.DEV_AUTH_BYPASS === "1") return NextResponse.next();
  return authkit(req, ev);
}

export const config = {
  matcher: [
    // Run on everything except Next internals and static files.
    "/((?!_next/static|_next/image|favicon.ico|.*\\.(?:svg|png|jpg|jpeg|gif|webp|ico)$).*)",
  ],
};
