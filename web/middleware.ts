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

// Constructed lazily + only when WorkOS is configured, so module load never crashes the public site
// when no WorkOS keys are present (the public Phase-1 launch).
let _authkit: ReturnType<typeof authkitMiddleware> | null = null;
function getAuthkit() {
  if (!_authkit) {
    _authkit = authkitMiddleware({
      middlewareAuth: {
        enabled: true,
        // PUBLIC allowlist — reachable WITHOUT signing in. The public Phase-1 surface
        // (STATE / EVIDENCE / RUNS) is open; everything else requires auth once WorkOS is configured.
        unauthenticatedPaths: [
          "/", "/login", "/callback",
          "/evidence", "/evidence/:path*",
          "/runs", "/runs/:path*",
        ],
      },
    });
  }
  return _authkit;
}

export default function middleware(req: NextRequest, ev: NextFetchEvent) {
  // DEV-ONLY bypass (server-only env). AND: when WorkOS isn't configured (public Phase-1 launch, no
  // keys), pass everything through — the site is fully public until WorkOS is set up, at which point
  // enforcement engages automatically. Per-surface guards still apply once a principal exists.
  if (process.env.DEV_AUTH_BYPASS === "1" || !process.env.WORKOS_API_KEY) {
    return NextResponse.next();
  }
  return getAuthkit()(req, ev);
}

export const config = {
  matcher: [
    // Run on everything except Next internals and static files.
    "/((?!_next/static|_next/image|favicon.ico|.*\\.(?:svg|png|jpg|jpeg|gif|webp|ico)$).*)",
  ],
};
