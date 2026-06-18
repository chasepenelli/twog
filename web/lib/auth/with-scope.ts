/**
 * `withScope()` — route guard wrapper for App Router Route Handlers.
 *
 * AUTH specialist owns lib/auth/*.
 *
 * Wrap an API handler so it only runs when the caller is signed in AND holds
 * the required scope; otherwise it returns 401/403 JSON. The resolved
 * {@link Principal} is injected as the handler's first argument so the body
 * never re-derives identity. Operator-only scopes (accept_capsule,
 * promote_candidate) protect the terminal write-gate endpoints.
 *
 * @example
 *   // app/api/capsules/[id]/accept/route.ts
 *   import { withScope } from "@/lib/auth/with-scope";
 *
 *   export const POST = withScope("accept_capsule", async (principal, req, ctx) => {
 *     // ...accept the capsule; principal.role === "operator" is guaranteed-ish
 *     return Response.json({ ok: true, acceptedBy: principal.email });
 *   });
 */
import { type NextRequest, NextResponse } from "next/server";
import type { Scope } from "@/lib/types/domain";
import { type Principal } from "@/lib/auth/principal";
import { assertScope, ScopeError } from "@/lib/auth/server";

/** A scope-guarded route handler. Receives the resolved principal first. */
export type GuardedHandler<Ctx = unknown> = (
  principal: Principal,
  request: NextRequest,
  context: Ctx,
) => Promise<Response> | Response;

/**
 * Returns a Route Handler that enforces `scope` before delegating to `handler`.
 * On failure it short-circuits with a JSON error (401 unauthenticated /
 * 403 forbidden) and never invokes the wrapped handler.
 */
export function withScope<Ctx = unknown>(
  scope: Scope,
  handler: GuardedHandler<Ctx>,
): (request: NextRequest, context: Ctx) => Promise<Response> {
  return async (request: NextRequest, context: Ctx): Promise<Response> => {
    let principal: Principal;
    try {
      principal = await assertScope(scope);
    } catch (err) {
      if (err instanceof ScopeError) {
        const status = err.reason === "unauthenticated" ? 401 : 403;
        return NextResponse.json(
          { error: err.reason, required_scope: err.scope, message: err.message },
          { status },
        );
      }
      throw err;
    }
    return handler(principal, request, context);
  };
}
