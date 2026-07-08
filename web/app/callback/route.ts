/**
 * WorkOS AuthKit OAuth callback.
 *
 * AUTH specialist owns this route.
 *
 * AuthKit redirects the browser back here after sign-in; `handleAuth` exchanges
 * the code, seals the session cookie, and redirects on. We send freshly
 * authenticated users to /contribute (the default collaborator surface); the
 * scope-aware nav (POLISH) surfaces /operate to those who hold operator scopes.
 */
import { handleAuth } from "@workos-inc/authkit-nextjs";

export const GET = handleAuth({ returnPathname: "/contribute" });
