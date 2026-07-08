/**
 * /login — kicks off WorkOS AuthKit sign-in.
 *
 * AUTH specialist owns app/login/*.
 *
 * AuthKit is headless, so "sign in" is simply a redirect to the hosted AuthKit
 * authorization URL (which WorkOS renders / themes). We compute it server-side
 * and 302 the browser. After auth, WorkOS returns to /callback (handleAuth),
 * which lands the user on /contribute.
 *
 * A `?return` query param lets a guard send the user back where they came from
 * once a backend honors it; for v1 we keep the default return path.
 */
import { redirect } from "next/navigation";
import { getSignInUrl } from "@workos-inc/authkit-nextjs";

export async function GET() {
  const signInUrl = await getSignInUrl();
  redirect(signInUrl);
}
