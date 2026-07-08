/**
 * Auth server actions — sign in / sign up / sign out.
 *
 * AUTH specialist owns lib/auth/*.
 *
 * These are Server Actions usable directly from a <form action={...}> or a
 * client component (e.g. a "Sign out" button in the app shell built by POLISH).
 * AuthKit is headless, so sign-in/up are redirects to the hosted authorization
 * URL; sign-out clears the sealed session cookie and ends the WorkOS session.
 */
"use server";

import { redirect } from "next/navigation";
import {
  getSignInUrl,
  getSignUpUrl,
  signOut,
} from "@workos-inc/authkit-nextjs";

/** Redirect to AuthKit hosted sign-in. */
export async function signInAction(): Promise<void> {
  redirect(await getSignInUrl());
}

/** Redirect to AuthKit hosted sign-up (apply flow lands collaborators in `pending`). */
export async function signUpAction(): Promise<void> {
  redirect(await getSignUpUrl());
}

/**
 * Clear the session. AuthKit's signOut() clears the sealed session cookie and
 * redirects to the logout-return configured in the WorkOS dashboard (set this
 * to the app landing page "/"). It takes no arguments in this AuthKit version.
 */
export async function signOutAction(): Promise<void> {
  await signOut();
}
