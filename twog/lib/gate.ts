// The (honest) gate for the deep experiment tier. There is NO real auth yet — the deep detail is behind a
// newsletter-style email capture: give an email (collected for early access), and a `twog_email` cookie
// unlocks the deep tier. `?view=public` always previews the teaser-only state.

import { cookies } from 'next/headers';

export const EMAIL_COOKIE = 'twog_email';

export interface Viewer {
  tier: 'member' | 'public';
  email?: string;
}

export async function getViewer(view?: string): Promise<Viewer> {
  if (view === 'public') return { tier: 'public' };
  const jar = await cookies(); // Next 16: async
  const email = jar.get(EMAIL_COOKIE)?.value;
  return email ? { tier: 'member', email } : { tier: 'public' };
}
