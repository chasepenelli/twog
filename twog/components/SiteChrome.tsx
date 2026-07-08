'use client';

import { usePathname } from 'next/navigation';
import Nav from './Nav';
import Footer from './Footer';

// Every page is now v4-dark with its own <SiteNav>, so the old LIGHT global Nav/Footer render nowhere
// (LIGHT_ROUTES empty). Kept as a gate in case a light page is reintroduced; Nav.tsx/Footer.tsx are now
// otherwise unused.
const LIGHT_ROUTES: string[] = [];
function isLight(pathname: string): boolean {
  return LIGHT_ROUTES.some((r) => pathname === r || pathname.startsWith(r + '/'));
}

export function GlobalNav() {
  return isLight(usePathname()) ? <Nav /> : null;
}
export function GlobalFooter() {
  return isLight(usePathname()) ? <Footer /> : null;
}
