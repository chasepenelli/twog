'use client';

import { usePathname } from 'next/navigation';
import Nav from './Nav';
import Footer from './Footer';

// The old LIGHT global chrome now renders only on the remaining light pages (methods, until they're
// reskinned). Every v4-dark surface brings its own <SiteNav> — this stops the double header/footer and
// the light-chrome-over-dark-hero. Once all pages are v4-dark, delete this + the light Nav/Footer.
const LIGHT_ROUTES = ['/methods'];
function isLight(pathname: string): boolean {
  return LIGHT_ROUTES.some((r) => pathname === r || pathname.startsWith(r + '/'));
}

export function GlobalNav() {
  return isLight(usePathname()) ? <Nav /> : null;
}
export function GlobalFooter() {
  return isLight(usePathname()) ? <Footer /> : null;
}
