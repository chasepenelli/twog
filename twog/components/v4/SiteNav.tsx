import Link from 'next/link';

// One shared v4-dark nav bar for the whole site (replaces the ad-hoc per-page v4-dnav link sets and the
// old light global Nav). `back` optionally overrides the home crumb (e.g. "Candidates" on a detail page).
export function SiteNav({ back }: { back?: { href: string; label: string } } = {}) {
  const home = back ?? { href: '/', label: 'twog' };
  return (
    <nav className="v4-dnav">
      <Link href={home.href} className="v4-dnav__home">{home.label}</Link>
      <Link href="/candidates">Candidates</Link>
      <Link href="/evidence">Evidence</Link>
      <Link href="/runs">Runs</Link>
      <Link href="/orchestration">The director</Link>
      <span className="v4-dnav__spacer" />
      <Link href="/involved">Get involved</Link>
    </nav>
  );
}
