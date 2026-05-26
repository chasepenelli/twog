import Link from 'next/link';

export const metadata = {
  title: 'Not Found — TWOG',
  description: 'The page you tried to load is not here. The Proof Network is.',
};

export default function NotFound() {
  return (
    <div className="site-shell page-shell">
      <section className="network-hero">
        <div className="network-hero-copy">
          <p className="section-kicker">TWOG / Proof Network</p>
          <h1>We couldn&rsquo;t find that page.</h1>
          <p>
            The page you tried to load isn&rsquo;t here. The Proof Network is
            still here, though.
          </p>
          <div className="network-hero-actions">
            <Link href="/network" className="network-cta primary">
              See open packets
            </Link>
            <Link href="/leaderboard" className="network-cta">
              Top contributors
            </Link>
            <Link href="/connect" className="network-cta">
              Install your agent
            </Link>
          </div>
        </div>
      </section>
    </div>
  );
}
