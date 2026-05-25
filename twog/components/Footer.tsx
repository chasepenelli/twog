import Link from 'next/link';
import { CONTACT_EMAIL, CONTACT_MAILTO } from '@/lib/constants';

export default function Footer() {
  return (
    <footer className="border-t border-[var(--gray-200)] py-16 px-6">
      <div className="max-w-6xl mx-auto flex flex-col gap-10">
        <div className="flex flex-col md:flex-row md:items-start md:justify-between gap-8">
          <div className="text-center md:text-left">
            <p className="text-[0.65rem] uppercase tracking-[0.15em] text-[var(--gray-400)] mb-3">
              Proof Network
            </p>
            <div className="flex flex-wrap items-center justify-center md:justify-start gap-x-6 gap-y-2">
              <Link
                href="/network"
                className="text-[0.6rem] uppercase tracking-[0.12em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors duration-300"
              >
                Open packets
              </Link>
              <Link
                href="/leaderboard"
                className="text-[0.6rem] uppercase tracking-[0.12em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors duration-300"
              >
                Leaderboard
              </Link>
              <Link
                href="/connect"
                className="text-[0.6rem] uppercase tracking-[0.12em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors duration-300"
              >
                Install agent
              </Link>
              <Link
                href="/network/flow"
                className="text-[0.6rem] uppercase tracking-[0.12em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors duration-300"
              >
                How work flows
              </Link>
            </div>
          </div>
        </div>
      <div className="flex flex-col md:flex-row items-center justify-between gap-6">
        <div className="text-center md:text-left">
          <p className="text-[0.65rem] uppercase tracking-[0.15em] text-[var(--gray-400)]">
            For Graffiti &amp; Brady &mdash; The Work of Graffiti &mdash; Built by Chase
          </p>
        </div>
        <div className="flex flex-wrap items-center justify-center gap-6">
          <Link
            href="/architecture"
            className="text-[0.6rem] uppercase tracking-[0.12em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors duration-300"
          >
            Architecture
          </Link>
          <a
            href={CONTACT_MAILTO}
            className="text-[0.6rem] uppercase tracking-[0.12em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors duration-300"
          >
            Contact
          </a>
          <a
            href="https://pushingc.substack.com"
            target="_blank"
            rel="noopener noreferrer"
            className="text-[0.6rem] uppercase tracking-[0.12em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors duration-300"
          >
            Substack
          </a>
          <a
            href="https://www.instagram.com/bradythecorgi/"
            target="_blank"
            rel="noopener noreferrer"
            className="text-[0.6rem] uppercase tracking-[0.12em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors duration-300"
          >
            Instagram
          </a>
          <a
            href={CONTACT_MAILTO}
            className="text-[0.6rem] uppercase tracking-[0.12em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors duration-300"
          >
            {CONTACT_EMAIL}
          </a>
        </div>
      </div>
      </div>
    </footer>
  );
}
