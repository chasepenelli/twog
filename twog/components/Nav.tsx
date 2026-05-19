'use client';

import { useRef, useState } from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { gsap } from 'gsap';
import { ScrollTrigger } from 'gsap/ScrollTrigger';
import { useGSAP } from '@gsap/react';
import { NAV_LINKS } from '@/lib/constants';

gsap.registerPlugin(ScrollTrigger);

export default function Nav() {
  const pathname = usePathname();
  const progressRef = useRef<HTMLDivElement>(null);
  const [menuOpen, setMenuOpen] = useState(false);

  useGSAP(() => {
    if (!progressRef.current) return;
    gsap.to(progressRef.current, {
      scaleX: 1,
      ease: 'none',
      scrollTrigger: {
        trigger: document.documentElement,
        start: 'top top',
        end: 'bottom bottom',
        scrub: 0.3,
      },
    });
  });

  return (
    <header className="fixed top-0 left-0 w-full z-50 px-6 py-4 flex items-center justify-between bg-[var(--background)] border-b border-[var(--gray-100)]">
      {/* Logo */}
      <Link
        href="/"
        className="text-[var(--foreground)] text-sm font-bold tracking-[0.15em] uppercase"
      >
        TWOG
      </Link>

      {/* Desktop links */}
      <nav className="hidden md:flex items-center gap-8">
        {NAV_LINKS.map(({ href, label }) => {
          const active = pathname === href;
          const className = `text-[0.7rem] uppercase tracking-[0.12em] transition-opacity duration-300 ${
            active
              ? 'text-[var(--foreground)] opacity-100'
              : 'text-[var(--foreground)] opacity-55 hover:opacity-100'
          }`;
          return href.startsWith('mailto:') ? (
            <a key={href} href={href} className={className}>
              {label}
            </a>
          ) : (
            <Link key={href} href={href} className={className}>
              {label}
            </Link>
          );
        })}
      </nav>

      {/* Mobile menu button */}
      <button
        className="md:hidden flex flex-col gap-[5px] p-1"
        onClick={() => setMenuOpen(!menuOpen)}
        aria-label="Toggle menu"
      >
        <span
          className="block w-5 h-[1.5px] bg-[var(--foreground)] transition-transform duration-300"
          style={menuOpen ? { transform: 'rotate(45deg) translate(2px, 2px)' } : {}}
        />
        <span
          className="block w-5 h-[1.5px] bg-[var(--foreground)] transition-opacity duration-300"
          style={menuOpen ? { opacity: 0 } : {}}
        />
        <span
          className="block w-5 h-[1.5px] bg-[var(--foreground)] transition-transform duration-300"
          style={menuOpen ? { transform: 'rotate(-45deg) translate(2px, -2px)' } : {}}
        />
      </button>

      {/* Live indicator */}
      <div className="hidden md:flex items-center gap-2">
        <span className="relative flex h-2 w-2">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75" />
          <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500" />
        </span>
        <span className="text-[0.65rem] uppercase tracking-[0.1em] text-[var(--gray-400)]">
          Pipeline Live
        </span>
      </div>

      {/* Mobile menu overlay */}
      {menuOpen && (
        <div className="md:hidden fixed inset-0 top-[57px] bg-[var(--background)] z-40 flex flex-col items-center pt-12 gap-6">
          {NAV_LINKS.map(({ href, label }) => (
            href.startsWith('mailto:') ? (
              <a
                key={href}
                href={href}
                onClick={() => setMenuOpen(false)}
                className="text-[1rem] uppercase tracking-[0.15em] transition-opacity duration-300 text-[var(--foreground)] opacity-55"
              >
                {label}
              </a>
            ) : (
              <Link
                key={href}
                href={href}
                onClick={() => setMenuOpen(false)}
                className={`text-[1rem] uppercase tracking-[0.15em] transition-opacity duration-300 ${
                  pathname === href
                    ? 'text-[var(--foreground)] opacity-100'
                    : 'text-[var(--foreground)] opacity-55'
                }`}
              >
                {label}
              </Link>
            )
          ))}
          <div className="flex items-center gap-2 mt-6">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75" />
              <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500" />
            </span>
            <span className="text-[0.65rem] uppercase tracking-[0.1em] text-[var(--gray-400)]">
              Pipeline Live
            </span>
          </div>
        </div>
      )}

      {/* Scroll progress bar */}
      <div
        ref={progressRef}
        className="absolute bottom-0 left-0 h-[2px] w-full origin-left"
        style={{ background: 'var(--accent)', transform: 'scaleX(0)' }}
      />
    </header>
  );
}
