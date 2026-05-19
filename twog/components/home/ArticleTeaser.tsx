'use client';

/**
 * ArticleTeaser — homepage preview of Issue 01.
 *
 * Shows the opening of the article, fades to the background color at the
 * bottom (so the prose feels "held back"), and counts down to the release
 * moment: Friday, April 24, 2026 at 12:00 noon Denver time (MDT, UTC-6).
 *
 * Before release: CTA is locked and reads "Unlocks Friday 12:00 MST".
 * After release: CTA becomes "Read Issue 01 →" and links to /issues/01.
 */

import { useEffect, useState } from 'react';
import Link from 'next/link';
import Image from 'next/image';
import graffitiPortrait from '@/app/assets/graffiti.png';

// Release moment — Denver local noon on Friday 2026-04-24.
// April is Mountain Daylight Time (UTC-6), so 12:00 MDT = 18:00 UTC.
const RELEASE_ISO = '2026-04-24T18:00:00.000Z';

function fmtRemaining(ms: number): string {
  if (ms <= 0) return 'Live';
  const s = Math.floor(ms / 1000);
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${pad(d)}d : ${pad(h)}h : ${pad(m)}m : ${pad(sec)}s`;
}

export default function ArticleTeaser() {
  const releaseAt = new Date(RELEASE_ISO).getTime();
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  const remaining = Math.max(0, releaseAt - now);
  const released = remaining === 0;

  return (
    <section
      className="px-6 py-20 md:py-28"
      aria-labelledby="teaser-title"
      style={{
        borderTop: '1px solid var(--gray-100)',
        borderBottom: '1px solid var(--gray-100)',
      }}
    >
      <div className="max-w-[820px] mx-auto">
        <div
          style={{
            fontFamily: 'var(--font-jetbrains-mono), monospace',
            fontSize: '0.68rem',
            letterSpacing: '0.26em',
            textTransform: 'uppercase',
            color: 'var(--gray-500)',
            marginBottom: 20,
            fontFeatureSettings: '"tnum" 1, "lnum" 1',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'baseline',
            gap: 16,
            flexWrap: 'wrap',
          }}
        >
          <span>Issue 01 &middot; Preview</span>
          <span style={{ color: 'var(--gray-400)' }}>Treatment track</span>
        </div>

        <h2
          id="teaser-title"
          style={{
            fontFamily: 'var(--font-space-mono), monospace',
            fontWeight: 700,
            fontSize: 'clamp(2.25rem, 5vw, 4rem)',
            letterSpacing: '-0.035em',
            lineHeight: 0.94,
            margin: 0,
            color: 'var(--foreground)',
            textTransform: 'none',
          }}
        >
          Three Drugs That Might Let Dogs Stay Home
        </h2>

        <div
          style={{
            fontFamily: 'var(--font-jetbrains-mono), monospace',
            fontSize: '0.7rem',
            letterSpacing: '0.22em',
            textTransform: 'uppercase',
            color: 'var(--gray-500)',
            marginTop: 18,
          }}
        >
          By Chase Penelli &middot; Issue 01 &middot; April 24, 2026
        </div>

        {/* Portrait with gradient fade into background */}
        <div
          className="teaser-portrait-wrap"
          style={{ position: 'relative', marginTop: 36, overflow: 'hidden', borderRadius: 3 }}
        >
          <Image
            src={graffitiPortrait}
            alt="Graffiti, a Pembroke Welsh Corgi, at home before his diagnosis."
            placeholder="blur"
            sizes="(max-width: 768px) 100vw, 820px"
            style={{
              width: '100%',
              height: 'auto',
              display: 'block',
              objectFit: 'cover',
            }}
          />

          <div
            aria-hidden
            className="teaser-fade"
            style={{
              position: 'absolute',
              left: 0,
              right: 0,
              bottom: 0,
              height: '45%',
              pointerEvents: 'none',
              background:
                'linear-gradient(180deg, rgba(255,255,255,0) 0%, var(--background) 95%)',
            }}
          />
        </div>

        {/* Countdown / CTA */}
        <div
          className="teaser-release"
          style={{
            marginTop: 28,
            paddingTop: 24,
            borderTop: '1px solid var(--gray-100)',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            flexWrap: 'wrap',
            gap: 20,
          }}
        >
          <div>
            <div
              style={{
                fontFamily: 'var(--font-jetbrains-mono), monospace',
                fontSize: '0.62rem',
                letterSpacing: '0.3em',
                textTransform: 'uppercase',
                color: 'var(--gray-500)',
                marginBottom: 8,
                fontFeatureSettings: '"tnum" 1, "lnum" 1',
              }}
            >
              {released ? 'Released' : 'Releases'}
            </div>
            <div
              style={{
                fontFamily: 'var(--font-space-mono), monospace',
                fontSize: '1.1rem',
                letterSpacing: '-0.01em',
                color: 'var(--foreground)',
                fontWeight: 700,
              }}
            >
              {released
                ? 'Friday, April 24 · 12:00 noon MST'
                : 'Friday, April 24 · 12:00 noon MST'}
            </div>
            <div
              className="teaser-countdown"
              style={{
                fontFamily: 'var(--font-jetbrains-mono), monospace',
                fontSize: '0.82rem',
                letterSpacing: '0.14em',
                color: 'var(--gray-600)',
                marginTop: 8,
                fontFeatureSettings: '"tnum" 1, "lnum" 1',
              }}
              aria-live="polite"
            >
              {released ? 'Now live' : fmtRemaining(remaining)}
            </div>
          </div>

          <div>
            {released ? (
              <Link
                href="/issues/01"
                className="teaser-cta teaser-cta-live"
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 10,
                  padding: '14px 22px',
                  fontFamily: 'var(--font-space-mono), monospace',
                  fontSize: '0.8rem',
                  letterSpacing: '0.2em',
                  textTransform: 'uppercase',
                  fontWeight: 700,
                  color: 'var(--background)',
                  background: 'var(--foreground)',
                  border: '1px solid var(--foreground)',
                  borderRadius: 3,
                  textDecoration: 'none',
                  minHeight: 44,
                }}
              >
                Read Issue 01 <span aria-hidden>&rarr;</span>
              </Link>
            ) : (
              <span
                className="teaser-cta teaser-cta-locked"
                aria-disabled="true"
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 10,
                  padding: '14px 22px',
                  fontFamily: 'var(--font-space-mono), monospace',
                  fontSize: '0.8rem',
                  letterSpacing: '0.2em',
                  textTransform: 'uppercase',
                  fontWeight: 700,
                  color: 'var(--gray-500)',
                  background: 'var(--background)',
                  border: '1px solid var(--gray-200)',
                  borderRadius: 3,
                  minHeight: 44,
                  cursor: 'not-allowed',
                }}
              >
                Unlocks Friday 12:00 MST
              </span>
            )}
          </div>
        </div>
      </div>

      <style jsx>{`
        .teaser-cta-live:hover,
        .teaser-cta-live:focus-visible {
          background: var(--background);
          color: var(--foreground);
        }
        .teaser-cta-live:focus-visible,
        .teaser-cta-locked:focus-visible {
          outline: 2px solid var(--green);
          outline-offset: 4px;
        }
      `}</style>
    </section>
  );
}
