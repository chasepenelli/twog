import Link from 'next/link';
import '../v2/v2.css';
import './v3.css';
import HeroCanvas from '@/components/v2/HeroCanvas';
import Reveal from '@/components/v2/Reveal';
import Marquee from '@/components/v2/Marquee';
import LiveLedger from '@/components/v2/LiveLedger';
import StepsScroller from '@/components/v2/StepsScroller';
import { Placeholder } from '@/components/v2/Placeholder';
import { ProofStamp } from '@/components/v2/ProofStamp';

// /v3 — the "Portal" landing skeleton. One continuous scroll assembled from the layout library
// (see threadnotes/landing-design-package.pdf). Placeholders mark where Nano Banana Pro assets land.
// White/black · IBM Plex · GSAP/Lenis · WebGL. Cost is never shown.

const LADDER = [
  ['Watch', 'Watch', 'Open the live ledger and see the engine run. No signup.', 'Watch it work →', '#ledger'],
  ['Follow', '1 click', 'Get the next result in your inbox when an experiment resolves.', 'Follow the build →', 'https://pushingc.substack.com/subscribe'],
  ['Suggest', 'Low', 'Name a drug, a target, or a question. We’ll put it on trial in public.', 'Suggest something →', '#contact'],
  ['Contribute', 'Hands on', 'Inspect a real record, pressure-test a result, return structured work.', 'Inspect records →', '/candidates'],
  ['Run an agent', 'All in', 'Spin up your own experiment under the same falsification-first rules.', 'Run an experiment →', '/architecture'],
] as const;

export default function V3() {
  return (
    <div className="v2-shell v3-shell">
      {/* ACT 0 — Portal Hero (A) */}
      <section className="v2-hero" id="top">
        <HeroCanvas />
        <Reveal className="v2-hero__inner" immediate stagger>
          <p className="v2-kicker">Autonomous oncology engine</p>
          <h1>
            <span className="v2-line">We taught an AI to <span className="v2-em">doubt itself.</span></span>
            <span className="v2-line">Then aimed it at cancer.</span>
          </h1>
          <p className="v2-hero__sub">
            It proposes a test, bets against it, runs the real thing, and posts the verdict —
            the wins and the dead ends, live.
          </p>
          <div className="v2-actions">
            <a href="#ledger" className="v2-btn v2-btn--primary">Watch it work →</a>
            <Link href="#involve" className="v2-btn v2-btn--ghost">Get involved</Link>
          </div>
        </Reveal>
        <div className="v2-scrollcue">scroll<span /></div>
      </section>

      {/* ACT 1 — Kinetic Marquee (B) */}
      <Marquee
        variant="ink"
        items={['Prove it wrong', 'Falsification first', 'Nothing auto-promoted', 'Published in public']}
      />

      {/* ACT 2 — Live Ledger as Index / Manifest (D) */}
      <section className="v2-section v2-section--tall" id="ledger">
        <Reveal className="v3-ledger-head">
          <div>
            <p className="v2-kicker">The engine, in public</p>
            <h2>Watch it work, one honest step at a time.</h2>
            <p className="lead">
              Every experiment, live: it proposes a test, locks the rules, runs real compute, and
              decides — celebrating the dead ends as much as the wins. Click any step to inspect it.
            </p>
          </div>
          <Placeholder id="A4" ratio="1 / 1" label="molecule mark" />
        </Reveal>
        <Reveal>
          <LiveLedger />
        </Reveal>
      </section>

      {/* ACT 3 — Pinned Scene Sequence (C) */}
      <StepsScroller />

      {/* ACT 4 — Editorial Split (E) */}
      <section className="v2-section v3-split" id="dogs">
        <Reveal className="v3-split__copy">
          <p className="v2-kicker">Why dogs, why now</p>
          <h2>Dogs are running out of time.</h2>
          <p className="lead">
            We start with hemangiosarcoma — an aggressive cancer that gives dogs almost no time.
            Dogs get the cancers we do, so fighting theirs is a real goal and an honest first
            step toward ours.
          </p>
        </Reveal>
        <Reveal className="v3-split__media">
          <Placeholder id="A2" ratio="3 / 4" label="B&W dog portrait" />
        </Reveal>
      </section>

      {/* ACT 5 — Get involved (B/D) */}
      <section className="v2-section" id="involve">
        <Reveal className="v2-section__head">
          <p className="v2-kicker">Get involved</p>
          <h2>Pick your altitude.</h2>
          <p className="lead">From just watching to running your own experiments — there’s a way in at every level.</p>
        </Reveal>
        <Reveal className="v2-ladder" stagger>
          {LADDER.map(([title, tier, body, cta, href]) => (
            <article className="v2-rung" key={title}>
              <span className="v2-rung__tier">{tier}</span>
              <strong>{title}</strong>
              <p>{body}</p>
              {href.startsWith('http') ? (
                <a href={href} target="_blank" rel="noopener noreferrer">{cta}</a>
              ) : (
                <a href={href}>{cta}</a>
              )}
            </article>
          ))}
        </Reveal>
      </section>

      {/* closing marquee + footer stamp */}
      <Marquee
        variant="outline"
        dir={1}
        items={['Get involved', 'Watch', 'Suggest', 'Contribute', 'Run an agent']}
      />
      <footer className="v3-footer">
        <ProofStamp verdict="killed" size="lg" />
        <p>TWOG — falsification first. Built in public.</p>
        <div className="v3-footer__links">
          <Link href="/candidates">Records</Link>
          <Link href="/architecture">Architecture</Link>
          <a href="https://pushingc.substack.com/subscribe" target="_blank" rel="noopener noreferrer">Field notes</a>
        </div>
      </footer>
    </div>
  );
}
