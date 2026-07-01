import Link from 'next/link';
import './v2.css';
import HeroCanvas from '@/components/v2/HeroCanvas';
import Reveal from '@/components/v2/Reveal';
import LiveLedger from '@/components/v2/LiveLedger';
import StepsScroller from '@/components/v2/StepsScroller';

// Homepage redesign — P1b preview at /v2. White/black · IBM Plex · GSAP · WebGL · full-viewport dynamic
// sections (Sui-inspired). Live homepage (app/page.tsx) untouched. Cost of runs is never shown.

const LADDER = [
  ['Watch', 'Watch', 'Open the live ledger and see the engine run. No signup.', 'Watch it work →', '#ledger'],
  ['Follow', '1 click', 'Get the next result in your inbox when an experiment resolves.', 'Follow the build →', 'https://pushingc.substack.com/subscribe'],
  ['Suggest', 'Low', 'Name a drug, a target, or a question. We’ll put it on trial in public.', 'Suggest something →', '#contact'],
  ['Contribute', 'Hands on', 'Inspect a real record, pressure-test a result, return structured work.', 'Inspect records →', '/candidates'],
  ['Run an agent', 'All in', 'Spin up your own experiment under the same falsification-first rules.', 'Run an experiment →', '/architecture'],
] as const;

export default function V2Home() {
  return (
    <div className="v2-shell">
      {/* HERO — full viewport, WebGL field behind big type */}
      <section className="v2-hero" id="top">
        <HeroCanvas />
        <Reveal className="v2-hero__inner" immediate stagger>
          <p className="v2-kicker">Autonomous oncology engine</p>
          <h1>
            <span className="v2-line">Built to prove</span>
            <span className="v2-line v2-em">itself wrong.</span>
          </h1>
          <p className="v2-hero__sub">
            TWOG attacks its own cancer-drug ideas, runs the real experiment, and publishes
            every result — the wins and the dead ends. Starting with dogs.
          </p>
          <div className="v2-actions">
            <a href="#ledger" className="v2-btn v2-btn--primary">Watch it work →</a>
            <Link href="#involve" className="v2-btn v2-btn--ghost">Get involved</Link>
          </div>
        </Reveal>
        <div className="v2-scrollcue">scroll<span /></div>
      </section>

      {/* LIVE LEDGER — its own full-viewport band */}
      <section className="v2-section v2-section--tall" id="ledger">
        <Reveal className="v2-section__head">
          <p className="v2-kicker">The engine, in public</p>
          <h2>Watch it work, one honest step at a time.</h2>
          <p className="lead">
            Every experiment, live: it proposes a test, locks the rules, runs real compute, and
            decides — celebrating the dead ends as much as the wins. Click any step to inspect it.
          </p>
        </Reveal>
        <Reveal>
          <LiveLedger />
        </Reveal>
      </section>

      {/* HOW IT WORKS — pinned numbered sequence */}
      <StepsScroller />

      {/* GET INVOLVED */}
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
    </div>
  );
}
