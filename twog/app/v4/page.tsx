import Link from 'next/link';
import './v4.css';
import '../v2/v2.css';
import Marquee from '@/components/v2/Marquee';
import Reveal from '@/components/v2/Reveal';
import LiveLedger from '@/components/v2/LiveLedger';
import StepsScroller from '@/components/v2/StepsScroller';
import { PortalGlyph } from '@/components/v2/PortalGlyph';
import { Placeholder } from '@/components/v2/Placeholder';
import { ProofStamp } from '@/components/v2/ProofStamp';
import { HeroLatest } from '@/components/v2/HeroLatest';

// /v4 — dark retro-terminal "boot sequence" (Nifty Portal homage), full page. Positive hero: PROOF.
// top marquee = the primitives (instruction set); bottom = the positive promise.
const TOP = ['Hypothesis', 'The bar', 'Run', 'Gate', 'Verdict', 'Capsule', 'Ledger', 'Write-gate'];
const BOTTOM = ['Real proof', 'Tested in the open', 'Worth believing', 'Proof engine 001', 'Starting with dogs'];
const CLOSE = ['Real proof', 'Worth believing', 'Tested in the open', 'Built in public', 'Starting with dogs'];

const LADDER = [
  ['Watch', 'Watch', 'Open the live ledger and see the engine run. No signup.', 'Watch it work →', '#ledger'],
  ['Follow', '1 click', 'Get the next result in your inbox when an experiment resolves.', 'Follow the build →', 'https://pushingc.substack.com/subscribe'],
  ['Suggest', 'Low', 'Name a drug, a target, or a question. We’ll put it to the test in public.', 'Suggest something →', '/involved#suggest'],
  ['Contribute', 'Hands on', 'Inspect a real record, pressure-test a result, return structured work.', 'Inspect records →', '/candidates'],
  ['Run an agent', 'All in', 'Spin up your own experiment under the same rules.', 'Run an experiment →', '/architecture'],
] as const;

export default function V4() {
  return (
    <div className="v4-shell">
      <div className="v4-grain" />

      {/* ── HERO FRAME ── */}
      <header className="v4-frame">
        <Marquee variant="hud" dir={-1} items={TOP} />

        <div className="v4-wordmark-row">
          <h1 className="v4-wordmark">PROOF</h1>
          <div className="v4-lockup">
            <span>THE</span>
            <span>TWOG</span>
            <span>ENGINE</span>
          </div>
        </div>

        <Marquee variant="hud" dir={1} items={BOTTOM} />

        <div className="v4-hud">
          <div className="v4-hud__status">
            <span>System 001</span>
            <span className="glyph">✳</span>
            <span>Engine</span>
            <span className="glyph">▦</span>
            <span><HeroLatest /></span>
          </div>
          <div className="v4-hud__body">
            <div className="v4-hud__lead">
              <p className="v4-eyebrow">Initiate sequence</p>
              <p className="v4-strap">
                Real proof for real cancer breakthroughs — tested in the open, and <em>worth believing.</em>
              </p>
              <div>
                <a className="v4-cta" href="#ledger">Watch it work →</a>
                <a className="v4-cta v4-cta--ghost" href="#involve">Get involved</a>
              </div>
            </div>
            <div className="v4-hud__glyph">
              <PortalGlyph className="v4-portal" />
              <p className="v4-cap">proof engine · online</p>
            </div>
          </div>
        </div>
      </header>

      {/* ── LEDGER ── */}
      <section className="v4-sec v4-sec--tall" id="ledger">
        <Reveal className="v4-sec__head">
          <p className="v2-kicker">The ledger</p>
          <h2 className="v4-h2">Every step, on the record.</h2>
          <p className="v4-lead">
            The engine works in public: it proposes a test, locks the bar, runs real compute, and
            posts a verdict — wins and dead ends alike. Click any step to inspect the proof.
          </p>
        </Reveal>
        <Reveal>
          <LiveLedger />
        </Reveal>
      </section>

      {/* ── HOW IT WORKS (pinned stepper) ── */}
      <StepsScroller />

      {/* ── WHY DOGS ── */}
      <section className="v4-sec v4-split" id="dogs">
        <Reveal className="v4-split__copy">
          <p className="v2-kicker">Why dogs, why now</p>
          <h2 className="v4-h2">Proof that helps dogs first.</h2>
          <p className="v4-lead">
            We start with hemangiosarcoma — an aggressive cancer that gives dogs almost no time.
            Dogs get the cancers we do, so proving what works for them is a real goal and an honest
            first step toward people.
          </p>
        </Reveal>
        <Reveal className="v4-split__media">
          <Placeholder id="A2" ratio="3 / 4" label="B&W dog portrait" />
        </Reveal>
      </section>

      {/* ── GET INVOLVED ── */}
      <section className="v4-sec" id="involve">
        <Reveal className="v4-sec__head">
          <p className="v2-kicker">Get involved</p>
          <h2 className="v4-h2">Pick your altitude.</h2>
          <p className="v4-lead">From just watching to running your own experiments — there’s a way in at every level.</p>
        </Reveal>
        <Reveal className="v4-ladder" stagger>
          {LADDER.map(([title, tier, body, cta, href]) => (
            <article className="v4-rung" key={title}>
              <span className="v4-rung__tier">{tier}</span>
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

      <Marquee variant="hud" dir={-1} items={CLOSE} />
      <footer className="v4-footer">
        <ProofStamp verdict="survived" size="lg" />
        <p>TWOG — proof, not promises. Built in public.</p>
        <div className="v4-footer__links">
          <Link href="/runs">Runs</Link>
          <Link href="/evidence">Evidence</Link>
          <Link href="/candidates">Records</Link>
          <Link href="/involved">Get involved</Link>
          <Link href="/architecture">Architecture</Link>
          <a href="https://pushingc.substack.com/subscribe" target="_blank" rel="noopener noreferrer">Field notes</a>
        </div>
      </footer>
    </div>
  );
}
