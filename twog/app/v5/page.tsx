import './v5.css';
import { Placeholder } from '@/components/v2/Placeholder';

// /v5 — homage to Vaayu (vaayu.tech): editorial climate-tech. Warm concrete + cream + olive-green,
// electric chartreuse-lime highlighter, Fraunces serif display + Plex sans, terrazzo speckle.
// Hero + value band first; extends below in a later pass. Hero copy = locked direction #2.

const STEPS = [
  ['01', 'Pick a fight', 'It proposes the test most likely to kill an idea.'],
  ['02', 'Lock the rules', 'A pass/fail bar, sealed before any compute runs.'],
  ['03', 'Run it for real', 'Real GPU experiments, fully autonomous.'],
  ['04', 'Show the receipt', 'Published in public. A human decides what’s real.'],
] as const;

export default function V5() {
  return (
    <div className="v5-shell">
      <header className="v5-top">
        <span className="v5-logo">twog</span>
        <nav>
          <a href="#ledger">Live ledger</a>
          <a href="#how">How it works</a>
          <a href="/candidates">Records</a>
        </nav>
        <a href="#ledger" className="v5-btn v5-btn--lime">Watch it work →</a>
      </header>

      {/* HERO */}
      <section className="v5-hero">
        <div className="v5-hero__copy">
          <p className="v5-eyebrow">Autonomous oncology engine</p>
          <h1 className="v5-display">
            We taught an AI to <span className="v5-mark">doubt itself.</span>{' '}
            <span className="it">Then</span> aimed it at cancer.
          </h1>
          <p className="v5-sub">
            It proposes a test, bets against it, runs the real thing, and posts the verdict —
            the wins and the dead ends, live.
          </p>
          <div className="v5-actions">
            <a href="#ledger" className="v5-btn v5-btn--lime">Watch it work →</a>
            <a href="#how" className="v5-btn v5-btn--ghost">How it works</a>
          </div>
        </div>
        <div className="v5-hero__media">
          <Placeholder id="A2" ratio="4 / 5" label="B&W documentary portrait" />
          <span className="v5-badge">
            <strong>Live</strong>
            run a4c2036
          </span>
        </div>
      </section>

      {/* VALUE BAND */}
      <section className="v5-value" id="how">
        <div className="v5-value__inner">
          <h2>
            Most AI tries to be right. This one tries to be <span className="v5-mark">wrong.</span>
          </h2>
          <ol className="v5-value__steps">
            {STEPS.map(([n, t, d]) => (
              <li key={n}>
                <code>{n}</code>
                <span>
                  <strong>{t}</strong> — <span className="d">{d}</span>
                </span>
              </li>
            ))}
          </ol>
        </div>
      </section>
    </div>
  );
}
