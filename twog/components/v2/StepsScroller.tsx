'use client';

// The dynamic centerpiece section (Sui-style numbered stack): the section PINS to the viewport and the
// active step advances 01 → 05 as you scroll, big number on the left, the full list on the right. With
// only a few sections, this turns one section into a paced, scroll-driven sequence.
// Fallback: prefers-reduced-motion (or no JS) → a normal static list, fully readable.

import { useEffect, useRef, useState } from 'react';
import { gsap } from 'gsap';
import { ScrollTrigger } from 'gsap/ScrollTrigger';

gsap.registerPlugin(ScrollTrigger);

const STEPS: [string, string, string][] = [
  ['01', 'Pick a fight', 'It chooses the test most likely to KILL a drug idea — not the one most likely to flatter it.'],
  ['02', 'Lock the rules', 'It writes down what counts as pass or fail and seals it before running. The goalposts can’t move.'],
  ['03', 'Get its own inputs', 'It fetches the molecular data itself — no human hand-feeding.'],
  ['04', 'Run the real test', 'A real physics simulation on a GPU.'],
  ['05', 'Show the receipt', 'It double-checks, publishes the record, and a human decides what’s real.'],
];

export default function StepsScroller() {
  const ref = useRef<HTMLDivElement>(null);
  const [active, setActive] = useState(0);

  useEffect(() => {
    const section = ref.current;
    if (!section) return;
    if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;
    const ctx = gsap.context(() => {
      ScrollTrigger.create({
        trigger: section,
        start: 'top top',
        end: `+=${STEPS.length * 85}%`,
        pin: true,
        scrub: 0.4,
        onUpdate(self) {
          setActive(Math.min(STEPS.length - 1, Math.floor(self.progress * STEPS.length)));
        },
      });
    }, section);
    return () => ctx.revert();
  }, []);

  return (
    <section className="v2-steps-pin" ref={ref} aria-label="How one experiment works">
      <div className="v2-steps-pin__inner">
        <div className="v2-steps-pin__lead">
          <p className="v2-kicker">How it works</p>
          <div className="v2-steps-pin__num" key={`n${active}`}>{STEPS[active][0]}</div>
          <h2 key={`h${active}`}>{STEPS[active][1]}</h2>
          <p key={`p${active}`}>{STEPS[active][2]}</p>
        </div>
        <ol className="v2-steps-pin__list">
          {STEPS.map(([n, title], i) => (
            <li
              key={n}
              className={i === active ? 'is-active' : i < active ? 'is-done' : ''}
              onClick={() => setActive(i)}
            >
              <code>{n}</code>
              <span>{title}</span>
            </li>
          ))}
        </ol>
      </div>
    </section>
  );
}
