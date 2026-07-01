'use client';

// Kinetic marquee band (the Nifty Portal signature): oversized type scrolls horizontally, auto by
// default and faster with scroll velocity (GSAP + ScrollTrigger, already Lenis-synced). Items are
// duplicated for a seamless loop. Respects prefers-reduced-motion (renders static). No assets needed.

import { useEffect, useRef } from 'react';
import { gsap } from 'gsap';
import { ScrollTrigger } from 'gsap/ScrollTrigger';

gsap.registerPlugin(ScrollTrigger);

export default function Marquee({
  items,
  dir = -1,
  variant = 'default',
}: {
  items: string[];
  dir?: 1 | -1;
  variant?: 'default' | 'ink' | 'outline' | 'hud';
}) {
  const trackRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const track = trackRef.current;
    if (!track) return;
    if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;

    let tick: ((t: number) => void) | null = null;
    const ctx = gsap.context(() => {
      const half = track.scrollWidth / 2;
      if (!half) return;
      const duration = Math.max(22, half / 30); // slow, stately drift
      const tl =
        dir === -1
          ? gsap.fromTo(track, { x: 0 }, { x: -half, duration, ease: 'none', repeat: -1 })
          : gsap.fromTo(track, { x: -half }, { x: 0, duration, ease: 'none', repeat: -1 });

      let speed = 1;
      let target = 1;
      const st = ScrollTrigger.create({
        trigger: track,
        start: 'top bottom',
        end: 'bottom top',
        onUpdate(self) {
          target = 1 + Math.min(3, Math.abs(self.getVelocity()) / 500);
        },
      });
      tick = () => {
        speed += (target - speed) * 0.07;
        target += (1 - target) * 0.028; // slower decay → a longer, organic coast-to-stop
        tl.timeScale(speed);
        // subtle motion-blur while moving fast, tapering to none at rest
        const blur = Math.min(1.4, (speed - 1) * 0.9);
        track.style.filter = blur > 0.06 ? `blur(${blur.toFixed(2)}px)` : '';
      };
      gsap.ticker.add(tick);
      // ensure cleanup of the ScrollTrigger via context revert
      void st;
    }, track);

    return () => {
      if (tick) gsap.ticker.remove(tick);
      track.style.filter = '';
      ctx.revert();
    };
  }, [dir]);

  return (
    <div className={`v2-marquee v2-marquee--${variant}`} aria-hidden>
      <div className="v2-marquee__track" ref={trackRef}>
        {[...items, ...items].map((t, i) => (
          <span key={i}>{t}</span>
        ))}
      </div>
    </div>
  );
}
