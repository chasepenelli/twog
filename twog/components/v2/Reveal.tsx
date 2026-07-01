'use client';

// GSAP reveal wrapper. `immediate` plays on mount (hero); otherwise it plays on scroll-in (ScrollTrigger,
// already Lenis-synced by SmoothScroll). `stagger` animates direct children in sequence. Respects
// prefers-reduced-motion (content simply shows, no animation).

import { useRef, useEffect } from 'react';
import { gsap } from 'gsap';
import { ScrollTrigger } from 'gsap/ScrollTrigger';

gsap.registerPlugin(ScrollTrigger);

export default function Reveal({
  children,
  className,
  immediate = false,
  y = 30,
  stagger = false,
  delay = 0,
}: {
  children: React.ReactNode;
  className?: string;
  immediate?: boolean;
  y?: number;
  stagger?: boolean;
  delay?: number;
}) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;
    const targets = stagger ? Array.from(el.children) : el;
    const ctx = gsap.context(() => {
      gsap.from(targets, {
        y,
        opacity: 0,
        duration: 0.9,
        ease: 'power3.out',
        delay,
        stagger: stagger ? 0.12 : 0,
        ...(immediate ? {} : { scrollTrigger: { trigger: el, start: 'top 82%', once: true } }),
      });
    }, el);
    return () => ctx.revert();
  }, [immediate, y, stagger, delay]);

  return (
    <div ref={ref} className={className}>
      {children}
    </div>
  );
}
