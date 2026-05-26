'use client';

import { useRef } from 'react';
import { gsap } from 'gsap';
import { ScrollTrigger } from 'gsap/ScrollTrigger';
import { useGSAP } from '@gsap/react';

gsap.registerPlugin(ScrollTrigger);

interface TargetRowProps {
  gene: string;
  fc: number;
  tier: number;
  aka?: string;
  role: string;
  maxFc?: number;
}

export default function TargetRow({ gene, fc, tier, aka, maxFc = 8.2 }: TargetRowProps) {
  const barRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const pct = (fc / maxFc) * 100;
  const tierLabel = tier === 1 ? 'T1' : tier === 2 ? 'T2' : 'T3';
  const tierColor = tier === 1 ? 'var(--white)' : tier === 2 ? 'var(--gray-400)' : 'var(--gray-500)';

  useGSAP(() => {
    const bar = barRef.current;
    if (!bar) return;

    gsap.fromTo(
      bar,
      { width: '0%' },
      {
        width: `${pct}%`,
        duration: 1,
        ease: 'power4.out',
        scrollTrigger: {
          trigger: containerRef.current,
          start: 'top 90%',
          toggleActions: 'play none none none',
        },
      }
    );
  }, { scope: containerRef });

  return (
    <div
      ref={containerRef}
      className="grid items-center gap-4 py-3 border-b border-[var(--gray-800)]"
      style={{ gridTemplateColumns: '2.5rem 7rem 1fr 3rem' }}
    >
      <span
        className="text-[0.55rem] uppercase tracking-[0.1em] text-center border border-current rounded px-1 py-0.5"
        style={{ color: tierColor, fontFamily: 'var(--font-mono), monospace' }}
      >
        {tierLabel}
      </span>

      <div>
        <span className="text-sm font-medium text-white" style={{ fontFamily: 'var(--font-mono), monospace' }}>
          {gene}
        </span>
        {aka && (
          <span className="ml-2 text-[0.6rem] text-[var(--gray-500)]">{aka}</span>
        )}
      </div>

      <div className="relative h-[2px] bg-[var(--gray-800)]">
        <div
          ref={barRef}
          className="absolute top-0 left-0 h-full bg-white"
          style={{ width: 0 }}
        />
      </div>

      <span
        className="text-right text-sm font-medium"
        style={{ color: tierColor, fontFamily: 'var(--font-mono), monospace' }}
      >
        {fc}x
      </span>
    </div>
  );
}
