'use client';

import { useRef } from 'react';
import { gsap } from 'gsap';
import { ScrollTrigger } from 'gsap/ScrollTrigger';
import { useGSAP } from '@gsap/react';

gsap.registerPlugin(ScrollTrigger);

interface SafetyBarProps {
  label: string;
  value: number;
  color?: string;
}

export default function SafetyBar({ label, value, color }: SafetyBarProps) {
  const barRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const barColor = color ?? (value > 60 ? 'var(--red)' : value > 35 ? 'var(--gold)' : 'var(--green)');

  useGSAP(() => {
    const bar = barRef.current;
    if (!bar) return;

    gsap.fromTo(
      bar,
      { width: '0%' },
      {
        width: `${value}%`,
        duration: 1.2,
        ease: 'power4.out',
        scrollTrigger: {
          trigger: containerRef.current,
          start: 'top 85%',
          toggleActions: 'play none none reverse',
        },
      }
    );
  }, { scope: containerRef });

  return (
    <div ref={containerRef} className="flex items-center gap-4">
      <span
        className="text-[0.65rem] uppercase tracking-[0.1em] w-24 shrink-0"
        style={{ color: 'var(--gray-400)', fontFamily: 'var(--font-mono), monospace' }}
      >
        {label}
      </span>
      <div className="flex-1 h-[2px] bg-[var(--gray-800)] relative">
        <div
          ref={barRef}
          className="absolute top-0 left-0 h-full"
          style={{ backgroundColor: barColor, width: 0 }}
        />
      </div>
      <span
        className="text-[0.65rem] w-10 text-right shrink-0"
        style={{ color: 'var(--gray-400)', fontFamily: 'var(--font-mono), monospace' }}
      >
        {value}%
      </span>
    </div>
  );
}
