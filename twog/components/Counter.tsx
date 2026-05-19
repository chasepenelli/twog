'use client';

import { useRef } from 'react';
import { gsap } from 'gsap';
import { ScrollTrigger } from 'gsap/ScrollTrigger';
import { useGSAP } from '@gsap/react';

gsap.registerPlugin(ScrollTrigger);

interface CounterProps {
  end: number;
  label: string;
  duration?: number;
  suffix?: string;
  variant?: 'dark' | 'light';
}

export default function Counter({ end, label, duration = 2, suffix = '', variant = 'dark' }: CounterProps) {
  const numRef = useRef<HTMLSpanElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  useGSAP(() => {
    const el = numRef.current;
    if (!el || end === 0) return;

    const obj = { val: 0 };

    gsap.to(obj, {
      val: end,
      duration,
      ease: 'power2.out',
      scrollTrigger: {
        trigger: containerRef.current,
        start: 'top 80%',
        toggleActions: 'play none none reverse',
        onLeaveBack() {
          obj.val = 0;
          el.textContent = '0';
        },
      },
      onUpdate() {
        el.textContent = Math.round(obj.val).toLocaleString() + suffix;
      },
    });
  }, { scope: containerRef, dependencies: [end] });

  return (
    <div ref={containerRef} className="text-center">
      <span
        ref={numRef}
        className={`block text-[8vw] md:text-[5vw] lg:text-[4vw] font-bold tracking-tight ${variant === 'light' ? 'text-black' : 'text-white'}`}
        style={{ fontFamily: 'var(--font-mono), monospace' }}
      >
        0
      </span>
      <span className={`block mt-3 text-[0.65rem] uppercase tracking-[0.15em] ${variant === 'light' ? 'text-gray-500' : 'text-[var(--gray-400)]'}`}>
        {label}
      </span>
    </div>
  );
}
