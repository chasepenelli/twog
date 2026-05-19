'use client';

import { useRef } from 'react';
import { gsap } from 'gsap';
import { ScrollTrigger } from 'gsap/ScrollTrigger';
import { useGSAP } from '@gsap/react';

gsap.registerPlugin(ScrollTrigger);

interface ScoreItem {
  label: string;
  value: number; // 0–100
  status: 'green' | 'yellow' | 'red' | 'gray';
  detail?: string;
}

interface ScoreRubricProps {
  scores: ScoreItem[];
}

const STATUS_COLORS: Record<string, string> = {
  green: 'var(--green)',
  yellow: '#EAB308',
  red: '#EF4444',
  gray: 'var(--gray-500)',
};

export function normalizeScore(
  raw: number | null | undefined,
  greenThreshold: number,
  yellowThreshold: number,
  invert = false,
): { value: number; status: 'green' | 'yellow' | 'red' | 'gray' } {
  if (raw == null) return { value: 0, status: 'gray' };
  const v = invert ? 100 - raw : raw;
  const clamped = Math.max(0, Math.min(100, v));
  let status: 'green' | 'yellow' | 'red';
  if (invert) {
    status = raw <= greenThreshold ? 'green' : raw <= yellowThreshold ? 'yellow' : 'red';
  } else {
    status = raw >= greenThreshold ? 'green' : raw >= yellowThreshold ? 'yellow' : 'red';
  }
  return { value: clamped, status };
}

export function enumToScore(
  val: string | null | undefined,
  greenVal: string,
  yellowVal: string,
): { value: number; status: 'green' | 'yellow' | 'red' | 'gray' } {
  if (!val) return { value: 0, status: 'gray' };
  const lower = val.toLowerCase();
  if (lower === greenVal.toLowerCase()) return { value: 90, status: 'green' };
  if (lower === yellowVal.toLowerCase()) return { value: 55, status: 'yellow' };
  return { value: 25, status: 'red' };
}

export default function ScoreRubric({ scores }: ScoreRubricProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  useGSAP(() => {
    const bars = containerRef.current?.querySelectorAll('.score-bar-fill');
    if (!bars) return;

    bars.forEach((bar) => {
      const width = bar.getAttribute('data-width') ?? '0';
      gsap.fromTo(
        bar,
        { width: '0%' },
        {
          width: `${width}%`,
          duration: 1.2,
          ease: 'power4.out',
          scrollTrigger: {
            trigger: bar,
            start: 'top 90%',
            toggleActions: 'play none none none',
          },
        },
      );
    });
  }, { scope: containerRef });

  return (
    <div ref={containerRef} className="flex flex-col gap-3">
      {scores.map((s) => (
        <div key={s.label} className="flex items-center gap-4">
          <span
            className="text-[0.6rem] uppercase tracking-[0.1em] w-28 shrink-0 text-right"
            style={{ color: 'var(--gray-400)', fontFamily: 'var(--font-mono), monospace' }}
          >
            {s.label}
          </span>
          <div className="flex-1 h-[3px] bg-[var(--gray-800)] relative rounded-full overflow-hidden">
            <div
              className="score-bar-fill absolute top-0 left-0 h-full rounded-full"
              data-width={s.value}
              style={{ backgroundColor: STATUS_COLORS[s.status], width: 0 }}
            />
          </div>
          <span
            className="text-[0.6rem] w-16 shrink-0"
            style={{
              color: STATUS_COLORS[s.status],
              fontFamily: 'var(--font-mono), monospace',
            }}
          >
            {s.status === 'gray' ? (s.detail || 'PENDING') : (s.detail ?? `${s.value}%`)}
          </span>
        </div>
      ))}
    </div>
  );
}
