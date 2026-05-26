'use client';

import { useRef, useEffect, useState } from 'react';
import { gsap } from 'gsap';
import { ScrollTrigger } from 'gsap/ScrollTrigger';
import { DrawSVGPlugin } from 'gsap/DrawSVGPlugin';
import { useGSAP } from '@gsap/react';
import { usePipelineStats } from '@/hooks/usePipelineStats';

gsap.registerPlugin(ScrollTrigger, DrawSVGPlugin);

const STAGES = [
  { key: 'read', label: 'Read', stat: 'papers', unit: 'papers scanned' },
  { key: 'design', label: 'Design', stat: 'molecules', unit: 'molecules generated' },
  { key: 'dock', label: 'Dock', stat: 'dockings', unit: 'binding simulations' },
  { key: 'score', label: 'Score', stat: 'discoveries', unit: 'discoveries' },
  { key: 'validate', label: 'Validate', stat: 'validated', unit: 'candidates tested', accent: true },
];

function StageCounter({ target, delay }: { target: number; delay: number }) {
  const ref = useRef<HTMLSpanElement>(null);
  const [val, setVal] = useState(0);

  useEffect(() => {
    if (!ref.current || target === 0) return;
    const obj = { v: 0 };
    gsap.to(obj, {
      v: target,
      duration: 2,
      delay,
      ease: 'power2.out',
      scrollTrigger: {
        trigger: ref.current,
        start: 'top 90%',
        toggleActions: 'play none none reverse',
        onLeaveBack: () => { obj.v = 0; setVal(0); },
      },
      onUpdate: () => setVal(Math.floor(obj.v)),
    });
  }, [target, delay]);

  return <span ref={ref}>{val.toLocaleString()}</span>;
}

export default function PipelineDiagram() {
  const stats = usePipelineStats();
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);

  useGSAP(() => {
    if (!containerRef.current) return;
    const nodes = containerRef.current.querySelectorAll('.p-node');

    /* Nodes fade in sequentially — each waits for its incoming line */
    gsap.fromTo(nodes, { opacity: 0, scale: 0.8 }, {
      opacity: 1, scale: 1, duration: 0.6, stagger: 0.2, ease: 'back.out(1.5)',
      scrollTrigger: { trigger: containerRef.current, start: 'top 80%', toggleActions: 'play none none reverse' },
    });

    /* SVG lines draw in with stagger */
    if (svgRef.current) {
      const lines = svgRef.current.querySelectorAll('.p-svg-line');
      gsap.fromTo(lines,
        { drawSVG: '0%' },
        {
          drawSVG: '100%',
          duration: 0.5,
          stagger: 0.2,
          ease: 'power2.out',
          scrollTrigger: { trigger: containerRef.current, start: 'top 80%', toggleActions: 'play none none reverse' },
        }
      );
    }
  }, { scope: containerRef, dependencies: [stats] });

  const getVal = (key: string) => {
    if (!stats) return 0;
    return (stats as unknown as Record<string, number>)[key] ?? 0;
  };

  /* Calculate positions for SVG lines between nodes.
     Each stage is ~100px wide, lines are ~40px between them. */
  const totalStages = STAGES.length + 1; // +1 for GO node
  const totalLines = totalStages; // lines between each stage + final line to GO

  return (
    <div ref={containerRef} className="flex flex-col items-center justify-center h-full w-full relative">
      {/* SVG overlay for DrawSVG lines */}
      <svg
        ref={svgRef}
        className="absolute inset-0 w-full h-full pointer-events-none"
        style={{ zIndex: 1 }}
        preserveAspectRatio="none"
      >
        {/* Lines are positioned via the flex layout — we draw them between node centers */}
        {Array.from({ length: totalLines }).map(() => null)}
      </svg>

      {/* Horizontal pipeline flow */}
      <div className="flex items-center justify-center gap-0 w-full px-4" style={{ position: 'relative', zIndex: 2 }}>
        {STAGES.map((stage, i) => (
          <div key={stage.key} className="flex items-center">
            {/* Connecting SVG line */}
            {i > 0 && (
              <svg className="w-6 md:w-10 h-[2px] overflow-visible" viewBox="0 0 40 2" preserveAspectRatio="none">
                <line
                  className="p-svg-line"
                  x1="0" y1="1" x2="40" y2="1"
                  stroke={stage.accent ? '#22C55E' : '#444'}
                  strokeWidth="2"
                />
              </svg>
            )}

            {/* Node */}
            <div className="p-node flex flex-col items-center" style={{ minWidth: 100 }}>
              {/* Circle */}
              <div
                className="w-12 h-12 md:w-14 md:h-14 rounded-full border-2 flex items-center justify-center mb-3"
                style={{
                  borderColor: stage.accent ? '#22C55E' : '#444',
                  background: stage.accent ? 'rgba(34,197,94,0.1)' : 'rgba(255,255,255,0.03)',
                  boxShadow: stage.accent ? '0 0 20px rgba(34,197,94,0.2)' : 'none',
                }}
              >
                <span
                  className="text-[0.65rem] font-bold"
                  style={{
                    color: stage.accent ? '#22C55E' : '#888',
                    fontFamily: 'var(--font-mono), monospace',
                  }}
                >
                  {String(i + 1).padStart(2, '0')}
                </span>
              </div>

              {/* Label */}
              <span
                className="text-[0.6rem] md:text-[0.7rem] font-bold uppercase tracking-[0.12em]"
                style={{ color: stage.accent ? '#22C55E' : '#fff' }}
              >
                {stage.label}
              </span>

              {/* Counter — cascaded with stagger delay */}
              <span
                className="text-[1.1rem] md:text-[1.4rem] font-bold mt-1"
                style={{
                  color: stage.accent ? '#22C55E' : '#fff',
                  fontFamily: 'var(--font-mono), monospace',
                }}
              >
                <StageCounter target={getVal(stage.stat)} delay={i * 0.3} />
              </span>

              {/* Unit */}
              <span
                className="text-[0.45rem] md:text-[0.5rem] uppercase tracking-[0.1em] mt-0.5"
                style={{ color: '#666' }}
              >
                {stage.unit}
              </span>
            </div>
          </div>
        ))}

        {/* Final SVG line to GO */}
        <svg className="w-6 md:w-10 h-[2px] overflow-visible" viewBox="0 0 40 2" preserveAspectRatio="none">
          <line
            className="p-svg-line"
            x1="0" y1="1" x2="40" y2="1"
            stroke="#22C55E"
            strokeWidth="2"
          />
        </svg>

        {/* GO node */}
        <div className="p-node flex flex-col items-center" style={{ minWidth: 80 }}>
          <div
            className="w-16 h-16 md:w-20 md:h-20 rounded-full border-2 flex items-center justify-center"
            style={{
              borderColor: '#22C55E',
              background: 'rgba(34,197,94,0.12)',
              boxShadow: '0 0 30px rgba(34,197,94,0.25)',
            }}
          >
            <span
              className="text-[1.2rem] md:text-[1.5rem] font-bold"
              style={{ color: '#22C55E', fontFamily: 'var(--font-mono), monospace', letterSpacing: '0.1em' }}
            >
              GO
            </span>
          </div>
          <span
            className="text-[0.5rem] uppercase tracking-[0.1em] mt-3"
            style={{ color: '#22C55E' }}
          >
            Synthesis
          </span>
        </div>
      </div>

      {/* Tagline */}
      <div className="mt-8 text-center">
        <span className="text-[0.5rem] uppercase tracking-[0.25em]" style={{ color: '#444' }}>
          Live data &middot; Updated every cycle
        </span>
      </div>
    </div>
  );
}
