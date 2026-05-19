'use client';

import { useRef } from 'react';
import { gsap } from 'gsap';
import { ScrollTrigger } from 'gsap/ScrollTrigger';
import { useGSAP } from '@gsap/react';
import ScrollReveal from '@/components/ScrollReveal';
import { TARGETS } from '@/lib/constants';
import type { TopCompound } from '@/hooks/useTopCompounds';

gsap.registerPlugin(ScrollTrigger);

const PATHWAY_GROUPS = [
  {
    name: 'Blood Supply & Spread',
    description: 'Tumors build their own blood vessels and lymphatic routes to grow and metastasize.',
    genes: ['cKDR', 'cFLT4', 'cPDGFRA', 'MET', 'PDGFRA', 'KDR'],
  },
  {
    name: 'Survival Signaling',
    description: 'Cancer cells hijack the PI3K/AKT/mTOR axis to resist death and keep dividing.',
    genes: ['cPIK3CA', 'cMTOR', 'AKT1', 'BCL2', 'PIK3CA'],
  },
  {
    name: 'Growth Signaling',
    description: 'The MAPK cascade sends nonstop growth signals through RAS and RAF mutations.',
    genes: ['BRAF', 'cNRAS', 'cEGFR', 'MAP2K2', 'MAP2K1', 'NRAS', 'EGFR'],
  },
  {
    name: 'Cell Cycle & Epigenetics',
    description: 'Checkpoint proteins and epigenetic regulators let cancer cells divide unchecked.',
    genes: ['CDK4', 'CDK6', 'HDAC1', 'HDAC2', 'cTP53', 'JAK1', 'JAK2'],
  },
];

interface TargetPathwayGridProps {
  alternatives: TopCompound[];
}

export default function TargetPathwayGrid({ alternatives }: TargetPathwayGridProps) {
  const gridRef = useRef<HTMLDivElement>(null);

  // Build a map of best compound per target_gene
  const compoundByTarget = new Map<string, TopCompound>();
  for (const c of alternatives) {
    if (c.target_gene && !compoundByTarget.has(c.target_gene)) {
      compoundByTarget.set(c.target_gene, c);
    }
  }

  // Build a map from TARGETS constant for expression data
  const targetData = new Map(TARGETS.map((t) => [t.gene as string, t]));

  useGSAP(() => {
    const bars = gridRef.current?.querySelectorAll('.target-bar-fill');
    if (!bars) return;
    bars.forEach((bar) => {
      const width = bar.getAttribute('data-width') ?? '0';
      gsap.fromTo(bar, { width: '0%' }, {
        width: `${width}%`,
        duration: 1,
        ease: 'power4.out',
        scrollTrigger: { trigger: bar, start: 'top 92%', toggleActions: 'play none none none' },
      });
    });
  }, { scope: gridRef });

  return (
    <section className="py-24 md:py-32 px-6">
      <div className="max-w-5xl mx-auto" ref={gridRef}>
        <ScrollReveal>
          <p className="text-[0.55rem] uppercase tracking-[0.25em] text-[var(--gray-400)] mb-3 text-center">
            16 Targets, 4 Pathways
          </p>
          <h2 className="text-[5vw] md:text-[3.5vw] lg:text-[2.8vw] mb-4 text-center">
            Blocking Every Escape Route
          </h2>
          <p className="text-[0.9rem] text-[var(--gray-500)] leading-[1.8] text-center max-w-2xl mx-auto mb-16">
            Hemangiosarcoma survives by rerouting around single blockades.
            The pipeline designs compounds against every pathway the cancer exploits.
          </p>
        </ScrollReveal>

        <div className="space-y-12">
          {PATHWAY_GROUPS.map((group, gi) => {
            // Dedupe: only show genes that exist in TARGETS or have a compound
            const visibleGenes = group.genes.filter(
              (g) => targetData.has(g) || compoundByTarget.has(g),
            );

            return (
              <ScrollReveal key={group.name} delay={gi * 0.1}>
                <div>
                  <div className="flex items-baseline gap-3 mb-2">
                    <h3 className="text-[0.65rem] uppercase tracking-[0.2em] font-bold">
                      {group.name}
                    </h3>
                    <span className="text-[0.55rem] text-[var(--gray-400)] leading-[1.6]">
                      {group.description}
                    </span>
                  </div>

                  <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3">
                    {visibleGenes.map((gene) => {
                      const td = targetData.get(gene);
                      const compound = compoundByTarget.get(gene);
                      const score = compound ? Number(compound.composite_score) : 0;
                      const barWidth = score > 0 ? Math.min(100, (score / 1.0) * 100) : 0;
                      const hasLead = !!compound;

                      return (
                        <div
                          key={gene}
                          className="border rounded-md px-3 py-3 transition-colors"
                          style={{
                            borderColor: hasLead ? 'var(--green)' : 'var(--gray-200)',
                            borderLeftWidth: hasLead ? 3 : 1,
                          }}
                        >
                          <div className="flex items-baseline justify-between mb-1">
                            <span className="text-[0.8rem] font-bold mono">
                              {(td && 'aka' in td ? td.aka : null) || gene}
                            </span>
                            {td && (
                              <span className="text-[0.55rem] mono text-[var(--gray-400)]">
                                {td.fc}x
                              </span>
                            )}
                          </div>

                          <p className="text-[0.5rem] uppercase tracking-[0.1em] text-[var(--gray-400)] mb-2">
                            {td?.role || gene}
                          </p>

                          {/* Score bar */}
                          <div className="h-[2px] bg-[var(--gray-100)] rounded-full overflow-hidden mb-1">
                            {barWidth > 0 && (
                              <div
                                className="target-bar-fill h-full rounded-full"
                                data-width={barWidth}
                                style={{ backgroundColor: 'var(--green)', width: 0 }}
                              />
                            )}
                          </div>

                          <div className="flex items-baseline justify-between">
                            <span className="text-[0.5rem] mono text-[var(--gray-400)]">
                              {compound ? compound.name : 'Designing...'}
                            </span>
                            {score > 0 && (
                              <span className="text-[0.5rem] mono font-bold" style={{ color: 'var(--green)' }}>
                                {score.toFixed(3)}
                              </span>
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              </ScrollReveal>
            );
          })}
        </div>
      </div>
    </section>
  );
}
