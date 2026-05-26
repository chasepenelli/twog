'use client';

import { useEffect, useRef, useState } from 'react';
import type { TopCompound } from '@/hooks/useTopCompounds';
import { getTargetDescription } from '@/hooks/useTopCompounds';
import type { PipelineStats } from '@/hooks/usePipelineStats';
import ScrollReveal from '@/components/ScrollReveal';
import SplitTextReveal from '@/components/SplitTextReveal';
import ScoreRubric, { normalizeScore } from '@/components/ScoreRubric';
import TargetPathwayGrid from '@/components/TargetPathwayGrid';
import Link from 'next/link';

interface MoleculeHeroProps {
  compound: TopCompound;
  alternatives?: TopCompound[];
  pipelineStats?: PipelineStats | null;
}

const STORAGE_BASE = 'https://ktkvqoaskukndgxhutzg.supabase.co/storage/v1/object/public/videos/molecules';

type MoleculeViewer = {
  addModel: (data: string, format: string) => void;
  setStyle: (selection: Record<string, never>, style: Record<string, unknown>) => void;
  zoomTo: () => void;
  zoom: (factor: number) => void;
  render: () => void;
  spin: (axis: string, speed: number) => void;
  clear: () => void;
};

type ThreeDmolModule = {
  createViewer: (element: HTMLDivElement, config: Record<string, unknown>) => MoleculeViewer;
};

/* Short role labels for each target */
const TARGET_ROLE: Record<string, string> = {
  cPIK3CA: 'Cell Survival',
  JAK1: 'Immune Evasion',
  CDK6: 'Cell Division',
  PDGFRA: 'Tumor Growth',
  BRAF: 'Growth Signaling',
  MET: 'Metastasis',
  cKDR: 'Blood Supply',
  cFLT4: 'Lymphatic Spread',
  cMTOR: 'Growth Switch',
  BCL2: 'Anti-Apoptosis',
  JAK2: 'Proliferation',
  CDK4: 'Cell Cycle',
  MAP2K2: 'MAPK Cascade',
  NRAS: 'RAS Pathway',
  EGFR: 'Growth Receptor',
};

export default function MoleculeHero({ compound, alternatives = [], pipelineStats }: MoleculeHeroProps) {
  const viewerRef = useRef<HTMLDivElement>(null);
  const viewerInstance = useRef<MoleculeViewer | null>(null);
  const [loaded, setLoaded] = useState(false);

  // Build top runners-up (different targets from lead)
  const seen = new Set<string>([compound.target_gene || '']);
  const runnersUp: TopCompound[] = [];
  for (const c of alternatives) {
    const gene = c.target_gene || '';
    if (gene && !seen.has(gene) && runnersUp.length < 3) {
      seen.add(gene);
      runnersUp.push(c);
    }
  }

  // 3D viewer
  useEffect(() => {
    if (!viewerRef.current || typeof window === 'undefined') return;
    let cancelled = false;

    async function init() {
      const $3Dmol = (await import('3dmol')) as unknown as ThreeDmolModule;
      if (cancelled || !viewerRef.current) return;

      const viewer = $3Dmol.createViewer(viewerRef.current, {
        backgroundColor: 'rgba(0,0,0,0)',
        antialias: true,
        cartoonQuality: 10,
        disableFog: true,
        disableMouse: true,
      });
      viewerInstance.current = viewer;

      const molUrl = `${STORAGE_BASE}/${compound.name}_3d.mol`;
      try {
        const resp = await fetch(molUrl);
        if (!resp.ok) throw new Error(`${resp.status}`);
        const molData = await resp.text();
        if (cancelled) return;

        viewer.addModel(molData, 'mol');
        viewer.setStyle({}, {
          stick: {
            radius: 0.12,
            colorscheme: {
              prop: 'elem',
              map: { C: 0xffffff, N: 0x22C55E, O: 0xff6b6b, S: 0xffd700, F: 0x66d9ef, Cl: 0x66d9ef, Br: 0xcc6633, H: 0x666666 },
            },
          },
          sphere: {
            scale: 0.25,
            colorscheme: {
              prop: 'elem',
              map: { C: 0xffffff, N: 0x22C55E, O: 0xff6b6b, S: 0xffd700, F: 0x66d9ef, Cl: 0x66d9ef, Br: 0xcc6633, H: 0x666666 },
            },
          },
        });

        viewer.zoomTo();
        viewer.zoom(0.85);
        viewer.render();
        viewer.spin('y', 0.8);
        setLoaded(true);
      } catch (e) {
        console.warn('[MoleculeHero] Failed to load MOL:', e);
      }
    }

    init();
    return () => {
      cancelled = true;
      if (viewerInstance.current) {
        try { viewerInstance.current.clear(); } catch {}
      }
    };
  }, [compound.name]);

  const score = Number(compound.composite_score);
  const qed = compound.qed_score != null ? Number(compound.qed_score) : null;
  const gene = compound.target_gene || '?';
  const role = TARGET_ROLE[gene] || gene;

  // Score rubric items
  const rubricScores = [
    normalizeScore(score * 100, 80, 60),
    normalizeScore(qed != null ? qed * 100 : null, 70, 50),
  ];
  const rubricItems = [
    { label: 'Composite', ...rubricScores[0], detail: score.toFixed(3) },
    { label: 'Drug-Likeness', ...rubricScores[1], detail: qed != null ? qed.toFixed(3) : undefined },
  ];

  return (
    <>
      {/* ── PART 1: What We're Designing For ── */}
      <section className="dark-section py-24 md:py-32 px-6">
        <div className="max-w-3xl mx-auto text-center">
          <ScrollReveal>
            <p className="text-[0.55rem] uppercase tracking-[0.25em] text-[var(--gray-500)] mb-8">
              What We&apos;re Designing For
            </p>
          </ScrollReveal>

          <SplitTextReveal
            as="h2"
            className="text-[7vw] md:text-[4.5vw] lg:text-[3.5vw] leading-[0.95] text-white mb-8"
            stagger={0.02}
            duration={1}
          >
            One Cancer. Sixteen Escape Routes.
          </SplitTextReveal>

          <ScrollReveal delay={0.3}>
            <p className="text-[1.05rem] md:text-[1.1rem] leading-[1.9] text-[var(--gray-300)] max-w-2xl mx-auto">
              Hemangiosarcoma doesn&apos;t rely on a single pathway to survive. It hijacks
              growth signals, builds its own blood supply, evades cell death, and overrides
              division checkpoints simultaneously. One drug can&apos;t block all of that.
              So we design molecules for every route it uses.
            </p>
          </ScrollReveal>
        </div>
      </section>

      {/* ── PART 2: Target Pathway Grid ── */}
      <TargetPathwayGrid alternatives={[compound, ...alternatives]} />

      {/* ── PART 3: Lead Compound Spotlight ── */}
      <section className="py-20 md:py-28 px-6">
        <div className="max-w-6xl mx-auto">
          <div className="flex flex-col lg:flex-row items-center gap-8 lg:gap-16">

            {/* Left: 3D molecular viewer */}
            <div className="shrink-0 flex flex-col items-center">
              <div
                style={{
                  width: 320,
                  height: 320,
                  position: 'relative',
                  filter: loaded
                    ? 'drop-shadow(0 0 50px rgba(34,197,94,0.12)) drop-shadow(0 12px 40px rgba(0,0,0,0.3))'
                    : 'none',
                }}
              >
                <div
                  ref={viewerRef}
                  style={{
                    width: '100%',
                    height: '100%',
                    borderRadius: '50%',
                    overflow: 'hidden',
                    pointerEvents: 'none',
                  }}
                />
                {!loaded && (
                  <div className="absolute inset-0 flex items-center justify-center" style={{ animation: 'pulse 2s ease-in-out infinite' }}>
                    <span className="text-[0.6rem] uppercase tracking-[0.2em] mono" style={{ color: '#22C55E' }}>
                      Loading molecule...
                    </span>
                  </div>
                )}
              </div>
              <p className="text-[0.5rem] uppercase tracking-[0.2em] text-[var(--gray-400)] mt-4 mono">
                {compound.name} &middot; 3D structure
              </p>
            </div>

            {/* Right: Lead info + rubric + runners-up */}
            <div className="flex-1 w-full">
              <ScrollReveal>
                <p className="text-[0.55rem] uppercase tracking-[0.25em] text-[var(--gray-400)] mb-2">
                  Current Lead
                </p>
                <h2 className="text-[2rem] md:text-[2.5rem] font-bold leading-[1.1] mb-1 mono">
                  {compound.name}
                </h2>
                <p className="text-[0.7rem] uppercase tracking-[0.15em] text-[var(--green)] mb-4 mono">
                  {gene} &middot; {role}
                </p>
                <p className="text-[0.9rem] text-[var(--gray-500)] leading-[1.8] mb-8 max-w-lg">
                  {getTargetDescription(compound.target_gene)}
                </p>
              </ScrollReveal>

              {/* Score Rubric */}
              <ScrollReveal delay={0.1}>
                <div className="mb-8">
                  <p className="text-[0.55rem] uppercase tracking-[0.2em] text-[var(--gray-400)] mb-3">
                    Score Breakdown
                  </p>
                  <ScoreRubric scores={rubricItems} />
                  <p className="text-[0.6rem] text-[var(--gray-400)] leading-[1.7] mt-3">
                    Composite combines binding affinity, drug-likeness, synthesizability,
                    and safety profile. Higher is better.
                  </p>
                </div>
              </ScrollReveal>

              {/* Runners-Up */}
              {runnersUp.length > 0 && (
                <ScrollReveal delay={0.2}>
                  <p className="text-[0.55rem] uppercase tracking-[0.2em] text-[var(--gray-400)] mb-3">
                    Next Best &mdash; Different Targets
                  </p>
                  <div className="space-y-3">
                    {runnersUp.map((c, i) => {
                      const cScore = Number(c.composite_score);
                      const cGene = c.target_gene || '?';
                      const cRole = TARGET_ROLE[cGene] || cGene;
                      return (
                        <div key={c.id} className="flex items-center gap-4">
                          <span className="text-[0.65rem] mono text-[var(--gray-300)] w-5 shrink-0">
                            {String(i + 2).padStart(2, '0')}
                          </span>
                          <div className="flex-1 flex items-baseline justify-between">
                            <div className="flex items-baseline gap-2">
                              <span className="text-[0.85rem] font-bold mono">{c.name}</span>
                              <span className="text-[0.55rem] uppercase tracking-[0.1em] text-[var(--gray-400)]">
                                {cGene} &middot; {cRole}
                              </span>
                            </div>
                            <span className="text-[0.8rem] font-bold mono">{cScore.toFixed(3)}</span>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </ScrollReveal>
              )}
            </div>
          </div>
        </div>
      </section>

      {/* ── PART 4: CTA Block ── */}
      <section className="dark-section py-20 md:py-28 px-6 text-center">
        <div className="max-w-2xl mx-auto">
          <ScrollReveal>
            <h2 className="text-[5vw] md:text-[3.5vw] lg:text-[2.8vw] mb-4 text-white">
              See the Evidence
            </h2>
            <p className="text-[var(--gray-300)] mb-10 text-[1rem] leading-[1.8]">
              Every compound. Every score. Every validation test. Shared openly.
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.15}>
            <div className="flex flex-wrap justify-center gap-4 mb-10">
              <Link href="/research" className="btn btn-dark">
                Read the Research
              </Link>
              <Link href="/validation" className="btn btn-dark">
                View Validation Data
              </Link>
            </div>
          </ScrollReveal>

          {pipelineStats && (
            <ScrollReveal delay={0.3}>
              <div className="flex flex-wrap justify-center gap-8 mb-8">
                {([
                  { value: pipelineStats.papers, label: 'Papers' },
                  { value: pipelineStats.molecules, label: 'Molecules' },
                  { value: pipelineStats.dockings, label: 'Binding Tests' },
                ] as const).map(({ value, label }) => (
                  <div key={label} className="text-center">
                    <span className="block text-[1.5rem] md:text-[2rem] font-bold mono text-white leading-none">
                      {value.toLocaleString()}
                    </span>
                    <span className="block mt-1 text-[0.5rem] uppercase tracking-[0.15em] text-[var(--gray-500)]">
                      {label}
                    </span>
                  </div>
                ))}
              </div>
            </ScrollReveal>
          )}

          <ScrollReveal delay={0.4}>
            <a
              href="https://pushingc.substack.com"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-block text-[0.6rem] uppercase tracking-[0.2em] text-[var(--gray-500)] hover:text-white transition-colors"
            >
              Follow the journey on Substack &rarr;
            </a>
          </ScrollReveal>
        </div>
      </section>
    </>
  );
}
