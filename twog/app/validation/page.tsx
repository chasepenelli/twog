'use client';

import ScrollReveal from '@/components/ScrollReveal';
import SplitTextReveal from '@/components/SplitTextReveal';
import { useEffect, useState } from 'react';
import { useSupabase } from '@/hooks/useSupabase';

interface V3Result {
  compound_id: number;
  stability_classification: string;
  ligand_rmsd_mean: number | null;
  protein_rmsd_mean: number | null;
  hbond_occupancy: any;
  contact_maintained: boolean | null;
  rmsd_plateaued: boolean | null;
  simulation_length_ns: number;
  compute_time_hours: number;
  target_structure_source: string;
  compound_name?: string;
  target_gene?: string;
  composite_score?: number;
}

function useValidationData() {
  const sb = useSupabase();
  const [data, setData] = useState<{
    v3Results: V3Result[];
    totalMd: number;
    loading: boolean;
  }>({ v3Results: [], totalMd: 0, loading: true });

  useEffect(() => {
    if (!sb) return;
    (async () => {
      // Get v3 results (explicit solvent — protein RMSD < 0.5nm)
      const { data: md } = await sb.from('md_validations')
        .select('compound_id, stability_classification, ligand_rmsd_mean, protein_rmsd_mean, hbond_occupancy, contact_maintained, rmsd_plateaued, simulation_length_ns, compute_time_hours, target_structure_source')
        .not('protein_rmsd_mean', 'is', null)
        .lt('protein_rmsd_mean', 0.5)
        .order('ligand_rmsd_mean', { ascending: true });

      // Get total MD count
      const { count } = await sb.from('md_validations')
        .select('id', { count: 'exact', head: true });

      if (!md?.length) {
        setData(d => ({ ...d, totalMd: count || 0, loading: false }));
        return;
      }

      // Get compound info
      const compIds = [...new Set(md.map(m => m.compound_id))];
      const { data: compounds } = await sb.from('designed_compounds')
        .select('id, name, target_gene, composite_score')
        .in('id', compIds);

      const compMap = new Map((compounds || []).map(c => [c.id, c]));

      const enriched: V3Result[] = md.map(m => ({
        ...m,
        compound_name: compMap.get(m.compound_id)?.name || `Compound #${m.compound_id}`,
        target_gene: compMap.get(m.compound_id)?.target_gene || 'unknown',
        composite_score: compMap.get(m.compound_id)?.composite_score || 0,
      }));

      setData({
        v3Results: enriched,
        totalMd: count || 0,
        loading: false,
      });
    })();
  }, [sb]);

  return data;
}

const STABILITY_COLORS: Record<string, { color: string; bg: string }> = {
  stable: { color: '#16a34a', bg: 'rgba(22,163,74,0.08)' },
  marginal: { color: '#d97706', bg: 'rgba(217,119,6,0.08)' },
  unstable: { color: '#dc2626', bg: 'rgba(220,38,38,0.08)' },
  failed: { color: '#6b7280', bg: 'rgba(107,114,128,0.08)' },
};

function StabilityBadge({ status }: { status: string }) {
  const config = STABILITY_COLORS[status] || STABILITY_COLORS.failed;
  return (
    <span
      className="inline-block text-[0.6rem] font-mono font-semibold px-2 py-0.5 rounded-full border"
      style={{ color: config.color, background: config.bg, borderColor: `${config.color}30` }}
    >
      {status.toUpperCase()}
    </span>
  );
}

function MetricBar({ label, value, max, unit, color }: { label: string; value: number; max: number; unit: string; color: string }) {
  const pct = Math.min((value / max) * 100, 100);
  return (
    <div>
      <div className="flex justify-between items-baseline mb-1">
        <span className="text-[0.55rem] uppercase tracking-wider text-[var(--gray-400)]">{label}</span>
        <span className="text-[0.65rem] font-mono font-bold" style={{ color }}>{value.toFixed(3)} {unit}</span>
      </div>
      <div className="h-1.5 rounded-full bg-[var(--gray-100)] overflow-hidden">
        <div className="h-full rounded-full transition-all" style={{ width: `${pct}%`, background: color }} />
      </div>
    </div>
  );
}

function ResultCard({ r }: { r: V3Result }) {
  const isStable = r.stability_classification === 'stable';
  const isMarginal = r.stability_classification === 'marginal';
  const borderColor = isStable ? 'rgba(22,163,74,0.3)' : isMarginal ? 'rgba(217,119,6,0.2)' : 'var(--gray-200)';
  const bgColor = isStable ? 'rgba(22,163,74,0.02)' : 'transparent';

  const ligRmsd = r.ligand_rmsd_mean || 0;
  const protRmsd = r.protein_rmsd_mean || 0;
  const hbond = typeof r.hbond_occupancy === 'number' ? r.hbond_occupancy : 0;

  const ligColor = ligRmsd < 0.3 ? '#16a34a' : ligRmsd < 0.5 ? '#d97706' : '#dc2626';
  const protColor = protRmsd < 0.2 ? '#16a34a' : protRmsd < 0.3 ? '#d97706' : '#dc2626';

  return (
    <div className="border rounded-xl p-5 transition-all" style={{ borderColor, background: bgColor }}>
      <div className="flex items-start justify-between gap-3 mb-3">
        <div>
          <h3 className="text-[0.95rem] font-bold">{r.compound_name}</h3>
          <p className="text-[0.7rem] font-mono text-[var(--gray-400)]">
            {r.target_gene} | {r.simulation_length_ns}ns | {((r.compute_time_hours || 0) * 60).toFixed(0)} min
          </p>
        </div>
        <StabilityBadge status={r.stability_classification} />
      </div>

      <div className="space-y-2">
        <MetricBar label="Ligand RMSD" value={ligRmsd} max={0.6} unit="nm" color={ligColor} />
        <MetricBar label="Protein RMSD" value={protRmsd} max={0.4} unit="nm" color={protColor} />
      </div>

      <div className="flex gap-4 mt-3 pt-3 border-t border-[var(--gray-100)]">
        <div className="flex-1">
          <span className="text-[0.5rem] uppercase tracking-wider text-[var(--gray-400)]">H-Bond</span>
          <p className="text-[0.75rem] font-mono font-bold" style={{ color: hbond > 0.5 ? '#16a34a' : '#d97706' }}>
            {(hbond * 100).toFixed(0)}%
          </p>
        </div>
        <div className="flex-1">
          <span className="text-[0.5rem] uppercase tracking-wider text-[var(--gray-400)]">Contact</span>
          <p className="text-[0.75rem] font-mono font-bold" style={{ color: r.contact_maintained ? '#16a34a' : '#dc2626' }}>
            {r.contact_maintained ? 'Held' : 'Lost'}
          </p>
        </div>
        <div className="flex-1">
          <span className="text-[0.5rem] uppercase tracking-wider text-[var(--gray-400)]">Plateau</span>
          <p className="text-[0.75rem] font-mono font-bold" style={{ color: r.rmsd_plateaued ? '#16a34a' : '#d97706' }}>
            {r.rmsd_plateaued ? 'Yes' : 'No'}
          </p>
        </div>
      </div>
    </div>
  );
}

function StatCard({ value, label, color }: { value: string | number; label: string; color?: string }) {
  return (
    <div className="text-center py-4 px-3 border border-[var(--gray-200)] rounded-xl">
      <div className="text-[2rem] font-bold font-mono" style={{ color: color || 'var(--foreground)' }}>{value}</div>
      <div className="text-[0.6rem] uppercase tracking-wider text-[var(--gray-400)] mt-1">{label}</div>
    </div>
  );
}

export default function ValidationPage() {
  const { v3Results, totalMd, loading } = useValidationData();

  const stable = v3Results.filter(r => r.stability_classification === 'stable');
  const marginal = v3Results.filter(r => r.stability_classification === 'marginal');
  const unstable = v3Results.filter(r => r.stability_classification === 'unstable');

  return (
    <>
      {/* ── HERO ── */}
      <section className="pt-32 pb-8 px-6 text-center">
        <div className="max-w-2xl mx-auto">
          <ScrollReveal>
            <p className="text-[0.6rem] uppercase tracking-[0.3em] text-[var(--gray-400)] mb-4 font-mono">
              Molecular Dynamics
            </p>
          </ScrollReveal>
          <SplitTextReveal
            as="h1"
            className="text-[2.5rem] md:text-[3.5rem] font-bold leading-[0.9] tracking-tight uppercase"
            stagger={0.03}
            duration={1}
          >
            Validation
          </SplitTextReveal>
          <ScrollReveal delay={0.3}>
            <p className="mt-6 text-[0.85rem] text-[var(--gray-500)] max-w-lg mx-auto leading-relaxed">
              Every molecule gets stress-tested with explicit solvent molecular dynamics.
              Real water. Real physics. 2 nanoseconds of simulated biological forces.
              If the drug stays in the protein pocket, it advances. If it drifts, it doesn&apos;t.
            </p>
          </ScrollReveal>
        </div>
      </section>

      {/* ── PROTOCOL ── */}
      {!loading && (
        <section className="px-6 pb-8">
          <ScrollReveal delay={0.4}>
            <div className="max-w-3xl mx-auto px-6 py-5 border border-[var(--gray-200)] rounded-xl bg-[var(--gray-100)]">
              <p className="text-[0.78rem] text-[var(--gray-600)] leading-relaxed">
                <strong>v3 Protocol (Corrected):</strong> TIP3P explicit solvent with 0.15M NaCl,
                Particle Mesh Ewald electrostatics, staged equilibration (NVT + NPT with backbone restraints),
                Kabsch-aligned RMSD measurement, AMBER14 protein force field + OpenFF SMIRNOFF ligand parameterization.
                Run on CUDA GPUs via OpenMM 8.2.
              </p>
            </div>
          </ScrollReveal>
        </section>
      )}

      {/* ── FUNNEL ── */}
      {!loading && (
        <section className="px-6 pb-12">
          <ScrollReveal delay={0.2}>
            <div className="max-w-4xl mx-auto">
              <h2 className="text-[1.1rem] font-bold uppercase tracking-wide mb-6 border-b border-[var(--gray-200)] pb-2">
                Results
              </h2>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                <StatCard value={v3Results.length} label="v3 Screened" />
                <StatCard value={stable.length} label="Stable" color={stable.length > 0 ? '#16a34a' : 'var(--foreground)'} />
                <StatCard value={marginal.length} label="Marginal" color={marginal.length > 0 ? '#d97706' : 'var(--foreground)'} />
                <StatCard value={totalMd} label="Total MD (all time)" />
              </div>
            </div>
          </ScrollReveal>
        </section>
      )}

      {/* ── LOADING ── */}
      {loading && (
        <section className="py-24 px-6 text-center">
          <p className="text-[0.8rem] font-mono text-[var(--gray-400)] animate-pulse">Loading validation data...</p>
        </section>
      )}

      {/* ── RESULT CARDS ── */}
      {!loading && v3Results.length > 0 && (
        <section className="px-6 pb-16">
          <ScrollReveal>
            <div className="max-w-4xl mx-auto">
              <h2 className="text-[1.1rem] font-bold uppercase tracking-wide mb-6 border-b border-[var(--gray-200)] pb-2">
                Compounds — Explicit Solvent MD
              </h2>
              <div className="grid gap-4 md:grid-cols-2">
                {v3Results.map((r, i) => (
                  <ResultCard key={`${r.compound_id}-${i}`} r={r} />
                ))}
              </div>
            </div>
          </ScrollReveal>
        </section>
      )}

      {/* ── EMPTY STATE ── */}
      {!loading && v3Results.length === 0 && (
        <section className="py-16 px-6 text-center">
          <div className="max-w-md mx-auto">
            <p className="text-[0.85rem] text-[var(--gray-400)] leading-relaxed">
              No compounds have been validated with the corrected v3 protocol yet.
              The GPU farm is processing the backlog. Results will appear here as they complete.
            </p>
          </div>
        </section>
      )}

      {/* ── DISCLAIMER ── */}
      <section className="px-6 pb-24">
        <ScrollReveal>
          <div className="max-w-3xl mx-auto px-5 py-4 border border-[var(--gray-200)] rounded-xl bg-[var(--gray-100)] text-center">
            <p className="text-[0.72rem] text-[var(--gray-500)] leading-relaxed">
              These are computational predictions, not experimental results.
              Molecular dynamics simulation estimates binding stability under simulated biological conditions.
              Lab validation is required before any compound advances to synthesis.
            </p>
          </div>
        </ScrollReveal>
      </section>
    </>
  );
}
