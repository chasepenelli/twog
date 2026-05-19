'use client';

import { useState } from 'react';
import Molecule3D from './Molecule3D';
import ScoreRubric, { normalizeScore, enumToScore } from './ScoreRubric';
import ScrollReveal from './ScrollReveal';
import type { ValidationCompound } from '@/hooks/useValidationData';

function parseJson(val: unknown): unknown[] {
  if (!val) return [];
  if (Array.isArray(val)) return val;
  if (typeof val === 'string') {
    try { const parsed = JSON.parse(val); return Array.isArray(parsed) ? parsed : []; } catch { return []; }
  }
  return [];
}

function recommendationBadge(compound: ValidationCompound) {
  const safety = compound.safety?.overall_safety_score ?? 0;
  const md = compound.md?.stability_classification;
  const fto = compound.ip?.freedom_to_operate;

  if (md === 'stable' && safety > 0.7 && fto !== 'blocked') return { label: 'GO', color: 'var(--green)' };
  if (md === 'unstable' || safety < 0.4) return { label: 'NO-GO', color: '#EF4444' };
  return { label: 'CONDITIONAL', color: '#EAB308' };
}

export default function ValidationCard({ compound }: { compound: ValidationCompound }) {
  const [expanded, setExpanded] = useState<string | null>(null);
  const badge = recommendationBadge(compound);

  const scores = [
    (() => {
      const rmsd = compound.md?.rmsd_mean;
      if (rmsd == null) return { label: 'MD Stability', value: 0, status: 'gray' as const, detail: 'Pending' };
      const pct = Math.max(0, Math.min(100, (1 - rmsd / 8) * 100));
      const status = rmsd < 3 ? 'green' as const : rmsd < 5 ? 'yellow' as const : 'red' as const;
      return { label: 'MD Stability', value: pct, status, detail: `${rmsd.toFixed(1)}A` };
    })(),
    (() => {
      const idx = compound.selectivity?.pi3ka_selectivity_index;
      if (idx == null) return { label: 'Selectivity', value: 0, status: 'gray' as const, detail: 'PENDING' };
      if (idx === 1) return { label: 'Selectivity', value: 0, status: 'gray' as const, detail: 'EST.' };
      const pct = Math.min(100, idx * 5);
      const status = idx > 10 ? 'green' as const : idx > 3 ? 'yellow' as const : 'red' as const;
      return { label: 'Selectivity', value: pct, status, detail: `${idx.toFixed(0)}x` };
    })(),
    (() => {
      const r = compound.safety?.herg_risk_score;
      if (r == null) return { label: 'hERG Safety', value: 0, status: 'gray' as const, detail: 'PENDING' };
      const s = normalizeScore(r * 100, 30, 50, true);
      return { label: 'hERG Safety', ...s, detail: compound.safety?.herg_risk_level?.toUpperCase() ?? '' };
    })(),
    { label: 'Metabolic', ...enumToScore(compound.safety?.metabolic_stability, 'high', 'moderate') },
    { label: 'Oral Route', ...enumToScore(compound.safety?.oral_feasibility, 'likely', 'possible') },
    (() => {
      const s = compound.synergy?.synergy_score;
      if (s == null) return { label: 'Synergy', value: 0, status: 'gray' as const, detail: 'PENDING' };
      const pct = Math.max(0, Math.min(100, (s + 1) * 50));
      const status = s > 0.1 ? 'green' as const : s > -0.1 ? 'yellow' as const : 'red' as const;
      return { label: 'Synergy', value: pct, status, detail: s > 0 ? `+${s.toFixed(2)}` : s.toFixed(2) };
    })(),
    { label: 'IP Status', ...enumToScore(compound.ip?.freedom_to_operate, 'clear', 'encumbered') },
    { label: 'Literature', ...enumToScore(compound.literature?.sar_agreement, 'confirmed', 'partially_confirmed') },
    (() => {
      const s = compound.safety?.overall_safety_score;
      if (s == null) return { label: 'Safety Score', value: 0, status: 'gray' as const, detail: 'PENDING' };
      const ns = normalizeScore(s * 100, 70, 50);
      return { label: 'Safety Score', ...ns, detail: s.toFixed(2) };
    })(),
  ];

  const toggle = (key: string) => setExpanded(expanded === key ? null : key);

  const patents = parseJson(compound.ip?.patent_hits) as { patent_id?: string }[];
  const publications = parseJson(compound.literature?.relevant_publications) as { pmid?: string; title?: string }[];

  return (
    <div>
      <div className="border border-[var(--gray-200)] rounded-lg overflow-hidden mb-12">
        {/* Header */}
        <div className="flex flex-col md:flex-row">
          {/* 3D Molecule */}
          <div className="w-full md:w-[400px] shrink-0 flex items-center justify-center py-6">
            <Molecule3D compoundName={compound.name} size={280} />
          </div>

          {/* Info + Rubric */}
          <div className="flex-1 p-6">
            <div className="flex items-start justify-between mb-1">
              <div>
                <h3 className="text-[1.4rem] font-bold">{compound.name}</h3>
                <p className="text-[0.65rem] uppercase tracking-[0.2em] text-[var(--gray-400)] mt-1">
                  {compound.target_gene} &middot; Design Score {compound.composite_score?.toFixed(4)}
                </p>
              </div>
              <span
                className="text-[0.6rem] font-bold uppercase tracking-[0.15em] px-3 py-1 rounded-full border"
                style={{
                  color: badge.color,
                  borderColor: badge.color,
                }}
              >
                {badge.label}
              </span>
            </div>

            <p className="text-[0.55rem] text-[var(--gray-500)] mb-5 break-all mono" style={{ fontFamily: 'var(--font-mono)' }}>
              {compound.smiles?.slice(0, 80)}{compound.smiles && compound.smiles.length > 80 ? '...' : ''}
            </p>

            <ScoreRubric scores={scores} />
          </div>
        </div>

        {/* Detail Accordions */}
        <div className="border-t border-[var(--gray-200)]">
          {/* Off-target */}
          {compound.offtargets && compound.offtargets.length > 0 && (
            <DetailSection title={`Off-Target Profile (${compound.offtargets.length} targets)`} isOpen={expanded === 'ot'} toggle={() => toggle('ot')}>
              <table className="w-full text-[0.65rem]" style={{ fontFamily: 'var(--font-mono)' }}>
                <thead>
                  <tr className="text-[var(--gray-400)] uppercase tracking-wider">
                    <th className="text-left py-1 pr-4">Target</th>
                    <th className="text-right py-1 pr-4">Score</th>
                    <th className="text-right py-1 pr-4">Ratio</th>
                    <th className="text-left py-1">Risk</th>
                  </tr>
                </thead>
                <tbody>
                  {compound.offtargets.slice(0, 15).map((ot, i) => (
                    <tr key={i} className="border-t border-[var(--gray-100)]">
                      <td className="py-1 pr-4 text-[var(--gray-600)]">{ot.target_name}</td>
                      <td className="py-1 pr-4 text-right text-[var(--gray-500)]">{ot.docking_score?.toFixed(1)}</td>
                      <td className="py-1 pr-4 text-right text-[var(--gray-500)]">{ot.selectivity_ratio?.toFixed(1)}x</td>
                      <td className="py-1">
                        <span style={{ color: ot.risk_level === 'high' || ot.risk_level === 'critical' ? '#EF4444' : ot.risk_level === 'moderate' ? '#EAB308' : 'var(--green)' }}>
                          {ot.risk_level}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </DetailSection>
          )}

          {/* Safety */}
          {compound.safety && (
            <DetailSection title="Safety & ADMET" isOpen={expanded === 'safety'} toggle={() => toggle('safety')}>
              <div className="grid grid-cols-2 md:grid-cols-3 gap-4 text-[0.65rem]" style={{ fontFamily: 'var(--font-mono)' }}>
                <Stat label="CYP Enzyme" value={compound.safety.primary_cyp_enzyme ?? 'N/A'} />
                <Stat label="Metabolites" value={String(compound.safety.metabolite_count ?? 0)} />
                <Stat label="Solubility" value={`${compound.safety.aqueous_solubility_mg_ml?.toFixed(4) ?? '?'} mg/mL`} />
                <Stat label="Caco-2" value={`${compound.safety.caco2_permeability ?? '?'} nm/s`} />
                <Stat label="Protein Binding" value={`${compound.safety.plasma_protein_binding_pct?.toFixed(0) ?? '?'}%`} />
                <Stat label="hERG Feature" value={compound.safety.herg_flagged_feature ?? 'None'} />
              </div>
              {compound.safety.recommended_formulation && (
                <p className="text-[0.65rem] text-[var(--gray-500)] mt-3 italic">
                  {compound.safety.recommended_formulation}
                </p>
              )}
            </DetailSection>
          )}

          {/* Analogs */}
          {compound.analogs && compound.analogs.length > 0 && (
            <DetailSection title={`Backup Analogs (${compound.analogs.length})`} isOpen={expanded === 'analogs'} toggle={() => toggle('analogs')}>
              <table className="w-full text-[0.65rem]" style={{ fontFamily: 'var(--font-mono)' }}>
                <thead>
                  <tr className="text-[var(--gray-400)] uppercase tracking-wider">
                    <th className="text-left py-1">#</th>
                    <th className="text-left py-1">Modification</th>
                    <th className="text-right py-1">Score</th>
                    <th className="text-right py-1">SA</th>
                  </tr>
                </thead>
                <tbody>
                  {compound.analogs.slice(0, 10).map((a, i) => (
                    <tr key={i} className="border-t border-[var(--gray-100)]">
                      <td className="py-1 text-[var(--gray-400)]">{a.rank ?? i + 1}</td>
                      <td className="py-1 text-[var(--gray-600)]">{a.modification_description ?? a.modification_type}</td>
                      <td className="py-1 text-right text-[var(--gray-500)]">{a.composite_score?.toFixed(3)}</td>
                      <td className="py-1 text-right text-[var(--gray-500)]">{a.sa_score?.toFixed(1)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </DetailSection>
          )}

          {/* Synergy */}
          {compound.synergy && (
            <DetailSection title="Combination Synergy" isOpen={expanded === 'synergy'} toggle={() => toggle('synergy')}>
              <p className="text-[0.65rem] text-[var(--gray-500)] leading-relaxed">
                {compound.synergy.synergy_mechanism}
              </p>
              {compound.synergy.pathway_nodes_affected && (
                <div className="flex flex-wrap gap-1 mt-3">
                  {(parseJson(compound.synergy.pathway_nodes_affected) as string[]).map((node, i) => (
                    <span key={i} className="text-[0.55rem] px-2 py-0.5 bg-[var(--gray-100)] rounded-full text-[var(--gray-600)]">
                      {node}
                    </span>
                  ))}
                </div>
              )}
            </DetailSection>
          )}

          {/* IP */}
          {compound.ip && (
            <DetailSection title={`IP & Patents (${patents.length} hits)`} isOpen={expanded === 'ip'} toggle={() => toggle('ip')}>
              <p className="text-[0.65rem] text-[var(--gray-500)]">{compound.ip.ip_risk_summary}</p>
            </DetailSection>
          )}

          {/* Literature */}
          {compound.literature && publications.length > 0 && (
            <DetailSection title={`Literature (${publications.length} refs)`} isOpen={expanded === 'lit'} toggle={() => toggle('lit')}>
              <ul className="space-y-1">
                {publications.slice(0, 8).map((pub, i) => (
                  <li key={i} className="text-[0.6rem] text-[var(--gray-500)]">
                    <span className="text-[var(--gray-400)]">PMID {pub.pmid}</span> &mdash; {pub.title}
                  </li>
                ))}
              </ul>
            </DetailSection>
          )}
        </div>
      </div>
    </div>
  );
}

function DetailSection({ title, isOpen, toggle, children }: {
  title: string; isOpen: boolean; toggle: () => void; children: React.ReactNode;
}) {
  return (
    <div className="border-t border-[var(--gray-100)]">
      <button
        onClick={toggle}
        className="w-full flex items-center justify-between px-6 py-3 text-left hover:bg-[var(--gray-100)] transition-colors"
      >
        <span className="text-[0.65rem] uppercase tracking-[0.15em] text-[var(--gray-500)]">{title}</span>
        <span className="text-[0.7rem] text-[var(--gray-400)]">{isOpen ? '\u2212' : '+'}</span>
      </button>
      {isOpen && <div className="px-6 pb-4">{children}</div>}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-[0.55rem] uppercase tracking-[0.1em] text-[var(--gray-400)] mb-0.5">{label}</p>
      <p className="text-[0.7rem] text-[var(--gray-600)]">{value}</p>
    </div>
  );
}
