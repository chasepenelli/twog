'use client';

import { useState } from 'react';
import ScrollReveal from './ScrollReveal';
import type { ValidationCompound } from '@/hooks/useValidationData';

const DIMENSIONS = [
  {
    key: 'md', label: 'MD Stability',
    eli5: 'Does the drug actually stay attached to its target? We simulate the molecule shaking around for 50 nanoseconds. If it holds on, it\'s stable.',
  },
  {
    key: 'selectivity', label: 'Selectivity',
    eli5: 'Does the drug hit only its intended target, or does it also hit other proteins? Higher ratio = more selective. We want at least 10x selectivity over similar proteins.',
  },
  {
    key: 'herg', label: 'hERG Safety',
    eli5: 'Will this drug mess with the heart? hERG is a heart channel that many drugs accidentally block, causing dangerous side effects. Low risk = safe.',
  },
  {
    key: 'metabolic', label: 'Metabolic',
    eli5: 'How fast does the body break this drug down? Too fast and it won\'t work. Too slow and it builds up. "High" stability means it lasts long enough to do its job.',
  },
  {
    key: 'oral', label: 'Oral Route',
    eli5: 'Can this be given as a pill? We check if it dissolves in water, gets absorbed through the gut, and survives the liver. "Likely" means a pill should work.',
  },
  {
    key: 'safety', label: 'Overall Safety',
    eli5: 'Combined safety score across all checks \u2014 heart risk, toxic byproducts, liver interactions, and more. Higher is safer. Above 0.7 is what we want.',
  },
  {
    key: 'ip', label: 'IP Status',
    eli5: 'Are there existing patents that cover this molecule? "Clear" means nobody owns it. "Encumbered" means there are nearby patents worth checking with a lawyer.',
  },
  {
    key: 'synergy', label: 'Synergy',
    eli5: 'Does this drug work better when combined with other drugs? We model the triple therapy (PI3K blocker + VEGFR blocker + propranolol) and check if 1+1+1 > 3.',
  },
] as const;

function getCell(compound: ValidationCompound, dim: string): { label: string; color: string } {
  switch (dim) {
    case 'md': {
      const c = compound.md?.stability_classification;
      if (!c) return { label: 'PENDING', color: 'var(--gray-300)' };
      return {
        label: c.toUpperCase(),
        color: c === 'stable' ? 'var(--green)' : c === 'marginal' ? '#EAB308' : '#EF4444',
      };
    }
    case 'selectivity': {
      const idx = compound.selectivity?.pi3ka_selectivity_index;
      if (idx == null) return { label: 'PENDING', color: 'var(--gray-300)' };
      // Flag estimated data (all scores identical = no real docking)
      if (idx === 1) return { label: 'EST.', color: 'var(--gray-400)' };
      return {
        label: `${idx.toFixed(0)}x`,
        color: idx > 10 ? 'var(--green)' : idx > 3 ? '#EAB308' : '#EF4444',
      };
    }
    case 'herg': {
      const l = compound.safety?.herg_risk_level;
      if (!l) return { label: 'PENDING', color: 'var(--gray-300)' };
      return {
        label: l.toUpperCase(),
        color: l === 'low' ? 'var(--green)' : l === 'moderate' ? '#EAB308' : '#EF4444',
      };
    }
    case 'metabolic': {
      const m = compound.safety?.metabolic_stability;
      if (!m) return { label: 'PENDING', color: 'var(--gray-300)' };
      return {
        label: m.toUpperCase(),
        color: m === 'high' ? 'var(--green)' : m === 'moderate' ? '#EAB308' : '#EF4444',
      };
    }
    case 'oral': {
      const o = compound.safety?.oral_feasibility;
      if (!o) return { label: 'PENDING', color: 'var(--gray-300)' };
      return {
        label: o.toUpperCase(),
        color: o === 'likely' ? 'var(--green)' : o === 'possible' ? '#EAB308' : '#EF4444',
      };
    }
    case 'safety': {
      const s = compound.safety?.overall_safety_score;
      if (s == null) return { label: 'PENDING', color: 'var(--gray-300)' };
      return {
        label: s.toFixed(2),
        color: s > 0.7 ? 'var(--green)' : s > 0.5 ? '#EAB308' : '#EF4444',
      };
    }
    case 'ip': {
      const f = compound.ip?.freedom_to_operate;
      if (!f) return { label: 'PENDING', color: 'var(--gray-300)' };
      return {
        label: f.toUpperCase(),
        color: f === 'clear' ? 'var(--green)' : f === 'encumbered' ? '#EAB308' : '#EF4444',
      };
    }
    case 'synergy': {
      const c = compound.synergy?.synergy_classification;
      if (!c) return { label: 'PENDING', color: 'var(--gray-300)' };
      return {
        label: c.toUpperCase(),
        color: c === 'synergistic' ? 'var(--green)' : c === 'additive' ? '#EAB308' : '#EF4444',
      };
    }
    default:
      return { label: 'PENDING', color: 'var(--gray-300)' };
  }
}

export default function ComparisonMatrix({ compounds }: { compounds: ValidationCompound[] }) {
  const [openInfo, setOpenInfo] = useState<string | null>(null);

  if (compounds.length === 0) return null;

  return (
    <ScrollReveal>
      {/* Info card (shows below matrix when a dimension is clicked) */}
      {openInfo && (
        <div className="mb-4 p-4 border border-[var(--gray-200)] rounded-lg bg-[var(--gray-100)]">
          <div className="flex items-start justify-between gap-4">
            <div>
              <p className="text-[0.65rem] uppercase tracking-[0.15em] text-[var(--gray-600)] font-bold mb-1">
                {DIMENSIONS.find(d => d.key === openInfo)?.label}
              </p>
              <p className="text-[0.75rem] text-[var(--gray-500)] leading-relaxed">
                {DIMENSIONS.find(d => d.key === openInfo)?.eli5}
              </p>
            </div>
            <button
              onClick={() => setOpenInfo(null)}
              className="text-[0.7rem] text-[var(--gray-400)] hover:text-[var(--foreground)] shrink-0"
            >
              &times;
            </button>
          </div>
        </div>
      )}

      <div className="overflow-x-auto">
        <table className="w-full text-[0.6rem]" style={{ fontFamily: 'var(--font-mono), monospace' }}>
          <thead>
            <tr>
              <th className="text-left py-2 pr-4 text-[var(--gray-400)] uppercase tracking-wider sticky left-0 bg-[var(--background)]">
                Dimension
              </th>
              {compounds.map((c) => (
                <th key={c.id} className="text-center py-2 px-3 text-[var(--gray-400)] uppercase tracking-wider min-w-[100px]">
                  <div>{c.name.replace('GRF-', '')}</div>
                  <div className="text-[0.5rem] text-[var(--gray-300)] normal-case tracking-normal mt-0.5">
                    {c.composite_score?.toFixed(3)}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {DIMENSIONS.map((dim) => (
              <tr key={dim.key} className="border-t border-[var(--gray-100)]">
                <td className="py-2 pr-4 sticky left-0 bg-[var(--background)]">
                  <button
                    onClick={() => setOpenInfo(openInfo === dim.key ? null : dim.key)}
                    className={`text-left uppercase tracking-wider transition-colors duration-200 flex items-center gap-1.5 ${
                      openInfo === dim.key ? 'text-[var(--foreground)]' : 'text-[var(--gray-500)] hover:text-[var(--foreground)]'
                    }`}
                  >
                    {dim.label}
                    <span className="text-[0.5rem] opacity-40">?</span>
                  </button>
                </td>
                {compounds.map((c) => {
                  const cell = getCell(c, dim.key);
                  return (
                    <td key={c.id} className="text-center py-2 px-3">
                      <span style={{ color: cell.color }}>{cell.label}</span>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </ScrollReveal>
  );
}
