'use client';

import { useState } from 'react';
import { useDesignLabCompounds, type CompoundWithMD } from '@/hooks/useDesignLabCompounds';
import type { TargetInfo } from '@/hooks/useDesignLabTargets';

function mdBadge(md: CompoundWithMD['md']) {
  if (!md) return { label: '--', color: '#444' };
  const s = md.stability_classification;
  if (s === 'stable') return { label: 'STABLE', color: '#22C55E' };
  if (s === 'marginal') return { label: 'MARGINAL', color: '#B8860B' };
  if (s === 'unstable') return { label: 'UNSTABLE', color: '#ff4444' };
  return { label: s?.toUpperCase() ?? '--', color: '#888' };
}

export default function CompoundBrowser({ targets }: { targets: TargetInfo[] }) {
  const { compounds, total, offset, limit, loading, filter, setFilter, nextPage, prevPage } = useDesignLabCompounds();
  const [expanded, setExpanded] = useState<number | null>(null);

  const targetGenes = ['', ...targets.map((t) => t.gene)];

  return (
    <div>
      {/* Filters */}
      <div className="flex items-center gap-3 mb-4" style={{ fontSize: '0.7rem', fontFamily: 'var(--font-mono)' }}>
        <select
          value={filter.target ?? ''}
          onChange={(e) => setFilter({ ...filter, target: e.target.value || undefined })}
          style={{ background: '#1a1a1a', color: '#ccc', border: '1px solid #333', padding: '0.3rem 0.5rem', borderRadius: '3px', fontSize: '0.7rem', fontFamily: 'var(--font-mono)' }}
        >
          <option value="">All targets</option>
          {targetGenes.filter(Boolean).map((g) => (
            <option key={g} value={g}>{g}</option>
          ))}
        </select>
        <select
          value={filter.mdStatus ?? 'any'}
          onChange={(e) => setFilter({ ...filter, mdStatus: e.target.value === 'any' ? undefined : e.target.value })}
          style={{ background: '#1a1a1a', color: '#ccc', border: '1px solid #333', padding: '0.3rem 0.5rem', borderRadius: '3px', fontSize: '0.7rem', fontFamily: 'var(--font-mono)' }}
        >
          <option value="any">Any MD status</option>
          <option value="stable">Stable</option>
          <option value="marginal">Marginal</option>
          <option value="unstable">Unstable</option>
          <option value="not_tested">Not tested</option>
        </select>
        <span style={{ color: '#666', marginLeft: 'auto' }}>
          {offset + 1}-{Math.min(offset + limit, total)} of {total.toLocaleString()}
        </span>
      </div>

      {/* Table */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontFamily: 'var(--font-mono)', fontSize: '0.65rem' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #333' }}>
              {['Name', 'Target', 'Score', 'QED', 'SA', 'Dock', 'MD', 'RMSD'].map((h) => (
                <th key={h} style={{ textAlign: 'left', padding: '0.4rem 0.5rem', color: '#888', fontWeight: 500, textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr><td colSpan={8} style={{ padding: '1rem', color: '#666' }}>Loading...</td></tr>
            )}
            {!loading && compounds.map((c) => {
              const badge = mdBadge(c.md);
              const isExpanded = expanded === c.id;
              return (
                <tr
                  key={c.id}
                  onClick={() => setExpanded(isExpanded ? null : c.id)}
                  style={{
                    borderBottom: '1px solid #1a1a1a',
                    cursor: 'pointer',
                    background: isExpanded ? '#111' : 'transparent',
                  }}
                >
                  <td style={{ padding: '0.4rem 0.5rem', color: '#ccc' }}>{c.name}</td>
                  <td style={{ padding: '0.4rem 0.5rem', color: '#888' }}>{c.target_gene}</td>
                  <td style={{ padding: '0.4rem 0.5rem', color: '#fff', fontWeight: 600 }}>{c.composite_score?.toFixed(3)}</td>
                  <td style={{ padding: '0.4rem 0.5rem', color: '#888' }}>{c.qed_score?.toFixed(2)}</td>
                  <td style={{ padding: '0.4rem 0.5rem', color: '#888' }}>{c.sa_score?.toFixed(1) ?? '--'}</td>
                  <td style={{ padding: '0.4rem 0.5rem', color: '#888' }}>{c.docking_confidence?.toFixed(3) ?? '--'}</td>
                  <td style={{ padding: '0.4rem 0.5rem', color: badge.color, fontWeight: 600 }}>{badge.label}</td>
                  <td style={{ padding: '0.4rem 0.5rem', color: '#888' }}>
                    {c.md?.ligand_rmsd_mean != null ? `${c.md.ligand_rmsd_mean.toFixed(3)}nm` : '--'}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Expanded detail */}
      {expanded && (() => {
        const c = compounds.find((x) => x.id === expanded);
        if (!c) return null;
        return (
          <div style={{ background: '#111', border: '1px solid #222', borderRadius: '4px', padding: '1rem', margin: '0.5rem 0', fontFamily: 'var(--font-mono)', fontSize: '0.65rem' }}>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem' }}>
              <div>
                <div style={{ color: '#888', marginBottom: '0.2rem' }}>SMILES</div>
                <div style={{ color: '#ccc', wordBreak: 'break-all', fontSize: '0.6rem' }}>{c.smiles}</div>
              </div>
              <div>
                <div style={{ color: '#888', marginBottom: '0.2rem' }}>ADMET</div>
                {c.admet_summary ? (
                  <div className="flex flex-wrap gap-2">
                    {Object.entries(c.admet_summary).map(([k, v]) => (
                      <span key={k} style={{ color: Number(v) > 0.5 ? '#ff4444' : '#22C55E' }}>
                        {k}: {Number(v).toFixed(2)}
                      </span>
                    ))}
                  </div>
                ) : (
                  <span style={{ color: '#444' }}>No ADMET data</span>
                )}
              </div>
              {c.md && (
                <>
                  <div>
                    <div style={{ color: '#888', marginBottom: '0.2rem' }}>MD Validation</div>
                    <div style={{ color: '#ccc' }}>
                      {c.md.simulation_length_ns}ns — Lig RMSD: {c.md.ligand_rmsd_mean?.toFixed(4)}nm — Prot RMSD: {c.md.protein_rmsd_mean?.toFixed(4)}nm
                    </div>
                    {c.md.protocol_version && <div style={{ color: '#B8860B' }}>Protocol: {c.md.protocol_version}</div>}
                  </div>
                  <div>
                    <div style={{ color: '#888', marginBottom: '0.2rem' }}>H-bond / Contacts</div>
                    <div style={{ color: '#ccc' }}>
                      H-bond: {c.md.hbond_occupancy != null ? JSON.stringify(c.md.hbond_occupancy) : '--'}
                    </div>
                  </div>
                </>
              )}
            </div>
          </div>
        );
      })()}

      {/* Pagination */}
      <div className="flex items-center justify-between mt-3" style={{ fontSize: '0.7rem', fontFamily: 'var(--font-mono)' }}>
        <button
          onClick={prevPage}
          disabled={offset === 0}
          style={{ color: offset === 0 ? '#444' : '#888', background: 'none', border: 'none', cursor: offset === 0 ? 'default' : 'pointer', fontFamily: 'var(--font-mono)' }}
        >
          &lt; Prev
        </button>
        <button
          onClick={nextPage}
          disabled={offset + limit >= total}
          style={{ color: offset + limit >= total ? '#444' : '#888', background: 'none', border: 'none', cursor: offset + limit >= total ? 'default' : 'pointer', fontFamily: 'var(--font-mono)' }}
        >
          Next &gt;
        </button>
      </div>
    </div>
  );
}
