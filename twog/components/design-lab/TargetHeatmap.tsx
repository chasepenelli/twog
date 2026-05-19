'use client';

import type { TargetInfo } from '@/hooks/useDesignLabTargets';

function passRateColor(rate: number): string {
  if (rate >= 0.2) return '#22C55E';
  if (rate >= 0.1) return '#B8860B';
  if (rate > 0) return '#ff8844';
  return '#444';
}

export default function TargetHeatmap({ targets, loading }: { targets: TargetInfo[]; loading: boolean }) {
  if (loading) {
    return (
      <div style={{ padding: '1rem', color: '#666', fontFamily: 'var(--font-mono)', fontSize: '0.75rem' }}>
        Loading targets...
      </div>
    );
  }

  return (
    <div>
      <h2 style={{ fontSize: '0.7rem', fontFamily: 'var(--font-mono)', color: '#888', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: '0.75rem' }}>
        Target Stability Rates
      </h2>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: '0.5rem' }}>
        {targets.map((t) => (
          <div
            key={t.gene}
            style={{
              background: '#111',
              border: `1px solid ${passRateColor(t.mdPassRate)}33`,
              borderRadius: '4px',
              padding: '0.6rem 0.75rem',
              fontFamily: 'var(--font-mono)',
            }}
          >
            <div className="flex items-center justify-between" style={{ marginBottom: '0.3rem' }}>
              <span style={{ fontSize: '0.75rem', color: '#fff', fontWeight: 600 }}>{t.gene}</span>
              {t.pdbId && (
                <span style={{ fontSize: '0.6rem', color: '#666' }}>{t.pdbId}</span>
              )}
            </div>
            <div className="flex items-center justify-between" style={{ fontSize: '0.65rem', color: '#888' }}>
              <span>{t.compoundCount.toLocaleString()} cpds</span>
              <span>{t.mdCount} MD</span>
            </div>
            <div className="flex items-center justify-between" style={{ fontSize: '0.65rem', marginTop: '0.2rem' }}>
              <span style={{ color: passRateColor(t.mdPassRate) }}>
                {t.mdCount > 0 ? `${(t.mdPassRate * 100).toFixed(0)}% pass` : 'no MD'}
              </span>
              <span style={{ color: '#22C55E' }}>{t.stableCount} stable</span>
            </div>
            {t.seeds.length > 0 && (
              <div style={{ fontSize: '0.6rem', color: '#666', marginTop: '0.3rem' }}>
                {t.seeds.length} seeds
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
