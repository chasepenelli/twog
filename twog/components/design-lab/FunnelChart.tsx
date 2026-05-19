'use client';

import type { PipelineStats } from '@/hooks/useDesignLabStats';

const STAGES = [
  { key: 'designed', label: 'DESIGNED', color: '#666' },
  { key: 'docked', label: 'DOCKED', color: '#58a6ff' },
  { key: 'mdScreened', label: 'MD SCREENED', color: '#B8860B' },
  { key: 'mdStable', label: 'MD STABLE', color: '#22C55E' },
  { key: 'v3Validated', label: 'V3 VALIDATED', color: '#00ff41' },
] as const;

export default function FunnelChart({ stats, loading }: { stats: PipelineStats | null; loading: boolean }) {
  if (loading || !stats) {
    return (
      <div style={{ padding: '1rem', color: '#666', fontFamily: 'var(--font-mono)', fontSize: '0.75rem' }}>
        Loading pipeline stats...
      </div>
    );
  }

  const maxVal = stats.designed || 1;

  return (
    <div>
      <h2 style={{ fontSize: '0.7rem', fontFamily: 'var(--font-mono)', color: '#888', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: '0.75rem' }}>
        Pipeline Funnel
      </h2>
      <div className="flex flex-col gap-2">
        {STAGES.map(({ key, label, color }) => {
          const val = stats[key] ?? 0;
          const pct = Math.max((val / maxVal) * 100, 0.5);
          return (
            <div key={key} className="flex items-center gap-3">
              <span style={{ width: '100px', fontSize: '0.65rem', fontFamily: 'var(--font-mono)', color: '#888', textAlign: 'right', flexShrink: 0 }}>
                {label}
              </span>
              <div style={{ flex: 1, background: '#1a1a1a', borderRadius: '2px', height: '20px', position: 'relative', overflow: 'hidden' }}>
                <div
                  style={{
                    width: `${pct}%`,
                    height: '100%',
                    background: color,
                    borderRadius: '2px',
                    transition: 'width 0.5s ease',
                    minWidth: '2px',
                    opacity: 0.8,
                  }}
                />
              </div>
              <span style={{ width: '80px', fontSize: '0.75rem', fontFamily: 'var(--font-mono)', color, fontWeight: 600 }}>
                {val.toLocaleString()}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
