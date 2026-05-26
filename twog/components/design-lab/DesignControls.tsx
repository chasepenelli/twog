'use client';

import { useState, useCallback } from 'react';
import type { TargetInfo } from '@/hooks/useDesignLabTargets';

type Mode = 'exploit' | 'explore' | 'moonshot';

const MODES: { key: Mode; label: string; desc: string; color: string }[] = [
  { key: 'exploit', label: 'EXPLOIT', desc: 'Optimize known winners. High similarity to MD-stable scaffolds.', color: '#22C55E' },
  { key: 'explore', label: 'EXPLORE', desc: 'Diverse scaffolds per target. Low similarity from known drugs.', color: '#58a6ff' },
  { key: 'moonshot', label: 'MOONSHOT', desc: 'Radical new approaches. Fragment recombination, scaffold hopping.', color: '#B8860B' },
];

const MODE_DEFAULTS: Record<Mode, { similarity: number; molecules: number }> = {
  exploit: { similarity: 0.5, molecules: 10 },
  explore: { similarity: 0.1, molecules: 15 },
  moonshot: { similarity: 0.0, molecules: 20 },
};

export default function DesignControls({ targets }: { targets: TargetInfo[]; onRefreshTargets: () => void }) {
  const [mode, setMode] = useState<Mode>('explore');
  const [selectedTargets, setSelectedTargets] = useState<string[]>([]);
  const [numMolecules, setNumMolecules] = useState(15);
  const [similarity, setSimilarity] = useState(0.1);
  const [diversity, setDiversity] = useState(0.25);
  const [launching, setLaunching] = useState(false);
  const [lastResult, setLastResult] = useState<{ sessionId: string; status: string } | null>(null);

  const handleModeChange = useCallback((m: Mode) => {
    setMode(m);
    setSimilarity(MODE_DEFAULTS[m].similarity);
    setNumMolecules(MODE_DEFAULTS[m].molecules);
  }, []);

  const toggleTarget = useCallback((gene: string) => {
    setSelectedTargets((prev) =>
      prev.includes(gene) ? prev.filter((g) => g !== gene) : [...prev, gene]
    );
  }, []);

  const handleLaunch = useCallback(async () => {
    if (selectedTargets.length === 0) return;
    setLaunching(true);
    setLastResult(null);

    // Gather seeds for selected targets
    const seeds: Record<string, string[]> = {};
    for (const gene of selectedTargets) {
      const t = targets.find((x) => x.gene === gene);
      if (t?.seeds?.length) seeds[gene] = t.seeds;
    }

    try {
      const res = await fetch('/api/design-lab/launch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          mode,
          targets: selectedTargets,
          seeds,
          num_molecules: numMolecules,
          similarity_threshold: similarity,
          diversity_penalty: diversity,
          rounds_per_target: Object.fromEntries(
            selectedTargets.map((g) => {
              const t = targets.find((x) => x.gene === g);
              return [g, t?.directiveRounds || 3];
            })
          ),
        }),
      });
      const data = await res.json();
      setLastResult(data);
    } catch (e) {
      setLastResult({ sessionId: '', status: `error: ${e}` });
    } finally {
      setLaunching(false);
    }
  }, [mode, selectedTargets, numMolecules, similarity, diversity, targets]);

  const directiveTargets = targets.filter((t) => t.directiveOrder >= 0);

  return (
    <div className="p-4 flex flex-col gap-4" style={{ fontFamily: 'var(--font-mono)' }}>
      {/* Section: Mode */}
      <div>
        <div style={{ fontSize: '0.7rem', color: '#888', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: '0.5rem' }}>
          Design Mode
        </div>
        <div className="flex gap-1">
          {MODES.map((m) => (
            <button
              key={m.key}
              onClick={() => handleModeChange(m.key)}
              style={{
                flex: 1,
                padding: '0.5rem',
                fontSize: '0.65rem',
                fontFamily: 'var(--font-mono)',
                background: mode === m.key ? `${m.color}22` : '#111',
                border: `1px solid ${mode === m.key ? m.color : '#333'}`,
                color: mode === m.key ? m.color : '#666',
                borderRadius: '3px',
                cursor: 'pointer',
                fontWeight: mode === m.key ? 700 : 400,
              }}
            >
              {m.label}
            </button>
          ))}
        </div>
        <div style={{ fontSize: '0.6rem', color: '#666', marginTop: '0.3rem' }}>
          {MODES.find((m) => m.key === mode)?.desc}
        </div>
      </div>

      {/* Section: Target Selection */}
      <div>
        <div style={{ fontSize: '0.7rem', color: '#888', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: '0.5rem' }}>
          Targets ({selectedTargets.length} selected)
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: '0.25rem', maxHeight: '160px', overflowY: 'auto', scrollbarWidth: 'thin', scrollbarColor: '#333 transparent' }}>
          {directiveTargets.map((t) => {
            const selected = selectedTargets.includes(t.gene);
            return (
              <button
                key={t.gene}
                onClick={() => toggleTarget(t.gene)}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  padding: '0.3rem 0.5rem',
                  fontSize: '0.6rem',
                  fontFamily: 'var(--font-mono)',
                  background: selected ? '#1a2a1a' : '#111',
                  border: `1px solid ${selected ? '#22C55E44' : '#222'}`,
                  color: selected ? '#22C55E' : '#888',
                  borderRadius: '3px',
                  cursor: 'pointer',
                  textAlign: 'left',
                }}
              >
                <span>{t.gene}</span>
                <span style={{ color: '#666', fontSize: '0.55rem' }}>{t.compoundCount > 0 ? `${(t.compoundCount / 1000).toFixed(0)}K` : '0'}</span>
              </button>
            );
          })}
        </div>
      </div>

      {/* Section: Parameters */}
      <div>
        <div style={{ fontSize: '0.7rem', color: '#888', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: '0.5rem' }}>
          Parameters
        </div>
        <div className="flex flex-col gap-2" style={{ fontSize: '0.65rem' }}>
          <div className="flex items-center justify-between">
            <span style={{ color: '#888' }}>Molecules per seed</span>
            <input
              type="number"
              min={5}
              max={30}
              value={numMolecules}
              onChange={(e) => setNumMolecules(Number(e.target.value))}
              style={{ width: '50px', background: '#1a1a1a', color: '#ccc', border: '1px solid #333', padding: '0.2rem 0.4rem', borderRadius: '3px', fontSize: '0.65rem', fontFamily: 'var(--font-mono)', textAlign: 'right' }}
            />
          </div>
          <div className="flex items-center justify-between">
            <span style={{ color: '#888' }}>Similarity threshold</span>
            <div className="flex items-center gap-2">
              <input
                type="range"
                min={0}
                max={1}
                step={0.05}
                value={similarity}
                onChange={(e) => setSimilarity(Number(e.target.value))}
                style={{ width: '80px', accentColor: '#22C55E' }}
              />
              <span style={{ color: '#ccc', width: '30px', textAlign: 'right' }}>{similarity.toFixed(2)}</span>
            </div>
          </div>
          <div className="flex items-center justify-between">
            <span style={{ color: '#888' }}>Diversity penalty</span>
            <div className="flex items-center gap-2">
              <input
                type="range"
                min={0}
                max={0.5}
                step={0.05}
                value={diversity}
                onChange={(e) => setDiversity(Number(e.target.value))}
                style={{ width: '80px', accentColor: '#B8860B' }}
              />
              <span style={{ color: '#ccc', width: '30px', textAlign: 'right' }}>{diversity.toFixed(2)}</span>
            </div>
          </div>
        </div>
      </div>

      {/* Launch Button */}
      <button
        onClick={handleLaunch}
        disabled={launching || selectedTargets.length === 0}
        style={{
          padding: '0.75rem',
          fontSize: '0.75rem',
          fontFamily: 'var(--font-mono)',
          fontWeight: 700,
          textTransform: 'uppercase',
          letterSpacing: '0.15em',
          background: selectedTargets.length === 0 ? '#222' : launching ? '#333' : '#22C55E',
          color: selectedTargets.length === 0 ? '#666' : launching ? '#888' : '#000',
          border: 'none',
          borderRadius: '4px',
          cursor: selectedTargets.length === 0 ? 'not-allowed' : 'pointer',
        }}
      >
        {launching ? 'Launching...' : `Launch ${mode.toUpperCase()} — ${selectedTargets.length} target${selectedTargets.length !== 1 ? 's' : ''}`}
      </button>

      {lastResult && (
        <div style={{ fontSize: '0.65rem', color: lastResult.sessionId ? '#22C55E' : '#ff4444', fontFamily: 'var(--font-mono)' }}>
          {lastResult.sessionId ? `Session ${lastResult.sessionId.slice(0, 8)} created (${lastResult.status})` : lastResult.status}
        </div>
      )}
    </div>
  );
}
