'use client';

import { useState } from 'react';
import { useDesignLabStats } from '@/hooks/useDesignLabStats';
import { useDesignLabTargets } from '@/hooks/useDesignLabTargets';
import { useDesignLabFeed } from '@/hooks/useDesignLabFeed';
import FunnelChart from '@/components/design-lab/FunnelChart';
import TargetHeatmap from '@/components/design-lab/TargetHeatmap';
import CompoundBrowser from '@/components/design-lab/CompoundBrowser';
import LiveFeed from '@/components/design-lab/LiveFeed';
import ChatPanel from '@/components/design-lab/ChatPanel';
import DesignControls from '@/components/design-lab/DesignControls';

type Tab = 'intelligence' | 'compounds';
type RightTab = 'chat' | 'feed';

export default function DesignLab() {
  const { stats, loading: statsLoading } = useDesignLabStats();
  const { targets, loading: targetsLoading, refresh: refreshTargets } = useDesignLabTargets();
  const { entries: feedEntries } = useDesignLabFeed();
  const [tab, setTab] = useState<Tab>('intelligence');
  const [rightTab, setRightTab] = useState<RightTab>('chat');

  return (
    <div className="dark-section h-screen flex flex-col overflow-hidden" style={{ background: 'var(--black)' }}>
      {/* ── TOP BAR ── */}
      <div
        className="shrink-0 px-6 py-3 flex items-center justify-between"
        style={{ borderBottom: '1px solid #222', background: '#111' }}
      >
        <div className="flex items-center gap-6">
          <h1 className="text-lg font-bold tracking-tight" style={{ color: '#fff', fontFamily: 'var(--font-mono)' }}>
            DESIGN LAB
          </h1>
          <div className="flex items-center gap-2">
            <div
              className="w-2 h-2 rounded-full"
              style={{ background: '#22C55E', animation: 'pulse 2s ease-in-out infinite' }}
            />
            <span style={{ color: '#888', fontSize: '0.75rem', fontFamily: 'var(--font-mono)' }}>
              LIVE
            </span>
          </div>
        </div>
        {stats && (
          <div className="flex items-center gap-6" style={{ fontSize: '0.75rem', fontFamily: 'var(--font-mono)', color: '#888' }}>
            <span>{stats.designed.toLocaleString()} designed</span>
            <span>{stats.mdScreened.toLocaleString()} MD tested</span>
            <span style={{ color: '#22C55E' }}>{stats.mdStable} stable</span>
            <span style={{ color: '#B8860B' }}>{stats.v3Validated} v3</span>
          </div>
        )}
      </div>

      {/* ── MAIN CONTENT ── */}
      <div className="flex-1 flex overflow-hidden">
        {/* LEFT PANEL (60%) */}
        <div className="flex-[3] flex flex-col overflow-hidden" style={{ borderRight: '1px solid #222' }}>
          {/* Tab switcher */}
          <div className="shrink-0 flex" style={{ borderBottom: '1px solid #222' }}>
            {(['intelligence', 'compounds'] as Tab[]).map((t) => (
              <button
                key={t}
                onClick={() => setTab(t)}
                style={{
                  padding: '0.5rem 1.5rem',
                  fontSize: '0.7rem',
                  fontFamily: 'var(--font-mono)',
                  textTransform: 'uppercase',
                  letterSpacing: '0.1em',
                  background: tab === t ? '#1a1a1a' : 'transparent',
                  color: tab === t ? '#fff' : '#666',
                  border: 'none',
                  borderBottom: tab === t ? '2px solid #22C55E' : '2px solid transparent',
                  cursor: 'pointer',
                }}
              >
                {t}
              </button>
            ))}
          </div>

          {/* Tab content */}
          <div className="flex-1 overflow-y-auto p-6" style={{ scrollbarWidth: 'thin', scrollbarColor: '#333 transparent' }}>
            {tab === 'intelligence' && (
              <div className="flex flex-col gap-6">
                <FunnelChart stats={stats} loading={statsLoading} />
                <TargetHeatmap targets={targets} loading={targetsLoading} />
              </div>
            )}
            {tab === 'compounds' && (
              <CompoundBrowser targets={targets} />
            )}
          </div>
        </div>

        {/* RIGHT PANEL (40%) */}
        <div className="flex-[2] flex flex-col overflow-hidden">
          {/* Design Controls (top) */}
          <div className="shrink-0 overflow-y-auto" style={{ maxHeight: '45%', borderBottom: '1px solid #222', scrollbarWidth: 'thin', scrollbarColor: '#333 transparent' }}>
            <DesignControls targets={targets} onRefreshTargets={refreshTargets} />
          </div>

          {/* Chat / Feed tabs (bottom) */}
          <div className="flex-1 flex flex-col overflow-hidden">
            <div className="shrink-0 flex" style={{ borderBottom: '1px solid #222' }}>
              {(['chat', 'feed'] as RightTab[]).map((t) => (
                <button
                  key={t}
                  onClick={() => setRightTab(t)}
                  style={{
                    padding: '0.5rem 1.5rem',
                    fontSize: '0.7rem',
                    fontFamily: 'var(--font-mono)',
                    textTransform: 'uppercase',
                    letterSpacing: '0.1em',
                    background: rightTab === t ? '#1a1a1a' : 'transparent',
                    color: rightTab === t ? '#fff' : '#666',
                    border: 'none',
                    borderBottom: rightTab === t ? '2px solid #22C55E' : '2px solid transparent',
                    cursor: 'pointer',
                  }}
                >
                  {t}
                </button>
              ))}
            </div>
            <div className="flex-1 overflow-hidden">
              {rightTab === 'chat' && <ChatPanel />}
              {rightTab === 'feed' && <LiveFeed entries={feedEntries} />}
            </div>
          </div>
        </div>
      </div>

      <style jsx global>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.4; }
        }
      `}</style>
    </div>
  );
}
