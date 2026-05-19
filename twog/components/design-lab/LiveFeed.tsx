'use client';

import { useRef, useEffect } from 'react';
import type { FeedEntry } from '@/hooks/useDesignLabFeed';

function levelColor(level: string): string {
  if (level === 'ERROR') return '#ff4444';
  if (level === 'WARNING') return '#ff8844';
  return '#666';
}

function formatTime(iso: string): string {
  return new Date(iso).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
}

export default function LiveFeed({ entries }: { entries: FeedEntry[] }) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (containerRef.current) {
      containerRef.current.scrollTop = 0;
    }
  }, [entries.length]);

  return (
    <div className="flex flex-col h-full">
      <div
        className="shrink-0 px-4 py-2"
        style={{ borderBottom: '1px solid #222', fontSize: '0.7rem', fontFamily: 'var(--font-mono)', color: '#888', textTransform: 'uppercase', letterSpacing: '0.1em' }}
      >
        Live Feed
      </div>
      <div
        ref={containerRef}
        className="flex-1 overflow-y-auto px-4 py-2"
        style={{ scrollbarWidth: 'thin', scrollbarColor: '#333 transparent' }}
      >
        {entries.length === 0 && (
          <div style={{ color: '#444', fontFamily: 'var(--font-mono)', fontSize: '0.7rem', padding: '1rem 0' }}>
            No recent design activity
          </div>
        )}
        {entries.map((entry) => (
          <div
            key={entry.id}
            className="log-entry"
            style={{
              fontFamily: 'var(--font-mono)',
              fontSize: '0.65rem',
              lineHeight: 1.5,
              padding: '0.15rem 0',
              color: levelColor(entry.level),
              borderBottom: '1px solid #111',
            }}
          >
            <span style={{ color: '#444', marginRight: '0.5rem' }}>{formatTime(entry.created_at)}</span>
            <span>{entry.message}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
