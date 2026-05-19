'use client';

import { memo } from 'react';
import type { NodeProps } from 'reactflow';
import type { TierNodeData } from '../pipelineGraph';

function TierNode({ data }: NodeProps<TierNodeData>) {
  return (
    <div
      style={{
        width: 1880,
        paddingBottom: 12,
        borderBottom: '1px solid var(--gray-200)',
        display: 'flex',
        alignItems: 'baseline',
        gap: 14,
        fontFamily: 'var(--font-jetbrains-mono), monospace',
        fontSize: '0.95rem',
        lineHeight: 1.2,
        letterSpacing: '0.26em',
        textTransform: 'uppercase',
        color: 'var(--gray-500)',
      }}
    >
      {data.index ? (
        <span
          style={{
            fontFamily: 'var(--font-jetbrains-mono), monospace',
            fontSize: '0.82rem',
            letterSpacing: '0.14em',
            color: 'var(--gray-300)',
            flexShrink: 0,
          }}
        >
          {data.index}
        </span>
      ) : null}
      <span style={{ flex: 1 }}>{data.label}</span>
    </div>
  );
}

export default memo(TierNode);
