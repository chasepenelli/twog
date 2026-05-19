'use client';

import { memo, useState } from 'react';
import { Handle, Position, type NodeProps } from 'reactflow';
import type { ReportNodeData } from '../pipelineGraph';

interface ReportNodeExtendedData extends ReportNodeData {
  mounted?: boolean;
  staggerMs?: number;
  reducedMotion?: boolean;
}

function ReportNode({ data }: NodeProps<ReportNodeExtendedData>) {
  const [hovered, setHovered] = useState(false);
  const pulsing = data.isPulsing === true;
  const mounted = data.mounted === true;
  const reducedMotion = data.reducedMotion === true;
  const staggerMs = typeof data.staggerMs === 'number' ? data.staggerMs : 0;

  const entranceStyle = reducedMotion
    ? {
        opacity: 1,
        transform: 'translateY(0)',
        transition: 'none',
      }
    : mounted
    ? {
        opacity: 1,
        transform: 'translateY(0)',
        transition:
          'opacity 600ms cubic-bezier(0.16, 1, 0.3, 1), transform 600ms cubic-bezier(0.16, 1, 0.3, 1)',
        transitionDelay: `${staggerMs}ms`,
      }
    : {
        opacity: 0,
        transform: 'translateY(6px)',
        transition: 'none',
      };

  return (
    <div style={{ position: 'relative', ...entranceStyle }}>
      <div
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
        style={{
          width: 280,
          padding: '20px 22px',
          border: `1px solid ${hovered ? 'var(--foreground)' : 'var(--gray-200)'}`,
          borderRadius: 4,
          background: 'var(--background)',
          position: 'relative',
          transition: 'border-color 180ms ease, opacity 1.2s ease-in-out, box-shadow 1.2s ease-in-out',
          animation: pulsing ? 'twog-schematic-pulse 2s ease-in-out infinite' : undefined,
        }}
      >
        <Handle
          type="target"
          position={Position.Left}
          style={{ opacity: 0, pointerEvents: 'none' }}
          isConnectable={false}
        />
        <Handle
          type="source"
          position={Position.Right}
          style={{ opacity: 0, pointerEvents: 'none' }}
          isConnectable={false}
        />

        <div
          style={{
            fontFamily: 'var(--font-space-mono), monospace',
            fontWeight: 700,
            fontSize: '1.15rem',
            lineHeight: 1.2,
            letterSpacing: '-0.005em',
            textTransform: 'uppercase',
            color: 'var(--foreground)',
          }}
        >
          {data.label}
        </div>

        <span
          style={{
            display: 'inline-block',
            marginTop: 10,
            padding: '4px 10px',
            borderRadius: 2,
            border: '1px solid var(--gray-300)',
            background: 'var(--gray-100)',
            color: 'var(--gray-600)',
            fontFamily: 'var(--font-jetbrains-mono), monospace',
            fontSize: '0.74rem',
            letterSpacing: '0.18em',
            textTransform: 'uppercase',
          }}
        >
          Output
        </span>

        {/* Tooltip */}
        <div
          style={{
            position: 'absolute',
            bottom: 'calc(100% + 8px)',
            left: '50%',
            transform: 'translateX(-50%)',
            background: '#1a1a1a',
            color: '#f0f0f0',
            fontFamily: 'var(--font-space-mono), monospace',
            fontSize: '0.78rem',
            lineHeight: 1.5,
            padding: '10px 12px',
            borderRadius: 4,
            maxWidth: 320,
            width: 'max-content',
            boxShadow: '0 8px 24px rgba(0,0,0,0.18)',
            opacity: hovered ? 1 : 0,
            transition: 'opacity 150ms ease',
            pointerEvents: 'none',
            zIndex: 50,
          }}
        >
          {data.tip}
        </div>
      </div>
    </div>
  );
}

export default memo(ReportNode);
