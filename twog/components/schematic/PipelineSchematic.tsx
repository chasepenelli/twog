'use client';

import { useEffect, useMemo, useState } from 'react';
import ReactFlow, {
  Background,
  BackgroundVariant,
  ReactFlowProvider,
  type Node,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { NODES, EDGES } from './pipelineGraph';
import TierNode from './nodes/TierNode';
import AgentNode from './nodes/AgentNode';
import ReportNode from './nodes/ReportNode';
import FlowEdge from './edges/FlowEdge';

const nodeTypes = { tier: TierNode, agent: AgentNode, report: ReportNode };
const edgeTypes = { flow: FlowEdge };

const PIPELINE_WIDTH = 1960;
const PIPELINE_HEIGHT = 3260;

function PipelineSchematicInner() {
  const [reducedMotion, setReducedMotion] = useState(() => {
    if (typeof window === 'undefined' || !window.matchMedia) return false;
    return window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  });
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    if (typeof window === 'undefined' || !window.matchMedia) return;
    const mq = window.matchMedia('(prefers-reduced-motion: reduce)');
    const handler = (e: MediaQueryListEvent) => setReducedMotion(e.matches);
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }, []);

  useEffect(() => {
    const t = window.setTimeout(() => setMounted(true), 80);
    return () => window.clearTimeout(t);
  }, []);

  const nodes: Node[] = useMemo(
    () =>
      NODES.map((n) => {
        if (n.type === 'tier') return n;
        const staggerMs = Math.max(0, ((n.position.y + 120) / PIPELINE_HEIGHT) * 700);
        return {
          ...n,
          data: {
            ...n.data,
            isPulsing: false,
            mounted,
            staggerMs,
            reducedMotion,
          },
        };
      }),
    [mounted, reducedMotion],
  );

  return (
    <div
      style={{
        width: '100%',
        overflowX: 'auto',
        overflowY: 'visible',
      }}
    >
      <div
        style={{
          width: PIPELINE_WIDTH,
          height: PIPELINE_HEIGHT,
          position: 'relative',
          margin: '0 auto',
        }}
      >
        <ReactFlow
          nodes={nodes}
          edges={EDGES}
          nodeTypes={nodeTypes}
          edgeTypes={edgeTypes}
          defaultViewport={{ x: 40, y: 40, zoom: 1 }}
          minZoom={1}
          maxZoom={1}
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable={false}
          panOnDrag={false}
          panOnScroll={false}
          zoomOnScroll={false}
          zoomOnPinch={false}
          zoomOnDoubleClick={false}
          preventScrolling={false}
          proOptions={{ hideAttribution: true }}
        >
          <Background
            variant={BackgroundVariant.Dots}
            gap={28}
            size={0.8}
            color="var(--gray-100)"
          />
        </ReactFlow>
      </div>
      <style jsx global>{`
        @media (prefers-reduced-motion: no-preference) {
          @keyframes twog-schematic-pulse {
            0%,
            100% {
              opacity: 0.85;
              box-shadow: 0 0 0 0 rgba(34, 197, 94, 0);
            }
            50% {
              opacity: 1;
              box-shadow: 0 0 0 8px rgba(34, 197, 94, 0.14);
            }
          }
        }
      `}</style>
    </div>
  );
}

export default function PipelineSchematic() {
  return (
    <ReactFlowProvider>
      <PipelineSchematicInner />
    </ReactFlowProvider>
  );
}
