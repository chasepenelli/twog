'use client';

import { BaseEdge, getSmoothStepPath, type EdgeProps } from 'reactflow';

export default function FlowEdge(props: EdgeProps) {
  const {
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    style = {},
    markerEnd,
    id,
    data,
  } = props;

  const [edgePath] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    borderRadius: 8,
  });

  const dashed = !!(data as { dashed?: boolean } | undefined)?.dashed;
  const hue = (data as { hue?: string } | undefined)?.hue;

  return (
    <BaseEdge
      id={id}
      path={edgePath}
      markerEnd={markerEnd}
      style={{
        stroke: hue || 'var(--gray-400)',
        strokeWidth: dashed ? 1 : 1.5,
        strokeDasharray: dashed ? '6 4' : undefined,
        opacity: dashed ? 0.7 : 1,
        ...style,
      }}
    />
  );
}
