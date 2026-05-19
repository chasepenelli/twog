'use client';

import { DISCOVERY_TYPES } from '@/lib/constants';
import Molecule3D from './Molecule3D';
import type { Discovery } from '@/hooks/useResearchData';

interface DiscoveryDetailProps {
  discovery: Discovery;
  onClose: () => void;
}

export default function DiscoveryDetail({ discovery, onClose }: DiscoveryDetailProps) {
  const meta = DISCOVERY_TYPES[discovery.discovery_type] ?? { label: discovery.discovery_type, icon: '?' };
  const date = new Date(discovery.created_at).toLocaleString('en-US', {
    month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit',
  });

  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center" onClick={onClose}>
      <div className="absolute inset-0 bg-black/40" />

      <div
        className="relative bg-[var(--background)] w-full max-w-3xl max-h-[80vh] overflow-y-auto border-t border-[var(--gray-200)] px-8 py-8"
        onClick={(e) => e.stopPropagation()}
      >
        <button
          onClick={onClose}
          className="absolute top-4 right-6 text-[var(--gray-400)] hover:text-[var(--foreground)] text-lg mono"
        >
          &times;
        </button>

        {/* 3D molecule */}
        <div className="flex justify-center mb-8">
          <Molecule3D compoundName={discovery.compound_name} size={240} />
        </div>

        {/* Header */}
        <span className="text-[0.55rem] uppercase tracking-[0.15em] text-[var(--gray-400)] mono">
          {meta.icon} {meta.label}
        </span>
        <h3 className="text-[1.5rem] font-bold mt-1 mb-1 normal-case tracking-normal leading-normal">
          {discovery.compound_name}
        </h3>
        <span className="text-[0.6rem] text-[var(--gray-400)] mono">{date}</span>

        {/* Stats row */}
        <div className="flex gap-8 mt-6 mb-6">
          <div>
            <span className="block text-[0.5rem] uppercase tracking-[0.1em] text-[var(--gray-400)]">Evidence</span>
            <span className="block text-[1.2rem] font-bold mono">{Number(discovery.evidence_score).toFixed(1)}</span>
          </div>
          <div>
            <span className="block text-[0.5rem] uppercase tracking-[0.1em] text-[var(--gray-400)]">Confidence</span>
            <span className="block text-[1.2rem] font-bold mono">{discovery.confidence}%</span>
          </div>
        </div>

        {/* Description */}
        <p className="text-[0.85rem] leading-relaxed mono text-[var(--gray-500)]">
          {discovery.description}
        </p>
      </div>
    </div>
  );
}
