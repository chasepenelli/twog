'use client';

import { DISCOVERY_TYPES } from '@/lib/constants';

interface DiscoveryCardProps {
  compound: string;
  type: string;
  description: string;
  score: number;
  confidence: number;
  createdAt: string;
  onClick: () => void;
}

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

export default function DiscoveryCard({ compound, type, description, score, confidence, createdAt, onClick }: DiscoveryCardProps) {
  const meta = DISCOVERY_TYPES[type] ?? { label: type, icon: '?' };
  const excerpt = description.length > 100 ? description.slice(0, 100) + '...' : description;

  return (
    <button
      onClick={onClick}
      className="text-left border border-[var(--gray-800)] p-5 transition-all duration-200 hover:border-[var(--gray-600)] hover:-translate-y-1 hover:shadow-[0_4px_20px_rgba(34,197,94,0.08)] w-full"
    >
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <span className="text-[0.5rem] uppercase tracking-[0.1em] text-[var(--gray-500)] mono">
          {meta.icon} {meta.label}
        </span>
        <span className="text-[0.5rem] text-[var(--gray-600)] mono">
          {timeAgo(createdAt)}
        </span>
      </div>

      {/* Compound */}
      <span className="block text-[0.85rem] font-bold mb-2 text-white truncate">
        {compound}
      </span>

      {/* Score */}
      <span className="block text-[1.5rem] font-bold mono text-white leading-none mb-1">
        {Number(score).toFixed(1)}
      </span>
      <span className="block text-[0.45rem] uppercase tracking-[0.15em] text-[var(--gray-500)] mb-3">
        Evidence Score
      </span>

      {/* Description excerpt */}
      <p className="text-[0.65rem] text-[var(--gray-400)] leading-relaxed mb-3 line-clamp-2">
        {excerpt}
      </p>

      {/* Confidence bar */}
      <div className="w-full h-1 bg-[var(--gray-800)] rounded-full overflow-hidden">
        <div
          className="h-full bg-[var(--gray-500)] rounded-full"
          style={{ width: `${Math.min(confidence, 100)}%` }}
        />
      </div>
    </button>
  );
}
