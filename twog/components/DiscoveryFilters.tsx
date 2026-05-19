'use client';

import { DISCOVERY_TYPES } from '@/lib/constants';
import type { Filters } from '@/hooks/useResearchData';

interface DiscoveryFiltersProps {
  filters: Filters;
  setFilters: (f: Filters) => void;
}

function Toggle({ active, label, onClick }: { active: boolean; label: string; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className={`text-[0.5rem] uppercase tracking-[0.08em] px-2 py-1 border transition-all duration-200 ${
        active
          ? 'bg-white text-[var(--black)] border-white'
          : 'bg-transparent text-[var(--gray-500)] border-[var(--gray-700)] hover:border-[var(--gray-500)]'
      }`}
    >
      {label}
    </button>
  );
}

export default function DiscoveryFilters({ filters, setFilters }: DiscoveryFiltersProps) {
  const types = [
    { key: 'all', label: 'All' },
    ...Object.entries(DISCOVERY_TYPES).map(([key, { label }]) => ({ key, label })),
  ];

  return (
    <div className="flex flex-wrap items-center justify-center gap-6 mb-8">
      {/* Type */}
      <div className="flex flex-wrap gap-1.5">
        {types.map((t) => (
          <Toggle
            key={t.key}
            active={filters.type === t.key}
            label={t.label}
            onClick={() => setFilters({ ...filters, type: t.key })}
          />
        ))}
      </div>

      <span className="w-px h-4 bg-[var(--gray-700)]" />

      {/* Sort */}
      <div className="flex gap-1.5">
        <Toggle active={filters.sort === 'recent'} label="Recent" onClick={() => setFilters({ ...filters, sort: 'recent' })} />
        <Toggle active={filters.sort === 'score'} label="Score" onClick={() => setFilters({ ...filters, sort: 'score' })} />
      </div>

      <span className="w-px h-4 bg-[var(--gray-700)]" />

      {/* Range */}
      <div className="flex gap-1.5">
        {(['24h', '7d', 'all'] as const).map((r) => (
          <Toggle key={r} active={filters.range === r} label={r} onClick={() => setFilters({ ...filters, range: r })} />
        ))}
      </div>
    </div>
  );
}
