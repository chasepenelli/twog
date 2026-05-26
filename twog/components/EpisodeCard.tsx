'use client';

/* eslint-disable @next/next/no-img-element */

interface EpisodeCardProps {
  episodeNumber: number;
  title: string;
  date: string;
  duration: number | null;
  coverUrl: string | null;
  onClick: () => void;
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
}

function formatDuration(seconds: number): string {
  const m = Math.floor(seconds / 60);
  return `${m} min`;
}

export default function EpisodeCard({
  episodeNumber,
  title,
  date,
  duration,
  coverUrl,
  onClick,
}: EpisodeCardProps) {
  return (
    <button
      onClick={onClick}
      className="text-left border border-[var(--gray-200)] transition-all duration-200 hover:border-[var(--gray-400)] hover:-translate-y-1 hover:shadow-[0_4px_20px_rgba(34,197,94,0.08)] w-full group"
    >
      {/* Cover */}
      <div className="aspect-square bg-[var(--black)] relative overflow-hidden">
        {coverUrl ? (
          <img
            src={coverUrl}
            alt={title}
            className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-500"
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center">
            <span className="text-[3rem] font-bold mono text-[var(--gray-700)] leading-none">
              {String(episodeNumber).padStart(2, '0')}
            </span>
          </div>
        )}
      </div>

      {/* Info */}
      <div className="p-4">
        <div className="flex items-center justify-between mb-2">
          <span className="text-[0.5rem] uppercase tracking-[0.15em] text-[var(--gray-400)] mono">
            EP {String(episodeNumber).padStart(2, '0')}
          </span>
          <span className="text-[0.5rem] text-[var(--gray-400)] mono">
            {formatDate(date)}
          </span>
        </div>

        <h3 className="text-[0.8rem] font-bold leading-tight line-clamp-2 mb-2">
          {title}
        </h3>

        {duration && (
          <span className="text-[0.5rem] mono text-[var(--gray-400)] uppercase tracking-[0.1em]">
            {formatDuration(duration)}
          </span>
        )}
      </div>
    </button>
  );
}
