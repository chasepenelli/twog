'use client';

interface StatusBadgeProps {
  status: 'running' | 'ok' | 'stopped' | 'error';
  label?: string;
}

const colors = {
  running: 'bg-green-500',
  ok: 'bg-green-500',
  stopped: 'bg-[var(--gray-400)]',
  error: 'bg-red-500',
};

const pingColors = {
  running: 'bg-green-400',
  ok: '',
  stopped: '',
  error: 'bg-red-400',
};

export default function StatusBadge({ status, label }: StatusBadgeProps) {
  const shouldPulse = status === 'running';

  return (
    <span className="inline-flex items-center gap-2">
      <span className="relative flex h-2 w-2">
        {shouldPulse && (
          <span className={`animate-ping absolute inline-flex h-full w-full rounded-full ${pingColors[status]} opacity-75`} />
        )}
        <span className={`relative inline-flex rounded-full h-2 w-2 ${colors[status]}`} />
      </span>
      {label && (
        <span className="text-[0.7rem] uppercase tracking-[0.15em] mono">
          {label}
        </span>
      )}
    </span>
  );
}
