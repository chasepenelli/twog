'use client';

import StatusBadge from './StatusBadge';

interface AgentRowProps {
  name: string;
  status: string;
  lastRun: string;
  message: string;
}

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

function cleanName(name: string): string {
  return name.replace(/_/g, ' ').replace(/\bagent\b/i, '').trim();
}

export default function AgentRow({ name, status, lastRun, message }: AgentRowProps) {
  const badgeStatus = status === 'ok' ? 'ok' : status === 'error' ? 'error' : 'stopped';

  return (
    <div className="grid grid-cols-[auto_1fr_auto] md:grid-cols-[auto_10rem_1fr_auto] items-center gap-3 md:gap-4 py-3 border-b border-[var(--gray-200)]">
      <StatusBadge status={badgeStatus} />
      <span className="text-[0.8rem] font-bold mono capitalize">{cleanName(name)}</span>
      <span className="hidden md:block text-[0.75rem] text-[var(--gray-400)] truncate">
        {message}
      </span>
      <span className="text-[0.65rem] text-[var(--gray-300)] mono text-right whitespace-nowrap">
        {timeAgo(lastRun)}
      </span>
    </div>
  );
}
