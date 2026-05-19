'use client';

interface ConfidenceMeterProps {
  score: number;
  maxScore?: number;
}

export default function ConfidenceMeter({ score, maxScore = 10 }: ConfidenceMeterProps) {
  const pct = (score / maxScore) * 100;
  const color = score >= 7 ? 'var(--green)' : score >= 5 ? 'var(--foreground)' : 'var(--gray-400)';

  return (
    <div className="flex items-center gap-6">
      {/* Large number */}
      <div className="text-center">
        <span className="block text-[3rem] font-bold mono leading-none" style={{ color }}>
          {score}
        </span>
        <span className="block text-[0.5rem] uppercase tracking-[0.2em] text-[var(--gray-400)] mt-1">
          / {maxScore}
        </span>
      </div>

      {/* Bar */}
      <div className="flex-1">
        <div className="h-1 bg-[var(--gray-200)] rounded-full overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-700"
            style={{ width: `${pct}%`, backgroundColor: color }}
          />
        </div>
        <span className="block text-[0.5rem] uppercase tracking-[0.15em] text-[var(--gray-400)] mt-2">
          Confidence
        </span>
      </div>
    </div>
  );
}
