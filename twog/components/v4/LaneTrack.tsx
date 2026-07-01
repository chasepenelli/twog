// The pre-registered test lanes (docking → cofolding → MD → omics) with each lane's status derived from
// the candidate's real capsules. A killed lane is the point — one failure rules the whole idea out.

import { verdictFromSignal } from '@/lib/verdict';
import type { Capsule } from '@/lib/types/public-detail';

const LANES = ['docking', 'cofolding', 'md', 'omics'] as const;
const LABEL: Record<string, string> = { docking: 'Docking', cofolding: 'Cofolding', md: 'MD', omics: 'Omics' };

export function LaneTrack({ capsules }: { capsules: Capsule[] }) {
  const byLane = new Map<string, Capsule>();
  for (const c of capsules) if (c.validation_type) byLane.set(c.validation_type, c);

  return (
    <div className="v4-lanes">
      {LANES.map((lane) => {
        const cap = byLane.get(lane);
        let cls = 'v4-lane--todo';
        let mark = '○';
        if (cap) {
          const v = verdictFromSignal(cap.signal);
          if (v === 'ruled-out') { cls = 'v4-lane--killed'; mark = '✕'; }
          else if (v === 'still-standing') { cls = 'v4-lane--held'; mark = '✓'; }
          else { cls = 'v4-lane--now'; mark = '●'; }
        }
        return (
          <span key={lane} className={`v4-lane ${cls}`}>
            <span aria-hidden>{mark}</span> {LABEL[lane]}
          </span>
        );
      })}
    </div>
  );
}
