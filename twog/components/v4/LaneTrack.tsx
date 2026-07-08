// The pre-registered test lanes (docking → cofolding → MD → omics) with each lane's status derived from
// the candidate's real capsules. A killed lane is the point — one failure rules the whole idea out.

import { verdictFromSignal } from '@/lib/verdict';
import type { Capsule } from '@/lib/types/public-detail';

const ORDER = ['docking', 'cofolding', 'md', 'omics'];
const LABEL: Record<string, string> = {
  docking: 'Docking', cofolding: 'Cofolding', md: 'MD', omics: 'Omics',
  binder_design: 'Binder design', degrader_design: 'Degrader design',
  genome_edit: 'Genome edit', cell_therapy: 'Cell therapy', mrna_vaccine: 'mRNA vaccine',
};
const label = (lane: string) => LABEL[lane] ?? lane.replace(/_/g, ' ');

export function LaneTrack({ capsules }: { capsules: Capsule[] }) {
  const byLane = new Map<string, Capsule>();
  for (const c of capsules) if (c.validation_type) byLane.set(c.validation_type, c);
  // The standard runnable track, plus any OTHER lane a capsule actually used (e.g. a future Stage-1
  // design-lane capsule) appended — so a real result is never silently dropped from the track.
  const lanes = [...ORDER, ...[...byLane.keys()].filter((l) => !ORDER.includes(l))];

  return (
    <div className="v4-lanes">
      {lanes.map((lane) => {
        const cap = byLane.get(lane);
        let cls = 'v4-lane--todo';
        let mark = '○';
        if (cap) {
          const v = verdictFromSignal(cap.signal);
          if (v === 'ruled-out') { cls = 'v4-lane--killed'; mark = '✕'; }
          // a gate-blocked (held) "supports" is NOT a survived lane — show it as in-progress, not ✓
          else if (v === 'still-standing' && !cap.held) { cls = 'v4-lane--held'; mark = '✓'; }
          else { cls = 'v4-lane--now'; mark = '●'; }
        }
        return (
          <span key={lane} className={`v4-lane ${cls}`}>
            <span aria-hidden>{mark}</span> {label(lane)}
          </span>
        );
      })}
    </div>
  );
}
