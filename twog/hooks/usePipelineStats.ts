'use client';

import { useEffect, useState } from 'react';
import { useSupabase } from './useSupabase';

export interface PipelineStats {
  papers: number;
  molecules: number;
  dockings: number;
  validated: number;
  cycles: number;
  discoveries: number;
}

const POLL_INTERVAL = 300_000; // refresh every 5 minutes

export function usePipelineStats() {
  const sb = useSupabase();
  const [stats, setStats] = useState<PipelineStats | null>(null);

  useEffect(() => {
    if (!sb) return;

    async function load() {
      const [papers, molecules, dockings, validated, cycles, discoveries] = await Promise.all([
        sb!.from('papers').select('id', { count: 'exact', head: true }),
        sb!.from('designed_compounds').select('id', { count: 'exact', head: true }).not('status', 'in', '("invalidated","failed","rejected")'),
        sb!.from('docking_results').select('id', { count: 'exact', head: true }),
        sb!.from('md_validations').select('id', { count: 'exact', head: true }),
        sb!.from('experiments').select('id', { count: 'exact', head: true }),
        sb!.from('discoveries').select('id', { count: 'exact', head: true }),
      ]);

      setStats({
        papers: papers.count ?? 0,
        molecules: molecules.count ?? 0,
        dockings: dockings.count ?? 0,
        validated: validated.count ?? 0,
        cycles: cycles.count ?? 0,
        discoveries: discoveries.count ?? 0,
      });
    }

    load();
    const id = setInterval(load, POLL_INTERVAL);
    return () => clearInterval(id);
  }, [sb]);

  return stats;
}
