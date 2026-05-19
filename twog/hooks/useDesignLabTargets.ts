'use client';

import { useEffect, useState, useCallback } from 'react';

export interface TargetInfo {
  gene: string;
  pdbId: string | null;
  uniprotId: string | null;
  pocket: [number, number, number] | null;
  compoundCount: number;
  mdCount: number;
  stableCount: number;
  mdPassRate: number;
  seeds: string[];
  directiveRounds: number;
  directiveOrder: number;
}

export function useDesignLabTargets() {
  const [targets, setTargets] = useState<TargetInfo[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchTargets = useCallback(async () => {
    try {
      const res = await fetch('/api/design-lab/targets');
      const data = await res.json();
      setTargets(data);
    } catch (e) {
      console.error('Failed to fetch targets:', e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchTargets();
  }, [fetchTargets]);

  return { targets, loading, refresh: fetchTargets };
}
