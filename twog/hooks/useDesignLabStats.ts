'use client';

import { useEffect, useState, useCallback } from 'react';

export interface PipelineStats {
  designed: number;
  docked: number;
  mdScreened: number;
  mdStable: number;
  v3Validated: number;
}

export function useDesignLabStats(pollInterval = 30000) {
  const [stats, setStats] = useState<PipelineStats | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchStats = useCallback(async () => {
    try {
      const res = await fetch('/api/design-lab/stats');
      const data = await res.json();
      setStats(data);
    } catch (e) {
      console.error('Failed to fetch design lab stats:', e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchStats();
    const interval = setInterval(fetchStats, pollInterval);
    return () => clearInterval(interval);
  }, [fetchStats, pollInterval]);

  return { stats, loading, refresh: fetchStats };
}
