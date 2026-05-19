'use client';

import { useEffect, useState, useCallback } from 'react';

export interface FeedEntry {
  id: number;
  level: string;
  message: string;
  created_at: string;
}

export function useDesignLabFeed(pollInterval = 15000) {
  const [entries, setEntries] = useState<FeedEntry[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchFeed = useCallback(async () => {
    try {
      const res = await fetch('/api/design-lab/feed?limit=200');
      const data = await res.json();
      setEntries(Array.isArray(data) ? data : []);
    } catch (e) {
      console.error('Failed to fetch feed:', e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchFeed();
    const interval = setInterval(fetchFeed, pollInterval);
    return () => clearInterval(interval);
  }, [fetchFeed, pollInterval]);

  return { entries, loading, refresh: fetchFeed };
}
