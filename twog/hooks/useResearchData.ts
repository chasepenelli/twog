'use client';

import { useEffect, useState } from 'react';
import { useSupabase } from './useSupabase';

export interface Digest {
  id: number;
  title: string;
  content: string;
  period: string;
  created_at: string;
}

export interface Highlight {
  id: number;
  title: string;
  body: string | null;
  highlight_type: string;
  created_at: string;
}

// Legacy types — kept for backward compat with unused components
export interface Discovery {
  id: number;
  compound_name: string;
  discovery_type: string;
  description: string;
  evidence_score: number;
  confidence: number;
  created_at: string;
}

export interface Hypothesis {
  id: number;
  hypothesis_text: string;
  rationale: string;
  confidence: number;
  novelty_score: number;
  feasibility_score: number;
  risk_level: string;
  actionable_steps: string;
  compounds_involved: string[];
  pathways_involved: string[];
  created_at: string;
}

export interface Filters {
  type: string;
  sort: 'recent' | 'score';
  range: '24h' | '7d' | 'all';
}

const POLL_INTERVAL = 600_000; // refresh every 10 minutes

export function useResearchData() {
  const sb = useSupabase();
  const [latestDigest, setLatestDigest] = useState<Digest | null>(null);
  const [previousDigests, setPreviousDigests] = useState<Digest[]>([]);
  const [highlights, setHighlights] = useState<Highlight[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!sb) return;

    async function fetchData() {
      // Get weekly narratives (preferred), fall back to thesis
      let { data: narratives } = await sb!
        .from('digests')
        .select('id, title, content, period, created_at')
        .eq('period', 'weekly_narrative')
        .order('created_at', { ascending: false })
        .limit(5);

      if (!narratives?.length) {
        const { data: theses } = await sb!
          .from('digests')
          .select('id, title, content, period, created_at')
          .eq('period', 'thesis')
          .order('created_at', { ascending: false })
          .limit(5);
        narratives = theses;
      }

      if (narratives?.length) {
        setLatestDigest(narratives[0] as Digest);
        setPreviousDigests(narratives.slice(1) as Digest[]);
      }

      // Get active highlights
      const { data: hl } = await sb!
        .from('highlights')
        .select('*')
        .eq('active', true)
        .order('created_at', { ascending: false })
        .limit(5);

      if (hl) setHighlights(hl as Highlight[]);

      setLoading(false);
    }

    fetchData();
    const id = setInterval(fetchData, POLL_INTERVAL);

    /* Realtime: re-fetch when digests are inserted or updated */
    const channel = sb.channel('digests_watch')
      .on('postgres_changes', {
        event: 'INSERT',
        schema: 'public',
        table: 'digests',
      }, () => {
        fetchData();
      })
      .on('postgres_changes', {
        event: 'UPDATE',
        schema: 'public',
        table: 'digests',
      }, () => {
        fetchData();
      })
      .subscribe();

    return () => {
      clearInterval(id);
      sb.removeChannel(channel);
    };
  }, [sb]);

  return { latestDigest, previousDigests, highlights, loading };
}
