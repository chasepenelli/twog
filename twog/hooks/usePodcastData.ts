'use client';

import { useEffect, useState } from 'react';
import { useSupabase } from './useSupabase';

export interface Episode {
  id: number;
  title: string;
  description: string | null;
  transcript: string;
  audio_url: string | null;
  cover_image_url: string | null;
  duration_seconds: number | null;
  file_size_bytes: number | null;
  source_type: string;
  source_id: number | null;
  speaker_1_name: string | null;
  speaker_2_name: string | null;
  status: string;
  created_at: string;
}

export function usePodcastData() {
  const sb = useSupabase();
  const [episodes, setEpisodes] = useState<Episode[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!sb) return;

    /* Initial fetch — only ready episodes */
    sb.from('podcast_episodes')
      .select('*')
      .eq('status', 'ready')
      .order('created_at', { ascending: false })
      .then(({ data }) => {
        if (data) setEpisodes(data as Episode[]);
        setLoading(false);
      });

    /* Realtime: new episodes or status changes */
    const channel = sb
      .channel('podcast_episodes_listen')
      .on(
        'postgres_changes',
        { event: '*', schema: 'public', table: 'podcast_episodes' },
        (payload) => {
          const entry = payload.new as Episode;
          if (!entry || entry.status !== 'ready') return;

          setEpisodes((prev) => {
            const exists = prev.find((e) => e.id === entry.id);
            if (exists) {
              return prev.map((e) => (e.id === entry.id ? entry : e));
            }
            return [entry, ...prev];
          });
        },
      )
      .subscribe();

    return () => {
      sb.removeChannel(channel);
    };
  }, [sb]);

  const latest = episodes[0] ?? null;

  return { episodes, latest, loading };
}
