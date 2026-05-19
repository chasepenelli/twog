'use client';

import { useEffect, useState } from 'react';
import { useSupabase } from './useSupabase';

export function usePipelineVideo() {
  const sb = useSupabase();
  const [videoUrl, setVideoUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!sb) return;

    sb.from('pipeline_videos')
      .select('public_url')
      .eq('video_type', 'pipeline_journey')
      .order('created_at', { ascending: false })
      .limit(1)
      .then(({ data }) => {
        if (data && data.length > 0) {
          setVideoUrl(data[0].public_url);
        }
        setLoading(false);
      });

    /* Realtime: update when new pipeline video is rendered */
    const channel = sb
      .channel('pipeline_video_updates')
      .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'pipeline_videos' }, (payload) => {
        const row = payload.new as { video_type: string; public_url: string };
        if (row.video_type === 'pipeline_journey') {
          setVideoUrl(row.public_url);
        }
      })
      .subscribe();

    return () => { sb.removeChannel(channel); };
  }, [sb]);

  return { videoUrl, loading };
}
