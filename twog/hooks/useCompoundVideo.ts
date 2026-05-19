'use client';

import { useEffect, useState } from 'react';
import { useSupabase } from './useSupabase';

export function useCompoundVideo(compoundId?: number, compoundName?: string) {
  const sb = useSupabase();
  const [videoUrl, setVideoUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!sb || (!compoundId && !compoundName)) {
      setLoading(false);
      return;
    }

    async function fetch() {
      let resolvedId = compoundId;

      // If no compoundId but we have a name, look it up
      if (!resolvedId && compoundName) {
        const { data: compound } = await sb!.from('designed_compounds')
          .select('id')
          .eq('name', compoundName)
          .limit(1);
        if (compound?.[0]) {
          resolvedId = compound[0].id;
        }
      }

      let query = sb!
        .from('pipeline_videos')
        .select('public_url')
        .eq('video_type', 'molecule_reveal')
        .order('created_at', { ascending: false })
        .limit(1);

      if (resolvedId) {
        query = query.eq('compound_id', resolvedId);
      }

      const { data } = await query;
      if (data && data.length > 0) {
        setVideoUrl(data[0].public_url);
      }
      setLoading(false);
    }

    fetch();
  }, [sb, compoundId, compoundName]);

  return { videoUrl, loading };
}
