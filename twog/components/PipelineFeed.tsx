'use client';

import { useEffect, useState } from 'react';
import { useSupabase } from '@/hooks/useSupabase';

export default function PipelineFeed() {
  const sb = useSupabase();
  const [feed, setFeed] = useState('');

  useEffect(() => {
    if (!sb) return;

    sb.from('agent_logs')
      .select('message, created_at')
      .order('created_at', { ascending: false })
      .limit(30)
      .then(({ data }) => {
        if (data && data.length > 0) {
          const text = data
            .map((l) => l.message)
            .join('    ·    ');
          setFeed(text);
        }
      });

    const channel = sb
      .channel('agent_logs_feed')
      .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'agent_logs' }, (payload) => {
        const msg = (payload.new as { message: string }).message;
        setFeed((prev) => prev ? `${msg}    ·    ${prev}` : msg);
      })
      .subscribe();

    return () => { sb.removeChannel(channel); };
  }, [sb]);

  if (!feed) return null;

  return (
    <div className="pipeline-marquee">
      <div className="pipeline-marquee-track">
        <span>{feed}</span>
        <span>{feed}</span>
      </div>
    </div>
  );
}
