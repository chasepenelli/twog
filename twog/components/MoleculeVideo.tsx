'use client';

import { useState } from 'react';
import { useCompoundVideo } from '@/hooks/useCompoundVideo';

interface MoleculeVideoProps {
  compoundId?: number;
  compoundName?: string;
  height?: string;
  className?: string;
  overlay?: boolean;
}

export default function MoleculeVideo({
  compoundId,
  compoundName,
  height = '300px',
  className = '',
  overlay = false,
}: MoleculeVideoProps) {
  const { videoUrl, loading } = useCompoundVideo(compoundId, compoundName);
  const [errored, setErrored] = useState(false);

  if (loading) {
    return (
      <div
        className={`flex items-center justify-center bg-[var(--black)] ${className}`}
        style={{ height, border: '1px solid rgba(34,197,94,0.3)', animation: 'pulse 2s ease-in-out infinite' }}
      >
        <span
          className="text-[0.6rem] uppercase tracking-[0.2em] mono"
          style={{ color: '#22C55E' }}
        >
          Rendering...
        </span>
      </div>
    );
  }

  if (!videoUrl || errored) {
    return (
      <div
        className={`flex items-center justify-center bg-[var(--black)] ${className}`}
        style={{ height, border: '1px solid #222' }}
      >
        <div className="text-center">
          <span
            className="block text-[1.2rem] font-bold mono"
            style={{ color: '#333' }}
          >
            {compoundName ?? '—'}
          </span>
          <span
            className="block text-[0.5rem] uppercase tracking-[0.15em] mt-2"
            style={{ color: '#444' }}
          >
            Video pending
          </span>
        </div>
      </div>
    );
  }

  return (
    <div className={`relative ${className}`} style={{ height }}>
      <video
        autoPlay
        loop
        muted
        playsInline
        className="w-full h-full object-cover"
        src={videoUrl}
        onError={() => setErrored(true)}
      />
      {overlay && (
        <div
          className="absolute inset-0"
          style={{ background: 'rgba(10,10,10,0.3)' }}
        />
      )}
    </div>
  );
}
