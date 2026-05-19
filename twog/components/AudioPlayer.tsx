'use client';

import { useRef, useState, useEffect, useCallback } from 'react';

interface AudioPlayerProps {
  audioUrl: string;
  title: string;
  duration: number;
}

function formatTime(seconds: number): string {
  const m = Math.floor(seconds / 60);
  const s = Math.floor(seconds % 60);
  return `${m}:${s.toString().padStart(2, '0')}`;
}

const SPEEDS = [1, 1.5, 2] as const;

export default function AudioPlayer({ audioUrl, title, duration }: AudioPlayerProps) {
  const audioRef = useRef<HTMLAudioElement>(null);
  const progressRef = useRef<HTMLDivElement>(null);

  const [playing, setPlaying] = useState(false);
  const [currentTime, setCurrentTime] = useState(0);
  const [audioDuration, setAudioDuration] = useState(duration);
  const [speedIdx, setSpeedIdx] = useState(0);
  const [dragging, setDragging] = useState(false);

  /* Sync duration once metadata loads */
  useEffect(() => {
    const audio = audioRef.current;
    if (!audio) return;

    const onMeta = () => {
      if (audio.duration && isFinite(audio.duration)) {
        setAudioDuration(audio.duration);
      }
    };
    const onTime = () => {
      if (!dragging) setCurrentTime(audio.currentTime);
    };
    const onEnd = () => setPlaying(false);

    audio.addEventListener('loadedmetadata', onMeta);
    audio.addEventListener('timeupdate', onTime);
    audio.addEventListener('ended', onEnd);

    return () => {
      audio.removeEventListener('loadedmetadata', onMeta);
      audio.removeEventListener('timeupdate', onTime);
      audio.removeEventListener('ended', onEnd);
    };
  }, [dragging]);

  /* Play / Pause */
  const toggle = useCallback(() => {
    const audio = audioRef.current;
    if (!audio) return;
    if (playing) {
      audio.pause();
    } else {
      audio.play();
    }
    setPlaying(!playing);
  }, [playing]);

  /* Speed cycle */
  const cycleSpeed = useCallback(() => {
    const next = (speedIdx + 1) % SPEEDS.length;
    setSpeedIdx(next);
    if (audioRef.current) {
      audioRef.current.playbackRate = SPEEDS[next];
    }
  }, [speedIdx]);

  /* Scrub via click / drag */
  const seek = useCallback(
    (clientX: number) => {
      const bar = progressRef.current;
      const audio = audioRef.current;
      if (!bar || !audio) return;

      const rect = bar.getBoundingClientRect();
      const pct = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
      const t = pct * audioDuration;
      audio.currentTime = t;
      setCurrentTime(t);
    },
    [audioDuration],
  );

  const onPointerDown = useCallback(
    (e: React.PointerEvent) => {
      setDragging(true);
      seek(e.clientX);
      (e.target as HTMLElement).setPointerCapture(e.pointerId);
    },
    [seek],
  );

  const onPointerMove = useCallback(
    (e: React.PointerEvent) => {
      if (dragging) seek(e.clientX);
    },
    [dragging, seek],
  );

  const onPointerUp = useCallback(() => {
    setDragging(false);
  }, []);

  const progress = audioDuration > 0 ? (currentTime / audioDuration) * 100 : 0;

  return (
    <div className="w-full">
      <audio ref={audioRef} src={audioUrl} preload="metadata" />

      {/* Title */}
      <p className="text-[0.55rem] uppercase tracking-[0.2em] text-[var(--gray-400)] mb-4 mono">
        Now Playing
      </p>

      {/* Controls row */}
      <div className="flex items-center gap-4 mb-3">
        {/* Play / Pause */}
        <button
          onClick={toggle}
          className="w-12 h-12 flex items-center justify-center border border-[var(--gray-200)] hover:bg-[var(--foreground)] hover:text-[var(--background)] hover:border-[var(--foreground)] transition-all duration-300 text-lg shrink-0"
          aria-label={playing ? 'Pause' : 'Play'}
        >
          {playing ? '\u23F8' : '\u25B6'}
        </button>

        {/* Progress bar */}
        <div className="flex-1 flex flex-col gap-1">
          <div
            ref={progressRef}
            className="w-full h-2 bg-[var(--gray-200)] cursor-pointer relative group"
            onPointerDown={onPointerDown}
            onPointerMove={onPointerMove}
            onPointerUp={onPointerUp}
          >
            <div
              className="h-full bg-[var(--green)] transition-[width] duration-100 ease-linear"
              style={{ width: `${progress}%` }}
            />
            {/* Scrub handle */}
            <div
              className="absolute top-1/2 -translate-y-1/2 w-3 h-3 bg-[var(--foreground)] rounded-full opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none"
              style={{ left: `calc(${progress}% - 6px)` }}
            />
          </div>

          {/* Time */}
          <div className="flex justify-between">
            <span className="text-[0.55rem] mono text-[var(--gray-400)]">
              {formatTime(currentTime)}
            </span>
            <span className="text-[0.55rem] mono text-[var(--gray-400)]">
              {formatTime(audioDuration)}
            </span>
          </div>
        </div>

        {/* Speed */}
        <button
          onClick={cycleSpeed}
          className="text-[0.6rem] mono border border-[var(--gray-200)] px-2 py-1 hover:bg-[var(--foreground)] hover:text-[var(--background)] hover:border-[var(--foreground)] transition-all duration-300 shrink-0"
          aria-label="Playback speed"
        >
          {SPEEDS[speedIdx]}x
        </button>

        {/* Download */}
        <a
          href={audioUrl}
          download
          className="text-[0.55rem] mono uppercase tracking-[0.1em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors shrink-0"
          aria-label={`Download ${title}`}
        >
          DL
        </a>
      </div>
    </div>
  );
}
