'use client';

import { useState } from 'react';
import ScrollReveal from '@/components/ScrollReveal';
import SplitTextReveal from '@/components/SplitTextReveal';
import AudioPlayer from '@/components/AudioPlayer';
import EpisodeCard from '@/components/EpisodeCard';
import TranscriptView from '@/components/TranscriptView';
import { usePodcastData } from '@/hooks/usePodcastData';
import type { Episode } from '@/hooks/usePodcastData';
import Link from 'next/link';

function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString('en-US', {
    weekday: 'long',
    month: 'long',
    day: 'numeric',
    year: 'numeric',
  });
}

function formatDuration(seconds: number): string {
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds % 60);
  if (m === 0) return `${s}s`;
  return s > 0 ? `${m}m ${s}s` : `${m} min`;
}

/** Split episodes into pairs: long (deep dive) and short (brief) */
function classifyEpisodes(episodes: Episode[]): {
  deepDive: Episode | null;
  quickBrief: Episode | null;
  archive: Episode[];
} {
  if (episodes.length === 0) return { deepDive: null, quickBrief: null, archive: [] };

  // Sort by date desc, then split: longest recent = deep dive, shortest recent = brief
  const sorted = [...episodes].sort(
    (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
  );

  // Take the two most recent — the longer one is the deep dive
  const recent = sorted.slice(0, 2);
  const rest = sorted.slice(2);

  if (recent.length === 1) {
    return { deepDive: recent[0], quickBrief: null, archive: rest };
  }

  const [a, b] = recent;
  const durA = a.duration_seconds ?? 0;
  const durB = b.duration_seconds ?? 0;

  if (durA >= durB) {
    return { deepDive: a, quickBrief: b, archive: rest };
  }
  return { deepDive: b, quickBrief: a, archive: rest };
}

export default function ListenPage() {
  const { episodes, loading } = usePodcastData();
  const { deepDive, quickBrief, archive } = classifyEpisodes(episodes);
  const [expanded, setExpanded] = useState<'deep' | 'brief' | null>(null);

  return (
    <>
      {/* -- HERO -- */}
      <section className="pt-32 pb-8 px-6 text-center">
        <div className="max-w-2xl mx-auto">
          <ScrollReveal>
            <p className="text-[0.65rem] uppercase tracking-[0.3em] text-[var(--gray-400)] mb-4">
              The TWOG Research Podcast
            </p>
          </ScrollReveal>
          <SplitTextReveal
            as="h1"
            className="text-[8vw] md:text-[6vw] lg:text-[5vw] leading-[0.9] font-bold"
            stagger={0.03}
            duration={1}
          >
            Listen
          </SplitTextReveal>
          <ScrollReveal delay={0.4}>
            <p className="text-[0.8rem] text-[var(--gray-500)] leading-relaxed mt-6 max-w-lg mx-auto">
              AI-generated conversations about our drug discovery pipeline.
              Choose the deep dive or get the quick version.
            </p>
          </ScrollReveal>
        </div>
      </section>

      {/* -- LOADING -- */}
      {loading && (
        <section className="py-24 px-6 text-center">
          <p className="text-[0.7rem] mono text-[var(--gray-400)] animate-pulse">
            Loading episodes...
          </p>
        </section>
      )}

      {/* -- EMPTY STATE -- */}
      {!loading && episodes.length === 0 && (
        <section className="py-24 px-6 text-center">
          <ScrollReveal>
            <div className="max-w-md mx-auto">
              <p className="text-[2rem] mb-4">--</p>
              <p className="text-[0.85rem] text-[var(--gray-400)] leading-relaxed mb-2">
                No episodes yet.
              </p>
              <p className="text-[0.7rem] text-[var(--gray-500)] leading-relaxed">
                The pipeline is preparing the first research podcast.
              </p>
            </div>
          </ScrollReveal>
        </section>
      )}

      {/* -- TWO-COLUMN LATEST EPISODES -- */}
      {(deepDive || quickBrief) && (
        <section className="py-16 md:py-24 px-6">
          <div className="max-w-6xl mx-auto">

            {/* Date header */}
            {deepDive && (
              <ScrollReveal>
                <p className="text-[0.55rem] uppercase tracking-[0.2em] text-[var(--gray-400)] mb-10 text-center">
                  Latest · {formatDate(deepDive.created_at)}
                </p>
              </ScrollReveal>
            )}

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 lg:gap-12">

              {/* -- DEEP DIVE COLUMN -- */}
              {deepDive && deepDive.audio_url && (
                <ScrollReveal>
                  <div className="border border-[var(--gray-200)] p-6 md:p-8">
                    {/* Label */}
                    <div className="flex items-center gap-3 mb-5">
                      <span className="text-[0.55rem] uppercase tracking-[0.25em] font-bold text-[var(--green)]">
                        Deep Dive
                      </span>
                      <span className="text-[0.5rem] mono text-[var(--gray-400)]">
                        {formatDuration(deepDive.duration_seconds ?? 0)}
                      </span>
                    </div>

                    {/* Cover */}
                    {deepDive.cover_image_url && (
                      <div className="aspect-video bg-[var(--black)] overflow-hidden mb-5">
                        <img
                          src={deepDive.cover_image_url}
                          alt={deepDive.title}
                          className="w-full h-full object-cover"
                        />
                      </div>
                    )}

                    {/* Title + Description */}
                    <h2 className="text-[1.3rem] md:text-[1.5rem] leading-[1.15] font-bold mb-3">
                      {deepDive.title}
                    </h2>
                    <p className="text-[0.75rem] text-[var(--gray-500)] leading-relaxed mb-2">
                      The full conversation. Two hosts break down the latest pipeline developments
                      in detail — the science, the strategy, and what it means for dogs like Graffiti.
                      Best for when you have time to really dig in.
                    </p>
                    {deepDive.description && (
                      <p className="text-[0.7rem] text-[var(--gray-400)] leading-relaxed mb-5 italic">
                        {deepDive.description}
                      </p>
                    )}

                    {/* Player */}
                    <AudioPlayer
                      audioUrl={deepDive.audio_url}
                      title={deepDive.title}
                      duration={deepDive.duration_seconds ?? 0}
                    />

                    {/* Transcript toggle */}
                    {deepDive.transcript && (
                      <div className="mt-4">
                        <TranscriptView transcript={deepDive.transcript} />
                      </div>
                    )}
                  </div>
                </ScrollReveal>
              )}

              {/* -- QUICK BRIEF COLUMN -- */}
              {quickBrief && quickBrief.audio_url && (
                <ScrollReveal delay={0.15}>
                  <div className="border border-[var(--gray-200)] p-6 md:p-8">
                    {/* Label */}
                    <div className="flex items-center gap-3 mb-5">
                      <span className="text-[0.55rem] uppercase tracking-[0.25em] font-bold" style={{ color: 'var(--foreground)' }}>
                        Quick Brief
                      </span>
                      <span className="text-[0.5rem] mono text-[var(--gray-400)]">
                        {formatDuration(quickBrief.duration_seconds ?? 0)}
                      </span>
                    </div>

                    {/* Cover */}
                    {quickBrief.cover_image_url && (
                      <div className="aspect-video bg-[var(--black)] overflow-hidden mb-5">
                        <img
                          src={quickBrief.cover_image_url}
                          alt={quickBrief.title}
                          className="w-full h-full object-cover"
                        />
                      </div>
                    )}

                    {/* Title + Description */}
                    <h2 className="text-[1.3rem] md:text-[1.5rem] leading-[1.15] font-bold mb-3">
                      {quickBrief.title}
                    </h2>
                    <p className="text-[0.75rem] text-[var(--gray-500)] leading-relaxed mb-2">
                      The headlines in under two minutes. Same story, same hosts — just the
                      key takeaways without the deep science. Perfect for a quick catch-up
                      on the go.
                    </p>
                    {quickBrief.description && (
                      <p className="text-[0.7rem] text-[var(--gray-400)] leading-relaxed mb-5 italic">
                        {quickBrief.description}
                      </p>
                    )}

                    {/* Player */}
                    <AudioPlayer
                      audioUrl={quickBrief.audio_url}
                      title={quickBrief.title}
                      duration={quickBrief.duration_seconds ?? 0}
                    />

                    {/* Transcript toggle */}
                    {quickBrief.transcript && (
                      <div className="mt-4">
                        <TranscriptView transcript={quickBrief.transcript} />
                      </div>
                    )}
                  </div>
                </ScrollReveal>
              )}

              {/* If only one episode, show placeholder for the other column */}
              {deepDive && !quickBrief && (
                <ScrollReveal delay={0.15}>
                  <div className="border border-dashed border-[var(--gray-300)] p-6 md:p-8 flex items-center justify-center min-h-[300px]">
                    <div className="text-center">
                      <p className="text-[0.55rem] uppercase tracking-[0.25em] text-[var(--gray-400)] mb-2">
                        Quick Brief
                      </p>
                      <p className="text-[0.75rem] text-[var(--gray-400)]">
                        Coming soon
                      </p>
                    </div>
                  </div>
                </ScrollReveal>
              )}
            </div>
          </div>
        </section>
      )}

      {/* -- ARCHIVE GRID -- */}
      {archive.length > 0 && (
        <section className="dark-section py-16 md:py-24 px-6">
          <div className="text-center mb-12">
            <ScrollReveal>
              <p className="text-[0.55rem] uppercase tracking-[0.2em] text-[var(--gray-500)] mb-3">
                Previous Episodes
              </p>
              <h2 className="text-[4vw] md:text-[3vw] lg:text-[2.5vw]">
                Archive
              </h2>
            </ScrollReveal>
          </div>

          <div className="max-w-6xl mx-auto grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {archive.map((ep, idx) => (
              <EpisodeCard
                key={ep.id}
                episodeNumber={archive.length - idx}
                title={ep.title}
                date={ep.created_at}
                duration={ep.duration_seconds}
                coverUrl={ep.cover_image_url}
                onClick={() => {
                  window.scrollTo({ top: 0, behavior: 'smooth' });
                }}
              />
            ))}
          </div>
        </section>
      )}

      {/* -- CTA -- */}
      <section className="py-16 px-6 text-center">
        <Link href="/" className="btn mr-4">Home</Link>
        <Link href="/research" className="btn mr-4">Research</Link>
        <a
          href="https://pushingc.substack.com"
          target="_blank"
          rel="noopener noreferrer"
          className="btn"
        >
          Subscribe on Substack
        </a>
      </section>
    </>
  );
}
