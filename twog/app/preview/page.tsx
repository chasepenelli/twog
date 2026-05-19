'use client';

import { useEffect, useRef, useState } from 'react';
import { gsap } from 'gsap';
import { ScrollTrigger } from 'gsap/ScrollTrigger';
import { useGSAP } from '@gsap/react';
import SplitTextReveal from '@/components/SplitTextReveal';
import ScrollReveal from '@/components/ScrollReveal';
import { usePipelineVideo } from '@/hooks/usePipelineVideo';
import { useSupabase } from '@/hooks/useSupabase';
import Link from 'next/link';
import graffitiImg from '@/app/assets/graffiti.png';
import { CONTACT_EMAIL, CONTACT_MAILTO } from '@/lib/constants';

gsap.registerPlugin(ScrollTrigger);

/** Live pulse */
function usePipelinePulse() {
  const sb = useSupabase();
  const [secondsAgo, setSecondsAgo] = useState<number | null>(null);

  useEffect(() => {
    if (!sb) return;
    async function fetch() {
      const { data } = await sb!.from('agent_status').select('last_run_at').order('last_run_at', { ascending: false }).limit(1);
      if (data?.[0]?.last_run_at) {
        const s = Math.floor((Date.now() - new Date(data[0].last_run_at).getTime()) / 1000);
        setSecondsAgo(s);
      }
    }
    fetch();
    const id = setInterval(fetch, 30_000);
    return () => clearInterval(id);
  }, [sb]);

  return secondsAgo;
}

function formatPulse(s: number | null): string {
  if (s === null) return '...';
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return `${Math.floor(s / 3600)}h ago`;
}

const MILESTONES = [
  { date: 'Apr 2026', text: 'First AI-designed compound holds stable in canine cancer protein', highlight: true },
  { date: 'Apr 2026', text: '3 designed molecules outperform FDA-approved drugs in physics simulation' },
  { date: 'Apr 2026', text: '7,200 research papers indexed and cross-referenced' },
  { date: 'Apr 2026', text: 'Simulation protocol validated against known cancer drugs' },
  { date: 'Mar 2026', text: '200,000+ novel molecules designed for canine protein targets' },
  { date: 'Mar 2026', text: 'Pipeline launched. First papers read. First molecules generated.' },
];

export default function PreviewHome() {
  const { videoUrl: pipelineVideoUrl } = usePipelineVideo();
  const secondsAgo = usePipelinePulse();
  const heroRef = useRef<HTMLDivElement>(null);
  const videoRef = useRef<HTMLDivElement>(null);
  const scrollIndicatorRef = useRef<HTMLDivElement>(null);

  const pulseActive = secondsAgo !== null && secondsAgo < 900;

  useGSAP(() => {
    if (heroRef.current) {
      gsap.to(heroRef.current, { y: -100, ease: 'none', scrollTrigger: { trigger: heroRef.current, start: 'top top', end: 'bottom top', scrub: true } });
    }
    if (videoRef.current) {
      gsap.fromTo(videoRef.current, { y: 40 }, { y: -40, ease: 'none', scrollTrigger: { trigger: videoRef.current, start: 'top bottom', end: 'bottom top', scrub: true } });
    }
    if (scrollIndicatorRef.current) {
      gsap.to(scrollIndicatorRef.current, { opacity: 0, y: 20, ease: 'none', scrollTrigger: { trigger: scrollIndicatorRef.current, start: 'top 95%', end: 'top 75%', scrub: true } });
    }
  });

  return (
    <>
      {/* ── HERO ── */}
      <section className="relative min-h-screen flex flex-col items-center justify-center px-6 text-center">
        <div ref={heroRef}>
          <ScrollReveal delay={0.2} y={10}>
            <div className="flex items-center justify-center gap-2 mb-8">
              <span className="inline-block w-2 h-2 rounded-full" style={{
                backgroundColor: pulseActive ? 'var(--green)' : 'var(--gray-400)',
                boxShadow: pulseActive ? '0 0 8px var(--green), 0 0 20px rgba(34,197,94,0.3)' : 'none',
                animation: pulseActive ? 'pulse-glow 2s ease-in-out infinite' : 'none',
              }} />
              <span className="text-[0.6rem] uppercase tracking-[0.25em] mono" style={{ color: pulseActive ? 'var(--green)' : 'var(--gray-400)' }}>
                {pulseActive ? `Running now · last cycle ${formatPulse(secondsAgo)}` : 'Pipeline active'}
              </span>
            </div>
          </ScrollReveal>

          <SplitTextReveal as="h1" className="text-[14vw] md:text-[11vw] leading-[0.85] font-bold" stagger={0.03} duration={1.2}>
            TWOG
          </SplitTextReveal>

          <ScrollReveal className="mt-8" delay={0.6} y={20}>
            <h2 className="text-[4.5vw] md:text-[2.8vw] lg:text-[2.2vw] leading-[1.1] font-bold uppercase tracking-[-0.02em]">
              Finding what medicine hasn&apos;t found yet
            </h2>
          </ScrollReveal>

          <ScrollReveal className="mt-4" delay={0.8} y={15}>
            <p className="text-[0.85rem] uppercase tracking-[0.3em] text-[var(--gray-400)]">
              What happens when you stop waiting and start building
            </p>
          </ScrollReveal>
        </div>

        <div ref={scrollIndicatorRef} className="absolute bottom-8 flex flex-col items-center gap-2">
          <span className="text-[0.55rem] uppercase tracking-[0.15em] text-[var(--gray-300)]">Scroll</span>
          <div className="w-[1px] h-8 bg-[var(--gray-200)] relative overflow-hidden">
            <div className="w-full bg-[var(--foreground)] absolute top-0" style={{ height: '50%', animation: 'scrollLine 2s cubic-bezier(0.16, 1, 0.3, 1) infinite' }} />
          </div>
        </div>

        <style jsx>{`
          @keyframes scrollLine { 0% { transform: translateY(-100%); } 50% { transform: translateY(200%); } 100% { transform: translateY(200%); } }
          @keyframes pulse-glow { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        `}</style>
      </section>

      {/* ── THE SHIFT ── */}
      <section className="dark-section py-24 md:py-36 px-6">
        <div className="max-w-3xl mx-auto text-center">
          <ScrollReveal>
            <p className="text-[0.55rem] uppercase tracking-[0.25em] text-[var(--gray-500)] mb-10">
              Something Changed
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.15}>
            <h2 className="text-[7vw] md:text-[4.5vw] lg:text-[3.5vw] text-white leading-[1.05] font-bold">
              Drug discovery used to take a billion dollars and a decade.
            </h2>
          </ScrollReveal>

          <ScrollReveal delay={0.3}>
            <p className="text-[1.1rem] md:text-[1.25rem] leading-[2] text-[var(--gray-400)] mt-10">
              AI can read every paper published on a disease in hours. It can design molecules
              that have never existed. Physics engines can simulate whether those molecules
              actually work. GPU clouds rent supercomputer time for the cost of dinner.
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.45}>
            <p className="text-[1.2rem] md:text-[1.35rem] leading-[2] text-white mt-8 font-bold">
              For the first time in history, one person with the right tools can do
              what a pharmaceutical company couldn&apos;t.
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.6}>
            <p className="text-[1rem] md:text-[1.1rem] leading-[2] text-[var(--gray-500)] mt-8">
              That is not a prediction. That is what is happening right now.
            </p>
          </ScrollReveal>
        </div>
      </section>

      {/* ── GRAFFITI ── */}
      <section className="py-24 md:py-32 px-6">
        <div className="max-w-3xl mx-auto text-center">
          <ScrollReveal>
            <div className="flex flex-col items-center mb-10">
              <div className="w-40 h-40 md:w-56 md:h-56 rounded-full overflow-hidden mb-8 border-2 border-[var(--gray-200)]">
                <img src={graffitiImg.src} alt="Graffiti the corgi" className="w-full h-full object-cover" />
              </div>
            </div>
          </ScrollReveal>

          <ScrollReveal delay={0.15}>
            <p className="text-[1.1rem] md:text-[1.2rem] leading-[2] text-[var(--gray-600)]">
              Our corgi Graffiti was diagnosed with hemangiosarcoma. A blood vessel cancer
              with no good treatment. The standard of care hasn&apos;t changed in thirty years.
              We built TWOG because we couldn&apos;t accept that.
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.3}>
            <p className="text-[1.2rem] md:text-[1.35rem] leading-[2] text-[var(--foreground)] mt-8 font-bold">
              For Graffiti. And for every dog that comes after him.
            </p>
          </ScrollReveal>
        </div>
      </section>

      {/* ── MARQUEE ── */}
      <div className="overflow-hidden py-6" style={{ maskImage: 'linear-gradient(to right, transparent, black 5%, black 95%, transparent)', WebkitMaskImage: 'linear-gradient(to right, transparent, black 5%, black 95%, transparent)' }}>
        <div className="whitespace-nowrap inline-flex" style={{ animation: 'marquee-home 35s linear infinite' }}>
          {[...Array(2)].map((_, i) => (
            <span key={i} className="text-[10vw] md:text-[7vw] font-bold uppercase tracking-[-0.03em] text-[var(--gray-100)] pr-20 select-none" style={{ WebkitTextStroke: '1px var(--gray-200)' }}>
              Finding what medicine hasn&apos;t found yet
              <span className="px-12 text-[var(--green)]">&bull;</span>
            </span>
          ))}
        </div>
      </div>

      {/* ── MILESTONES ── */}
      <section className="py-24 md:py-32 px-6">
        <div className="max-w-2xl mx-auto">
          <ScrollReveal>
            <p className="text-[0.55rem] uppercase tracking-[0.25em] text-[var(--gray-400)] mb-12 text-center">
              What Has Happened So Far
            </p>
          </ScrollReveal>

          <div className="space-y-6">
            {MILESTONES.map((m, i) => (
              <ScrollReveal key={i} delay={0.1 * i}>
                <div className={`flex items-start gap-4 ${m.highlight ? 'pl-4 border-l-2 border-[var(--green)]' : 'pl-4 border-l border-[var(--gray-200)]'}`}>
                  <div>
                    <span className="text-[0.6rem] uppercase tracking-wider text-[var(--gray-400)] mono">{m.date}</span>
                    <p className={`text-[0.95rem] leading-relaxed mt-1 ${m.highlight ? 'text-[var(--foreground)] font-semibold' : 'text-[var(--gray-600)]'}`}>
                      {m.text}
                    </p>
                  </div>
                </div>
              </ScrollReveal>
            ))}
          </div>
        </div>
      </section>

      {/* ── HOW IT WORKS (video) ── */}
      <section className="py-24 md:py-36 px-6 text-center">
        <div className="max-w-2xl mx-auto">
          <ScrollReveal>
            <h2 className="text-[5vw] md:text-[4vw] lg:text-[3.5vw] mb-16">
              From Paper to Molecule in Minutes
            </h2>
          </ScrollReveal>
        </div>

        <div ref={videoRef} className="max-w-6xl mx-auto aspect-video bg-[var(--black)] rounded-lg overflow-hidden mb-16">
          <video autoPlay loop muted playsInline className="w-full h-full object-cover" src={pipelineVideoUrl ?? '/pipeline.mp4'} />
        </div>

        <ScrollReveal>
          <div className="flex flex-wrap items-center justify-center gap-4 md:gap-6">
            {['Read', 'Analyze', 'Design', 'Simulate', 'Validate', 'Evolve'].map((step, i) => (
              <span key={step} className="flex items-center gap-4 md:gap-6">
                <span className="text-[0.75rem] md:text-[0.85rem] uppercase tracking-[0.15em] text-[var(--gray-500)] mono">{step}</span>
                {i < 5 && <span className="text-[var(--gray-300)]">&rarr;</span>}
              </span>
            ))}
          </div>
        </ScrollReveal>
      </section>

      {/* ── WHY THIS MATTERS ── */}
      <section className="dark-section py-24 md:py-36 px-6">
        <div className="max-w-3xl mx-auto text-center">
          <ScrollReveal>
            <p className="text-[0.55rem] uppercase tracking-[0.25em] text-[var(--gray-500)] mb-10">
              Why This Matters
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.15}>
            <h2 className="text-[6vw] md:text-[4vw] lg:text-[3vw] text-white leading-[1.1] font-bold">
              Thousands of diseases have no one working on them.
            </h2>
          </ScrollReveal>

          <ScrollReveal delay={0.3}>
            <p className="text-[1.05rem] md:text-[1.15rem] leading-[2] text-[var(--gray-400)] mt-10">
              If a disease affects too few patients, the economics do not justify the research.
              No funding. No clinical trials. No new drugs. The families dealing with these
              diagnoses hear the same thing every time: there is nothing new.
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.45}>
            <p className="text-[1.05rem] md:text-[1.15rem] leading-[2] text-[var(--gray-400)] mt-6">
              Hemangiosarcoma in dogs is one of those diseases. Thirty years. Same treatment.
              Same outcomes. Not because the science is impossible. Because nobody with
              resources decided it was worth doing.
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.6}>
            <p className="text-[1.15rem] md:text-[1.25rem] leading-[2] text-white mt-8 font-bold">
              AI changes that equation. When the tools are accessible, the only thing
              you need is a reason to care.
            </p>
          </ScrollReveal>
        </div>
      </section>

      {/* ── HOW AI CHANGES THIS ── */}
      <section className="py-24 md:py-36 px-6">
        <div className="max-w-3xl mx-auto text-center">
          <ScrollReveal>
            <p className="text-[0.55rem] uppercase tracking-[0.25em] text-[var(--gray-400)] mb-10">
              A New Kind of Lab
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.15}>
            <p className="text-[1.1rem] md:text-[1.2rem] leading-[2] text-[var(--gray-600)]">
              TWOG is built by one person. Not a pharmaceutical company. Not a university lab.
              One person with AI tools that did not exist three years ago.
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.3}>
            <p className="text-[1.1rem] md:text-[1.2rem] leading-[2] text-[var(--gray-600)] mt-6">
              The pipeline reads more papers in a day than a researcher reads in a year.
              It designs molecules faster than a medicinal chemistry team. It tests them
              with real physics on rented GPU servers that cost less than a coffee per hour.
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.45}>
            <p className="text-[1.1rem] md:text-[1.2rem] leading-[2] text-[var(--gray-600)] mt-6">
              This is not about replacing scientists. It is about giving the problems
              that scientists never get funded to work on a fighting chance.
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.6}>
            <p className="text-[1.2rem] md:text-[1.35rem] leading-[2] text-[var(--foreground)] mt-10 font-bold">
              What happens when anyone with a reason to care has access to these tools?
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.7}>
            <p className="text-[1rem] text-[var(--gray-500)] mt-4">
              We are about to find out.
            </p>
          </ScrollReveal>
        </div>
      </section>

      {/* ── THE ROAD AHEAD ── */}
      <section className="dark-section py-24 md:py-36 px-6">
        <div className="max-w-3xl mx-auto text-center">
          <ScrollReveal>
            <p className="text-[0.55rem] uppercase tracking-[0.25em] text-[var(--gray-500)] mb-10">
              The Road Ahead
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.15}>
            <h2 className="text-[6vw] md:text-[4vw] lg:text-[3vw] text-white leading-[1.1] font-bold">
              This is the first page. Not the last.
            </h2>
          </ScrollReveal>

          <div className="mt-12 space-y-8 text-left max-w-xl mx-auto">
            {[
              { label: 'Now', text: 'AI-designed molecules validated against canine cancer proteins with real physics.' },
              { label: 'Next', text: 'Lab synthesis and cell-based testing of top candidates.' },
              { label: 'Then', text: 'Expand to all 15 canine cancer targets. Open-source every finding.' },
              { label: 'Beyond', text: 'A framework any disease community can use. Not just hemangiosarcoma. Any neglected disease. Any species.' },
            ].map((item, i) => (
              <ScrollReveal key={i} delay={0.15 * (i + 1)}>
                <div className="flex items-start gap-4">
                  <span className="text-[0.7rem] uppercase tracking-wider text-[var(--green)] mono font-bold mt-1 w-16 flex-shrink-0">{item.label}</span>
                  <p className="text-[1rem] md:text-[1.1rem] leading-[1.8] text-[var(--gray-300)]">{item.text}</p>
                </div>
              </ScrollReveal>
            ))}
          </div>
        </div>
      </section>

      {/* ── JOIN ── */}
      <section className="py-24 md:py-36 px-6">
        <div className="max-w-2xl mx-auto text-center">
          <ScrollReveal>
            <h2 className="text-[5vw] md:text-[3.5vw] lg:text-[2.8vw] font-bold leading-[1.1] mb-8">
              Follow the Search
            </h2>
          </ScrollReveal>

          <ScrollReveal delay={0.15}>
            <p className="text-[1rem] md:text-[1.1rem] text-[var(--gray-500)] leading-relaxed mb-10">
              Every breakthrough and every mistake is shared openly.
              Subscribe to follow what happens next, or email {CONTACT_EMAIL}.
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.3}>
            <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
              <a href="https://pushingc.substack.com" target="_blank" rel="noopener noreferrer"
                className="px-8 py-3 bg-[var(--foreground)] text-white rounded-lg text-[0.8rem] font-bold uppercase tracking-wider hover:opacity-90 transition-opacity">
                Subscribe on Substack
              </a>
              <Link href="/treatments"
                className="px-8 py-3 border-2 border-[var(--foreground)] rounded-lg text-[0.8rem] font-bold uppercase tracking-wider hover:bg-[var(--foreground)] hover:text-white transition-all">
                View Treatments
              </Link>
              <a href={CONTACT_MAILTO}
                className="px-8 py-3 border-2 border-[var(--foreground)] rounded-lg text-[0.8rem] font-bold uppercase tracking-wider hover:bg-[var(--foreground)] hover:text-white transition-all">
                Contact
              </a>
            </div>
          </ScrollReveal>

          <ScrollReveal delay={0.5}>
            <p className="mt-12 text-[0.85rem] text-[var(--gray-400)] leading-relaxed">
              If you are here because your dog was just diagnosed, I am sorry.
              Everything the pipeline finds is shared at{' '}
              <Link href="/treatments" className="text-[var(--foreground)] underline">twog.bio/treatments</Link>.
              Talk to your vet. You are not alone in this.
            </p>
          </ScrollReveal>

          <ScrollReveal delay={0.6}>
            <div className="flex items-center justify-center gap-6 mt-10">
              <a href="https://pushingc.substack.com" target="_blank" rel="noopener noreferrer" className="text-[0.6rem] uppercase tracking-[0.12em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors">Substack</a>
              <a href="https://www.instagram.com/bradythecorgi/" target="_blank" rel="noopener noreferrer" className="text-[0.6rem] uppercase tracking-[0.12em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors">Instagram</a>
              <a href="https://graffitihsa.substack.com" target="_blank" rel="noopener noreferrer" className="text-[0.6rem] uppercase tracking-[0.12em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors">Walking With Graffiti</a>
              <a href={CONTACT_MAILTO} className="text-[0.6rem] uppercase tracking-[0.12em] text-[var(--gray-400)] hover:text-[var(--foreground)] transition-colors">Contact</a>
            </div>
          </ScrollReveal>
        </div>
      </section>

      {/* ── CLOSING ── */}
      <section className="dark-section py-16 px-6 text-center">
        <ScrollReveal>
          <p className="text-[1.1rem] md:text-[1.3rem] text-white font-bold">
            For Graffiti. For Brady. For every dog after them.
          </p>
          <p className="text-[0.75rem] text-[var(--gray-500)] mt-4 uppercase tracking-widest">
            Built by Chase &middot; Denver, CO &middot; {CONTACT_EMAIL}
          </p>
        </ScrollReveal>
      </section>
    </>
  );
}
