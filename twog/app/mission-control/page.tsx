'use client';

import { useState, useMemo, useRef, useEffect } from 'react';
import { gsap } from 'gsap';
import StatusBadge from '@/components/StatusBadge';
import { useMissionData } from '@/hooks/useMissionData';

/* ── Pipeline module definitions ── */
const TIERS = [
  {
    label: 'Literature',
    modules: [
      { key: 'Literature', display: 'PubMed' },
      { key: 'SemanticScholar', display: 'Semantic Scholar' },
      { key: 'Biorxiv', display: 'bioRxiv' },
      { key: 'EuropePMC', display: 'Europe PMC' },
    ],
  },
  {
    label: 'Analysis',
    modules: [
      { key: 'Analysis', display: 'Analysis' },
      { key: 'Scoring', display: 'Scoring' },
      { key: 'Synthesis', display: 'Synthesis' },
    ],
  },
  {
    label: 'Enrichment',
    modules: [
      { key: 'PubChem', display: 'PubChem' },
      { key: 'ChEMBL', display: 'ChEMBL' },
      { key: 'OpenTargets', display: 'OpenTargets' },
      { key: 'ClinicalTrials', display: 'Clinical Trials' },
      { key: 'FDA', display: 'FDA' },
      { key: 'rnaseq_agent', display: 'RNA-Seq' },
    ],
  },
  {
    label: 'Deep Analysis',
    modules: [
      { key: 'Hypothesis', display: 'Hypothesis' },
      { key: 'Repurposing', display: 'Repurposing' },
      { key: 'ADMET', display: 'ADMET' },
      { key: 'combination', display: 'Combinations' },
    ],
  },
  {
    label: 'Design',
    modules: [
      { key: 'structure_agent', display: 'Structure' },
      { key: 'docking_agent', display: 'DiffDock' },
      { key: 'molgen_agent', display: 'MolGen' },
      { key: 'bionemo_agent', display: 'BioNeMo' },
      { key: 'design_loop_agent', display: 'Design Loop' },
    ],
  },
  {
    label: 'Validation',
    modules: [
      { key: 'validation_canine_homology', display: 'Canine Homology' },
      { key: 'validation_md_simulation', display: 'MD Simulation' },
      { key: 'validation_analog_generator', display: 'Analog Generator' },
      { key: 'validation_offtarget', display: 'Off-Target' },
      { key: 'validation_safety', display: 'Safety & ADMET' },
      { key: 'validation_synergy', display: 'Synergy' },
      { key: 'validation_ip_scout', display: 'IP & Literature' },
    ],
  },
  {
    label: 'Reporting',
    modules: [
      { key: 'report_agent', display: 'Report' },
      { key: 'content_agent', display: 'Content' },
      { key: 'thesis_agent', display: 'Thesis' },
    ],
  },
];

function formatUptime(seconds: number): string {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (h > 24) return `${Math.floor(h / 24)}d ${h % 24}h`;
  return `${h}h ${m}m`;
}

function formatTime(iso: string): string {
  return new Date(iso).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
}

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'now';
  if (mins < 60) return `${mins}m`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h`;
  return `${Math.floor(hrs / 24)}d`;
}

type LogFilter = 'all' | 'INFO' | 'ERROR';

export default function MissionControl() {
  const { health, logs, loading } = useMissionData();
  const [logFilter, setLogFilter] = useState<LogFilter>('all');

  /* Build a status map from logs: for each module key, find its most recent log.
     Log messages look like [ScoringAgent], [validation_canine_homology], etc.
     We match if the bracketed name starts with the module key (case-insensitive). */
  const statusMap = useMemo(() => {
    const allKeys = TIERS.flatMap((t) => t.modules.map((a) => a.key));
    const map: Record<string, { message: string; created_at: string; level: string }> = {};

    for (const log of logs) {
      const match = log.message.match(/^\[([^\]]+)\]/);
      if (!match) continue;
      const logName = match[1].toLowerCase();

      for (const key of allKeys) {
        if (map[key]) continue;
        if (logName.startsWith(key.toLowerCase())) {
          map[key] = { message: log.message, created_at: log.created_at, level: log.level };
        }
      }
    }
    return map;
  }, [logs]);

  const filteredLogs = logFilter === 'all'
    ? logs
    : logs.filter((l) => l.level === logFilter);

  const pipelineStatus = health?.pipeline_status?.toLowerCase() === 'running'
    ? 'running' as const
    : 'stopped' as const;

  /* Animate new log entries sliding in */
  const logContainerRef = useRef<HTMLDivElement>(null);
  const prevLogCountRef = useRef(0);

  useEffect(() => {
    if (!logContainerRef.current) return;
    const newCount = filteredLogs.length;
    const added = newCount - prevLogCountRef.current;
    if (added > 0 && prevLogCountRef.current > 0) {
      const entries = logContainerRef.current.querySelectorAll('.log-entry');
      const newEntries = Array.from(entries).slice(0, Math.min(added, 10));
      if (newEntries.length > 0) {
        gsap.from(newEntries, { x: 20, opacity: 0, duration: 0.3, stagger: 0.05, ease: 'power2.out' });
      }
    }
    prevLogCountRef.current = newCount;
  }, [filteredLogs.length]);

  /* Track previous status map for pulse detection */
  const prevStatusRef = useRef<Record<string, string>>({});
  const cardRefs = useRef<Record<string, HTMLDivElement | null>>({});

  useEffect(() => {
    for (const [key, status] of Object.entries(statusMap)) {
      const prevTime = prevStatusRef.current[key];
      if (prevTime && prevTime !== status.created_at && cardRefs.current[key]) {
        gsap.fromTo(cardRefs.current[key],
          { boxShadow: '0 0 0 rgba(34,197,94,0)' },
          { boxShadow: '0 0 16px rgba(34,197,94,0.4)', duration: 0.4, yoyo: true, repeat: 1, ease: 'power2.out' }
        );
      }
    }
    prevStatusRef.current = Object.fromEntries(
      Object.entries(statusMap).map(([k, v]) => [k, v.created_at])
    );
  }, [statusMap]);

  return (
    <div className="h-screen flex flex-col overflow-hidden pt-14">
      {/* ── TOP BAR ── */}
      <div className="shrink-0 px-6 py-3 flex items-center justify-between border-b border-[var(--gray-200)] bg-[var(--background)]">
        <div className="flex items-center gap-6">
          <h1 className="text-[0.7rem] font-bold uppercase tracking-[0.15em]">
            Mission Control
          </h1>
          <StatusBadge
            status={loading ? 'stopped' : pipelineStatus}
            label={loading ? '...' : (health?.pipeline_status ?? '—')}
          />
        </div>
        <div className="flex items-center gap-6">
          {health && (
            <>
              <Stat label="Cycle" value={`#${health.last_cycle_number}`} />
              <Stat label="Uptime" value={formatUptime(health.uptime_seconds)} />
              <Stat label="Errors" value={String(health.agent_errors_24h)} warn={health.agent_errors_24h > 0} />
              <Stat label="Papers" value={health.papers_total.toLocaleString()} />
              <Stat label="Molecules" value={health.designed_compounds_total.toLocaleString()} />
              <Stat label="Discoveries" value={health.discoveries_total.toLocaleString()} />
            </>
          )}
        </div>
      </div>

      {/* ── MAIN GRID ── */}
      <div className="flex-1 grid grid-cols-1 md:grid-cols-[1fr_400px] min-h-0">

        {/* LEFT — Pipeline Modules Grid */}
        <div className="flex flex-col min-h-0 border-r border-[var(--gray-200)]">
          <div className="shrink-0 px-5 py-2.5 border-b border-[var(--gray-200)]">
            <span className="text-[0.55rem] uppercase tracking-[0.2em] text-[var(--gray-400)]">
              Pipeline Modules
            </span>
          </div>
          <div className="flex-1 overflow-y-auto px-5 py-3">
            {TIERS.map((tier) => (
              <div key={tier.label} className="mb-4">
                {/* Tier label with line */}
                <div className="flex items-center gap-3 mb-2">
                  <span className="text-[0.55rem] uppercase tracking-[0.15em] text-[var(--gray-400)] shrink-0 mono">
                    {tier.label}
                  </span>
                  <span className="flex-1 h-px bg-[var(--gray-100)]" />
                </div>
                {/* Agent cards */}
                <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-2">
                  {tier.modules.map((mod) => {
                    const status = statusMap[mod.key];
                    const isActive = !!status;
                    const isError = status?.level === 'ERROR';
                    return (
                      <div
                        key={mod.key}
                        ref={(el) => { cardRefs.current[mod.key] = el; }}
                        className={`border rounded px-3 py-2.5 transition-all duration-200 ${
                          isError
                            ? 'border-red-200 bg-red-50'
                            : isActive
                              ? 'border-[var(--gray-200)] hover:border-[var(--gray-400)]'
                              : 'border-[var(--gray-100)] opacity-50'
                        }`}
                      >
                        <div className="flex items-center gap-2 mb-1">
                          <StatusBadge
                            status={isError ? 'error' : isActive ? 'ok' : 'stopped'}
                          />
                          <span className="text-[0.7rem] font-bold truncate">
                            {mod.display}
                          </span>
                        </div>
                        <span className="block text-[0.55rem] text-[var(--gray-300)] mono">
                          {status ? timeAgo(status.created_at) : '—'}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* RIGHT — Activity Log */}
        <div className="flex flex-col min-h-0">
          <div className="shrink-0 px-5 py-2.5 border-b border-[var(--gray-200)] flex items-center justify-between">
            <span className="text-[0.55rem] uppercase tracking-[0.2em] text-[var(--gray-400)]">
              System Log
            </span>
            <div className="flex gap-1.5">
              {(['all', 'INFO', 'ERROR'] as LogFilter[]).map((f) => (
                <button
                  key={f}
                  onClick={() => setLogFilter(f)}
                  className={`text-[0.5rem] uppercase tracking-[0.08em] px-2 py-1 border transition-all duration-200 ${
                    logFilter === f
                      ? 'bg-[var(--foreground)] text-[var(--background)] border-[var(--foreground)]'
                      : 'bg-transparent text-[var(--gray-400)] border-[var(--gray-200)] hover:border-[var(--gray-400)]'
                  }`}
                >
                  {f}
                </button>
              ))}
            </div>
          </div>
          <div ref={logContainerRef} className="flex-1 overflow-y-auto px-5">
            {filteredLogs.map((l) => (
              <div
                key={l.id}
                className="log-entry flex gap-3 py-2 border-b border-[var(--gray-100)] items-baseline"
              >
                <span className="text-[0.5rem] text-[var(--gray-300)] mono shrink-0">
                  {formatTime(l.created_at)}
                </span>
                <span className={`text-[0.65rem] mono leading-relaxed ${
                  l.level === 'ERROR' ? 'text-red-500' : 'text-[var(--gray-500)]'
                }`}>
                  {l.message.replace(/^\[[^\]]+\]\s*/, '')}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

/* Small stat chip for the top bar */
function Stat({ label, value, warn }: { label: string; value: string; warn?: boolean }) {
  return (
    <span className="text-[0.55rem] uppercase tracking-[0.08em] text-[var(--gray-400)] hidden md:inline">
      {label}{' '}
      <span className={`mono font-bold ${warn ? 'text-red-500' : 'text-[var(--foreground)]'}`}>
        {value}
      </span>
    </span>
  );
}
