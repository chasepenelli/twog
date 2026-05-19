'use client';

import { useEffect, useState, useCallback } from 'react';
import { useSupabase } from './useSupabase';

export interface ServerHealth {
  pipeline_status: string;
  uptime_seconds: number;
  last_cycle_at: string;
  last_cycle_number: number;
  agent_errors_24h: number;
  papers_total: number;
  discoveries_total: number;
  designed_compounds_total: number;
  docking_results_total: number;
  memory_used_mb: number;
  memory_total_mb: number;
  cpu_load_1m: number;
}

export interface AgentStatus {
  agent_name: string;
  last_status: string;
  last_run_at: string;
  last_result: { message: string };
  run_count_today: number;
  error_count_today: number;
}

export interface LogEntry {
  id: number;
  level: string;
  message: string;
  created_at: string;
}

export function useMissionData() {
  const sb = useSupabase();
  const [health, setHealth] = useState<ServerHealth | null>(null);
  const [agents, setAgents] = useState<AgentStatus[]>([]);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchAll = useCallback(async () => {
    if (!sb) return;

    const [healthRes, agentsRes, logsRes] = await Promise.all([
      sb.from('server_health')
        .select('pipeline_status, uptime_seconds, last_cycle_at, last_cycle_number, agent_errors_24h, papers_total, discoveries_total, designed_compounds_total, docking_results_total, memory_used_mb, memory_total_mb, cpu_load_1m')
        .order('checked_at', { ascending: false })
        .limit(1)
        .single(),
      sb.from('agent_status')
        .select('agent_name, last_status, last_run_at, last_result, run_count_today, error_count_today')
        .order('last_run_at', { ascending: false }),
      sb.from('agent_logs')
        .select('id, level, message, created_at')
        .order('created_at', { ascending: false })
        .limit(200),
    ]);

    if (healthRes.data) setHealth(healthRes.data as ServerHealth);
    if (agentsRes.data) setAgents(agentsRes.data as AgentStatus[]);
    if (logsRes.data) setLogs(logsRes.data as LogEntry[]);
    setLoading(false);
  }, [sb]);

  useEffect(() => {
    if (!sb) return;

    /* Initial fetch */
    fetchAll();

    /* Poll every hour — realtime subscriptions handle live updates */
    const interval = setInterval(fetchAll, 3_600_000);

    /* Realtime: new log entries appear instantly */
    const logChannel = sb
      .channel('mission_logs')
      .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'agent_logs' }, (payload) => {
        const entry = payload.new as LogEntry;
        setLogs((prev) => [entry, ...prev].slice(0, 200));
      })
      .subscribe();

    /* Realtime: agent status updates */
    const agentChannel = sb
      .channel('mission_agents')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'agent_status' }, () => {
        /* Re-fetch all agent statuses on any change */
        sb.from('agent_status')
          .select('agent_name, last_status, last_run_at, last_result, run_count_today, error_count_today')
          .order('last_run_at', { ascending: false })
          .then(({ data }) => { if (data) setAgents(data as AgentStatus[]); });
      })
      .subscribe();

    /* Realtime: server health updates */
    const healthChannel = sb
      .channel('mission_health')
      .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'server_health' }, () => {
        sb.from('server_health')
          .select('pipeline_status, uptime_seconds, last_cycle_at, last_cycle_number, agent_errors_24h, papers_total, discoveries_total, designed_compounds_total, docking_results_total, memory_used_mb, memory_total_mb, cpu_load_1m')
          .order('checked_at', { ascending: false })
          .limit(1)
          .single()
          .then(({ data }) => { if (data) setHealth(data as ServerHealth); });
      })
      .subscribe();

    return () => {
      clearInterval(interval);
      sb.removeChannel(logChannel);
      sb.removeChannel(agentChannel);
      sb.removeChannel(healthChannel);
    };
  }, [sb, fetchAll]);

  return { health, agents, logs, loading };
}
