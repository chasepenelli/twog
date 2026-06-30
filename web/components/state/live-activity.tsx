"use client";

import { useEffect, useState } from "react";

import { api } from "@/lib/api";
import type { ActivityFeed } from "@/lib/types/domain";

/* LiveActivity — the REAL engine ledger (replaces the old curated "watch it think" script). Polls
   /public/activity and renders the merged reverse-chron stream of what the engine ACTUALLY did, with
   real statuses + an HONEST idle/online indicator (idle when there's no runnable work — never faked busy).
   `variant="dark"` is the terminal-style hero panel; "light" is the in-page section. */

const LIGHT_BADGE: Record<string, { cls: string; label: string }> = {
  compute: { cls: "b-blue", label: "COMPUTE" },
  agent: { cls: "b-muted", label: "AGENT" },
  capsule: { cls: "b-green", label: "CAPSULE" },
  campaign: { cls: "b-muted", label: "CAMPAIGN" },
};
const DARK_TAG: Record<string, { label: string; color: string }> = {
  compute: { label: "COMPUTE", color: "#6f9bff" },
  agent: { label: "AGENT", color: "#8a8f98" },
  capsule: { label: "CAPSULE", color: "#3ec27a" },
  campaign: { label: "CAMPAIGN", color: "#8a8f98" },
};
const SIG_LIGHT: Record<string, string> = { supports: "var(--green)", refutes: "var(--red)", neutral: "var(--muted)" };
const SIG_DARK: Record<string, string> = { supports: "#3ec27a", refutes: "#ff6f63", neutral: "#8a8f98" };

function ago(iso: string | null): string {
  if (!iso) return "";
  const s = Math.max(0, Math.floor((Date.now() - new Date(iso).getTime()) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}

function useFeed() {
  const [feed, setFeed] = useState<ActivityFeed | null>(null);
  const [err, setErr] = useState(false);
  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const f = await api.engine.activity();
        if (alive) { setFeed(f); setErr(false); }
      } catch {
        if (alive) setErr(true);
      }
    };
    load();
    const id = setInterval(load, 8000);
    return () => { alive = false; clearInterval(id); };
  }, []);
  return { feed, err };
}

export function LiveActivity({ variant = "light" }: { variant?: "light" | "dark" }) {
  const { feed, err } = useFeed();
  const idle = feed?.idle ?? true;
  const running = feed?.running_jobs ?? 0;
  const events = feed?.events ?? [];

  if (variant === "dark") {
    return (
      <div className="mono" style={{ background: "var(--ink)", color: "#e9eaec", borderRadius: 16,
                                     border: "1px solid #1c1c1c", overflow: "hidden",
                                     boxShadow: "0 30px 80px -40px rgba(0,0,0,.55)" }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between",
                      padding: "13px 18px", borderBottom: "1px solid #1c1c1c", background: "#070707" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 12, letterSpacing: "0.12em", color: "#8a8f98" }}>
            <span style={{ display: "inline-flex", gap: 6 }}>
              <i style={{ width: 9, height: 9, borderRadius: "50%", background: "#2a2a2a", display: "inline-block" }} />
              <i style={{ width: 9, height: 9, borderRadius: "50%", background: "#2a2a2a", display: "inline-block" }} />
              <i style={{ width: 9, height: 9, borderRadius: "50%", background: "#2a2a2a", display: "inline-block" }} />
            </span>
            <span>engine · activity ledger</span>
          </div>
          <span style={{ display: "inline-flex", alignItems: "center", gap: 7, fontSize: 12, fontWeight: 600,
                         color: idle ? "#8a8f98" : "#3ec27a" }}>
            <i style={{ width: 8, height: 8, borderRadius: "50%", display: "inline-block",
                        background: idle ? "#4a4a4a" : "#3ec27a" }} />
            {idle ? "idle" : `running · ${running}`}
          </span>
        </div>
        <div style={{ padding: "16px 18px 20px", height: 340, overflowY: "auto", display: "flex", flexDirection: "column",
                      gap: 11, fontSize: 13.5, lineHeight: 1.5 }}>
          {err && !feed ? <div style={{ color: "#8a8f98" }}>activity feed unavailable</div> : null}
          {events.map((e, i) => {
            const tag = DARK_TAG[e.type] ?? DARK_TAG.agent;
            return (
              <div key={i} style={{ display: "flex", gap: 12, alignItems: "baseline" }}>
                <span style={{ color: "#4a4a4a", fontSize: 11.5, flexShrink: 0, width: 56 }}>{ago(e.occurred_at)}</span>
                <span style={{ color: tag.color, fontSize: 11, letterSpacing: "0.08em", flexShrink: 0, width: 70, fontWeight: 600 }}>{tag.label}</span>
                <span style={{ color: e.signal ? SIG_DARK[e.signal] : "#c4c6ca" }}>{e.title}</span>
              </div>
            );
          })}
          {feed && events.length === 0 && !err ? (
            <div style={{ color: "#8a8f98" }}>
              idle — no runnable work. The loop waits until there&rsquo;s a falsifiable test to run.
            </div>
          ) : null}
        </div>
      </div>
    );
  }

  // light in-page variant
  if (err && !feed) return <div className="muted mono" style={{ fontSize: 12 }}>activity feed unavailable</div>;
  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12 }}>
        <span style={{ width: 8, height: 8, borderRadius: "50%", display: "inline-block",
                       background: idle ? "var(--muted)" : "var(--green)" }} />
        <span className="mono" style={{ fontSize: 12, letterSpacing: "0.12em", color: idle ? "var(--muted)" : "var(--green)" }}>
          {idle ? (feed?.idle_reason ?? "IDLE — NO RUNNABLE WORK") : `RUNNING · ${running} job${running === 1 ? "" : "s"}`}
        </span>
      </div>
      <div style={{ maxHeight: 420, overflowY: "auto" }}>
        {events.map((e, i) => {
          const badge = LIGHT_BADGE[e.type] ?? LIGHT_BADGE.agent;
          return (
            <div key={i} style={{ display: "flex", gap: 10, alignItems: "baseline", padding: "8px 0",
                                  borderTop: i ? "1px solid var(--line)" : "none" }}>
              <span className={`badge ${badge.cls}`} style={{ fontSize: 10.5, minWidth: 82, justifyContent: "center" }}>{badge.label}</span>
              <span style={{ fontSize: 13.5, flex: 1, lineHeight: 1.4, color: e.signal ? SIG_LIGHT[e.signal] : "var(--ink)" }}>{e.title}</span>
              <span className="mono muted" style={{ fontSize: 11 }}>{e.status}</span>
              <span className="mono muted" style={{ fontSize: 11, minWidth: 56, textAlign: "right" }}>{ago(e.occurred_at)}</span>
            </div>
          );
        })}
        {feed && events.length === 0 ? (
          <div className="muted" style={{ fontSize: 13 }}>No recent activity — the engine is idle until there&rsquo;s runnable work.</div>
        ) : null}
      </div>
    </div>
  );
}
