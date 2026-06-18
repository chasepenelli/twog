"use client";

import { useEffect, useRef, useState } from "react";

/**
 * LiveThink — the "watch it think" stream. A dark, inked terminal-style panel that narrates the engine
 * working in real time: propose → resolve inputs → refuse-on-unverified → dock → verdict → seal+sign.
 * This is the centerpiece of STATE: tech-bio credibility (a real machine, working at scale) with a
 * splash of Linear's live agent feed. The script is curated to surface the IP — it refuses a bad input
 * with $0 spent, and it keeps a refutation. New lines append + the newest types in; oldest scroll off.
 */

type Kind =
  | "candidate" | "crux" | "resolve" | "gate" | "refuse" | "compute"
  | "kill" | "reframe" | "stands" | "capsule" | "sign";

const TAG: Record<Kind, { label: string; color: string }> = {
  candidate: { label: "CANDIDATE", color: "#9db4ff" },
  crux: { label: "CRUX", color: "#8a8f98" },
  resolve: { label: "RESOLVE", color: "#8a8f98" },
  gate: { label: "GATE", color: "#e0a44a" },
  refuse: { label: "REFUSE", color: "#ff6f63" },
  compute: { label: "COMPUTE", color: "#6f9bff" },
  kill: { label: "FALSIFY", color: "#ff6f63" },
  reframe: { label: "REFRAME", color: "#6f9bff" },
  stands: { label: "VERDICT", color: "#3ec27a" },
  capsule: { label: "CAPSULE", color: "#3ec27a" },
  sign: { label: "SIGN", color: "#8a8f98" },
};

// One "chapter" = the loop attacking a single moonshot candidate through to a sealed, comprehensive
// capsule. A capsule is NOT a per-dock receipt — it bundles the whole validation packet (claim, runs,
// gate verdicts, confidence, limits) under the candidate, then is hash-linked + signed.
const SCRIPT: Array<{ k: Kind; t: string }> = [
  // chapter 1 — a moonshot that survives, but only after its first framing is killed
  { k: "candidate", t: "Moonshot — PIK3CA activation drives immunosuppression in the tumor microenvironment" },
  { k: "crux", t: "Pre-registering the cheapest experiment that could kill it" },
  { k: "resolve", t: "Real Megquier cohort · expression + strata pulled" },
  { k: "gate", t: "Tumor-purity confound detected · deconvolution adjusted before any claim" },
  { k: "kill", t: "Immunosuppression signal reverses after adjustment — first framing KILLED" },
  { k: "reframe", t: "Reframed — PIK3CA→endothelial axis · re-tested across dog and human" },
  { k: "stands", t: "Cross-species association holds · p = 0.001 — claim STANDS" },
  { k: "capsule", t: "Sealing capsule — claim, runs, confound + provenance verdicts, confidence, limits, all bundled" },
  { k: "sign", t: "content sha256 9f3a… · Ed25519 signed · versioned lineage · re-checkable by anyone" },
  // chapter 2 — a moonshot refused at the input gate, $0 spent
  { k: "candidate", t: "Moonshot — mTOR co-inhibition is required for alpelisib durability" },
  { k: "resolve", t: "Target mTOR → proposed co-crystal structure · chain A" },
  { k: "refuse", t: "Native-ligand redock RMSD 5.91 Å > 2.0 — input refused before a cent of compute" },
  { k: "capsule", t: "Capsule sealed REFUTED — the refusal is the evidence, and it is kept" },
];

const TICK_MS = 2400;
const TYPE_MS = 16;
const MAX_VISIBLE = 7;

type Line = { id: number; k: Kind; t: string; clock: string };

function clockAt(base: number): string {
  // cosmetic monotonic session clock — purely visual, not real wall-time
  const total = 40_000 + base * 7;
  const m = Math.floor(total / 60_000);
  const s = Math.floor((total % 60_000) / 1000);
  return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

export function LiveThink() {
  // seed with the first 3 lines so server + first client render match (no hydration flash)
  const seed: Line[] = SCRIPT.slice(0, 3).map((e, i) => ({ id: i, k: e.k, t: e.t, clock: clockAt(i) }));
  const [lines, setLines] = useState<Line[]>(seed);
  const [typed, setTyped] = useState<string>(seed[seed.length - 1].t);
  const cursor = useRef(3);

  useEffect(() => {
    // respect reduced-motion + don't animate a backgrounded tab — keep the editorial calm
    if (typeof window !== "undefined" && window.matchMedia?.("(prefers-reduced-motion: reduce)").matches) return;

    let typer: ReturnType<typeof setInterval> | null = null;

    const advance = () => {
      const e = SCRIPT[cursor.current % SCRIPT.length];
      const id = cursor.current;
      cursor.current += 1;
      setLines((prev) => [...prev.slice(-(MAX_VISIBLE - 1)), { id, k: e.k, t: e.t, clock: clockAt(id) }]);
      // type the freshly-appended line
      setTyped("");
      let n = 0;
      if (typer) clearInterval(typer);
      typer = setInterval(() => {
        n += 2;
        if (n >= e.t.length) {
          setTyped(e.t);
          if (typer) clearInterval(typer);
          typer = null;
        } else {
          setTyped(e.t.slice(0, n));
        }
      }, TYPE_MS);
    };

    const tick = setInterval(advance, TICK_MS);
    return () => {
      clearInterval(tick);
      if (typer) clearInterval(typer);
    };
  }, []);

  return (
    <div
      className="mono"
      style={{
        background: "var(--ink)",
        color: "#e9eaec",
        borderRadius: 16,
        border: "1px solid #1c1c1c",
        overflow: "hidden",
        position: "relative",
        boxShadow: "0 30px 80px -40px rgba(0,0,0,.55)",
      }}
    >
      {/* header bar */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "13px 18px",
          borderBottom: "1px solid #1c1c1c",
          background: "#070707",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 12, letterSpacing: "0.12em", color: "#8a8f98" }}>
          <span style={{ display: "inline-flex", gap: 6 }}>
            <i style={{ width: 9, height: 9, borderRadius: "50%", background: "#2a2a2a", display: "inline-block" }} />
            <i style={{ width: 9, height: 9, borderRadius: "50%", background: "#2a2a2a", display: "inline-block" }} />
            <i style={{ width: 9, height: 9, borderRadius: "50%", background: "#2a2a2a", display: "inline-block" }} />
          </span>
          <span>engine · falsification loop</span>
        </div>
        <span style={{ display: "inline-flex", alignItems: "center", gap: 7, fontSize: 12, color: "#3ec27a", fontWeight: 600 }}>
          <i className="ldot" style={{ background: "#3ec27a" }} />
          thinking
        </span>
      </div>

      {/* stream */}
      <div style={{ padding: "16px 18px 20px", minHeight: 286, display: "flex", flexDirection: "column", gap: 11, fontSize: 13.5, lineHeight: 1.5 }}>
        {lines.map((l, i) => {
          const isLast = i === lines.length - 1;
          const tag = TAG[l.k];
          return (
            <div key={l.id} style={{ display: "flex", gap: 12, alignItems: "baseline", opacity: isLast ? 1 : 0.5 + i * 0.07, transition: "opacity .4s" }}>
              <span style={{ color: "#4a4a4a", fontSize: 11.5, flexShrink: 0, width: 40 }}>{l.clock}</span>
              <span style={{ color: tag.color, fontSize: 11, letterSpacing: "0.08em", flexShrink: 0, width: 64, fontWeight: 600 }}>{tag.label}</span>
              <span style={{ color: isLast ? "#f2f3f4" : "#c4c6ca" }}>
                {isLast ? typed : l.t}
                {isLast ? <span style={{ animation: "blink 1.05s step-end infinite", color: tag.color }}>▋</span> : null}
              </span>
            </div>
          );
        })}
      </div>

      {/* bottom fade — the clip reads as a continuous stream, not an overflow bug */}
      <div
        aria-hidden
        style={{
          position: "absolute",
          left: 0,
          right: 0,
          bottom: 0,
          height: 56,
          background: "linear-gradient(to bottom, rgba(10,10,10,0), var(--ink))",
          pointerEvents: "none",
        }}
      />
    </div>
  );
}
