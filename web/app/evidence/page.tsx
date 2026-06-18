import Link from "next/link";

import { api } from "@/lib/api";
import type { GateVerdict, ProofCapsule } from "@/lib/types/domain";

export const metadata = { title: "Evidence" };

const SIG_LABEL: Record<string, string> = { supports: "SUPPORTS", refutes: "REFUTES", neutral: "NEUTRAL" };

function GateBadge({ label, v }: { label: string; v?: GateVerdict }) {
  const map: Record<string, [string, string]> = {
    pass: ["b-green", "✓"],
    fail: ["b-red", "✗"],
    pending: ["b-amber", "·"],
    unknown: ["b-muted", "?"],
  };
  const [cls, mark] = map[v ?? "unknown"];
  return <span className={`badge ${cls}`}>{mark} {label} {v ?? "—"}</span>;
}

const STRIPE: Record<string, string> = { supports: "var(--green)", refutes: "var(--red)", neutral: "var(--muted)" };

function CapsuleCard({ c }: { c: ProofCapsule }) {
  return (
    <Link
      href={`/evidence/${c.capsule_id}`}
      className="panel"
      style={{ display: "block", padding: "34px 34px", borderLeft: `3px solid ${STRIPE[c.signal] ?? "var(--line)"}`, transition: "border-color .15s" }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 16, flexWrap: "wrap" }}>
        <span className={`sig ${c.signal} mono`} style={{ fontSize: 13, letterSpacing: "0.14em" }}>
          {SIG_LABEL[c.signal] ?? c.signal}
        </span>
        <span className="mono accent" style={{ fontSize: 12 }}>view receipt →</span>
      </div>

      <div style={{ fontSize: "clamp(20px,2vw,26px)", fontWeight: 700, letterSpacing: "-0.03em", marginTop: 12, maxWidth: "46ch", lineHeight: 1.22 }}>
        {c.claim ?? c.candidate_id}
      </div>
      {c.method ? <div className="muted" style={{ marginTop: 9, fontSize: 15.5, maxWidth: "52ch" }}>{c.method}</div> : null}
      {c.plain ? <div style={{ marginTop: 12, fontSize: 15.5, lineHeight: 1.55, maxWidth: "54ch" }}>{c.plain}</div> : null}
      {c.readout ? <div className="mono muted" style={{ marginTop: 12, fontSize: 13 }}>{c.readout}</div> : null}

      {typeof c.confidence === "number" ? (
        <div style={{ marginTop: 16, maxWidth: 300 }}>
          <div className="mono muted" style={{ fontSize: 11, letterSpacing: "0.04em" }}>confidence {Math.round(c.confidence * 100)}%</div>
          <div className="bar"><i style={{ width: `${Math.round(c.confidence * 100)}%` }} /></div>
        </div>
      ) : null}

      <div style={{ display: "flex", gap: 9, flexWrap: "wrap", alignItems: "center", marginTop: 20 }}>
        <GateBadge label="confound" v={c.confound_verdict} />
        <GateBadge label="provenance" v={c.provenance_verdict} />
        {c.signature ? <span className="badge b-blue">✓ signed</span> : <span className="badge b-muted">custodial</span>}
        {c.produced_by ? <span className="mono muted" style={{ fontSize: 12, marginLeft: 4 }}>{c.produced_by}</span> : null}
      </div>
    </Link>
  );
}

export default async function EvidencePage() {
  const capsules = await api.capsules.list();

  return (
    <div className="wrap" style={{ paddingTop: 52 }}>
      <div className="kick">Proof</div>
      <h1 style={{ fontSize: "clamp(34px,5vw,56px)", fontWeight: 700, letterSpacing: "-0.04em", margin: "12px 0 0" }}>
        Evidence, <span className="em">audited.</span>
      </h1>
      <p className="lede">
        Evidence is an object. Every result is a portable, verifiable <span className="em">receipt</span> —
        what was run, against what, the directional signal, the confidence, and how far it goes. Audit any of them.
      </p>
      <p className="mono muted" style={{ fontSize: 13, marginTop: 14 }}>
        Refuting evidence is a result, not a failure — it counts the same as support.
      </p>

      {/* legend — so a first-timer can read the cards */}
      <div
        className="mono"
        style={{ display: "flex", flexWrap: "wrap", gap: "10px 22px", marginTop: 30, padding: "16px 18px", border: "1px solid var(--line)", borderRadius: 13, fontSize: 12.5, color: "var(--muted)", background: "var(--soft)" }}
      >
        <span>Each card is one test.</span>
        <span><span className="sig supports">●</span> verdict — supports / refutes / neutral</span>
        <span>▮ bar — how sure</span>
        <span>● pills — the safety gates it had to clear</span>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 28, marginTop: 32 }}>
        {capsules.map((c) => <CapsuleCard key={c.capsule_id} c={c} />)}
      </div>
    </div>
  );
}
