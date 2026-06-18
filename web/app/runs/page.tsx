import Link from "next/link";

import { api } from "@/lib/api";
import type { RunManifest } from "@/lib/types/domain";

export const metadata = { title: "Runs" };

const OUTCOME: Record<string, { cls: string; label: string }> = {
  standing: { cls: "b-green", label: "standing" },
  refuted: { cls: "b-red", label: "refuted" },
  underpowered: { cls: "b-amber", label: "underpowered" },
};

function outcomeChips(status: Record<string, number>) {
  return Object.entries(status).map(([k, n]) => {
    const o = OUTCOME[k] ?? { cls: "b-muted", label: k };
    return <span key={k} className={`badge ${o.cls}`}>{n} {o.label}</span>;
  });
}

function dominantStripe(status: Record<string, number>): string {
  if ((status.refuted ?? 0) > 0) return "var(--red)";
  if ((status.standing ?? 0) > 0) return "var(--green)";
  if ((status.underpowered ?? 0) > 0) return "var(--amber)";
  return "var(--line)";
}

function RunCard({ r }: { r: RunManifest }) {
  const { rollup } = r;
  return (
    <Link
      href={`/runs/${r.manifest_id}`}
      className="panel"
      style={{ display: "block", padding: "34px 34px", borderLeft: `3px solid ${dominantStripe(rollup.leading_hypothesis_status)}` }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 16, flexWrap: "wrap" }}>
        <span className="mono muted" style={{ fontSize: 12.5 }}>{r.ran_at ?? ""} · {r.runner_kind}</span>
        <span className="mono accent" style={{ fontSize: 12 }}>open report →</span>
      </div>
      <div style={{ fontSize: "clamp(20px,2vw,26px)", fontWeight: 700, letterSpacing: "-0.03em", marginTop: 10 }}>
        {r.title ?? r.manifest_id}
      </div>
      <div className="muted" style={{ marginTop: 9, fontSize: 15.5 }}>
        {rollup.candidates_processed} of {rollup.candidates_selected} hypotheses run through the gates.
      </div>
      <div style={{ display: "flex", gap: 9, flexWrap: "wrap", alignItems: "center", marginTop: 18 }}>
        {outcomeChips(rollup.leading_hypothesis_status)}
      </div>
      <div style={{ display: "flex", gap: 9, flexWrap: "wrap", alignItems: "center", marginTop: 14 }}>
        <span className="badge b-blue">✓ nothing auto-promoted</span>
        <span className="mono muted" style={{ fontSize: 12.5 }}>
          ${rollup.total_est_cost_usd.toFixed(2)} spent{rollup.budget_exhausted ? " · budget reached" : ""}
        </span>
      </div>
    </Link>
  );
}

export default async function RunsPage() {
  const runs = await api.campaigns.list();
  return (
    <div className="wrap" style={{ paddingTop: 52 }}>
      <div className="kick">Runs</div>
      <h1 style={{ fontSize: "clamp(34px,5vw,56px)", fontWeight: 700, letterSpacing: "-0.04em", margin: "12px 0 0" }}>
        What we tried to <span className="em">kill.</span>
      </h1>
      <p className="lede">
        A run pits a batch of hypotheses against their cheapest pre-registered kill-test. Some survive, some
        are refuted, some run out of road. None are promoted automatically — a human still holds the gate.
      </p>
      {/* legend — the decoder ring for outcomes */}
      <div
        className="mono"
        style={{ display: "flex", flexWrap: "wrap", gap: "10px 22px", marginTop: 30, padding: "16px 18px", border: "1px solid var(--line)", borderRadius: 13, fontSize: 12.5, color: "var(--muted)", background: "var(--soft)" }}
      >
        <span><span style={{ color: "var(--green)" }}>●</span> standing — survived every test</span>
        <span><span style={{ color: "var(--red)" }}>●</span> refuted — a kill-criterion was met</span>
        <span><span style={{ color: "var(--amber)" }}>●</span> underpowered — ran out of runnable tests</span>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 28, marginTop: 32 }}>
        {runs.map((r) => <RunCard key={r.manifest_id} r={r} />)}
      </div>
    </div>
  );
}
