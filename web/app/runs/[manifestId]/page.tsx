import Link from "next/link";

import { api } from "@/lib/api";
import type { CampaignRow, RunManifest } from "@/lib/types/domain";

const OUTCOME: Record<string, { cls: string; label: string; gloss: string; stripe: string }> = {
  standing: { cls: "b-green", label: "STANDING", gloss: "Survived every falsification test the engine could run.", stripe: "var(--green)" },
  refuted: { cls: "b-red", label: "REFUTED", gloss: "A pre-registered kill-criterion was met — the idea was disproven.", stripe: "var(--red)" },
  underpowered: { cls: "b-amber", label: "UNDERPOWERED", gloss: "Ran out of runnable tests before a verdict — needs more.", stripe: "var(--amber)" },
};

function Section({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <section style={{ padding: "26px 0", borderTop: "1px solid var(--line)" }}>
      <div className="h2" style={{ marginBottom: 14 }}>{label}</div>
      {children}
    </section>
  );
}

export default async function RunReportPage({ params }: { params: { manifestId: string } }) {
  let run: RunManifest | null = null;
  try {
    run = await api.campaigns.get(params.manifestId);
  } catch {
    run = null;
  }
  if (!run) {
    return (
      <div className="wrap" style={{ paddingTop: 60 }}>
        <Link href="/runs" className="mono accent" style={{ fontSize: 13 }}>← Runs</Link>
        <h1 style={{ marginTop: 20, fontSize: 28, fontWeight: 700 }}>Run not found</h1>
      </div>
    );
  }

  const candidates = await api.candidates.list().catch(() => []);
  const titleOf = (id: string) => candidates.find((c) => c.candidate_id === id)?.title ?? id;
  const r = run;
  const { rollup } = r;

  return (
    <div className="wrap" style={{ paddingTop: 48, maxWidth: 900 }}>
      <Link href="/runs" className="mono accent" style={{ fontSize: 13 }}>← Runs</Link>
      <div className="mono muted" style={{ fontSize: 12.5, marginTop: 22 }}>{r.ran_at ?? ""} · {r.runner_kind}</div>
      <h1 style={{ fontSize: "clamp(30px,4vw,46px)", fontWeight: 700, letterSpacing: "-0.035em", margin: "10px 0 0" }}>
        {r.title ?? r.manifest_id}
      </h1>

      {/* the trust banner */}
      <div className="panel" style={{ marginTop: 24, padding: "20px 22px", borderLeft: "3px solid var(--accent)" }}>
        <div style={{ fontWeight: 600, fontSize: 16 }}>Nothing was promoted.</div>
        <p className="muted" style={{ fontSize: 14.5, marginTop: 6, maxWidth: "62ch" }}>
          A run only produces evidence and verdicts — it never advances a candidate on its own. A human
          operator still holds the terminal accept/promote decision.
        </p>
      </div>

      <Section label="Outcome">
        <div style={{ display: "flex", gap: 9, flexWrap: "wrap", alignItems: "center" }}>
          {Object.entries(rollup.leading_hypothesis_status).map(([k, n]) => {
            const o = OUTCOME[k] ?? { cls: "b-muted", label: k, gloss: "" };
            return <span key={k} className={`badge ${o.cls}`}>{n} {o.label.toLowerCase()}</span>;
          })}
          <span className="mono muted" style={{ fontSize: 12.5, marginLeft: 4 }}>
            {rollup.candidates_processed}/{rollup.candidates_selected} run
          </span>
        </div>
      </Section>

      <Section label="What we tried to kill">
        <div style={{ display: "flex", flexDirection: "column", gap: 0 }}>
          {r.rows.map((row: CampaignRow, i) => {
            const o = OUTCOME[row.leading_hypothesis_status] ?? { cls: "b-muted", label: row.leading_hypothesis_status, gloss: "", stripe: "var(--line)" };
            return (
              <div
                key={row.candidate_id}
                style={{ padding: "20px 0 20px 18px", borderLeft: `3px solid ${o.stripe}`, borderTop: i === 0 ? "none" : "1px solid var(--line)" }}
              >
                <div style={{ display: "flex", justifyContent: "space-between", gap: 16, flexWrap: "wrap", alignItems: "baseline" }}>
                  <div style={{ fontSize: 17, fontWeight: 600, letterSpacing: "-0.01em", maxWidth: "52ch" }}>{titleOf(row.candidate_id)}</div>
                  <span className={`badge ${o.cls}`}>{o.label}</span>
                </div>
                <p className="muted" style={{ fontSize: 14.5, marginTop: 7, maxWidth: "60ch" }}>{row.plain ?? o.gloss}</p>
                <div style={{ display: "flex", gap: 14, flexWrap: "wrap", alignItems: "center", marginTop: 10 }}>
                  {row.capsule_ids.length ? (
                    <span style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                      {row.capsule_ids.map((cid) => (
                        <Link key={cid} href={`/evidence/${cid}`} className="mono accent" style={{ fontSize: 12.5 }}>{cid} →</Link>
                      ))}
                    </span>
                  ) : (
                    <span className="mono muted" style={{ fontSize: 12.5 }}>Not run yet — no evidence to show.</span>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </Section>
    </div>
  );
}
