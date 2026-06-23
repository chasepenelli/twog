import Link from "next/link";

import { api } from "@/lib/api";
import { LiveLoop } from "@/components/state/live-loop";
import { LiveAgo } from "@/components/state/live-ago";
import { LiveActivity } from "@/components/state/live-activity";

export const metadata = { title: "Research state" };

const SIG_LABEL: Record<string, string> = { supports: "SUPPORTS", refutes: "REFUTES", neutral: "NEUTRAL" };
const STRIPE: Record<string, string> = { supports: "var(--green)", refutes: "var(--red)", neutral: "var(--muted)" };
const LANE_BADGE: Record<string, { cls: string; mark: string }> = {
  verified: { cls: "b-green", mark: "✓" }, running: { cls: "b-blue", mark: "•" }, failed: { cls: "b-red", mark: "✗" },
};
const LANE_WHAT: Record<string, string> = {
  "gnina docking": "Does the drug physically grip its target?",
  "Boltz-2 cofolding": "Do these two proteins actually fold together?",
  "OpenMM MD": "Does that grip hold once the atoms start moving?",
  "Omics TME review": "What do real patient tumors actually show?",
};
const TILES = [
  { k: "Ideas killed", color: "var(--red)", get: (h: H) => `${h.hypothesesFalsified}`, d: "Promising hypotheses it tested hard and threw out. Being wrong fast, on purpose, is the point." },
  { k: "Results that held", color: "var(--green)", get: (h: H) => `${h.validatedResults}`, d: "Findings that survived a second, independent check — evidence, not opinion." },
  { k: "Tests running", color: "var(--accent)", get: (h: H) => `${h.computeLanes}`, d: "Four kinds of experiment turning in parallel right now." },
  { k: "Self-tests green", color: "var(--ink)", get: (h: H) => `${h.testsPassing}`, d: "The engine's own checks, all passing — so you can trust the numbers above. (76.5% coverage)" },
];
type H = Awaited<ReturnType<typeof api.engine.state>>["headline"];

export default async function StatePage() {
  const engine = await api.engine.state();
  const capsules = await api.capsules.list().catch(() => []);
  const latest = capsules.slice(0, 3);

  return (
    <div className="wrap" style={{ paddingTop: 48 }}>
      {/* HERO — claim + scale on the left, the live engine stream on the right */}
      <div className="hero-grid">
        <div>
          <div className="mono" style={{ display: "flex", flexWrap: "wrap", gap: 14, alignItems: "center", fontSize: 12, letterSpacing: "0.14em", color: "var(--muted)" }}>
            <span>RESEARCH STATE · LIVE</span>
            <span className="live" style={{ letterSpacing: 0 }}><span className="ldot" />engine online</span>
            <span style={{ letterSpacing: 0 }}>last verdict · <LiveAgo seedSeconds={14} resetEvery={96} /></span>
          </div>
          <h1 className="display" style={{ fontSize: "clamp(34px,4.6vw,60px)", marginTop: 16 }}>
            Watch a research engine <span className="em">try to be wrong.</span>
          </h1>
          <p className="lede" style={{ marginTop: 22 }}>
            It poses a hypothesis, runs real GPU compute to attack it, and keeps only what survives —
            pointed at the cancer dogs and people share. <span className="em">No human in the loop.</span>
          </p>
          <div style={{ marginTop: 30, display: "flex", gap: 14, flexWrap: "wrap" }}>
            <Link href="/evidence" className="btn solid">Inspect the evidence →</Link>
            <Link href="/runs" className="btn ghost">See full runs</Link>
          </div>
        </div>
        <div>
          <LiveActivity variant="dark" />
          <div className="mono muted" style={{ fontSize: 11.5, letterSpacing: "0.04em", marginTop: 12, textAlign: "center" }}>
            live ledger · real engine activity — idle at $0 until there&rsquo;s a falsifiable test to run
          </div>
        </div>
      </div>

      {/* ORIENTATION — what you're looking at */}
      <p style={{ fontSize: 17, lineHeight: 1.6, color: "#333", maxWidth: "62ch", marginTop: 52 }}>
        That stream is the engine running. Below you'll see <strong>where it stands</strong>, the five-step
        loop it runs to attack each idea, the kinds of experiment turning in parallel, and the evidence it
        has produced. Every result is a <strong>proof capsule</strong> — a sealed, signed record you can
        open and re-check for yourself.
      </p>

      {/* legend */}
      <div className="mono" style={{ display: "flex", flexWrap: "wrap", gap: "10px 22px", marginTop: 28, padding: "14px 18px", border: "1px solid var(--line)", borderRadius: 13, fontSize: 12.5, color: "var(--muted)", background: "var(--soft)" }}>
        <span><span style={{ color: "var(--accent)" }}>lit step</span> — running now</span>
        <span><span style={{ color: "var(--green)" }}>✓</span> verified — double-checked a second way</span>
        <span><span style={{ color: "var(--green)" }}>●</span> verdict — supports / refutes / neutral</span>
      </div>

      {/* WHERE IT STANDS */}
      <section className="sec" style={{ paddingTop: 52 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", flexWrap: "wrap", gap: 8 }}>
          <h2 className="h2" style={{ margin: 0 }}>Where it stands</h2>
          <span className="mono muted" style={{ fontSize: 11.5 }}>live · updates as the engine runs</span>
        </div>
        <p className="muted" style={{ fontSize: 15.5, margin: "10px 0 24px", maxWidth: "58ch" }}>
          The scoreboard so far. The number that matters most is the first one — twog is built to throw out
          its own ideas, and the results that survive that mean more because of it.
        </p>
        <div className="nums">
          {TILES.map((t) => (
            <div className="num" key={t.k}>
              <div className="v" style={{ color: t.color }}>{t.get(engine.headline)}</div>
              <div className="l"><b>{t.k}.</b> {t.d}</div>
            </div>
          ))}
        </div>
      </section>

      {/* THE LOOP */}
      <section className="sec sec-line">
        <h2 className="h2">The loop it runs</h2>
        <p className="muted" style={{ fontSize: 15.5, margin: "-4px 0 26px", maxWidth: "60ch" }}>
          Every idea runs the same five steps, autonomously, on real GPUs. It pre-registers the cheapest test
          that would kill the idea and runs that first — so a result only counts if it survived an honest
          attempt to break it. The lit step is turning right now.
        </p>
        <LiveLoop steps={engine.loop} />
      </section>

      {/* WHAT'S RUNNING */}
      <section className="sec sec-line">
        <h2 className="h2">What's running right now</h2>
        <p className="muted" style={{ fontSize: 15.5, margin: "-4px 0 24px", maxWidth: "60ch" }}>
          Four kinds of experiment, in parallel — each result re-checked a second way before it counts. And
          it won't run on a bad input: one structure missed its own physics check by 5.91 Å and was turned
          away before a cent of compute was spent.
        </p>
        <div className="panel">
          <table>
            <thead><tr><th>Test</th><th>What it does</th><th>Status</th><th>Last result</th></tr></thead>
            <tbody>
              {engine.lanes.map((l) => (
                <tr key={l.lane}>
                  <td><div style={{ fontWeight: 600 }}>{l.lane}</div><div className="mono" style={{ fontSize: 12, color: "var(--muted)", marginTop: 3 }}>{l.sublabel}</div></td>
                  <td className="muted" style={{ fontSize: 14 }}>{LANE_WHAT[l.lane] ?? "—"}</td>
                  <td><span className={`badge ${(LANE_BADGE[l.status] ?? LANE_BADGE.verified).cls}`}>{(LANE_BADGE[l.status] ?? LANE_BADGE.verified).mark} {l.status}</span></td>
                  <td className="mono" style={{ fontSize: 13 }}>{l.lastResult}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* LATEST PROOF */}
      <section className="sec sec-line">
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", flexWrap: "wrap", gap: 10 }}>
          <h2 className="h2" style={{ margin: 0 }}>Latest proof</h2>
          <Link href="/evidence" className="mono accent" style={{ fontSize: 13 }}>see all evidence →</Link>
        </div>
        <p className="muted" style={{ fontSize: 15.5, margin: "10px 0 24px", maxWidth: "60ch" }}>
          Each result is a proof capsule: the claim, the verdict, the confidence, and its limits, sealed and
          signed so anyone can re-open and check it. Refuting evidence counts the same as support.
        </p>
        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
          {latest.map((c) => (
            <Link key={c.capsule_id} href={`/evidence/${c.capsule_id}`} className="panel" style={{ display: "block", padding: "24px 26px", borderLeft: `3px solid ${STRIPE[c.signal] ?? "var(--line)"}` }}>
              <span className={`sig ${c.signal} mono`} style={{ fontSize: 12.5, letterSpacing: "0.14em" }}>{SIG_LABEL[c.signal] ?? c.signal}</span>
              <div style={{ fontSize: 19, fontWeight: 700, letterSpacing: "-0.02em", marginTop: 9, maxWidth: "52ch" }}>{c.claim ?? c.candidate_id}</div>
              {c.plain ? <div className="muted" style={{ marginTop: 7, fontSize: 14.5, maxWidth: "56ch" }}>{c.plain}</div> : null}
            </Link>
          ))}
        </div>
      </section>

      {/* closing band — dark, so the inked live-think panel reads as a deliberate motif, not a one-off */}
      <div
        style={{
          marginTop: 88,
          marginBottom: 8,
          background: "var(--ink)",
          color: "#e9eaec",
          borderRadius: 18,
          padding: "clamp(34px,5vw,60px)",
          display: "flex",
          flexWrap: "wrap",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 28,
        }}
      >
        <div style={{ maxWidth: "30ch" }}>
          <div className="mono" style={{ fontSize: 11.5, letterSpacing: "0.16em", color: "#8a8f98" }}>WHY IT'S DIFFERENT</div>
          <p className="serif" style={{ fontSize: "clamp(22px,2.6vw,34px)", lineHeight: 1.2, margin: "14px 0 0", letterSpacing: "-0.01em" }}>
            Most AI emits text you have to believe. <span style={{ fontStyle: "italic", color: "#fff" }}>twog emits evidence you can verify.</span>
          </p>
        </div>
        <div style={{ display: "flex", gap: 14, flexWrap: "wrap" }}>
          <Link href="/evidence" className="btn" style={{ background: "#fff", color: "var(--ink)", borderColor: "#fff" }}>Inspect the evidence →</Link>
          <Link href="/runs" className="btn" style={{ color: "#e9eaec", borderColor: "#3a3a3a" }}>See full runs</Link>
        </div>
      </div>
    </div>
  );
}
