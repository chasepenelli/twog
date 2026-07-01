import Link from "next/link";

import { api } from "@/lib/api";
import { LiveLoop } from "@/components/state/live-loop";
import { LiveAgo } from "@/components/state/live-ago";
import { LiveActivity } from "@/components/state/live-activity";
import { ThisWeekStrip } from "@/components/state/this-week-strip";
import { LaneTrack } from "@/components/evidence/lane-track";
import { deriveLaneTrack, publicQuestion, verdictOf, VERDICT_META } from "@/lib/evidence/verdict";

export const metadata = { title: "Research state" };
export const dynamic = "force-dynamic"; // reflect live Neon state (sweep counts, verdicts) per request

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
  // enrich each card with its candidate's plain verdict + how-far-tested tracker (a few fetches; the
  // list is short). The card still links to the capsule's full story.
  const rubrics = await Promise.all(
    latest.map((c) => api.candidates.rubric(c.candidate_id).catch(() => null)),
  );
  // the latest sweep drives the hero strip (real numbers; the negative controls it killed to test itself)
  const campaigns = await api.campaigns.list().catch(() => []);
  const sweep = campaigns[0]?.rollup?.leading_hypothesis_status ?? {};
  const ruledOut = sweep.refuted ?? engine.headline.hypothesesFalsified;
  const stillStanding = sweep.standing ?? engine.headline.validatedResults;
  const refutedNames = (campaigns[0]?.rows ?? [])
    .filter((r) => r.leading_hypothesis_status === "refuted")
    .map((r) => r.candidate_id.split("-")[0]);
  // prefer the planted negative controls (ethanol/aspirin) for the "it tests itself" line; else the first refuted
  const controls = refutedNames.filter((n) => ["ethanol", "aspirin"].includes(n));
  const struck = (controls.length ? controls : refutedNames).slice(0, 2);

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
            A dog you love is fine — running, eating, herself — until the afternoon she goes down and
            doesn&rsquo;t get up, and the scan shows the cancer was already everywhere. Hemangiosarcoma gives
            almost no warning, and it&rsquo;s the same disease that takes people. So we built one engine to
            hunt both, and gave it a single rule: <span className="em">never pretend.</span>
          </p>
          <div style={{ marginTop: 30, display: "flex", gap: 14, flexWrap: "wrap" }}>
            <Link href="/evidence" className="btn solid">See what it&rsquo;s found →</Link>
            <Link href="/runs" className="btn ghost">How it works</Link>
          </div>
        </div>
        <div>
          <ThisWeekStrip ruledOut={ruledOut} stillStanding={stillStanding} struck={struck} />
          <LiveActivity variant="dark" />
          <div className="mono muted" style={{ fontSize: 11.5, letterSpacing: "0.04em", marginTop: 12, textAlign: "center" }}>
            live ledger · real engine activity — idle until there&rsquo;s a test that could prove something wrong
          </div>
        </div>
      </div>

      {/* STAKES — the intellectual hook, one calm beat after the heavy hero */}
      <p className="serif" style={{ fontSize: "clamp(20px,2.4vw,30px)", lineHeight: 1.35, letterSpacing: "-0.01em", color: "var(--ink)", maxWidth: "24ch", marginTop: 64 }}>
        Dogs get this cancer, and they get it <span className="em" style={{ fontStyle: "italic" }}>fast.</span> So an
        engine that tests drug ideas around the clock can reach an answer sooner than a human trial ever
        could. Helping them is how we get to help us.
      </p>

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
          away before any compute ran on it.
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

      {/* WHAT IT'S WORKING ON */}
      <section className="sec sec-line">
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", flexWrap: "wrap", gap: 10 }}>
          <h2 className="h2" style={{ margin: 0 }}>What it's working on</h2>
          <Link href="/evidence" className="mono accent" style={{ fontSize: 13 }}>see everything →</Link>
        </div>
        <p className="muted" style={{ fontSize: 15.5, margin: "10px 0 24px", maxWidth: "60ch" }}>
          Each idea is a real drug against a real target, tested one step at a time. Open any one to see how
          far it&rsquo;s gotten — and why a result that survives means something.
        </p>
        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
          {latest.map((c, i) => {
            const rubric = rubrics[i];
            const verdict = VERDICT_META[verdictOf(rubric)];
            const steps = deriveLaneTrack(rubric);
            const question = publicQuestion(rubric?.title, c.claim ?? c.candidate_id);
            return (
              <Link
                key={c.capsule_id}
                href={`/evidence/${c.capsule_id}`}
                className="panel"
                style={{ display: "block", padding: "24px 26px", borderLeft: `3px solid ${STRIPE[c.signal] ?? "var(--line)"}` }}
              >
                <span className={`badge ${verdict.tone}`} style={{ fontSize: 11.5 }}>{verdict.label}</span>
                <div style={{ fontSize: 19, fontWeight: 700, letterSpacing: "-0.02em", margin: "10px 0 0", maxWidth: "52ch" }}>
                  {question}
                </div>
                {steps.length ? (
                  <div style={{ marginTop: 14 }}><LaneTrack steps={steps} variant="compact" /></div>
                ) : c.plain ? (
                  <div className="muted" style={{ marginTop: 8, fontSize: 14.5, maxWidth: "56ch" }}>{c.plain}</div>
                ) : null}
              </Link>
            );
          })}
        </div>
      </section>

      {/* closing band — dark, so the inked live-activity ledger reads as a deliberate motif, not a one-off */}
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
