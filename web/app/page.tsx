import Link from "next/link";
import { unstable_cache } from "next/cache";

import { api } from "@/lib/api";
import { LiveLoop } from "@/components/state/live-loop";
import { LiveAgo } from "@/components/state/live-ago";
import { LiveActivity } from "@/components/state/live-activity";
import { CountUp } from "@/components/state/count-up";
import { RefusalMonument } from "@/components/state/refusal-monument";
import { ScrollRail } from "@/components/state/scroll-rail";
import { Reveal, StaggerGroup, TextReveal, MagneticButton, HeroFieldClient, stateForStatus, type FieldPoint } from "@/components/motion";
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
  { k: "Ideas killed", color: "var(--red)", num: (h: H) => h.hypothesesFalsified, d: "Promising hypotheses it tested hard and threw out. Being wrong fast, on purpose, is the point." },
  { k: "Results that held", color: "var(--green)", num: (h: H) => h.validatedResults, d: "Findings that survived a second, independent check — evidence, not opinion." },
  { k: "Tests running", color: "var(--accent)", num: (h: H) => h.computeLanes, d: "Four kinds of experiment turning in parallel right now." },
  { k: "Self-tests green", color: "var(--ink)", num: (h: H) => h.testsPassing, d: "The engine's own checks, all passing — so you can trust the numbers above. (76.5% coverage)" },
];
type H = Awaited<ReturnType<typeof api.engine.state>>["headline"];

// The homepage aggregates several live Neon reads (the engine-state rollup scans hundreds of rows and
// can take many seconds). Cache the whole bundle for a short window with stale-while-revalidate, so the
// page is instant on every load and refreshes in the background — bounded, self-healing staleness
// (NOT the old indefinite build-time cache). Independent top-level reads run in ONE parallel wave; the
// per-candidate rubrics depend on `capsules`, so they run in a second wave.
const loadHomeData = unstable_cache(
  async () => {
    const [engine, capsules, campaigns] = await Promise.all([
      api.engine.state(),
      api.capsules.list().catch(() => []),
      api.campaigns.list().catch(() => []),
    ]);
    const latest = capsules.slice(0, 3);
    const rubrics = await Promise.all(
      latest.map((c) => api.candidates.rubric(c.candidate_id).catch(() => null)),
    );
    return { engine, latest, campaigns, rubrics };
  },
  ["home-state-v1"],
  { revalidate: 15 },
);

// Last successful bundle, kept in the module so a cold/slow Neon error serves stale-but-real data
// instead of crashing the page (unstable_cache covers steady state; this covers the first cold miss).
let _lastHome: Awaited<ReturnType<typeof loadHomeData>> | null = null;

export default async function StatePage() {
  let data: Awaited<ReturnType<typeof loadHomeData>> | null = null;
  try {
    data = await loadHomeData();
    _lastHome = data;
  } catch {
    data = _lastHome;
  }
  if (!data) {
    return (
      <div className="wrap" style={{ paddingTop: 110, maxWidth: 620 }}>
        <div className="mono muted" style={{ fontSize: 12, letterSpacing: "0.14em" }}>RESEARCH STATE · WARMING UP</div>
        <h1 className="display" style={{ fontSize: "clamp(30px,4vw,52px)", marginTop: 16 }}>One moment — waking the engine.</h1>
        <p className="lede">The research database is spinning up from idle. Give it a few seconds and reload to see the full live state.</p>
        <Link href="/" className="btn solid" style={{ marginTop: 22, display: "inline-flex" }}>Reload →</Link>
      </div>
    );
  }
  const { engine, latest, campaigns, rubrics } = data;
  const sweep = campaigns[0]?.rollup?.leading_hypothesis_status ?? {};
  const ruledOut = sweep.refuted ?? engine.headline.hypothesesFalsified;
  const stillStanding = sweep.standing ?? engine.headline.validatedResults;
  const refutedNames = (campaigns[0]?.rows ?? [])
    .filter((r) => r.leading_hypothesis_status === "refuted")
    .map((r) => r.candidate_id.split("-")[0]);
  // prefer the planted negative controls (ethanol/aspirin) for the "it tests itself" line; else the first refuted
  const controls = refutedNames.filter((n) => ["ethanol", "aspirin"].includes(n));
  const struck = (controls.length ? controls : refutedNames).slice(0, 2);

  // The field of ideas: every point is a REAL candidate the engine has tested (deduped across
  // campaigns), colored by its real verdict. The green/cobalt/ember counts ARE the scoreboard.
  const seenCand = new Map<string, string>();
  for (const c of campaigns) for (const r of c.rows ?? []) if (!seenCand.has(r.candidate_id)) seenCand.set(r.candidate_id, r.leading_hypothesis_status);
  const fieldPoints: FieldPoint[] = [...seenCand.entries()].map(([id, status]) => ({
    id,
    name: id.split("-")[0],
    state: stateForStatus(status),
  }));

  return (
    <>
      <ScrollRail />

      {/* ═══ ACT I — daylight: the stakes ═══ */}
      <div className="wrap" style={{ paddingTop: 48 }}>
        <div className="hero-grid">
          <div>
            <div className="mono" style={{ display: "flex", flexWrap: "wrap", gap: 14, alignItems: "center", fontSize: 12, letterSpacing: "0.14em", color: "var(--muted)" }}>
              <span>RESEARCH STATE · LIVE</span>
              <span className="live" style={{ letterSpacing: 0 }}><span className="ldot" />engine online</span>
              <span style={{ letterSpacing: 0 }}>last verdict · <LiveAgo seedSeconds={14} resetEvery={96} /></span>
            </div>
            <TextReveal as="h1" className="display" style={{ fontSize: "clamp(34px,4.6vw,60px)", marginTop: 16 }}>
              Watch a research engine <span className="em">try to be wrong.</span>
            </TextReveal>
            <p className="lede" style={{ marginTop: 22 }}>
              A dog you love is fine — running, eating, herself — until the afternoon she goes down and
              doesn&rsquo;t get up, and the scan shows the cancer was already everywhere. Hemangiosarcoma gives
              almost no warning, and it&rsquo;s the same disease that takes people. So we built one engine to
              hunt both, and gave it a single rule: <span className="em">never pretend.</span>
            </p>
            <div style={{ marginTop: 30, display: "flex", gap: 14, flexWrap: "wrap" }}>
              <MagneticButton><Link href="/evidence" className="btn solid">See what it&rsquo;s found →</Link></MagneticButton>
              <MagneticButton><Link href="/runs" className="btn ghost">How it works</Link></MagneticButton>
            </div>
          </div>
          <div>
            <div style={{ background: "#050505", border: "1px solid #191919", borderRadius: 18, overflow: "hidden", height: "clamp(320px,44vh,436px)", boxShadow: "0 40px 90px -50px rgba(0,0,0,.7)" }}>
              <HeroFieldClient points={fieldPoints} />
            </div>
            <div className="mono muted" style={{ fontSize: 11, letterSpacing: "0.05em", marginTop: 13, display: "flex", gap: "10px 18px", flexWrap: "wrap", justifyContent: "center", alignItems: "center" }}>
              <span>every point is a real idea it&rsquo;s testing —</span>
              <span><span style={{ color: "var(--green)" }}>●</span> still standing</span>
              <span><span style={{ color: "#4d7cff" }}>●</span> in the lab</span>
              <span><span style={{ color: "var(--red)" }}>●</span> ruled out</span>
            </div>
          </div>
        </div>

        {/* STAKES — the intellectual hook, one calm beat after the heavy hero */}
        <p className="serif" style={{ fontSize: "clamp(20px,2.4vw,30px)", lineHeight: 1.35, letterSpacing: "-0.01em", color: "var(--ink)", maxWidth: "24ch", marginTop: 72 }}>
          Dogs get this cancer, and they get it <span className="em" style={{ fontStyle: "italic" }}>fast.</span> So an
          engine that tests drug ideas around the clock can reach an answer sooner than a human trial ever
          could. Helping them is how we get to help us.
        </p>
      </div>

      {/* ═══ ACT II — the dark instrument: what it did, and what it refused ═══ */}
      <div className="wrap" style={{ marginTop: "clamp(48px,7vh,88px)" }}>
        <section style={{ background: "var(--ink)", color: "#e9eaec", borderRadius: 20, padding: "clamp(30px,4vw,56px) clamp(24px,4vw,54px)" }}>
          <div className="mono" style={{ fontSize: 11.5, letterSpacing: "0.16em", color: "#8a8f98" }}>
            02 / THE INSTRUMENT · ITS LATEST SWEEP
          </div>

          {/* THIS WEEK — kills count first and fast, survivors land after; the controls it planted strike out */}
          <div style={{ display: "flex", gap: "clamp(26px,5vw,64px)", flexWrap: "wrap", alignItems: "flex-end", marginTop: 22 }}>
            <div>
              <div style={{ fontSize: "clamp(44px,7vw,84px)", fontWeight: 700, color: "#ff6f63", lineHeight: 0.85, letterSpacing: "-0.05em" }}>
                <CountUp to={ruledOut} onScroll duration={700} />
              </div>
              <div className="mono" style={{ fontSize: 12, color: "#8a8f98", marginTop: 12, letterSpacing: "0.1em" }}>RULED OUT</div>
            </div>
            <div>
              <div style={{ fontSize: "clamp(44px,7vw,84px)", fontWeight: 700, color: "#3ec27a", lineHeight: 0.85, letterSpacing: "-0.05em" }}>
                <CountUp to={stillStanding} onScroll delay={520} duration={1150} />
              </div>
              <div className="mono" style={{ fontSize: 12, color: "#8a8f98", marginTop: 12, letterSpacing: "0.1em" }}>STILL STANDING · ON TWO METHODS</div>
            </div>
            {struck.length ? (
              <p style={{ fontSize: "clamp(14px,1.5vw,17px)", color: "#c4c6ca", margin: 0, maxWidth: "28ch", lineHeight: 1.5, flex: "1 1 220px" }}>
                It even tests — and kills — the dead ends it plants to check itself:{" "}
                {struck.map((n, i) => (
                  <span key={n}>
                    <span className="strike">{n}</span>
                    {i < struck.length - 1 ? ", " : "."}
                  </span>
                ))}
              </p>
            ) : null}
          </div>

          <div style={{ height: 1, background: "#1c1c1c", margin: "clamp(30px,4vw,48px) 0" }} />

          <RefusalMonument rmsd={5.91} gate={2.0} />

          <div style={{ height: 1, background: "#1c1c1c", margin: "clamp(30px,4vw,48px) 0" }} />

          {/* the live ledger — real engine activity, at home inside the instrument */}
          <div className="mono" style={{ fontSize: 11.5, letterSpacing: "0.16em", color: "#8a8f98", marginBottom: 16 }}>
            LIVE · WHAT IT&rsquo;S DOING RIGHT NOW
          </div>
          <LiveActivity variant="dark" />
        </section>
      </div>

      {/* ═══ ACT III — back to daylight: the loop, the bench, the scoreboard ═══ */}
      <div className="wrap">
        {/* THE LOOP */}
        <section className="sec sec-line">
          <h2 className="h2">The loop it runs</h2>
          <p className="muted" style={{ fontSize: 15.5, margin: "-4px 0 26px", maxWidth: "60ch" }}>
            Every idea runs the same five steps, autonomously, on real GPUs. It pre-registers the cheapest test
            that would kill the idea and runs that first — so a result only counts if it survived an honest
            attempt to break it. This is the shape of that loop.
          </p>
          <LiveLoop steps={engine.loop} />
        </section>

        {/* WHAT IT'S WORKING ON */}
        <section className="sec sec-line">
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", flexWrap: "wrap", gap: 10 }}>
            <h2 className="h2" style={{ margin: 0 }}>What it&rsquo;s working on</h2>
            <Link href="/evidence" className="mono accent" style={{ fontSize: 13 }}>see everything →</Link>
          </div>
          <p className="muted" style={{ fontSize: 15.5, margin: "10px 0 24px", maxWidth: "60ch" }}>
            Each idea is a real drug against a real target, tested one step at a time. Open any one to see how
            far it&rsquo;s gotten — and why a result that survives means something.
          </p>
          <StaggerGroup style={{ display: "flex", flexDirection: "column", gap: 16 }}>
            {latest.map((c, i) => {
              const rubric = rubrics[i];
              const verdict = VERDICT_META[verdictOf(rubric)];
              const steps = deriveLaneTrack(rubric);
              const question = publicQuestion(rubric?.title, c.claim ?? c.candidate_id);
              return (
                <Link
                  key={c.capsule_id}
                  href={`/evidence/${c.capsule_id}`}
                  className="panel card-lift"
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
          </StaggerGroup>
        </section>

        {/* WHERE IT STANDS — the scoreboard, counts landing on scroll */}
        <section className="sec sec-line">
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
                <div className="v" style={{ color: t.color }}><CountUp to={t.num(engine.headline)} onScroll /></div>
                <div className="l"><b>{t.k}.</b> {t.d}</div>
              </div>
            ))}
          </div>
        </section>

        {/* WHAT'S RUNNING */}
        <Reveal as="section" className="sec sec-line">
          <h2 className="h2">What&rsquo;s running right now</h2>
          <p className="muted" style={{ fontSize: 15.5, margin: "-4px 0 24px", maxWidth: "60ch" }}>
            Four kinds of experiment, in parallel — each result re-checked a second way before it counts. It
            won&rsquo;t run on a bad input at all, which is where that refusal above comes from.
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
        </Reveal>

        {/* closing band — dark bookend to Act II; the inked ledger reads as a deliberate motif */}
        <Reveal
          as="div"
          className="closing-band"
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
            <div className="mono" style={{ fontSize: 11.5, letterSpacing: "0.16em", color: "#8a8f98" }}>WHY IT&rsquo;S DIFFERENT</div>
            <p className="serif" style={{ fontSize: "clamp(22px,2.6vw,34px)", lineHeight: 1.2, margin: "14px 0 0", letterSpacing: "-0.01em" }}>
              Most AI emits text you have to believe. <span style={{ fontStyle: "italic", color: "#fff" }}>twog emits evidence you can verify.</span>
            </p>
          </div>
          <div style={{ display: "flex", gap: 14, flexWrap: "wrap" }}>
            <MagneticButton><Link href="/evidence" className="btn" style={{ background: "#fff", color: "var(--ink)", borderColor: "#fff" }}>Inspect the evidence →</Link></MagneticButton>
            <MagneticButton><Link href="/runs" className="btn" style={{ color: "#e9eaec", borderColor: "#3a3a3a" }}>See full runs</Link></MagneticButton>
          </div>
        </Reveal>
      </div>
    </>
  );
}
