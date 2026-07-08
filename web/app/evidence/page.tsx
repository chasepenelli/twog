import Link from "next/link";

import { api } from "@/lib/api";
import type { ProofCapsule } from "@/lib/types/domain";
import { LANE_SHORT, VERDICT_META, type Verdict } from "@/lib/evidence/verdict";

export const metadata = { title: "Evidence" };

/** "honokiol-pik3ca" -> "honokiol × PIK3CA" (best-effort; the last token is the target). */
function candidateLabel(cid: string): string {
  const parts = cid.replace(/-(auto|demo|crux)$/i, "").split("-");
  if (parts.length < 2) return cid;
  const target = parts.pop()!;
  return `${parts.join("-")} × ${target.toUpperCase()}`;
}

export const dynamic = "force-dynamic"; // live Neon data per request
/** Verdict for a candidate from its capsules' real signals: a single refuting result rules it out. */
function verdictFrom(caps: ProofCapsule[]): Verdict {
  if (caps.some((c) => c.signal === "refutes")) return "ruled-out";
  if (caps.some((c) => c.signal === "supports")) return "still-standing";
  return "needs-more";
}

export default async function EvidencePage() {
  const capsules = await api.capsules.list();

  // group by candidate — each card is one IDEA, not one test receipt
  const byCand = new Map<string, ProofCapsule[]>();
  for (const c of capsules) {
    const arr = byCand.get(c.candidate_id) ?? [];
    arr.push(c);
    byCand.set(c.candidate_id, arr);
  }
  const order: Record<Verdict, number> = { "still-standing": 0, "needs-more": 1, "ruled-out": 2 };
  const cards = [...byCand.entries()]
    .map(([cid, caps]) => ({ cid, caps, verdict: verdictFrom(caps), lanes: [...new Set(caps.map((c) => c.validation_type))], rep: caps[0] }))
    .sort((a, b) => order[a.verdict] - order[b.verdict]);

  return (
    <div className="wrap" style={{ paddingTop: 52 }}>
      <div className="kick">Evidence</div>
      <h1 style={{ fontSize: "clamp(34px,5vw,56px)", fontWeight: 700, letterSpacing: "-0.04em", margin: "12px 0 0" }}>
        Every idea, <span className="em">and how it&rsquo;s holding up.</span>
      </h1>
      <p className="lede">
        Each of these is a real drug against a real target in the cancer dogs and people share. We test each
        one to try to prove it wrong — and keep only what survives. Open any idea to see how far it&rsquo;s
        gotten and why.
      </p>
      <p className="mono muted" style={{ fontSize: 13, marginTop: 14 }}>
        An idea being ruled out is a result, not a failure — being wrong fast is the point.
      </p>

      {/* legend */}
      <div className="mono" style={{ display: "flex", flexWrap: "wrap", gap: "10px 22px", marginTop: 30, padding: "16px 18px", border: "1px solid var(--line)", borderRadius: 13, fontSize: 12.5, color: "var(--muted)", background: "var(--soft)" }}>
        <span><span className="badge b-green" style={{ fontSize: 11 }}>still standing</span> survived every test so far</span>
        <span><span className="badge b-amber" style={{ fontSize: 11 }}>needs more testing</span> no verdict yet</span>
        <span><span className="badge b-red" style={{ fontSize: 11 }}>ruled out</span> disproven</span>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 16, marginTop: 32 }}>
        {cards.map(({ cid, lanes, verdict, rep }) => {
          const v = VERDICT_META[verdict];
          const tested = lanes.map((l) => LANE_SHORT[l] ?? l).join(" · ");
          const stripe = v.tone === "b-green" ? "var(--green)" : v.tone === "b-red" ? "var(--red)" : "var(--amber)";
          return (
            <Link
              key={cid}
              href={`/evidence/${rep.capsule_id}`}
              className="panel"
              style={{ display: "block", padding: "24px 26px", borderLeft: `3px solid ${stripe}` }}
            >
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 14, flexWrap: "wrap" }}>
                <span className={`badge ${v.tone}`} style={{ fontSize: 11.5 }}>{v.label}</span>
                <span className="mono accent" style={{ fontSize: 12 }}>see how far →</span>
              </div>
              <div style={{ fontSize: "clamp(19px,1.8vw,24px)", fontWeight: 700, letterSpacing: "-0.02em", marginTop: 11 }}>
                {candidateLabel(cid)}
              </div>
              {tested ? <div className="mono muted" style={{ fontSize: 12.5, marginTop: 9 }}>tested via {tested}</div> : null}
            </Link>
          );
        })}
      </div>
    </div>
  );
}
