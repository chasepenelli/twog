import Link from "next/link";

import { api } from "@/lib/api";
import type { CapsuleProvenance, MoonshotRubric, ProofCapsule } from "@/lib/types/domain";
import { getPrincipal } from "@/lib/auth/server";
import { LaneTrack } from "@/components/evidence/lane-track";
import { deriveLaneTrack, publicQuestion, verdictOf, VERDICT_META } from "@/lib/evidence/verdict";

import { MoonshotRubricView } from "./_components/moonshot-rubric";
import { ProvenancePanel } from "./_components/provenance-panel";

/* The capsule page is two-tier on ONE route:
   - PUBLIC (always): a conversational teaser — We asked → why this → how far we've checked → where it
     stands → if it holds, the hope → how we know it's real. Light, plain, honest ("nothing proven").
   - LOGGED-IN (getPrincipal() truthy): the full pre-registered rubric + raw provenance, appended below.
   getPrincipal() returns null when WorkOS isn't wired (public by default) and honors DEV_AUTH_BYPASS for
   local preview; ?view=public lets a signed-in member preview the public teaser. */

export const dynamic = "force-dynamic"; // live Neon data per request
function Section({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <section style={{ padding: "26px 0", borderTop: "1px solid var(--line)" }}>
      <div className="mono" style={{ fontSize: 12, fontWeight: 700, letterSpacing: "0.1em", color: "var(--muted)", marginBottom: 12 }}>{label}</div>
      {children}
    </section>
  );
}

function confidenceWord(c: number): string {
  if (c < 0.34) return "still thin";
  if (c < 0.67) return "moderate";
  return "strong — but never proof";
}

export default async function CapsulePage({
  params,
  searchParams,
}: {
  params: { capsuleId: string };
  searchParams?: { view?: string };
}) {
  let capsule: ProofCapsule | null = null;
  try {
    capsule = await api.capsules.get(params.capsuleId);
  } catch {
    capsule = null;
  }

  if (!capsule) {
    return (
      <div className="wrap" style={{ paddingTop: 60 }}>
        <Link href="/evidence" className="mono accent" style={{ fontSize: 13 }}>← Evidence</Link>
        <h1 style={{ marginTop: 20, fontSize: 28, fontWeight: 700 }}>Capsule not found</h1>
        <p className="muted" style={{ marginTop: 10 }}>No proof capsule with id <span className="mono">{params.capsuleId}</span>.</p>
      </div>
    );
  }
  const c = capsule;

  let rubric: MoonshotRubric | null = null;
  try {
    rubric = await api.candidates.rubric(c.candidate_id);
  } catch {
    rubric = null;
  }
  let provBundle: CapsuleProvenance | null = null;
  try {
    provBundle = await api.capsules.provenance(c.capsule_id);
  } catch {
    provBundle = null;
  }
  const principal = await getPrincipal();
  const isMember = !!principal && searchParams?.view !== "public";

  // ---- derive the public story from real fields ----
  const verdict = VERDICT_META[
    rubric ? verdictOf(rubric) : c.signal === "refutes" ? "ruled-out" : c.signal === "supports" ? "still-standing" : "needs-more"
  ];
  const steps = deriveLaneTrack(rubric);
  const question = publicQuestion(rubric?.title, c.claim ?? c.candidate_id);
  const target = rubric?.targets_needed?.[0]?.target;
  const compound = rubric?.compounds_needed?.[0]?.name;
  const premise = rubric?.mechanistic_premise || rubric?.premises?.[0]?.claim || "";
  const stakes = rubric?.cross_species?.disease_context || "the cancer dogs and people share";
  const payoff = rubric?.expected_payoff;
  const confound = c.confound_verdict;
  const signed = provBundle?.signed ? (provBundle.signature_valid ? "verified" : "INVALID") : "custodial";

  return (
    <div className="wrap" style={{ paddingTop: 48, maxWidth: 760 }}>
      <Link href="/evidence" className="mono accent" style={{ fontSize: 13 }}>← Evidence</Link>

      {/* VERDICT + QUESTION */}
      <div style={{ marginTop: 22, display: "flex", alignItems: "center", gap: 12, flexWrap: "wrap" }}>
        <span className={`badge ${verdict.tone}`} style={{ fontSize: 12 }}>{verdict.label}</span>
        {provBundle?.signed && provBundle.signature_valid ? <span className="badge b-blue" style={{ fontSize: 12 }}>✓ signed &amp; verifiable</span> : null}
      </div>
      <h1 style={{ fontSize: "clamp(26px,3.4vw,40px)", fontWeight: 700, letterSpacing: "-0.03em", lineHeight: 1.14, margin: "14px 0 0", maxWidth: "24ch" }}>
        We asked: {question}
      </h1>

      {/* WHY THIS, WHY NOW */}
      <Section label="WHY THIS, WHY NOW">
        {premise ? (
          <p style={{ fontSize: 17, lineHeight: 1.6, margin: "0 0 10px", maxWidth: "58ch" }}>
            {premise.charAt(0).toUpperCase() + premise.slice(1)}.
          </p>
        ) : null}
        <p style={{ fontSize: 17, lineHeight: 1.6, margin: 0, maxWidth: "58ch", color: "#333" }}>
          {target ? <strong>{target}</strong> : "This target"} helps drive {stakes}. If {compound ?? "this drug"} truly
          grips it, the idea earns a hard, honest test — and that&rsquo;s exactly what the engine does next.
        </p>
      </Section>

      {/* HOW FAR WE'VE CHECKED */}
      <Section label="HOW FAR WE'VE CHECKED">
        {steps.length ? (
          <>
            <LaneTrack steps={steps} variant="full" />
            <p className="muted" style={{ fontSize: 13.5, marginTop: 16, maxWidth: "60ch" }}>
              Pre-registered tests, each locked to a kill criterion <em>before</em> it ran. They run in order, and
              any single one failing kills the whole idea.
            </p>
          </>
        ) : (
          <p style={{ fontSize: 16, lineHeight: 1.6, maxWidth: "56ch" }}>{c.plain ?? c.readout ?? c.method ?? "—"}</p>
        )}
      </Section>

      {/* WHERE IT STANDS */}
      <Section label={`WHERE IT STANDS — ${verdict.label.toUpperCase()}`}>
        <p style={{ fontSize: 17, lineHeight: 1.6, margin: 0, maxWidth: "58ch" }}>{verdict.gloss}</p>
        {typeof c.confidence === "number" ? (
          <p className="muted" style={{ fontSize: 14.5, marginTop: 10 }}>
            Confidence so far: {Math.round(c.confidence * 100)}% — {confidenceWord(c.confidence)}.
          </p>
        ) : null}
        <p className="muted" style={{ fontSize: 14.5, marginTop: 10, maxWidth: "58ch" }}>
          We try to prove ideas wrong and keep only what refuses to die. That&rsquo;s why a result that survives
          here means something — and why we never call anything &ldquo;proven.&rdquo;
        </p>
      </Section>

      {/* IF THIS HOLDS UP, THE HOPE */}
      {payoff?.if_survives ? (
        <Section label="IF THIS HOLDS UP, HERE'S THE HOPE">
          <p style={{ fontSize: 17, lineHeight: 1.6, margin: 0, maxWidth: "58ch" }}>{payoff.if_survives}</p>
          {payoff.next_step ? (
            <p className="muted" style={{ fontSize: 14.5, marginTop: 10 }}>Next step → {payoff.next_step}</p>
          ) : null}
          {payoff.caveat ? (
            <p className="muted" style={{ fontSize: 13, marginTop: 12, maxWidth: "60ch", fontStyle: "italic" }}>{payoff.caveat}</p>
          ) : null}
        </Section>
      ) : null}

      {/* HOW WE KNOW IT'S REAL — the trust row (caveat said ONCE) */}
      <Section label="HOW WE KNOW IT'S REAL">
        <div style={{ display: "flex", gap: 9, flexWrap: "wrap", alignItems: "center" }}>
          {confound ? <span className={`badge ${confound === "pass" ? "b-green" : confound === "fail" ? "b-red" : "b-muted"}`}>confound check · {confound}</span> : null}
          <span className={`badge ${signed === "verified" ? "b-green" : signed === "INVALID" ? "b-red" : "b-muted"}`}>
            {signed === "verified" ? "✓ signed & verifiable" : signed === "INVALID" ? "✕ signature invalid" : "custodial"}
          </span>
          {provBundle?.lineage?.chain_ok ? <span className="badge b-green">✓ lineage intact</span> : null}
        </div>
        <p className="muted" style={{ fontSize: 13.5, marginTop: 14, maxWidth: "62ch" }}>
          Clearing these checks is necessary, not sufficient — a human still holds the final call, and nothing
          is ever promoted automatically. Every result is hashed and signed, so anyone can re-derive it.
        </p>
      </Section>

      {/* the wall */}
      {isMember ? (
        <>
          {rubric ? (
            <Section label="THE FULL EXPERIMENT PLAN">
              <MoonshotRubricView rubric={rubric} />
            </Section>
          ) : null}
          <Section label="PROVENANCE — VERIFY / RE-DERIVE">
            <ProvenancePanel prov={provBundle} producedBy={c.produced_by} />
          </Section>
        </>
      ) : (
        <div style={{ marginTop: 30, padding: "26px 28px", borderRadius: 16, background: "var(--soft)", border: "1px solid var(--line)", textAlign: "center" }}>
          <div style={{ fontSize: 17, fontWeight: 600, maxWidth: "48ch", margin: "0 auto" }}>
            Want the whole picture? See the full pre-registered experiment plan — every target, compound, kill
            criterion, and the verifiable provenance behind this result.
          </div>
          <Link href="/login" className="btn solid" style={{ marginTop: 18, display: "inline-flex" }}>
            Log in to see the full experiment plan →
          </Link>
        </div>
      )}
    </div>
  );
}
