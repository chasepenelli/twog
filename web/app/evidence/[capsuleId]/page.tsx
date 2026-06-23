import Link from "next/link";

import { api } from "@/lib/api";
import type { CapsuleProvenance, GateVerdict, MoonshotRubric, ProofCapsule } from "@/lib/types/domain";

import { MoonshotRubricView } from "./_components/moonshot-rubric";
import { ProvenancePanel } from "./_components/provenance-panel";

const SIG_LABEL: Record<string, string> = { supports: "SUPPORTS", refutes: "REFUTES", neutral: "NEUTRAL" };

function gateLine(label: string, v?: GateVerdict): { cls: string; text: string } {
  switch (v) {
    case "pass": return { cls: "b-green", text: `The ${label} gate passed — the result survived this check.` };
    case "fail": return { cls: "b-red", text: `The ${label} gate FAILED — this capsule cannot be accepted as-is.` };
    case "pending": return { cls: "b-amber", text: `The ${label} gate is pending — not yet checked.` };
    default: return { cls: "b-muted", text: `The ${label} gate has not been evaluated.` };
  }
}

function Section({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <section style={{ padding: "26px 0", borderTop: "1px solid var(--line)" }}>
      <div className="h2" style={{ marginBottom: 12 }}>{label}</div>
      {children}
    </section>
  );
}

export default async function CapsulePage({ params }: { params: { capsuleId: string } }) {
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
  const confound = gateLine("confound-control", c.confound_verdict);
  const provenance = gateLine("provenance", c.provenance_verdict);

  let rubric: MoonshotRubric | null = null;
  try {
    rubric = await api.candidates.rubric(c.candidate_id);
  } catch {
    rubric = null; // the rubric is enrichment; never block the capsule view on it
  }

  let provBundle: CapsuleProvenance | null = null;
  try {
    provBundle = await api.capsules.provenance(c.capsule_id);
  } catch {
    provBundle = null;
  }

  return (
    <div className="wrap" style={{ paddingTop: 48, maxWidth: 860 }}>
      <Link href="/evidence" className="mono accent" style={{ fontSize: 13 }}>← Evidence</Link>

      <div style={{ marginTop: 24, display: "flex", alignItems: "center", gap: 14, flexWrap: "wrap" }}>
        <span className={`sig ${c.signal} mono`} style={{ fontSize: 14, letterSpacing: "0.14em" }}>
          {SIG_LABEL[c.signal] ?? c.signal}
        </span>
        <span className="mono muted" style={{ fontSize: 12 }}>{c.capsule_id}</span>
      </div>

      <h1 style={{ fontSize: "clamp(28px,3.6vw,42px)", fontWeight: 700, letterSpacing: "-0.035em", lineHeight: 1.12, margin: "14px 0 0", maxWidth: "20ch" }}>
        {c.claim ?? c.candidate_id}
      </h1>

      <Section label="What was run">
        <p style={{ fontSize: 17, lineHeight: 1.6, maxWidth: "56ch" }}>{c.method ?? "—"}</p>
        <div className="mono muted" style={{ fontSize: 13, marginTop: 10 }}>lane · {c.validation_type}</div>
      </Section>

      <Section label="Readout">
        {c.plain ? <p style={{ fontSize: 19, lineHeight: 1.6, maxWidth: "54ch", margin: 0 }}>{c.plain}</p> : null}
        <div className="mono muted" style={{ fontSize: 14, marginTop: c.plain ? 14 : 0 }}>{c.readout ?? "—"}</div>
      </Section>

      {typeof c.confidence === "number" ? (
        <Section label="Confidence">
          <div style={{ maxWidth: 360 }}>
            <div className="bar" style={{ height: 8 }}><i style={{ width: `${Math.round(c.confidence * 100)}%` }} /></div>
            <div className="mono muted" style={{ fontSize: 12, marginTop: 8 }}>{Math.round(c.confidence * 100)}% — the capsule states its own confidence, then its limits.</div>
          </div>
        </Section>
      ) : null}

      {c.limitations && c.limitations.length ? (
        <Section label="How far it goes">
          <ul style={{ margin: 0, paddingLeft: 18, fontSize: 16, lineHeight: 1.7, color: "#333", maxWidth: "56ch" }}>
            {c.limitations.map((l, i) => <li key={i}>{l}</li>)}
          </ul>
        </Section>
      ) : null}

      <Section label="Gates it had to clear">
        <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
          <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
            <span className={`badge ${confound.cls}`} style={{ minWidth: 118, justifyContent: "center" }}>confound · {c.confound_verdict ?? "—"}</span>
            <span className="muted" style={{ fontSize: 14.5 }}>{confound.text}</span>
          </div>
          <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
            <span className={`badge ${provenance.cls}`} style={{ minWidth: 118, justifyContent: "center" }}>provenance · {c.provenance_verdict ?? "—"}</span>
            <span className="muted" style={{ fontSize: 14.5 }}>{provenance.text}</span>
          </div>
        </div>
        <p className="muted" style={{ fontSize: 13.5, marginTop: 16, maxWidth: "60ch" }}>
          Clearing the gates is necessary, not sufficient — a human operator still holds the terminal accept/promote decision. Nothing auto-promotes.
        </p>
      </Section>

      <Section label="Provenance — verify / re-derive this result">
        <ProvenancePanel prov={provBundle} producedBy={c.produced_by} />
      </Section>

      <Section label={rubric ? "The moonshot behind this capsule" : "Candidate"}>
        <span className="mono" style={{ fontSize: 14 }}>{c.candidate_id}</span>
        {rubric ? (
          <>
            <p className="muted" style={{ fontSize: 14, marginTop: 12, maxWidth: "62ch", lineHeight: 1.6 }}>
              This capsule is one receipt from a larger, pre-registered falsification program. The rubric below
              is the &ldquo;whole shabang&rdquo;: the thesis, the proteins and compounds it needs, and the ordered
              test plan — each step locked to a kill criterion <em>before</em> it runs.
            </p>
            <MoonshotRubricView rubric={rubric} />
          </>
        ) : null}
      </Section>
    </div>
  );
}
