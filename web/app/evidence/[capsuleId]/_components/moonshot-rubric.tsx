import type {
  MoonshotRubric,
  RubricCompound,
  RubricExpectedPayoff,
  RubricInferenceLink,
  RubricKillCriterion,
  RubricLaneTest,
  RubricPremise,
  RubricTarget,
} from "@/lib/types/domain";

/* The pre-registered "whole shabang" behind a capsule: what the moonshot must show and exactly how it
   will be tested. Rendered on the capsule detail page so a capsule reads as one receipt from a full
   falsification program — not "just a simple question". Server component, no interactivity. */

const STANDING_BADGE: Record<string, { cls: string; label: string }> = {
  supports_unaudited: { cls: "b-green", label: "supports · unaudited" },
  refuted: { cls: "b-red", label: "refuted" },
  queued: { cls: "b-blue", label: "queued next" },
  controlled: { cls: "b-green", label: "controlled" },
  untested: { cls: "b-muted", label: "untested" },
};

const READINESS_BADGE: Record<string, string> = {
  resolved: "b-green",
  needs_verification: "b-amber",
  missing: "b-muted",
};

function badge(cls: string, text: string, key?: string) {
  return (
    <span key={key} className={`badge ${cls}`} style={{ fontSize: 11.5 }}>
      {text}
    </span>
  );
}

function KillCriterion({ kc }: { kc: RubricKillCriterion | null }) {
  if (!kc) return null;
  return (
    <div className="mono muted" style={{ fontSize: 12.5, marginTop: 8, lineHeight: 1.55 }}>
      <span style={{ color: "var(--red)", fontWeight: 600 }}>kills if</span>{" "}
      {kc.metric} {kc.comparator} {String(kc.threshold)} → {kc.kills_on}
      {kc.rationale ? <div style={{ marginTop: 3, color: "var(--muted)" }}>{kc.rationale}</div> : null}
    </div>
  );
}

function TargetRow({ t }: { t: RubricTarget }) {
  const v = t.verification;
  const cls = v === "verified" ? "b-green" : v === "unverified" ? "b-amber" : "b-muted";
  return (
    <div style={{ display: "flex", gap: 10, alignItems: "baseline", flexWrap: "wrap", padding: "7px 0" }}>
      <span className="mono" style={{ fontSize: 14, fontWeight: 600, minWidth: 92 }}>{t.target}</span>
      {badge(cls, `docking ${v}`)}
      <span className="mono muted" style={{ fontSize: 12 }}>
        {t.role}
        {t.pdb_id ? ` · PDB ${t.pdb_id}${t.chain ? `/${t.chain}` : ""}` : ""}
        {t.uniprot ? ` · ${t.uniprot}` : ""}
        {typeof t.redock_rmsd === "number" ? ` · redock ${t.redock_rmsd.toFixed(2)} Å` : ""}
      </span>
    </div>
  );
}

function CompoundRow({ c }: { c: RubricCompound }) {
  return (
    <div style={{ display: "flex", gap: 10, alignItems: "baseline", flexWrap: "wrap", padding: "7px 0" }}>
      <span className="mono" style={{ fontSize: 14, fontWeight: 600, minWidth: 92 }}>{c.name}</span>
      {badge(READINESS_BADGE[c.readiness] ?? "b-muted", `SMILES ${c.readiness}`)}
      <span className="mono muted" style={{ fontSize: 12 }}>{c.role} · {c.resolution_source}</span>
      {c.smiles ? (
        <span className="mono muted" style={{ fontSize: 11.5, opacity: 0.8, wordBreak: "break-all", maxWidth: "100%" }}>{c.smiles}</span>
      ) : null}
    </div>
  );
}

function LaneTest({ t }: { t: RubricLaneTest }) {
  const st = STANDING_BADGE[t.standing] ?? STANDING_BADGE.untested;
  const md = t.inputs.md_schedule;
  return (
    <li style={{ borderTop: "1px solid var(--line)", padding: "16px 0", listStyle: "none" }}>
      <div style={{ display: "flex", gap: 10, alignItems: "baseline", flexWrap: "wrap" }}>
        <span className="mono accent" style={{ fontSize: 12, fontWeight: 700 }}>{t.order}.</span>
        <span className="mono" style={{ fontSize: 14, fontWeight: 700, letterSpacing: "0.02em" }}>{t.lane}</span>
        {badge(st.cls, st.label)}
        {t.is_proposed ? badge("b-blue", "next to run") : null}
        {t.maturity === "smoke" ? badge("b-amber", "smoke · ≤1000 steps") : null}
        {badge(t.inputs_ready ? "b-green" : "b-muted", t.inputs_ready ? "inputs ready" : "inputs missing")}
        {t.addresses_confound ? badge("b-muted", `audits ${t.addresses_confound}`) : null}
      </div>
      <p style={{ fontSize: 15.5, lineHeight: 1.55, margin: "10px 0 0", maxWidth: "62ch" }}>{t.objective}</p>
      {t.probes && t.probes.length ? (
        <div style={{ marginTop: 8 }}>
          <span className="mono muted" style={{ fontSize: 11, fontWeight: 700, letterSpacing: "0.08em" }}>INTERROGATES</span>
          <ul style={{ margin: "4px 0 0", paddingLeft: 18, fontSize: 14, lineHeight: 1.55, color: "#333", maxWidth: "62ch" }}>
            {t.probes.map((p, i) => <li key={i}>{p}</li>)}
          </ul>
        </div>
      ) : null}
      {t.why_it_bears ? (
        <p className="muted" style={{ fontSize: 13.5, lineHeight: 1.55, margin: "8px 0 0", maxWidth: "62ch" }}>{t.why_it_bears}</p>
      ) : null}
      <KillCriterion kc={t.kill_criterion} />
      {(t.interpretation && (t.interpretation.supports || t.interpretation.refutes)) ? (
        <div style={{ marginTop: 10, display: "flex", flexDirection: "column", gap: 5 }}>
          <div className="mono muted" style={{ fontSize: 11, fontWeight: 700, letterSpacing: "0.08em" }}>WHAT EACH OUTCOME MEANS</div>
          {t.interpretation.refutes ? (
            <div style={{ fontSize: 13, lineHeight: 1.5 }}>
              <span className="sig refutes mono" style={{ fontSize: 11.5 }}>refutes →</span> <span className="muted">{t.interpretation.refutes}</span>
            </div>
          ) : null}
          {t.interpretation.supports ? (
            <div style={{ fontSize: 13, lineHeight: 1.5 }}>
              <span className="sig supports mono" style={{ fontSize: 11.5 }}>supports →</span> <span className="muted">{t.interpretation.supports}</span>
            </div>
          ) : null}
        </div>
      ) : null}
      {t.inputs.missing.length ? (
        <div className="mono muted" style={{ fontSize: 12, marginTop: 6 }}>missing inputs: {t.inputs.missing.join(", ")}</div>
      ) : null}
      {md ? (
        <div className="tile" style={{ marginTop: 12, padding: 14, background: "var(--soft)" }}>
          <div className="mono" style={{ fontSize: 11.5, fontWeight: 700, letterSpacing: "0.1em", color: "var(--muted)" }}>MD SCHEDULE (pre-registered)</div>
          <div className="mono muted" style={{ fontSize: 12.5, marginTop: 8, lineHeight: 1.6 }}>
            {md.simulation_steps} steps · {md.temperature}K{md.ph != null ? ` · pH ${md.ph}` : ""}
            {md.solvent_model ? ` · ${md.solvent_model}` : ""}
            {md.force_field ? <div style={{ marginTop: 3 }}>force field: {md.force_field}</div> : null}
            {md.equilibration ? <div style={{ marginTop: 3 }}>equilibration: {md.equilibration}</div> : null}
            {md.preparation_method ? <div style={{ marginTop: 3 }}>preparation: {md.preparation_method}</div> : null}
          </div>
        </div>
      ) : null}
    </li>
  );
}

function Block({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div style={{ marginTop: 22 }}>
      <div className="mono" style={{ fontSize: 11.5, fontWeight: 700, letterSpacing: "0.12em", color: "var(--muted)", marginBottom: 8 }}>
        {label}
      </div>
      {children}
    </div>
  );
}

function PremiseBlock({ premise, mechanistic }: { premise?: RubricPremise; mechanistic: string }) {
  if (!premise && !mechanistic) return null;
  const specified = premise?.is_specified;
  const sCls = premise?.strength === "high" ? "b-green" : premise?.strength === "medium" ? "b-amber" : "b-muted";
  return (
    <Block label="WHY THIS IS PLAUSIBLE — the premise (because of A)">
      {specified ? (
        <>
          {premise?.claim ? (
            <p style={{ fontSize: 16, lineHeight: 1.6, margin: 0, maxWidth: "62ch", fontWeight: 600 }}>{premise.claim}</p>
          ) : null}
          {premise?.basis ? (
            <p className="muted" style={{ fontSize: 14, lineHeight: 1.6, margin: "8px 0 0", maxWidth: "62ch" }}>{premise.basis}</p>
          ) : null}
          <div style={{ marginTop: 10 }}>{badge(sCls, `stated evidence: ${premise?.strength}`)}</div>
        </>
      ) : (
        <p className="muted" style={{ fontSize: 14, lineHeight: 1.6, margin: 0, maxWidth: "62ch" }}>
          Mechanism unstated upstream — treated as a <strong>hypothesis to be tested</strong>, not an established
          premise. The engine does not invent one.
        </p>
      )}
    </Block>
  );
}

function InferenceChainBlock({ chain }: { chain: RubricInferenceLink[] }) {
  if (!chain || !chain.length) return null;
  return (
    <Block label="LINE OF REASONING — conjunctive survival; any single refutation kills it">
      <ol style={{ margin: 0, padding: 0, listStyle: "none", display: "flex", flexDirection: "column", gap: 12 }}>
        {chain.map((link) => (
          <li key={link.step} style={{ borderLeft: "2px solid var(--line)", paddingLeft: 14 }}>
            <div style={{ fontSize: 14.5, lineHeight: 1.55, maxWidth: "64ch" }}>
              <span className="mono accent" style={{ fontWeight: 700 }}>{link.step}.</span> {link.infers}
            </div>
            {link.if_broken ? (
              <div className="mono" style={{ fontSize: 12, marginTop: 4, maxWidth: "64ch" }}>
                <span style={{ color: "var(--red)", fontWeight: 600 }}>✕ if this link breaks → </span>
                <span className="muted">{link.if_broken}</span>
              </div>
            ) : null}
          </li>
        ))}
      </ol>
    </Block>
  );
}

function ExpectedPayoffBlock({ payoff }: { payoff?: RubricExpectedPayoff }) {
  if (!payoff || (!payoff.if_survives && !payoff.translational_claim)) return null;
  return (
    <Block label="WHAT IT WOULD MEAN IF THIS SURVIVES — the earned payoff">
      <p style={{ fontSize: 16, lineHeight: 1.6, margin: 0, maxWidth: "64ch" }}>{payoff.if_survives}</p>
      {payoff.next_step ? (
        <div className="mono muted" style={{ fontSize: 13, marginTop: 8 }}>next step → {payoff.next_step}</div>
      ) : null}
      {typeof payoff.value_of_information === "number" ? (
        <div className="mono muted" style={{ fontSize: 12, marginTop: 4 }}>
          value of information: {Math.round(payoff.value_of_information * 100)}%
        </div>
      ) : null}
      {payoff.caveat ? (
        <p className="muted" style={{ fontSize: 12.5, marginTop: 10, maxWidth: "64ch", fontStyle: "italic" }}>{payoff.caveat}</p>
      ) : null}
    </Block>
  );
}

export function MoonshotRubricView({ rubric }: { rubric: MoonshotRubric }) {
  const r = rubric;
  return (
    <div className="panel" style={{ padding: "26px 24px", marginTop: 14 }}>
      <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
        {badge(r.moonshot_grade ? "b-green" : "b-muted", r.moonshot_grade ? "moonshot-grade" : "not moonshot-grade")}
        {typeof r.moonshot_score === "number" ? badge("b-blue", `score ${r.moonshot_score.toFixed(2)}`) : null}
        <span className={`sig ${r.net_signal === "none" ? "neutral" : r.net_signal} mono`} style={{ fontSize: 12.5 }}>
          net {r.net_signal} · {Math.round(r.net_confidence * 100)}%
        </span>
        {badge(r.has_falsifiable_plan ? "b-green" : "b-red", r.has_falsifiable_plan ? "falsifiable plan" : "no falsifiable plan")}
      </div>

      <h2 style={{ fontSize: 22, fontWeight: 700, letterSpacing: "-0.02em", lineHeight: 1.2, margin: "14px 0 0", maxWidth: "30ch" }}>
        {r.title}
      </h2>
      {r.thesis ? <p style={{ fontSize: 16, lineHeight: 1.6, margin: "10px 0 0", maxWidth: "60ch", color: "#333" }}>{r.thesis}</p> : null}

      <PremiseBlock premise={r.premises?.[0]} mechanistic={r.mechanistic_premise} />

      {r.targets_needed.length ? (
        <Block label="TARGETS & PROTEINS NEEDED">
          {r.targets_needed.map((t) => <TargetRow key={t.target} t={t} />)}
        </Block>
      ) : null}

      {r.compounds_needed.length ? (
        <Block label="COMPOUNDS & SMILES NEEDED">
          {r.compounds_needed.map((c) => <CompoundRow key={c.name} c={c} />)}
        </Block>
      ) : null}

      {r.test_plan.length ? (
        <Block label="ORDERED TEST PLAN — EACH WITH ITS PRE-REGISTERED KILL CRITERION">
          <ul style={{ margin: 0, padding: 0 }}>
            {r.test_plan.map((t) => <LaneTest key={t.order} t={t} />)}
          </ul>
        </Block>
      ) : null}

      <InferenceChainBlock chain={r.inference_chain} />

      {(r.inputs_rollup.ready_to_run_lanes.length || r.inputs_rollup.missing_lanes.length || r.inputs_rollup.blockers.length) ? (
        <Block label="INPUTS READINESS (the spend gate — only ready lanes may dispatch real GPU)">
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            {r.inputs_rollup.ready_to_run_lanes.map((l) => badge("b-green", `ready: ${l}`, `rdy-${l}`))}
            {r.inputs_rollup.needs_verification_lanes.map((l) => badge("b-amber", `needs verification: ${l}`, `nv-${l}`))}
            {r.inputs_rollup.missing_lanes.map((l) => badge("b-muted", `missing: ${l}`, `ms-${l}`))}
          </div>
          {r.inputs_rollup.blockers.length ? (
            <div className="mono muted" style={{ fontSize: 12, marginTop: 10 }}>{r.inputs_rollup.blockers.join(" · ")}</div>
          ) : null}
        </Block>
      ) : null}

      {(r.confounds.open.length || r.confounds.controlled.length) ? (
        <Block label="CONFOUNDS TO AUDIT (no `supports` is trusted until controlled)">
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            {r.confounds.open.map((c, i) => badge("b-amber", `open: ${c.kind}`, `oc-${i}`))}
            {r.confounds.controlled.map((c, i) => badge("b-green", `controlled: ${c.kind}`, `cc-${i}`))}
          </div>
          {r.confounds.audit_policy ? (
            <p className="muted" style={{ fontSize: 13, marginTop: 10, maxWidth: "62ch" }}>{r.confounds.audit_policy}</p>
          ) : null}
        </Block>
      ) : null}

      {r.cross_species.replication_axis ? (
        <Block label="CROSS-SPECIES REPLICATION (canine HSA × human AS)">
          <p style={{ fontSize: 15, lineHeight: 1.55, margin: 0, maxWidth: "62ch" }}>{r.cross_species.replication_axis}</p>
          <div className="mono muted" style={{ fontSize: 12, marginTop: 6 }}>
            {r.cross_species.species.join(" × ")}
            {r.cross_species.replication_lane ? ` · lane: ${r.cross_species.replication_lane}` : ""}
            {r.cross_species.orthogonal_cohort_required ? " · orthogonal cohort required" : ""}
          </div>
          <KillCriterion kc={r.cross_species.kill_criterion} />
        </Block>
      ) : null}

      <ExpectedPayoffBlock payoff={r.expected_payoff} />

      <Block label="PROMOTION BAR">
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 8 }}>
          {badge("b-blue", "never auto-promoted")}
          {r.promotion.required_surviving_lanes.map((l) => badge("b-muted", `survive: ${l}`, `pl-${l}`))}
          {r.promotion.cross_species_replication_required ? badge("b-muted", "cross-species replication") : null}
          {badge("b-muted", `≥${r.promotion.min_signalful_capsules} signalful capsules`)}
        </div>
        {r.promotion.statement ? (
          <p className="muted" style={{ fontSize: 13.5, margin: 0, maxWidth: "64ch", lineHeight: 1.6 }}>{r.promotion.statement}</p>
        ) : null}
      </Block>

      {r.risks.length ? (
        <Block label="RISKS">
          <ul style={{ margin: 0, paddingLeft: 18, fontSize: 14, lineHeight: 1.7, color: "#333", maxWidth: "62ch" }}>
            {r.risks.map((x, i) => <li key={i}>{x}</li>)}
          </ul>
        </Block>
      ) : null}
    </div>
  );
}
