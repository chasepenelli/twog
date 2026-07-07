import '../v4/v4.css';
import '../v2/v2.css';
import '../detail.css';
import './architecture.css';
import { SiteNav } from '@/components/v4/SiteNav';

const architectureNodes = [
  { step: '01', label: 'Ingest', mode: 'deterministic', detail: 'Source APIs and research signal enter without LLM mutation.' },
  { step: '02', label: 'Normalize', mode: 'schema-validated', detail: 'Records, chunks, citations, entities, provenance, and lineage.' },
  { step: '03', label: 'Materialize', mode: 'Dagster assets', detail: 'Ingestion, synthesis, validation, health, embeddings, and compute lanes.' },
  { step: '04', label: 'Store', mode: 'Neon + SQLite', detail: 'Hosted runtime state plus local reproducibility from the same inputs.' },
  { step: '05', label: 'Argue', mode: 'recommend-only LLMs', detail: 'Agents critique citations, confounders, gaps, and weak claims.' },
  { step: '06', label: 'Publish', mode: 'public records', detail: 'Versioned methods, content hashes, JSON payloads, and decisions.' },
  { step: '07', label: 'Triage', mode: 'operator gate', detail: 'Preview routes first. Writes require approval and dry_run=false.' },
  { step: '08', label: 'Compute', mode: 'approval-first', detail: 'GPU/Docker compute jobs are ledgered, artifact-backed, and gated.' },
] as const;

const sourceTags = ['PubMed', 'Europe PMC', 'PMC OA', 'OpenAlex', 'Crossref', 'ClinicalTrials.gov', 'PubChem', 'ChEMBL', 'UniProt', 'RCSB PDB', 'OpenFDA animal events', 'research social signal'] as const;

const boundaries = [
  { label: 'Evidence boundary', detail: 'Deterministic ingestion, LLMs for synthesis and critique only, never for silent data mutation.' },
  { label: 'Public boundary', detail: 'Public submissions do not mutate candidate state. Contributions enter intake; writes require operator action.' },
  { label: 'Compute boundary', detail: 'GPU jobs are approval-first and ledgered. Public contribution does not trigger compute.' },
] as const;

const testingPath = [
  { label: 'Proof record', mode: 'inspectable substrate', detail: 'The candidate record captures mechanism, citations, risks, methods, decision history, payload links, and known gaps.' },
  { label: 'Autonomous crux', mode: 'falsification-first loop', detail: 'TWOG proposes the next test most likely to kill the leading hypothesis, pre-registers a hashed kill-criterion before compute, and resolves its own target/therapy inputs. Confound and provenance gates must pass; nothing is auto-promoted.' },
  { label: 'Validation packet', mode: 'explicit test plan', detail: 'A promising record becomes a bounded question with required inputs, readouts, controls, blockers, and success criteria.' },
  { label: 'Simulation lane', mode: 'artifact-backed compute', detail: 'Approval-gated Docker and GPU jobs can produce docking, MD smoke, plots, logs, and reproducible configuration artifacts.' },
  { label: 'Review gate', mode: 'specialist critique', detail: 'Agents and operators decide whether simulated findings are coherent enough to justify more evidence or lab discussion.' },
  { label: 'Lab handoff', mode: 'confirmation-ready', detail: 'The system prepares assay context, controls, thresholds, materials, and metrics for humans or partners to review.' },
  { label: 'Result update', mode: 'record revision', detail: 'Wet-lab confirmation, failure, or ambiguity feeds back into the candidate record and decision log.' },
] as const;

const stack = ['Dagster', 'Neon Postgres', 'TypeScript / Next.js', 'Python research bridge', 'Modal', 'Docker workers', 'MCP service boundary'] as const;

export const metadata = {
  title: 'Architecture — TWOG',
  description: 'The technical architecture behind TWOG: deterministic ingestion, Dagster assets, operator-gated writes, public candidate records, approval-first compute, and validation-ready testing packets.',
};

export default function ArchitecturePage() {
  return (
    <div className="v4-shell">
      <div className="v4-grain" />
      <div className="v4-detail v4-detail--wide">
        <SiteNav />

        <p className="v4-kick">System architecture</p>
        <h1 className="v4-dh1">The engine, end to end.</h1>
        <p className="v4-lead">
          TWOG moves from inspectable proof records to durable validation packets, approval-gated compute,
          and lab-ready confirmation plans. <strong>Simulations prioritize hypotheses; lab confirmation
          decides what survives.</strong> Evidence enters through deterministic rails — claims leave as
          testable records.
        </p>

        <section className="v4-sec2">
          <div className="v4-dh2">The loop — evidence in, testable records out</div>
          <div className="v4-arch-grid">
            {architectureNodes.map((n) => (
              <article key={n.step} className="v4-arch-node">
                <code>{n.step}</code>
                <div className="v4-arch-node__head">
                  <strong>{n.label}</strong>
                  <span className="v4-chip">{n.mode}</span>
                </div>
                <p>{n.detail}</p>
              </article>
            ))}
          </div>
        </section>

        <section className="v4-sec2">
          <div className="v4-dh2">Source rails</div>
          <div className="v4-prov__badges">
            {sourceTags.map((s) => <span key={s} className="v4-chip">{s}</span>)}
          </div>
          <p className="v4-note">MCP-compatible service boundary for future agents, tools, and external review systems.</p>
        </section>

        <section className="v4-sec2">
          <div className="v4-dh2">Proof → lab: the durable testing framework</div>
          <p className="v4-lead" style={{ marginBottom: '1.2rem' }}>
            Public proofs become validation packets, then confirmation work — without pretending simulation is proof.
          </p>
          <div className="v4-arch-grid">
            {testingPath.map((t) => (
              <article key={t.label} className="v4-arch-node">
                <div className="v4-arch-node__head">
                  <strong>{t.label}</strong>
                  <span className="v4-chip">{t.mode}</span>
                </div>
                <p>{t.detail}</p>
              </article>
            ))}
          </div>
        </section>

        <section className="v4-sec2">
          <div className="v4-dh2">Three deliberate gates</div>
          {boundaries.map((b) => (
            <div key={b.label} className="v4-trust" style={{ marginTop: '0.9rem' }}>
              <strong>{b.label}</strong>
              <p>{b.detail}</p>
            </div>
          ))}
        </section>

        <section className="v4-sec2">
          <div className="v4-dh2">Current rails</div>
          <div className="v4-prov__badges">
            {stack.map((s) => <span key={s} className="v4-chip">{s}</span>)}
          </div>
        </section>
      </div>
    </div>
  );
}
