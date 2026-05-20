const architectureSections = [
  {
    step: '01',
    label: 'Ingestion',
    detail:
      'Structured harvesters pull PubChem, ChEMBL, UniProt, RCSB PDB, OpenFDA animal adverse events, PubMed, Europe PMC, PMC OA, OpenAlex, Crossref, ClinicalTrials.gov, and monitored research social signal. Sources are pulled idempotently, normalized into typed research records, and chunked deterministically. LLMs are not in the ingestion path.',
  },
  {
    step: '02',
    label: 'Orchestration',
    detail:
      'Dagster materializes assets across ingestion, synthesis, validation, source health, embeddings, and compute lanes. Every materialized asset carries upstream lineage. Re-runs are reproducible from inputs.',
  },
  {
    step: '03',
    label: 'Storage',
    detail:
      'Hosted Postgres on Neon holds runtime state with branchable research databases. SQLite supports local development. Typed contracts are enforced with schema-validated boundaries.',
  },
  {
    step: '04',
    label: 'Synthesis and critique',
    detail:
      'Specialist agents argue with the evidence: search, challenge, repair, synthesize. Agents surface citations, flag underpowered claims and confounders, request missing evidence, and preserve held-back leads as research notes rather than candidates.',
  },
  {
    step: '05',
    label: 'Public proof layer',
    detail:
      'Candidate snapshots are public-facing by design with versioned methods, content-hashed snapshots, and machine-readable JSON payloads. External readers can check out a snapshot, do outside work, and check in structured contribution packets through a gated intake endpoint.',
  },
  {
    step: '06',
    label: 'Triage',
    detail:
      'Public contributions land in Neon-backed intake. A Dagster job lets operators preview and explicitly route packets into evidence review, validation planning, compute review, request-more-information, rejection, or archive. The default mode is preview; writes require dry_run=false.',
  },
  {
    step: '07',
    label: 'Compute',
    detail:
      'RunPod-backed Docker workers run computational tasks, including MD smoke tests, behind approval-first gating. Compute jobs are ledgered with artifact persistence. GPU work is not initiated by public contribution.',
  },
  {
    step: '08',
    label: 'Service boundary',
    detail:
      'An MCP-compatible service surface is planned for future agent and tool consumption. TWOG is designed to be readable by other AI systems, not just human readers.',
  },
] as const;

const boundaries = [
  {
    label: 'Evidence boundary',
    detail: 'Deterministic ingestion, LLMs for synthesis and critique only, never for silent data mutation.',
  },
  {
    label: 'Public boundary',
    detail: 'Public submissions do not mutate candidate state. Contributions enter intake; writes require operator action.',
  },
  {
    label: 'Compute boundary',
    detail: 'GPU jobs are approval-first and ledgered. Public contribution does not trigger compute.',
  },
] as const;

const stack = [
  'Dagster',
  'Neon Postgres',
  'TypeScript / Next.js',
  'Python research bridge',
  'RunPod',
  'Docker workers',
  'MCP service boundary',
] as const;

export const metadata = {
  title: 'Architecture — TWOG',
  description:
    'The technical architecture behind TWOG: deterministic ingestion, Dagster materialized assets, operator-gated writes, public candidate records, and approval-first compute.',
};

export default function ArchitecturePage() {
  return (
    <div className="site-shell page-shell architecture-page">
      <section className="page-hero architecture-hero">
        <div>
          <p className="section-kicker">System architecture</p>
          <h1>Architecture</h1>
          <p>
            TWOG runs as a Dagster asset graph over typed research contracts, with
            deterministic ingestion, versioned methods, and operator-gated writes.
          </p>
        </div>
        <aside className="method-status-card architecture-status-card">
          <span className="lab-label">Operating posture</span>
          <strong>LLMs argue and synthesize. Operator approval is the write gate.</strong>
        </aside>
      </section>

      <section className="method-protocol architecture-protocol">
        <article className="method-thesis">
          <p className="section-kicker">System shape</p>
          <h2>Evidence enters through deterministic rails. Claims leave through public records.</h2>
          <p>
            The system separates source ingestion, agent synthesis, public publication,
            operator triage, and compute dispatch. That separation is the point: each
            lane has its own provenance, lineage, and gate.
          </p>
        </article>

        <div className="method-flow-list architecture-flow-list" aria-label="TWOG architecture layers">
          {architectureSections.map((item) => (
            <article key={item.step}>
              <span>{item.step}</span>
              <h3>{item.label}</h3>
              <p>{item.detail}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="method-audit-section architecture-boundaries-section">
        <div className="section-heading layered-heading" data-layer="GATES">
          <p className="section-kicker">Boundaries</p>
          <h2>Three deliberate gates shape what the system will and will not do.</h2>
        </div>

        <div className="architecture-boundary-grid">
          {boundaries.map((boundary) => (
            <article key={boundary.label}>
              <h3>{boundary.label}</h3>
              <p>{boundary.detail}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="record-panel method-limits architecture-stack">
        <p className="section-kicker">Stack</p>
        <h2>Current rails</h2>
        <div className="architecture-stack-list" aria-label="TWOG technical stack">
          {stack.map((item) => (
            <span key={item}>{item}</span>
          ))}
        </div>
      </section>
    </div>
  );
}
