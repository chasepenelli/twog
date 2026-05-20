const architectureNodes = [
  {
    step: '01',
    label: 'Ingest',
    mode: 'deterministic',
    detail: 'Source APIs and research signal enter without LLM mutation.',
  },
  {
    step: '02',
    label: 'Normalize',
    mode: 'schema-validated',
    detail: 'Records, chunks, citations, entities, provenance, and lineage.',
  },
  {
    step: '03',
    label: 'Materialize',
    mode: 'Dagster assets',
    detail: 'Ingestion, synthesis, validation, health, embeddings, and compute lanes.',
  },
  {
    step: '04',
    label: 'Store',
    mode: 'Neon + SQLite',
    detail: 'Hosted runtime state plus local reproducibility from the same inputs.',
  },
  {
    step: '05',
    label: 'Argue',
    mode: 'recommend-only LLMs',
    detail: 'Agents critique citations, confounders, gaps, and weak claims.',
  },
  {
    step: '06',
    label: 'Publish',
    mode: 'public records',
    detail: 'Versioned methods, content hashes, JSON payloads, and decisions.',
  },
  {
    step: '07',
    label: 'Triage',
    mode: 'operator gate',
    detail: 'Preview routes first. Writes require approval and dry_run=false.',
  },
  {
    step: '08',
    label: 'Compute',
    mode: 'approval-first',
    detail: 'RunPod/Docker jobs are ledgered, artifact-backed, and gated.',
  },
] as const;

const sourceTags = [
  'PubMed',
  'Europe PMC',
  'PMC OA',
  'OpenAlex',
  'Crossref',
  'ClinicalTrials.gov',
  'PubChem',
  'ChEMBL',
  'UniProt',
  'RCSB PDB',
  'OpenFDA animal events',
  'research social signal',
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

const serviceBoundary =
  'MCP-compatible service boundary for future agents, tools, and external review systems.';

function ArchitectureNode({ node }: { node: (typeof architectureNodes)[number] }) {
  return (
    <article className="architecture-node">
      <code>{node.step}</code>
      <div>
        <h3>{node.label}</h3>
        <span>{node.mode}</span>
      </div>
      <p>{node.detail}</p>
    </article>
  );
}

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
            TWOG is a research engine with typed lanes, versioned methods, public
            records, and operator-gated writes.
          </p>
        </div>
        <aside className="method-status-card architecture-status-card">
          <span className="lab-label">Operating posture</span>
          <strong>LLMs argue and synthesize. Operator approval is the write gate.</strong>
        </aside>
      </section>

      <section className="architecture-map-section">
        <div className="section-heading layered-heading" data-layer="SYSTEM MAP">
          <p className="section-kicker">System shape</p>
          <h2>Evidence enters through deterministic rails. Claims leave through public records.</h2>
        </div>

        <div className="architecture-map-shell" aria-label="TWOG architecture system map">
          <div className="architecture-node-grid">
            {architectureNodes.slice(0, 4).map((node) => (
              <ArchitectureNode node={node} key={node.step} />
            ))}
          </div>

          <div className="architecture-core" aria-label="TWOG architecture core">
            <span>TWOG core loop</span>
            <strong>Evidence</strong>
            <em>Review</em>
            <strong>Record</strong>
            <p>Provenance and lineage stay attached.</p>
          </div>

          <div className="architecture-node-grid">
            {architectureNodes.slice(4).map((node) => (
              <ArchitectureNode node={node} key={node.step} />
            ))}
          </div>
        </div>

        <div className="architecture-source-band" aria-label="TWOG source rails">
          {sourceTags.map((source) => (
            <span key={source}>{source}</span>
          ))}
        </div>

        <div className="architecture-service-band">
          <span>Service boundary</span>
          <p>{serviceBoundary}</p>
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
