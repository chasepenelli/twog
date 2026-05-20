import { notFound } from 'next/navigation';
import Link from 'next/link';
import { getMethod, methods } from '@/lib/methods';

const recordFlow = [
  {
    step: '01',
    label: 'Internal evidence',
    detail:
      'The source material starts as TWOG research artifacts: briefs, therapy ideas, validation packets, citation refs, decision events, and reproducibility metadata.',
  },
  {
    step: '02',
    label: 'Public snapshot',
    detail:
      'The exporter creates a bounded public payload. It keeps the hypothesis, status, rationale, evidence refs, risk notes, known blockers, and linked records.',
  },
  {
    step: '03',
    label: 'Citation expansion',
    detail:
      'Internal labels such as C1, C20, or C22 are expanded into a reference dossier with title, DOI, PMID, PMCID, source, evidence kind, supported claim, and provenance counts.',
  },
  {
    step: '04',
    label: 'Decision trail',
    detail:
      'Status changes and snapshot events are preserved as dated decision entries. A candidate should never silently move from proposed to investigating to advanced.',
  },
  {
    step: '05',
    label: 'Hash and publish',
    detail:
      'The public snapshot receives a content hash. The page can be cited, compared, challenged, and regenerated without pretending the record is more final than it is.',
  },
];

const auditFields = [
  ['What is being proposed', 'The candidate ID, status, target family, therapy family, and current priority score.'],
  ['What evidence supports it', 'Short citation labels expanded into readable source compartments and supported claims.'],
  ['What is still weak', 'Known limitations, missing data, citation gaps, and reasons the record has not advanced.'],
  ['Why the status changed', 'Timestamped rationale for snapshot generation, evidence updates, status changes, and reviewer actions.'],
  ['How this version was made', 'Pipeline version, source brief/evaluation IDs, committee run IDs, content hash, and method reference.'],
];

const payloadAccess = [
  {
    label: 'Checkout / one candidate',
    path: '/api/public-candidates/{candidate_id}',
    detail:
      'Returns one public candidate record: metadata, latest snapshot, rationale, literature, decision events, and reproducibility fields.',
  },
  {
    label: 'Browse / all candidates',
    path: '/api/public-candidates',
    detail:
      'Returns the exported public candidate dataset so readers can find available records and compare snapshot metadata.',
  },
  {
    label: 'Example checkout',
    path: '/api/public-candidates/twog-candidate-447eb8089965',
    detail:
      'The current public example used by the site. Display IDs such as TWOG-15F50D resolve to the same record page; the API uses the stable candidate ID.',
  },
  {
    label: 'Evidence bundle',
    path: '/api/public-candidates/twog-candidate-447eb8089965/evidence-bundle',
    detail:
      'Returns the actionable checkout packet: source-document dossier, chunk manifest, artifact manifest, compute/MD reproducibility contract, and check-in endpoints.',
  },
  {
    label: 'Contribution template',
    path: '/api/public-candidates/twog-candidate-447eb8089965/contribution-template',
    detail:
      'Returns a fillable contribution packet for evidence, critique, replication notes, artifacts, or validation proposals tied to this snapshot.',
  },
  {
    label: 'Check-in endpoint',
    path: '/api/public-candidates/twog-candidate-447eb8089965/contributions',
    detail:
      'Accepts a completed contribution packet and queues it for TWOG intake when Neon-backed storage is configured.',
  },
];

const exchangeSteps = [
  {
    label: 'Check out',
    detail:
      'A reader opens the candidate payload for the exact snapshot, then the evidence bundle for source refs, chunk provenance, artifact manifests, and compute settings.',
  },
  {
    label: 'Do outside work',
    detail:
      'They can replicate a claim, add missing evidence, challenge a citation, attach an artifact, or rerun a docking/MD method against the same snapshot hash.',
  },
  {
    label: 'Check in',
    detail:
      'They submit a structured contribution packet through the check-in endpoint or page form. The packet receives a durable intake ID.',
  },
  {
    label: 'Queue with gates',
    detail:
      'TWOG operators and agents triage the packet before anything changes: provenance review, citation dedupe, evidence review, validation planning, or compute review.',
  },
];

const triageStages = [
  {
    label: 'Intake',
    detail: 'The contribution lands in Neon as queued_for_intake with contributor, route request, evidence, artifacts, and the source snapshot hash.',
  },
  {
    label: 'Operator triage',
    detail: 'The Command Center previews or applies an explicit decision: request more information, reject, archive, or accept into a review lane.',
  },
  {
    label: 'Specialist lane',
    detail: 'Accepted contributions become evidence-review, validation-queue, or compute-review work. They still do not overwrite the public record.',
  },
  {
    label: 'Record update',
    detail: 'Only after review clears does TWOG create a new candidate decision entry, updated payload, and fresh content hash.',
  },
];

const interpretationRules = [
  'A record is an inspectable research artifact, not a clinical recommendation.',
  'Evidence labels are local to the source brief; the reference dossier is the public decoding layer.',
  'A candidate can be promising and still not validation-ready.',
  'Human analog evidence is treated as context unless canine-specific evidence closes the bridge.',
  'A content hash identifies the public snapshot, not permanent scientific truth.',
];

export function generateStaticParams() {
  return methods.map((method) => ({
    methodId: method.methodId,
  }));
}

export async function generateMetadata({ params }: { params: Promise<{ methodId: string }> }) {
  const { methodId } = await params;
  const method = getMethod(methodId);
  return {
    title: method ? `${method.title} — TWOG Methods` : 'TWOG Method',
    description: method?.summary ?? 'TWOG public method record.',
  };
}

export default async function MethodDetailPage({ params }: { params: Promise<{ methodId: string }> }) {
  const { methodId } = await params;
  const method = getMethod(methodId);
  if (!method) notFound();

  return (
    <div className="site-shell page-shell">
      <section className="page-hero method-hero">
        <div>
          <p className="section-kicker">{method.version} / public record method</p>
          <h1>{method.title}</h1>
          <p>{method.summary}</p>
          <div className="method-actions">
            <Link href="/candidates/twog-15f50d" className="artifact-button primary">
              View example record
            </Link>
            <Link href="/candidates" className="artifact-button">
              Candidate index
            </Link>
          </div>
        </div>
        <aside className="method-status-card">
          <span className="lab-label">Method scope</span>
          <strong>Public proof layer</strong>
          <dl>
            <div>
              <dt>Applies to</dt>
              <dd>candidate pages</dd>
            </div>
            <div>
              <dt>Claims level</dt>
              <dd>research artifact</dd>
            </div>
            <div>
              <dt>Medical use</dt>
              <dd>not allowed</dd>
            </div>
          </dl>
        </aside>
      </section>

      <section className="method-protocol">
        <article className="method-thesis">
          <p className="section-kicker">What this governs</p>
          <h2>A candidate page is a frozen public argument, not a polished claim.</h2>
          <p>
            Candidate Record v1 explains how TWOG turns internal research state into a
            public page that someone else can inspect. The method is intentionally
            conservative: it preserves uncertainty, separates source evidence from
            interpretation, and keeps every status change attached to a rationale.
          </p>
          <p className="operator-gate-line">LLMs argue and synthesize. Operator approval is the write gate.</p>
        </article>

        <div className="method-flow-list" aria-label="Candidate record generation flow">
          {recordFlow.map((item) => (
            <article key={item.step}>
              <span>{item.step}</span>
              <h3>{item.label}</h3>
              <p>{item.detail}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="method-body method-notes">
        {method.sections.map((section) => (
          <article className="record-panel" key={section.heading}>
            <p className="section-kicker">Method note</p>
            <h2>{section.heading}</h2>
            <p>{section.body}</p>
          </article>
        ))}
      </section>

      <section className="method-audit-section">
        <div className="section-heading layered-heading" data-layer="INSPECT">
          <p className="section-kicker">Reader inspection</p>
          <h2>What a reader should be able to verify.</h2>
          <p>
            This is the minimum public surface for a candidate record. The page should
            let a reader understand the idea, inspect the evidence, see the unresolved
            gaps, and follow the reason the system changed its mind.
          </p>
        </div>

        <div className="method-audit-grid">
          {auditFields.map(([field, description]) => (
            <article key={field}>
              <h3>{field}</h3>
              <p>{description}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="method-payload-section">
        <div className="section-heading layered-heading" data-layer="EXCHANGE">
          <p className="section-kicker">Public payload access</p>
          <h2>Check out the record. Check in better evidence.</h2>
          <p>
            The page is the readable version. The public payload is the machine-readable
            version. The long-term pattern is an evidence exchange: readers can inspect
            the snapshot, do outside work, and submit structured contributions into a
            gated intake queue.
          </p>
        </div>

        <div className="payload-exchange-board">
          <article className="payload-exchange-primary">
            <span>Live checkout</span>
            <h3>Public candidate payload</h3>
            <p>
              The live endpoint returns the exact public snapshot used by the page:
              candidate metadata, content hash, rationale, expanded references,
              decision log, and reproducibility fields.
            </p>
            <code>/api/public-candidates/twog-candidate-447eb8089965</code>
            <div className="method-actions">
              <a href="/api/public-candidates/twog-candidate-447eb8089965" className="record-link" target="_blank" rel="noreferrer">
                Open example JSON
              </a>
              <a
                href="/api/public-candidates/twog-candidate-447eb8089965/evidence-bundle"
                className="record-link"
                target="_blank"
                rel="noreferrer"
              >
                Open evidence bundle
              </a>
              <a
                href="/api/public-candidates/twog-candidate-447eb8089965/contribution-template"
                className="record-link"
                target="_blank"
                rel="noreferrer"
              >
                Open check-in template
              </a>
              <Link
                href="/api/public-candidates/twog-candidate-447eb8089965/contributions"
                className="record-link"
                prefetch={false}
                target="_blank"
                rel="noreferrer"
              >
                Inspect check-in API
              </Link>
            </div>
          </article>

          <div className="payload-exchange-steps" aria-label="Public evidence exchange flow">
            {exchangeSteps.map((step, index) => (
              <article key={step.label}>
                <span>{String(index + 1).padStart(2, '0')}</span>
                <h3>{step.label}</h3>
                <p>{step.detail}</p>
              </article>
            ))}
          </div>
        </div>

        <div className="payload-endpoint-grid">
          {payloadAccess.map((item) => {
            const isConcrete = !item.path.includes('{candidate_id}');
            return (
              <article key={item.label}>
                <span>{item.label}</span>
                <code>{item.path}</code>
                <p>{item.detail}</p>
                {isConcrete ? <Link href={item.path} prefetch={false}>Open route</Link> : null}
              </article>
            );
          })}
        </div>

        <article className="payload-explainer">
          <h3>Why the bundle is more than a citation list</h3>
          <p>
            The evidence bundle is the actionable checkout layer. It keeps the source
            dossier, chunk IDs, research object IDs, artifact manifest, snapshot hash,
            and compute/MD reproducibility contract together so a reviewer can work
            against the same record TWOG published.
          </p>
          <p>
            The public site should not let outside submissions directly change a
            candidate or dispatch validation jobs. A checked-in contribution should
            become a queue item first: provenance review, citation repair, duplication
            checks, safety boundaries, and then promotion into the internal validation
            lane if it clears review.
          </p>
        </article>

        <div className="payload-endpoint-grid triage-stage-grid">
          {triageStages.map((stage) => (
            <article key={stage.label}>
              <span>After check-in</span>
              <h3>{stage.label}</h3>
              <p>{stage.detail}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="method-interpretation">
        <article>
          <p className="section-kicker">Reading rules</p>
          <h2>How to interpret a TWOG candidate record.</h2>
        </article>
        <ol>
          {interpretationRules.map((rule) => (
            <li key={rule}>{rule}</li>
          ))}
        </ol>
      </section>

      <section className="record-panel method-limits">
        <p className="section-kicker">Boundary condition</p>
        <h2>What this method does not certify</h2>
        <p>
          Candidate Record v1 does not certify efficacy, safety, dosing, clinical
          readiness, regulatory fitness, or veterinary use. It only makes the research
          reasoning chain easier to inspect: source refs, rationale, risks, decisions,
          reproducibility metadata, and the current state of confidence.
        </p>
      </section>
    </div>
  );
}
