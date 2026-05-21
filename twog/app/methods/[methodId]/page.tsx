import { notFound } from 'next/navigation';
import Link from 'next/link';
import { getMethod, methods } from '@/lib/methods';

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
          <p className="section-kicker">
            {method.version} / {method.category}
          </p>
          <h1>{method.title}</h1>
          <p>{method.summary}</p>
          <div className="method-actions">
            <Link href="/methods" className="artifact-button primary">
              All methods
            </Link>
            <Link href="/candidates" className="artifact-button">
              Candidate records
            </Link>
          </div>
        </div>
        <aside className="method-status-card">
          <span className="lab-label">Method scope</span>
          <strong>{method.status}</strong>
          <dl>
            <div>
              <dt>Applies to</dt>
              <dd>{method.appliesTo}</dd>
            </div>
            <div>
              <dt>Claims level</dt>
              <dd>{method.claimsLevel}</dd>
            </div>
            <div>
              <dt>Version</dt>
              <dd>{method.version}</dd>
            </div>
          </dl>
        </aside>
      </section>

      <section className="method-protocol">
        <article className="method-thesis">
          <p className="section-kicker">What this governs</p>
          <h2>{method.heroStatement}</h2>
          <p>{method.summary}</p>
          {method.operatorLine ? <p className="operator-gate-line">{method.operatorLine}</p> : null}
        </article>

        <div className="method-flow-list" aria-label={`${method.title} flow`}>
          {method.flow.map((item, index) => (
            <article key={item.label}>
              <span>{String(index + 1).padStart(2, '0')}</span>
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
          <h2>What this method asks a reader to verify.</h2>
          <p>
            Each method exists to make one part of TWOG more inspectable. The fields
            below are the minimum surface a public record should expose before a claim
            is treated as traceable.
          </p>
        </div>

        <div className="method-audit-grid">
          {method.auditFields.map(([field, description]) => (
            <article key={field}>
              <h3>{field}</h3>
              <p>{description}</p>
            </article>
          ))}
        </div>
      </section>

      {method.endpoints?.length ? (
        <section className="method-payload-section">
          <div className="section-heading layered-heading" data-layer="ACCESS">
            <p className="section-kicker">Public routes</p>
            <h2>Where this method is inspectable today.</h2>
            <p>
              These routes are the public interface for this method. Some are concrete
              live examples; template routes show the shape used by stable candidate
              IDs.
            </p>
          </div>

          <div className="payload-endpoint-grid">
            {method.endpoints.map((item) => (
              <article key={item.label}>
                <span>{item.label}</span>
                <code>{item.path}</code>
                <p>{item.detail}</p>
                {item.href ? (
                  <Link href={item.href} prefetch={false}>
                    Open route
                  </Link>
                ) : null}
              </article>
            ))}
          </div>
        </section>
      ) : null}

      <section className="method-interpretation">
        <article>
          <p className="section-kicker">Reading rules</p>
          <h2>How to interpret this method.</h2>
        </article>
        <ol>
          {method.interpretationRules.map((rule) => (
            <li key={rule}>{rule}</li>
          ))}
        </ol>
      </section>

      <section className="record-panel method-limits">
        <p className="section-kicker">Boundary condition</p>
        <h2>What this method does not certify</h2>
        <p>{method.boundary}</p>
      </section>
    </div>
  );
}
