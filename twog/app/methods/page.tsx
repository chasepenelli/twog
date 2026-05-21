import Link from 'next/link';
import { methods } from '@/lib/methods';

export const metadata = {
  title: 'Methods — TWOG',
  description: 'Versioned TWOG public research record methods.',
};

export default function MethodsPage() {
  return (
    <div className="site-shell page-shell">
      <section className="page-hero">
        <p className="section-kicker">Versioned methodology</p>
        <h1>Methods</h1>
        <p>
          TWOG separates each public record from the method used to create it. This is
          the public rulebook for candidate pages, evidence bundles, contribution
          intake, compute smoke tests, citation repair, and omics readouts.
        </p>
      </section>

      <section className="method-catalog-intro" aria-label="Methods catalog summary">
        <article>
          <span>{methods.length}</span>
          <p>versioned public methods</p>
        </article>
        <article>
          <span>3</span>
          <p>hard boundaries: evidence, public write gates, compute</p>
        </article>
        <article>
          <span>0</span>
          <p>medical claims certified by these pages</p>
        </article>
      </section>

      <section className="record-list method-catalog-list">
        {methods.map((method) => (
          <Link href={`/methods/${method.methodId}`} className="candidate-row" key={method.methodId}>
            <div>
              <p className="row-kicker">
                {method.version} / {method.category}
              </p>
              <h2>{method.title}</h2>
              <p>{method.summary}</p>
              <div className="method-row-tags" aria-label={`${method.title} status`}>
                <span>{method.status}</span>
                <span>{method.claimsLevel}</span>
              </div>
            </div>
            <div className="row-meta">
              <span>{method.sections.length} sections</span>
              <span>{method.appliesTo}</span>
            </div>
          </Link>
        ))}
      </section>
    </div>
  );
}
