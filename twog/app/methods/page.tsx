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
          TWOG separates the public record from the method used to generate it. When the
          system changes, new records can point to a new method version without erasing
          what came before.
        </p>
      </section>

      <section className="record-list">
        {methods.map((method) => (
          <Link href={`/methods/${method.methodId}`} className="candidate-row" key={method.methodId}>
            <div>
              <p className="row-kicker">{method.version}</p>
              <h2>{method.title}</h2>
              <p>{method.summary}</p>
            </div>
            <div className="row-meta">
              <span>{method.sections.length} sections</span>
              <span>public proof layer</span>
            </div>
          </Link>
        ))}
      </section>
    </div>
  );
}
