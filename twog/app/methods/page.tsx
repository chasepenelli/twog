import Link from 'next/link';
import '../v4/v4.css';
import '../v2/v2.css';
import '../detail.css';
import { SiteNav } from '@/components/v4/SiteNav';
import { methods } from '@/lib/methods';

export const metadata = {
  title: 'Methods — TWOG',
  description: 'Versioned TWOG public research record methods.',
};

export default function MethodsPage() {
  return (
    <div className="v4-shell">
      <div className="v4-grain" />
      <div className="v4-detail v4-detail--wide">
        <SiteNav />

        <p className="v4-kick">Versioned methodology</p>
        <h1 className="v4-dh1">The rulebook.</h1>
        <p className="v4-lead">
          TWOG separates each public record from the method used to create it — the public rulebook for
          candidate pages, evidence bundles, contribution intake, compute smoke tests, citation repair, and
          omics readouts.
        </p>

        <div className="v4-rollup">
          <div><div className="v4-rollup__n v4-rollup__n--ok">{methods.length}</div><div className="v4-rollup__l">versioned methods</div></div>
          <div><div className="v4-rollup__n v4-rollup__n--run">3</div><div className="v4-rollup__l">hard boundaries</div></div>
          <div><div className="v4-rollup__n v4-rollup__n--ko">0</div><div className="v4-rollup__l">medical claims certified</div></div>
        </div>

        <section className="v4-sec2">
          <div className="v4-cards">
            {methods.map((method) => (
              <Link href={`/methods/${method.methodId}`} className="v4-card v4-card--stripe" key={method.methodId}>
                <div className="v4-card__row">
                  <span className="v4-card__title">{method.title}</span>
                  <span className="v4-card__meta">{method.version} · {method.category}</span>
                </div>
                <p className="v4-card__meta" style={{ marginTop: '0.5rem', textTransform: 'none', letterSpacing: 0 }}>{method.summary}</p>
                <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap', marginTop: '0.7rem' }}>
                  <span className="v4-chip">{method.status}</span>
                  <span className="v4-chip">{method.claimsLevel}</span>
                  <span className="v4-chip">{method.sections.length} sections</span>
                  <span className="v4-chip">{method.appliesTo}</span>
                </div>
              </Link>
            ))}
          </div>
        </section>
      </div>
    </div>
  );
}
