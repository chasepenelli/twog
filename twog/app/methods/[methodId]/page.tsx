import { notFound } from 'next/navigation';
import Link from 'next/link';
import '../../v4/v4.css';
import '../../v2/v2.css';
import '../../detail.css';
import { SiteNav } from '@/components/v4/SiteNav';
import { getMethod, methods } from '@/lib/methods';

export function generateStaticParams() {
  return methods.map((method) => ({ methodId: method.methodId }));
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
    <div className="v4-shell">
      <div className="v4-grain" />
      <div className="v4-detail">
        <SiteNav back={{ href: '/methods', label: 'Methods' }} />

        <p className="v4-kick">{method.version} · {method.category}</p>
        <h1 className="v4-dh1">{method.title}</h1>
        <p className="v4-lead">{method.summary}</p>
        <div className="v4-prov__badges">
          <span className="v4-chip">{method.status}</span>
          <span className="v4-chip">applies to · {method.appliesTo}</span>
          <span className="v4-chip">claims · {method.claimsLevel}</span>
        </div>

        <section className="v4-sec2">
          <div className="v4-dh2">What this governs</div>
          <p className="v4-lead">{method.heroStatement}</p>
          {method.operatorLine ? <p className="v4-note">{method.operatorLine}</p> : null}
          <div className="v4-rows" style={{ marginTop: '1rem' }}>
            {method.flow.map((item, i) => (
              <div key={item.label} className="v4-rrow">
                <div className="v4-rrow__top">
                  <span className="v4-rrow__name">{String(i + 1).padStart(2, '0')} · {item.label}</span>
                </div>
                <p className="v4-rrow__meta" style={{ marginTop: '0.35rem', textTransform: 'none', letterSpacing: 0 }}>{item.detail}</p>
              </div>
            ))}
          </div>
        </section>

        {method.sections.map((section) => (
          <section className="v4-sec2" key={section.heading}>
            <div className="v4-dh2">{section.heading}</div>
            <p className="v4-lead">{section.body}</p>
          </section>
        ))}

        <section className="v4-sec2">
          <div className="v4-dh2">What a reader can verify</div>
          <dl className="v4-kv">
            {method.auditFields.map(([field, description]) => (
              <div key={field} style={{ display: 'contents' }}>
                <dt>{field}</dt>
                <dd>{description}</dd>
              </div>
            ))}
          </dl>
        </section>

        {method.endpoints?.length ? (
          <section className="v4-sec2">
            <div className="v4-dh2">Where it's inspectable — public routes</div>
            <div className="v4-cards">
              {method.endpoints.map((item) => (
                <div key={item.label} className="v4-card">
                  <div className="v4-card__row">
                    <span className="v4-card__title" style={{ fontSize: '0.95rem' }}>{item.label}</span>
                    {item.href ? <Link href={item.href} prefetch={false} className="v4-chip">open route →</Link> : null}
                  </div>
                  <code className="v4-mono" style={{ display: 'block', marginTop: '0.4rem' }}>{item.path}</code>
                  <p className="v4-card__meta" style={{ marginTop: '0.4rem', textTransform: 'none', letterSpacing: 0 }}>{item.detail}</p>
                </div>
              ))}
            </div>
          </section>
        ) : null}

        <section className="v4-sec2">
          <div className="v4-dh2">How to interpret this method</div>
          <ul className="v4-limits">
            {method.interpretationRules.map((rule) => <li key={rule}>{rule}</li>)}
          </ul>
        </section>

        <section className="v4-sec2">
          <div className="v4-trust">
            <strong>What this method does not certify</strong>
            <p>{method.boundary}</p>
          </div>
        </section>
      </div>
    </div>
  );
}
