// The deep tier (email-gated): the pre-registered kill-criterion (locked + hashed before the run), the
// measured metrics, and the limitations — the real "full experiment plan" from the capsule's Neon payload.

import { Fragment } from 'react';
import type { Capsule } from '@/lib/types/public-detail';

function fmt(v: unknown): string {
  if (v == null) return '—';
  if (typeof v === 'number') return String(v);
  if (typeof v === 'string') return v;
  if (Array.isArray(v)) return v.map(fmt).join(', ');
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}
const label = (k: string) => k.replace(/_/g, ' ');

export function MoonshotRubricView({ capsule }: { capsule: Capsule }) {
  const prereg = (capsule.preregistration ?? {}) as Record<string, unknown>;
  const metrics = (capsule.metrics ?? {}) as Record<string, unknown>;
  const preregKeys = Object.keys(prereg);
  const metricKeys = Object.keys(metrics);

  return (
    <div className="v4-plan">
      <div className="v4-dh2" style={{ marginBottom: 0 }}>Pre-registered kill-criterion</div>
      <p className="v4-note">
        Written down and hashed <em>before</em> any compute ran — so the goalposts can’t move. This is
        the exact bar the result had to clear (or fail).
      </p>
      {preregKeys.length ? (
        <dl className="v4-kv">
          {preregKeys.map((k) => (
            <Fragment key={k}>
              <dt>{label(k)}</dt>
              <dd className="v4-mono">{fmt(prereg[k])}</dd>
            </Fragment>
          ))}
        </dl>
      ) : (
        <p className="v4-note">No pre-registration payload recorded for this capsule.</p>
      )}

      {metricKeys.length ? (
        <>
          <div className="v4-dh2" style={{ marginTop: '1.5rem', marginBottom: 0 }}>What the run measured</div>
          <dl className="v4-kv">
            {metricKeys.map((k) => (
              <Fragment key={k}>
                <dt>{label(k)}</dt>
                <dd className="v4-mono">{fmt(metrics[k])}</dd>
              </Fragment>
            ))}
          </dl>
        </>
      ) : null}

      {capsule.limitations.length ? (
        <>
          <div className="v4-dh2" style={{ marginTop: '1.5rem', marginBottom: 0 }}>Limitations (stated up front)</div>
          <ul className="v4-limits">
            {capsule.limitations.map((l, i) => (
              <li key={i}>{l}</li>
            ))}
          </ul>
        </>
      ) : null}
    </div>
  );
}
