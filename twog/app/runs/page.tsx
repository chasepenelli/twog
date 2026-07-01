import Link from 'next/link';
import { connection } from 'next/server';
import '../v4/v4.css';
import '../v2/v2.css';
import '../detail.css';
import { listRuns } from '@/lib/public-runs';

export const metadata = {
  title: 'Runs — TWOG',
  description: 'Every falsification campaign the engine has run — what it tried to kill.',
};

export default async function RunsPage() {
  await connection(); // Next 16: read live per request (pg reads aren't auto-detected as dynamic)
  const runs = await listRuns(50);
  return (
    <div className="v4-shell">
      <div className="v4-grain" />
      <div className="v4-detail">
        <nav className="v4-dnav">
          <Link href="/" className="v4-dnav__home">twog</Link>
          <Link href="/evidence">Evidence</Link>
          <span className="v4-dnav__spacer" />
          <Link href="/involved">Get involved</Link>
        </nav>

        <p className="v4-kick">Runs</p>
        <h1 className="v4-dh1">What we tried to kill.</h1>
        <p className="v4-lead">
          A run pits a batch of hypotheses against their cheapest pre-registered kill-test. Some survive,
          some are ruled out, some run out of road. <strong>None are promoted automatically</strong> — a
          human holds the gate.
        </p>

        {runs.length ? (
          <div className="v4-cards">
            {runs.map((r) => {
              const s = r.rollup.leading_hypothesis_status;
              const stripe = (s.refuted ?? 0) > 0 ? 'var(--kill)' : (s.standing ?? 0) > 0 ? 'var(--bone)' : 'var(--line-2)';
              return (
                <Link
                  key={r.manifest_id}
                  href={`/runs/${r.manifest_id}`}
                  className="v4-card v4-card--stripe"
                  style={{ ['--spine' as string]: stripe } as React.CSSProperties}
                >
                  <div className="v4-card__row">
                    <span className="v4-card__title">{r.title}</span>
                    <span className="v4-card__meta">{r.ran_at} · {r.runner_kind}</span>
                  </div>
                  <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap', marginTop: '0.8rem' }}>
                    {s.refuted ? <span className="v4-badge v4-badge--ko">{s.refuted} ruled out</span> : null}
                    {s.standing ? <span className="v4-badge v4-badge--ok">{s.standing} still standing</span> : null}
                    {s.underpowered ? <span className="v4-badge v4-badge--run">{s.underpowered} needs more</span> : null}
                    <span className="v4-chip">✓ nothing auto-promoted</span>
                  </div>
                </Link>
              );
            })}
          </div>
        ) : (
          <p className="v4-lead v4-muted" style={{ marginTop: '2rem' }}>
            No runs to show yet — campaigns appear here as the engine runs them.
          </p>
        )}
      </div>
    </div>
  );
}
