import Link from 'next/link';
import { notFound } from 'next/navigation';
import '../../v4/v4.css';
import '../../v2/v2.css';
import '../../detail.css';
import { getRun } from '@/lib/public-runs';
import { STATUS_META, prettyCandidate } from '@/lib/verdict';

export default async function RunDetail({ params }: { params: Promise<{ manifestId: string }> }) {
  const { manifestId } = await params;
  const run = await getRun(manifestId);
  if (!run) notFound();
  const s = run.rollup.leading_hypothesis_status;

  return (
    <div className="v4-shell">
      <div className="v4-grain" />
      <div className="v4-detail v4-detail--wide">
        <nav className="v4-dnav">
          <Link href="/runs" className="v4-dnav__home">Runs</Link>
          <Link href="/evidence">Evidence</Link>
          <span className="v4-dnav__spacer" />
          <Link href="/involved">Get involved</Link>
        </nav>

        <p className="v4-kick">Run</p>
        <h1 className="v4-dh1">{run.title}</h1>
        <div className="v4-card__meta" style={{ marginTop: '0.8rem' }}>{run.ran_at} · {run.runner_kind}</div>

        <div className="v4-rollup">
          <div>
            <div className="v4-rollup__n v4-rollup__n--ko">{s.refuted ?? 0}</div>
            <div className="v4-rollup__l">ruled out</div>
          </div>
          <div>
            <div className="v4-rollup__n v4-rollup__n--ok">{s.standing ?? 0}</div>
            <div className="v4-rollup__l">still standing</div>
          </div>
          <div>
            <div className="v4-rollup__n v4-rollup__n--run">{s.underpowered ?? 0}</div>
            <div className="v4-rollup__l">needs more</div>
          </div>
        </div>

        <section className="v4-sec2">
          <div className="v4-trust">
            <strong>Nothing was promoted.</strong>
            <p>
              A run only produces evidence and verdicts — it never advances a candidate on its own. A
              human operator holds the terminal accept/promote decision.
            </p>
          </div>
        </section>

        <section className="v4-sec2">
          <div className="v4-dh2">
            What we tried to kill — {run.rollup.candidates_processed}/{run.rollup.candidates_selected} run
          </div>
          <div className="v4-rows">
            {run.rows.map((row) => {
              const meta = STATUS_META[row.leading_hypothesis_status] ?? { label: row.leading_hypothesis_status, tone: 'run' as const };
              const stripe = meta.tone === 'ko' ? 'var(--kill)' : meta.tone === 'ok' ? 'var(--bone)' : 'var(--line-2)';
              const cap = row.capsule_ids[0];
              return (
                <div key={row.candidate_id} className="v4-rrow" style={{ ['--spine' as string]: stripe } as React.CSSProperties}>
                  <div className="v4-rrow__top">
                    <Link href={`/candidates/${row.candidate_id}`} className="v4-rrow__name">{prettyCandidate(row.candidate_id)}</Link>
                    <span className={`v4-badge v4-badge--${meta.tone}`}>{meta.label}</span>
                  </div>
                  <div className="v4-rrow__meta" style={{ marginTop: '0.4rem' }}>
                    {cap ? (
                      <Link href={`/evidence/${cap}`}>open the evidence →</Link>
                    ) : (
                      <span>terminal: {row.terminal_reason.replace(/_/g, ' ')} — no capsule to show</span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </section>
      </div>
    </div>
  );
}
