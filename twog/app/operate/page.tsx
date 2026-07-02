import Link from 'next/link';
import { connection } from 'next/server';
import '../v4/v4.css';
import '../v2/v2.css';
import '../detail.css';
import { isOperator, isOperatorConfigured } from '@/lib/operator-gate';
import { listContributions, actionForRoute, PENDING_INTAKE_STATUSES } from '@/lib/contribution-triage';
import { prettyCandidate } from '@/lib/verdict';
import { OperatorLogin, OperatorLogout } from './_components/OperatorLogin';
import { TriageActions } from './_components/TriageActions';

export const metadata = {
  title: 'Operator — TWOG',
  robots: { index: false, follow: false },
};

const STATUS_TONE: Record<string, string> = {
  queued_for_intake: 'run',
  triage_in_progress: 'run',
  needs_more_information: 'ko',
};

export default async function OperatePage() {
  await connection();

  if (!(await isOperator())) {
    return (
      <div className="v4-shell">
        <div className="v4-grain" />
        <div className="v4-detail">
          <p className="v4-kick">Operator</p>
          <h1 className="v4-dh1">Triage.</h1>
          <p className="v4-lead">
            Operator access only. Enter the passphrase to review the contribution queue. This is a temporary
            gate — real sign-in (WorkOS) comes with the collaborator console.
          </p>
          <div style={{ marginTop: '1.6rem' }}>
            <OperatorLogin configured={isOperatorConfigured()} />
          </div>
        </div>
      </div>
    );
  }

  const { rows, summary } = await listContributions({ statuses: [...PENDING_INTAKE_STATUSES], limit: 200 });

  return (
    <div className="v4-shell">
      <div className="v4-grain" />
      <div className="v4-detail v4-detail--wide">
        <nav className="v4-dnav">
          <Link href="/" className="v4-dnav__home">twog</Link>
          <Link href="/candidates">Candidates</Link>
          <Link href="/evidence">Evidence</Link>
          <span className="v4-dnav__spacer" />
          <OperatorLogout />
        </nav>

        <p className="v4-kick">Operator · contribution triage</p>
        <h1 className="v4-dh1">The queue.</h1>

        <div className="v4-rollup">
          <div><div className="v4-rollup__n v4-rollup__n--run">{summary.queued_for_intake}</div><div className="v4-rollup__l">queued</div></div>
          <div><div className="v4-rollup__n v4-rollup__n--run">{summary.triage_in_progress}</div><div className="v4-rollup__l">in triage</div></div>
          <div><div className="v4-rollup__n v4-rollup__n--ko">{summary.needs_more_information}</div><div className="v4-rollup__l">needs info</div></div>
          <div><div className="v4-rollup__n v4-rollup__n--ok">{summary.actionable_count}</div><div className="v4-rollup__l">actionable</div></div>
        </div>

        <section className="v4-sec2">
          <div className="v4-trust">
            <strong>Triage routes, it doesn’t execute.</strong>
            <p>
              Accepting a contribution sets its status + queue tag for downstream work — it never changes a
              public record on its own. Nothing here is published automatically.
            </p>
          </div>
        </section>

        {rows.length ? (
          <div className="v4-rows">
            {rows.map((c) => {
              const tone = STATUS_TONE[c.status] ?? 'run';
              const stripe = tone === 'ko' ? 'var(--kill)' : tone === 'ok' ? 'var(--bone)' : 'var(--line-2)';
              return (
                <div key={c.contribution_id} className="v4-rrow v4-triage" style={{ ['--spine' as string]: stripe } as React.CSSProperties}>
                  <div className="v4-rrow__top">
                    <span className="v4-rrow__name">{c.title ?? 'Contribution'}</span>
                    <span className={`v4-badge v4-badge--${tone}`}>{c.status.replace(/_/g, ' ')}</span>
                  </div>

                  <div className="v4-triage__meta">
                    {c.candidate_id ? (
                      <Link href={`/candidates/${c.candidate_id}`}>{prettyCandidate(c.candidate_id)} →</Link>
                    ) : <span>—</span>}
                    <span>·</span>
                    <span>{c.contribution_type ?? '—'}</span>
                    <span>·</span>
                    <span>{c.relation_to_current_record ?? '—'}</span>
                    <span>·</span>
                    <span>wants: {(c.requested_system_action ?? '—').replace(/_/g, ' ')}</span>
                    <span>·</span>
                    <span>{c.contact ?? 'no contact'}</span>
                  </div>

                  <div className="v4-triage__rec">
                    <span className="v4-chip">suggested: {c.recommended_route.replace(/_/g, ' ')}</span>
                    <span className="v4-triage__reason">{c.route_reason}</span>
                  </div>

                  {(c.summary || c.claim || c.evidence.length) ? (
                    <details className="v4-triage__more">
                      <summary>the packet</summary>
                      {c.claim ? <p className="v4-triage__p"><strong>Claim.</strong> {c.claim}</p> : null}
                      {c.summary ? <p className="v4-triage__p"><strong>Summary.</strong> {c.summary}</p> : null}
                      {c.conflicts ? <p className="v4-triage__p"><strong>Conflicts.</strong> {c.conflicts}</p> : null}
                      {c.evidence.length ? (
                        <ul className="v4-limits">
                          {c.evidence.map((e, i) => (
                            <li key={i}>
                              {e.url ? <a href={e.url} target="_blank" rel="noopener noreferrer">{e.title || e.url}</a> : e.title || 'evidence'}
                              {e.notes ? ` — ${e.notes}` : ''}
                            </li>
                          ))}
                        </ul>
                      ) : null}
                      {c.review_notes ? <pre className="v4-triage__notes">{c.review_notes}</pre> : null}
                    </details>
                  ) : null}

                  <TriageActions contributionId={c.contribution_id} defaultAction={actionForRoute(c.recommended_route)} />
                </div>
              );
            })}
          </div>
        ) : (
          <p className="v4-lead v4-muted" style={{ marginTop: '2rem' }}>
            Nothing in the queue. Contributions appear here as they’re submitted.
          </p>
        )}
      </div>
    </div>
  );
}
