import Link from 'next/link';
import { connection } from 'next/server';
import '../v4/v4.css';
import '../v2/v2.css';
import '../detail.css';
import './orchestration.css';
import { listDecisions, summarize } from '@/lib/research-director';

export const metadata = {
  title: 'The Director — TWOG',
  description: 'A frontier reasoner deciding what the engine should test next — grounded in real evidence, pivoting to novel modalities.',
};

const MODALITY_LABEL: Record<string, string> = {
  stapled_peptide: 'stapled peptide',
  peptide_protac: 'peptide-PROTAC',
  degrader: 'degrader',
  molecular_glue: 'molecular glue',
  base_edit: 'base edit',
  prime_edit: 'prime edit',
  car_t: 'CAR-T',
  mrna_vaccine: 'mRNA vaccine',
  adc: 'antibody-drug conjugate',
  covalent: 'covalent',
  allele_specific: 'allele-specific',
  synthetic_lethality: 'synthetic lethality',
};

export default async function OrchestrationPage() {
  await connection();
  const decisions = await listDecisions();
  const s = summarize(decisions);

  return (
    <div className="v4-shell">
      <div className="v4-grain" />
      <div className="v4-detail v4-detail--wide">
        <nav className="v4-dnav">
          <Link href="/" className="v4-dnav__home">twog</Link>
          <Link href="/candidates">Candidates</Link>
          <Link href="/evidence">Evidence</Link>
          <Link href="/runs">Runs</Link>
          <span className="v4-dnav__spacer" />
          <Link href="/involved">Get involved</Link>
        </nav>

        <p className="v4-kick">The director · what to test next</p>
        <h1 className="v4-dh1">Where the engine points next.</h1>
        <p className="v4-lead">
          A frontier reasoner reads the whole proof ledger and decides the highest-leverage next moves —
          pivoting past conventional repurposing toward <strong>novel modalities</strong> (peptides, degraders,
          gene edits) against the targets the engine has already validated. Every call cites real evidence.
        </p>

        {decisions.length ? (
          <>
            <div className="v4-rollup">
              <div><div className="v4-rollup__n v4-rollup__n--ok">{s.frontier}</div><div className="v4-rollup__l">frontier bets</div></div>
              <div><div className="v4-rollup__n v4-rollup__n--ok">{s.testable_now}</div><div className="v4-rollup__l">testable now</div></div>
              <div><div className="v4-rollup__n v4-rollup__n--ko">{s.deprioritize}</div><div className="v4-rollup__l">deprioritized</div></div>
              <div><div className="v4-rollup__n v4-rollup__n--run">${s.est_next_cost_usd.toFixed(2)}</div><div className="v4-rollup__l">est. next-test cost</div></div>
            </div>

            <section className="v4-sec2">
              <div className="v4-trust">
                <strong>The director recommends — it never acts.</strong>
                <p>
                  Nothing here runs on its own. Every decision cites the real capsules it's built on, and no
                  compute is dispatched without a human and an explicit spend approval. Ideas that can't be
                  tested structurally (gene edits, cell therapy, mRNA) are marked as needing a different kind of
                  evidence — never given a verdict they can't have.
                </p>
              </div>
            </section>

            <section className="v4-sec2">
              <div className="v4-dh2">The queue — ranked by leverage</div>
              <div className="v4-rows">
                {decisions.map((d) => {
                  const propose = d.action_kind !== 'deprioritize';
                  const stripe = propose ? 'var(--bone)' : 'var(--kill)';
                  return (
                    <div key={d.decision_id} className="v4-rrow v4-dir" style={{ ['--spine' as string]: stripe } as React.CSSProperties}>
                      <div className="v4-dir__head">
                        <span className="v4-dir__rank">{d.priority ?? '—'}</span>
                        <div className="v4-dir__title">{d.decision}</div>
                        <span className={`v4-badge v4-badge--${propose ? 'ok' : 'ko'}`}>{propose ? 'propose' : 'deprioritize'}</span>
                      </div>

                      <div className="v4-dir__chips">
                        {d.modality ? <span className="v4-chip v4-chip--mod">{MODALITY_LABEL[d.modality] ?? d.modality}</span> : null}
                        {d.target ? <span className="v4-chip">target · {d.target}</span> : null}
                        {propose ? (
                          d.testable_now
                            ? <span className="v4-badge v4-badge--ok">testable now{d.next_lane ? ` · ${d.next_lane}` : ''}</span>
                            : <span className="v4-badge v4-badge--run">needs non-structural eval</span>
                        ) : (
                          d.current_verdict ? <span className="v4-chip">{d.current_verdict.replace(/-/g, ' ')}</span> : null
                        )}
                        {typeof d.confidence === 'number' ? <span className="v4-chip">confidence {Math.round(d.confidence * 100)}%</span> : null}
                        {propose && d.testable_now && typeof d.est_cost_usd === 'number' && d.est_cost_usd > 0 ? <span className="v4-chip">~${d.est_cost_usd.toFixed(2)}</span> : null}
                      </div>

                      <p className="v4-dir__why">{d.rationale}</p>

                      {d.cited_capsule_ids.length ? (
                        <div className="v4-dir__cites">
                          <span>grounded in</span>
                          {d.cited_capsule_ids.slice(0, 6).map((cid) => (
                            <Link key={cid} href={`/evidence/${cid}`} className="v4-chip">{cid.slice(0, 8)}</Link>
                          ))}
                          {!propose && d.candidate_id ? (
                            <Link href={`/candidates/${d.candidate_id}`} className="v4-chip">the idea →</Link>
                          ) : null}
                        </div>
                      ) : null}
                    </div>
                  );
                })}
              </div>
            </section>

            <p className="v4-note">
              Director pass {s.run_tag ?? ''}{s.ran_at ? ` · ${s.ran_at.slice(0, 10)}` : ''}. Re-derivable: every
              decision points back to the exact capsules it reasoned from.
            </p>
          </>
        ) : (
          <p className="v4-lead v4-muted" style={{ marginTop: '2rem' }}>
            No director pass yet — decisions appear here as the director reasons over the ledger.
          </p>
        )}
      </div>
    </div>
  );
}
