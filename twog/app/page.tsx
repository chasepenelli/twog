import Link from 'next/link';
import { ContactForm } from '@/components/ContactForm';
import { CONTACT_EMAIL, CONTACT_MAILTO } from '@/lib/constants';
import { getFeaturedCandidate, shortHash } from '@/lib/public-candidates';

const OPERATING_LOOP = [
  {
    label: 'Evidence',
    text: 'Scientific sources enter an owned evidence layer with citations, chunks, entities, identifiers, and provenance.',
  },
  {
    label: 'Review',
    text: 'Specialist agents attack the thesis, expose weak assumptions, request missing proof, and preserve the critique.',
  },
  {
    label: 'Record',
    text: 'Operator-approved ideas become public proof records with status, rationale, risks, methods, and decisions.',
  },
] as const;

const ENGINE_STEPS = [
  ['01', 'Acquire', 'Papers, datasets, molecules, safety signals, and field notes become structured source records.'],
  ['02', 'Challenge', 'Agents compare the evidence, surface contradictions, and ask for the missing citations.'],
  ['03', 'Promote', 'Only ideas with enough signal move into public candidate records or validation packets.'],
  ['04', 'Test', 'Strong records can route toward compute, partner review, assay design, and lab-ready handoff.'],
] as const;

const PROOF_POINTS = [
  ['Claim', 'What the system believes might be true.'],
  ['Evidence', 'Which citations, datasets, and artifacts support it.'],
  ['Decision', 'Why the idea advanced, stalled, or got held back.'],
  ['Next Test', 'The readout that would actually change confidence.'],
] as const;

const VALIDATION_PATH = [
  ['Proof record', 'Inspectable thesis, evidence, methods, status, and decision history.'],
  ['Validation packet', 'Readouts, controls, blockers, missing inputs, and acceptance criteria.'],
  ['Compute artifact', 'Docking, MD smoke, omics readouts, plots, logs, configs, and hashes where useful.'],
  ['Lab handoff', 'A clearer experimental question for collaborators, reviewers, and wet-lab confirmation.'],
] as const;

export default function Home() {
  const featured = getFeaturedCandidate();
  const candidate = featured?.candidate;
  const snapshot = featured?.latest_snapshot;
  const hash = shortHash(candidate?.content_hash ?? snapshot?.content_hash);

  return (
    <div className="site-shell home-shell">
      <section className="home-hero portal-hero">
        <div className="portal-shell" aria-label="TWOG research portal entrance">
          <div className="portal-hero-copy">
            <p className="section-kicker">TWOG / public research engine</p>
            <h1 className="portal-wordmark">
              <span>TWOG</span>
              <em>A Living Research Engine</em>
            </h1>
            <p className="hero-subhead">
              TWOG turns fragmented biomedical evidence into inspectable discovery
              records: hypotheses, citations, agent critique, validation packets, compute
              artifacts, and the next experiment worth running.
            </p>
            <div className="hero-actions">
              <Link href="/candidates" className="artifact-button primary">
                Inspect the first record
              </Link>
              <Link href="/architecture" className="artifact-button">
                Read the architecture
              </Link>
              <Link href="#contact" className="artifact-button">
                Contact
              </Link>
            </div>
          </div>
        </div>
      </section>

      <section className="home-section home-thesis-section" data-marker="THESIS / COMPARATIVE ONCOLOGY WEDGE">
        <div className="home-section-grid">
          <div className="home-section-header">
            <p className="section-kicker">The thesis</p>
            <h2>The scarce layer is not information. It is judgment that compounds.</h2>
          </div>
          <div className="home-section-copy">
            <p>
              Biology is drowning in papers, source databases, omics files, adverse-event
              fragments, and half-remembered leads. TWOG is being built as the operating
              layer that turns that material into durable scientific signal.
            </p>
            <p>
              The first wedge is canine hemangiosarcoma: urgent, under-organized, and
              translationally relevant. The larger play is a repeatable research engine
              for overlooked disease areas where evidence exists but decision
              infrastructure does not.
            </p>
          </div>
        </div>
      </section>

      <section className="home-section home-loop-section" data-marker="OPERATING LOOP / EVIDENCE REVIEW RECORD">
        <div className="home-section-header narrow">
          <p className="section-kicker">Operating loop</p>
          <h2>Evidence. Review. Record.</h2>
          <p>
            TWOG is designed around a simple discipline: no claim gets promoted without a
            trail someone else can inspect.
          </p>
        </div>
        <div className="home-principle-grid">
          {OPERATING_LOOP.map((item) => (
            <article className="home-principle" key={item.label}>
              <span>{item.label}</span>
              <p>{item.text}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="home-section home-proof-section" data-marker="PUBLIC PROOF / RECORD LAYER">
        <div className="home-section-grid">
          <div className="home-section-header">
            <p className="section-kicker">Public proof layer</p>
            <h2>Proof records turn AI output into an asset you can inspect.</h2>
            <p>
              A serious discovery engine cannot hide behind a polished summary. If TWOG
              advances an idea, the record should show the mechanism, citation trail,
              method version, risks, decision log, and remaining uncertainty.
            </p>
          </div>

          <article className="home-record-card editorial-record-card" aria-label="TWOG proof record preview">
            <div className="proof-header">
              <span>Candidate snapshot</span>
              <code>{candidate?.display_id ?? 'TWOG'}</code>
            </div>
            <h3>{candidate?.title ?? 'Candidate proof record'}</h3>
            <p>{candidate?.summary ?? 'Static candidate snapshots make the research trail inspectable.'}</p>
            <div className="proof-metrics">
              <span>Status / {candidate?.public_status ?? 'draft'}</span>
              <span>Hash / {hash}</span>
              <span>Evidence / {candidate?.evidence_refs?.length ?? 0} refs</span>
            </div>
            {candidate && (
              <Link href={`/candidates/${candidate.candidate_id}`} className="record-link">
                Open record
              </Link>
            )}
          </article>
        </div>

        <div className="proof-point-grid">
          {PROOF_POINTS.map(([label, text]) => (
            <article className="proof-point" key={label}>
              <span>{label}</span>
              <p>{text}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="home-section home-engine-section" data-marker="ENGINE / AGENT CRITIQUE">
        <div className="home-section-grid">
          <div className="home-section-header">
            <p className="section-kicker">Research engine</p>
            <h2>The loop is the leverage.</h2>
            <p>
              A source is not valuable because it was collected. It becomes valuable when
              it is structured, challenged, tied to the right question, and either
              promoted or held back with a clear reason.
            </p>
            <p className="operator-line">
              LLMs argue and synthesize. Operator approval is the write gate.
            </p>
          </div>

          <div className="home-process-list" aria-label="TWOG operating loop">
            {ENGINE_STEPS.map(([number, label, text]) => (
              <article className="home-process-step" key={label}>
                <code>{number}</code>
                <div>
                  <h3>{label}</h3>
                  <p>{text}</p>
                </div>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="home-section home-validation-section" data-marker="VALIDATION / HANDOFF PATH">
        <div className="home-section-header narrow">
          <p className="section-kicker">Durable testing framework</p>
          <h2>From public proof to validation-grade handoff.</h2>
          <p>
            Simulations can prioritize hypotheses and produce reproducible artifacts, but
            they do not prove biology. The business-grade bridge is cheaper
            prioritization upstream, cleaner review in the middle, and better questions
            arriving at the lab.
          </p>
        </div>

        <div className="validation-path-list">
          {VALIDATION_PATH.map(([label, text], index) => (
            <article className="validation-path-step" key={label}>
              <code>{String(index + 1).padStart(2, '0')}</code>
              <h3>{label}</h3>
              <p>{text}</p>
            </article>
          ))}
        </div>
      </section>

      <section id="contact" className="home-section cta-section" data-marker="PUBLIC BUILD / FIELD NOTES">
        <div className="public-cta-grid">
          <div className="public-cta-copy">
            <p className="section-kicker">Built in public</p>
            <h2>Follow the build of a research engine that can publish its own proof.</h2>
            <p>
              TWOG is being built where reviewers, collaborators, and serious backers can
              inspect the work: the strange leads, the promising candidates, the dead
              ends, the rewrites, the receipts, and the moments where a question turns
              into something testable.
            </p>
            <p>
              Sign up for field notes from the build: candidate drops, evidence gaps,
              public proof records, method notes, and the operating lessons behind a new
              kind of AI-native research infrastructure.
            </p>
          </div>

          <div className="signup-console contact-console" aria-label="TWOG field notes and contact">
            <div className="signup-header">
              <span>TWOG field notes</span>
              <code>subscribe / contact</code>
            </div>
            <div className="signup-terminal">
              <p>Receive:</p>
              <span>candidate records</span>
              <span>evidence gaps</span>
              <span>decision logs</span>
              <span>method notes</span>
            </div>
            <a
              href="https://pushingc.substack.com/subscribe"
              target="_blank"
              rel="noopener noreferrer"
              className="signup-button"
            >
              Sign up on Substack
            </a>
            <ContactForm />
            <div className="signup-links">
              <Link href="/candidates">Inspect candidates</Link>
              <a href={CONTACT_MAILTO}>{CONTACT_EMAIL}</a>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
