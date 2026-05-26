import Link from 'next/link';
import { ContactForm } from '@/components/ContactForm';
import { CONTACT_EMAIL, CONTACT_MAILTO } from '@/lib/constants';
import { getFeaturedCandidate, shortHash } from '@/lib/public-candidates';

const OPERATING_LOOP = [
  {
    label: 'Evidence',
    text: 'Source records keep citations, chunks, identifiers, provenance, and enough context for another reviewer to reconstruct the claim.',
  },
  {
    label: 'Review',
    text: 'Specialist agents challenge the thesis, flag weak evidence, request missing proof, and preserve the critique instead of smoothing it over.',
  },
  {
    label: 'Record',
    text: 'Operator-approved ideas become public proof records with status, rationale, risks, methods, decisions, and the next test.',
  },
] as const;

const ENGINE_STEPS = [
  ['01', 'Publish the proof', 'A candidate record exposes the thesis, citations, methods, risks, decision log, and machine-readable payload.'],
  ['02', 'Check it out', 'A reviewer or sandbox receives a scoped manifest with the candidate snapshot, evidence bundle, and allowed work types.'],
  ['03', 'Check it back in', 'Returned ProofCapsules capture critique, repaired citations, artifacts, omics notes, or validation proposals.'],
  ['04', 'Route the work', 'Operator gates move useful packets toward evidence review, validation planning, compute review, or lab handoff.'],
] as const;

const PROOF_POINTS = [
  ['Claim', 'What the system believes might be true.'],
  ['Evidence', 'Which citations, datasets, and artifacts support it.'],
  ['Decision', 'Why the idea advanced, stalled, or got held back.'],
  ['Next Test', 'The readout that would actually change confidence.'],
] as const;

const VALIDATION_PATH = [
  ['Proof record', 'Inspectable thesis, evidence, methods, status, and decision history.'],
  ['ProofCapsule', 'Structured outside work that can be reviewed without mutating the public record.'],
  ['Validation packet', 'Readouts, controls, blockers, missing inputs, and acceptance criteria.'],
  ['Compute artifact', 'Docking, MD smoke, omics readouts, plots, logs, configs, and hashes where useful.'],
  ['Lab handoff', 'A clearer experimental question for collaborators, reviewers, and wet-lab confirmation.'],
] as const;

const HERO_MARKERS = [
  ['Public proof records', 'candidate state, citations, methods'],
  ['Operator-gated writes', 'agents recommend, humans approve'],
  ['Structured check-ins', 'ProofCapsules from reviewers and sandboxes'],
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
              TWOG turns biomedical evidence into public proof records, structured
              check-ins, and validation-ready handoffs: what supports the idea, why it
              moved, what is still weak, and what should be tested next.
            </p>
            <div className="home-hero-markers" aria-label="TWOG operating markers">
              {HERO_MARKERS.map(([label, detail]) => (
                <span key={label}>
                  <strong>{label}</strong>
                  <em>{detail}</em>
                </span>
              ))}
            </div>
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
            <h2>The scarce layer is not information. It is durable judgment.</h2>
          </div>
          <div className="home-section-copy">
            <p>
              Biology is not short on papers, databases, omics files, adverse-event
              fragments, or half-remembered leads. It is short on systems that keep the
              reasoning attached as an idea moves from signal to testable record.
            </p>
            <p>
              The first wedge is canine hemangiosarcoma: urgent, under-organized, and
              relevant to comparative oncology. The larger play is repeatable
              infrastructure for overlooked disease areas where evidence exists but
              decision infrastructure does not.
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
              A serious discovery engine cannot hide behind a polished summary. If an
              idea advances, the record should show the mechanism, citation trail,
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
            <p className="section-kicker">Proof network</p>
            <h2>The record is not the endpoint. It is the work surface.</h2>
            <p>
              TWOG is moving toward a checkout/check-in model for science: publish the
              record, give collaborators the evidence bundle, accept structured returned
              work, and route useful packets through review gates.
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
            The goal is not to pretend simulation proves biology. The goal is to lower
            the cost of being wrong, preserve the work that changed confidence, and send
            stronger questions toward expert review, compute, assay design, and lab
            confirmation.
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
              TWOG is being built where reviewers, collaborators, and serious backers
              can inspect the work: the strange leads, the promising candidates, the
              dead ends, the receipts, and the moments where a question becomes
              structured enough to test.
            </p>
            <p>
              Sign up for field notes from the build: candidate drops, evidence gaps,
              public proof records, method notes, and the operating lessons behind
              AI-native research infrastructure that can be audited.
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
