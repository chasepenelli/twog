import Link from 'next/link';
import { CONTACT_EMAIL, CONTACT_MAILTO } from '@/lib/constants';
import { getFeaturedCandidate, shortHash } from '@/lib/public-candidates';

const ENGINE_STEPS = [
  ['01', 'Collect', 'Papers, datasets, molecules, safety records, and field notes enter one evidence library.'],
  ['02', 'Translate', 'The system turns raw sources into claims, citations, chunks, entities, and searchable context.'],
  ['03', 'Argue', 'Specialist agents compare the evidence, identify weak spots, and ask for missing proof.'],
  ['04', 'Decide', 'Strong research programs become therapy ideas, validation plans, and candidate records.'],
  ['05', 'Publish', 'The public page preserves rationale, methods, citations, status, decisions, and hashes.'],
] as const;

const PROOF_PRIMITIVES = [
  {
    label: 'Inspectable record',
    text: 'A candidate page shows the idea, status, evidence, risks, methods, and decision history in one place.',
  },
  {
    label: 'Citation trail',
    text: 'Every evidence marker expands into a human-readable claim, source title, identifier, and link.',
  },
  {
    label: 'Method versions',
    text: 'Computational and review methods are versioned so old records stay explainable when protocols improve.',
  },
  {
    label: 'Decision log',
    text: 'Status changes are recorded with a rationale instead of disappearing into private notes.',
  },
] as const;

const RECORD_LAYERS = [
  ['Rationale', 'Why this candidate, why now'],
  ['Evidence', 'Citation-backed claims, not loose labels'],
  ['Methods', 'Versioned process behind the record'],
  ['Decision', 'Status changes with a reason attached'],
] as const;

const AI_GOOD_POINTS = [
  ['Learn faster', 'Ask for the lesson, then keep asking better questions.'],
  ['Organize the forgotten', 'Turn scattered neglected-disease evidence into usable context.'],
  ['Challenge weak claims', 'Make missing citations, gaps, and uncertainty visible.'],
  ['Build in public', 'Publish records that others can inspect, correct, and improve.'],
] as const;

const MOVEMENT_INPUTS = ['Papers', 'Omics', 'Safety', 'Molecules'];
const REVIEW_STACK = ['Search', 'Challenge', 'Repair', 'Synthesize'];
const MOVEMENT_OUTPUTS = ['Idea', 'Packet', 'Decision', 'Record'];
const HERO_LOOP_PATH =
  'M104 240 C104 122 242 68 398 94 C486 109 534 109 622 94 C778 68 916 122 916 240 C916 358 778 412 622 386 C534 371 486 371 398 386 C242 412 104 358 104 240 Z';

const LESSON_COLUMNS = [
  ['Ask', 'Start with the honest question.', 'Explain VEGFR signaling like I am new to oncology.'],
  ['Unpack', 'Turn the answer into a map.', 'Define the terms, pathway, species context, and weak assumptions.'],
  ['Follow', 'Walk the evidence trail.', 'Show the citations, contradictions, missing data, and next search query.'],
  ['Make', 'Convert learning into work.', 'Draft the brief, validation packet, or public record update.'],
] as const;

export default function Home() {
  const featured = getFeaturedCandidate();
  const candidate = featured?.candidate;
  const snapshot = featured?.latest_snapshot;

  return (
    <div className="site-shell home-shell">
      <section className="home-hero">
        <div className="home-hero-copy">
          <p className="section-kicker">TWOG / public research engine</p>
          <div className="hero-wordmark-wrap" aria-label="TWOG animated wordmark">
            <div className="hero-loop-system" aria-hidden="true">
              <svg className="hero-loop-svg" viewBox="0 0 1020 480" role="presentation">
                <path className="hero-loop-shadow" d={HERO_LOOP_PATH} />
                <path className="hero-loop-path" d={HERO_LOOP_PATH} />
                {[
                  ['source', '-0s'],
                  ['agent', '-14s'],
                  ['record', '-28s'],
                ].map(([label, begin]) => (
                  <g className="hero-loop-particle" key={label}>
                    <circle cx="0" cy="0" r="6" />
                    <animateMotion dur="42s" begin={begin} repeatCount="indefinite" path={HERO_LOOP_PATH} />
                  </g>
                ))}
              </svg>
              <div className="hero-loop-stations">
                <span className="loop-station loop-station-one">Evidence</span>
                <span className="loop-station loop-station-two">Review</span>
                <span className="loop-station loop-station-three">Record</span>
              </div>
            </div>
            <h1>
              <span>TWOG</span>
              <em>A Living Research Engine</em>
            </h1>
          </div>
          <p className="hero-subhead">
            An AI-assisted system for turning scattered cancer research into public,
            inspectable records: what was found, what was proposed, why it moved, and
            what still needs to be proven.
          </p>
          <div className="hero-actions">
            <Link href="/candidates" className="artifact-button primary">
              Inspect the first record
            </Link>
            <Link href="/methods" className="artifact-button">
              See how records work
            </Link>
            <a href={CONTACT_MAILTO} className="artifact-button">
              Contact
            </a>
          </div>
        </div>
      </section>

      <section className="origin-panel">
        <div className="origin-grid">
          <div className="origin-copy layered-heading" data-layer="AI FOR GOOD">
            <p className="section-kicker">Why it exists</p>
            <h2>Modern AI should make hard problems more reachable.</h2>
            <p>
              TWOG began because hemangiosarcoma leaves too many families with too few
              options and too little organized evidence. The project is built for Graffiti,
              Brady, and every dog after them.
            </p>
            <p>
              The positive case for AI is simple: it can help more people learn faster,
              organize ignored information, ask sharper questions, and turn care into
              useful infrastructure. Not magic. Not replacement expertise. A force
              multiplier for people willing to do the work in the open.
            </p>
            <p className="origin-manifesto">
              When the tools can read with us, teach us, challenge us, and help us publish
              the trail, neglected disease research can become less lonely and more
              inspectable.
            </p>
          </div>

          <div className="ai-good-system" aria-label="Modern AI for good diagram">
            <div className="good-core">
              <span>Modern AI for good</span>
              <strong>Ask. Learn. Build.</strong>
            </div>
            <div className="good-ray good-ray-one" />
            <div className="good-ray good-ray-two" />
            <div className="good-ray good-ray-three" />
            <div className="good-ray good-ray-four" />
            <div className="good-points">
              {AI_GOOD_POINTS.map(([label, text]) => (
                <article className="good-point" key={label}>
                  <span>{label}</span>
                  <p>{text}</p>
                </article>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="record-preview-section">
        <div className="record-preview-grid">
          <div className="section-heading layered-heading" data-layer="PUBLIC PROOF">
            <p className="section-kicker">Public proof layer</p>
            <h2>The candidate page is the audit trail.</h2>
            <p>
              TWOG should not ask readers to trust a black box. If the system advances an
              idea, the public page should make the reasoning inspectable: the claim, the
              evidence behind it, the method used, the decision made, and the uncertainty
              still attached.
            </p>
            <p>
              The goal is not to make a candidate look finished. The goal is to make the
              state of the work legible enough that a scientist, collaborator, or curious
              reader can see what would need to be checked next.
            </p>
          </div>

          <div className="proof-instrument" aria-label="Public proof record diagram">
            <div className="record-sheet">
              <div className="sheet-rule" />
              <span className="lab-label">Public record</span>
              <strong>{candidate?.display_id ?? 'TWOG'}</strong>
              <em>{shortHash(candidate?.content_hash ?? snapshot?.content_hash)}</em>
            </div>
            <div className="record-layers" aria-hidden="true">
              {RECORD_LAYERS.map(([label, text]) => (
                <div className="record-layer" key={label}>
                  <span>{label}</span>
                  <p>{text}</p>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="candidate-snapshot-row">
          <article className="home-record-card" aria-label="TWOG proof record preview">
            <div className="proof-header">
              <span>Candidate snapshot</span>
              <code>{candidate?.display_id ?? 'TWOG'}</code>
            </div>
            <h3>{candidate?.title ?? 'Candidate proof record'}</h3>
            <p>{candidate?.summary ?? 'Static candidate snapshots make the research trail inspectable.'}</p>
            <div className="proof-metrics">
              <span>Status / {candidate?.public_status ?? 'draft'}</span>
              <span>Hash / {shortHash(candidate?.content_hash ?? snapshot?.content_hash)}</span>
              <span>Evidence / {candidate?.evidence_refs?.length ?? 0} refs</span>
            </div>
            {candidate && (
              <Link href={`/candidates/${candidate.candidate_id}`} className="record-link">
                Open record
              </Link>
            )}
          </article>

          <div className="proof-constellation">
            {PROOF_PRIMITIVES.map((item) => (
              <article className="proof-fragment" key={item.label}>
                <span>{item.label}</span>
                <p>{item.text}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="engine-section">
        <div className="section-heading layered-heading" data-layer="CLOSED LOOP">
          <p className="section-kicker">How it moves</p>
          <h2>Evidence enters. A record emerges.</h2>
          <p>
            TWOG is built to turn research motion into something inspectable. A source is
            not useful just because it was collected. It has to be parsed, challenged,
            connected to the right question, and either promoted into a public record or
            held back with a clear reason.
          </p>
        </div>

        <div className="movement-lab" aria-label="TWOG evidence movement diagram">
          <div className="movement-panel intake-panel">
            <span className="lab-label">01 / Intake</span>
            <h3>Raw evidence is messy.</h3>
            <p>
              Literature, expression data, molecule records, and safety signals arrive
              with different formats, confidence levels, and missing context.
            </p>
            <div className="packet-stack" aria-hidden="true">
              {MOVEMENT_INPUTS.map((label) => (
                <span key={label}>{label}</span>
              ))}
            </div>
          </div>

          <div className="review-chamber">
            <div className="chamber-lines" aria-hidden="true">
              <span />
              <span />
              <span />
            </div>
            <span className="lab-label">02 / Agent review chamber</span>
            <h3>Agents do not just summarize. They argue with the evidence.</h3>
            <p>
              The useful behavior is not one perfect answer. It is a loop where specialist
              agents surface citations, expose weak assumptions, request missing evidence,
              and narrow the next testable question.
            </p>
            <div className="review-stack" aria-hidden="true">
              {REVIEW_STACK.map((label) => (
                <span key={label}>{label}</span>
              ))}
            </div>
          </div>

          <div className="movement-panel output-panel">
            <span className="lab-label">03 / Public proof</span>
            <h3>The output is a record, not a vibe.</h3>
            <p>
              If an idea moves forward, the page should show the rationale, citations,
              methods, risks, blockers, and decision history that got it there.
            </p>
            <div className="record-output-stack" aria-hidden="true">
              {MOVEMENT_OUTPUTS.map((label) => (
                <span key={label}>{label}</span>
              ))}
            </div>
          </div>
        </div>

        <div className="movement-notes">
          <article>
            <span>What moves forward</span>
            <p>
              Ideas with enough evidence, a clear mechanism, and a concrete next readout
              become candidate records or validation packets.
            </p>
          </article>
          <article>
            <span>What gets stopped</span>
            <p>
              Weak citations, missing provenance, unclear species transfer, or unsupported
              claims are routed back into follow-up research instead of being polished up.
            </p>
          </article>
          <article>
            <span>What becomes public</span>
            <p>
              The public layer shows the current state of the reasoning chain. It is built
              for inspection, correction, and expert review.
            </p>
          </article>
        </div>

        <div className="flow-map">
          {ENGINE_STEPS.map(([number, label, text]) => (
            <article className="flow-step" key={label}>
              <code>{number}</code>
              <h3>{label}</h3>
              <p>{text}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="learning-section">
        <div className="learning-column-shell">
          <div className="learning-column-header layered-heading" data-layer="LEARN / BUILD">
            <p className="section-kicker">The learning layer</p>
            <h2>The real unlock is being able to ask for the lesson.</h2>
            <p>
              TWOG is also a teaching surface. The same system that can search, cite, and
              challenge research can slow down and explain the science. You can ask for a
              pathway lesson, a citation map, a contradiction check, or the next practical
              experiment.
            </p>
            <p>
              That matters because learning is no longer separate from building. The lesson
              becomes a better question. The better question becomes a focused search. The
              focused search becomes a record someone else can inspect.
            </p>
          </div>

          <div className="learning-column-layout" aria-label="Learning column workflow">
            <article className="learning-thesis-column">
              <span>Learning is infrastructure</span>
              <p>
                The point is not to get a clever answer and move on. The point is to turn
                curiosity into a reusable research object.
              </p>
            </article>

            {LESSON_COLUMNS.map(([label, title, example], index) => (
              <article className="lesson-column" key={label}>
                <code>{String(index + 1).padStart(2, '0')}</code>
                <h3>{label}</h3>
                <p>{title}</p>
                <span>{example}</span>
              </article>
            ))}
          </div>

          <div className="learning-rule-line" aria-hidden="true">
            <span>A question becomes a lesson</span>
            <span>A lesson becomes a search</span>
            <span>A search becomes a record</span>
          </div>
        </div>
      </section>

      <section className="cta-section">
        <div className="public-cta-grid">
          <div className="public-cta-copy layered-heading" data-layer="FIELD NOTES">
            <p className="section-kicker">Built in public</p>
            <h2>Follow the search while the engine is still warm.</h2>
            <p>
              TWOG is being built where people can see the work: the strange leads, the
              promising candidates, the dead ends, the rewrites, the receipts, and the
              moments where a question finally turns into something testable.
            </p>
            <p>
              Sign up for field notes from the build: candidate drops, research lessons,
              public proof records, and the occasional hard-won answer from the machine.
            </p>
          </div>

          <div className="signup-console" aria-label="TWOG field notes signup">
            <div className="signup-header">
              <span>TWOG field notes</span>
              <code>public / useful / weird</code>
            </div>
            <div className="signup-terminal">
              <p>Receive:</p>
              <span>candidate records</span>
              <span>research lessons</span>
              <span>decision logs</span>
              <span>build notes</span>
            </div>
            <a
              href="https://pushingc.substack.com/subscribe"
              target="_blank"
              rel="noopener noreferrer"
              className="signup-button"
            >
              Sign up on Substack
            </a>
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
