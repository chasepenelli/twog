import Link from 'next/link';
import { notFound } from 'next/navigation';
import { CandidateContributionPanel } from '@/components/CandidateContributionPanel';
import {
  formatPublicDate,
  getCandidate,
  LiteratureRecord,
  publicCandidates,
  publicCandidatePayloadPath,
  readableKind,
  shortHash,
} from '@/lib/public-candidates';

export function generateStaticParams() {
  return publicCandidates.flatMap(({ candidate }) => [
    { candidateId: candidate.candidate_id },
    ...(candidate.display_id ? [{ candidateId: candidate.display_id.toLowerCase() }] : []),
  ]);
}

function referenceGroup(item: LiteratureRecord): string {
  if (item.evidence_kind?.includes('target_expression')) return 'Canine target expression';
  if (item.evidence_kind?.includes('direct_canine')) return 'Canine disease evidence';
  if (item.evidence_kind?.includes('human')) return 'Human analog evidence';
  return 'Supporting context';
}

function uniqueGroups(literature: LiteratureRecord[]): string[] {
  return Array.from(new Set(literature.map(referenceGroup)));
}

function identifiersLine(item: LiteratureRecord): string {
  return Object.entries(item.identifiers ?? {})
    .filter(([, value]) => value)
    .map(([key, value]) => `${key.toUpperCase()}: ${value}`)
    .join(' / ');
}

function compactList(values?: string[], limit = 4): string[] {
  return (values ?? []).filter(Boolean).slice(0, limit);
}

export async function generateMetadata({ params }: { params: Promise<{ candidateId: string }> }) {
  const { candidateId } = await params;
  const detail = getCandidate(candidateId);
  return {
    title: detail ? `${detail.candidate.display_id ?? detail.candidate.candidate_id} — TWOG Candidate` : 'TWOG Candidate',
    description: detail?.candidate.summary ?? 'TWOG public candidate record.',
  };
}

export default async function CandidateDetailPage({ params }: { params: Promise<{ candidateId: string }> }) {
  const { candidateId } = await params;
  const detail = getCandidate(candidateId);
  if (!detail) notFound();

  const candidate = detail.candidate;
  const snapshot = detail.latest_snapshot;
  const payload = snapshot?.payload;
  const rationale = payload?.rationale;
  const evidence = payload?.evidence;
  const literature = payload?.literature ?? [];
  const decisions = detail.decision_events ?? [];
  const validationDecision = evidence?.validation_decisions?.[0];
  const displayId = candidate.display_id ?? candidate.candidate_id;
  const referenceGroups = uniqueGroups(literature);
  const payloadPath = publicCandidatePayloadPath(candidate.candidate_id);

  return (
    <div className="site-shell page-shell">
      <section className="candidate-hero candidate-story-hero">
        <div className="candidate-hero-copy">
          <p className="section-kicker">{displayId} / technical story</p>
          <h1>{candidate.title}</h1>
          <p>{candidate.summary}</p>
          <div className="candidate-status-strip" aria-label="Candidate record status">
            <span>{readableKind(candidate.public_status)}</span>
            <span>{readableKind(candidate.candidate_kind)}</span>
            <span>score {candidate.priority_score ?? 'pending'}</span>
            <span>hash {shortHash(candidate.content_hash ?? snapshot?.content_hash)}</span>
          </div>
        </div>
        <aside className="candidate-record-card">
          <span className="lab-label">Record state</span>
          <strong>{displayId}</strong>
          <dl>
            <div>
              <dt>Updated</dt>
              <dd>{formatPublicDate(candidate.updated_at)}</dd>
            </div>
            <div>
              <dt>Evidence strength</dt>
              <dd>{readableKind(evidence?.evidence_strength)}</dd>
            </div>
            <div>
              <dt>Validation ready</dt>
              <dd>{validationDecision?.validation_ready ? 'yes' : 'not yet'}</dd>
            </div>
            <div>
              <dt>Program signal</dt>
              <dd>{readableKind(validationDecision?.broader_program_signal)}</dd>
            </div>
          </dl>
        </aside>
      </section>

      <section className="candidate-technical-story">
        <article className="candidate-story-main">
          <div className="story-chapter">
            <p className="section-kicker">01 / Technical thesis</p>
            <h2>Why this candidate exists</h2>
            <p className="story-lede">{rationale?.hypothesis ?? candidate.summary}</p>
            {(rationale?.rationale_md ?? candidate.rationale_md) ? (
              <p>{rationale?.rationale_md ?? candidate.rationale_md}</p>
            ) : null}
          </div>

          <div className="story-chapter">
            <p className="section-kicker">02 / Mechanistic argument</p>
            <h2>The mechanism the system is trying to test</h2>
            {rationale?.mechanism ? <p>{rationale.mechanism}</p> : null}
            {rationale?.translational_path ? (
              <p className="story-note">
                <span>Proposed path</span>
                {rationale.translational_path}
              </p>
            ) : null}
          </div>

          <div className="story-chapter">
            <p className="section-kicker">03 / Evidence interpretation</p>
            <h2>What the evidence currently supports</h2>
            <p>
              The page treats these references as an argument map, not a verdict. Each
              record below explains what part of the candidate story it supports and
              where the signal still needs stronger source-traceable evidence.
            </p>
            <div className="evidence-claim-grid">
              {literature.map((item) => (
                <article className="evidence-claim" key={item.ref}>
                  <div>
                    <strong>{item.ref}</strong>
                    <span>{referenceGroup(item)}</span>
                  </div>
                  <h3>{item.title}</h3>
                  <p>{item.supports}</p>
                </article>
              ))}
            </div>
          </div>
        </article>

        <aside className="candidate-story-rail">
          <article>
            <p className="section-kicker">Targets</p>
            <div className="tag-grid">
              {(candidate.targets ?? []).map((target) => (
                <span key={target}>{target}</span>
              ))}
              {(candidate.biomarkers ?? []).map((biomarker) => (
                <span key={biomarker}>{biomarker}</span>
              ))}
              {(candidate.candidate_therapies ?? []).map((therapy) => (
                <span key={therapy}>{therapy}</span>
              ))}
            </div>
          </article>

          <article>
            <p className="section-kicker">Current limits</p>
            <ul className="compact-list">
              {compactList(evidence?.risks ?? candidate.risk_flags, 5).map((risk) => (
                <li key={risk}>{risk}</li>
              ))}
            </ul>
          </article>

          <article>
            <p className="section-kicker">Would change confidence</p>
            <ul className="compact-list">
              {compactList(validationDecision?.confidence_changers, 4).map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </article>
        </aside>
      </section>

      <section className="candidate-layout">
        <article className="record-panel wide technical-readout-panel">
          <p className="section-kicker">Next technical readouts</p>
          <h2>What would make this record more convincing</h2>
          <ul className="compact-list technical-readout-list">
            {(evidence?.next_experiments ?? []).map((experiment) => (
              <li key={experiment}>{experiment}</li>
            ))}
          </ul>
        </article>

        <article className="record-panel wide">
          <p className="section-kicker">Decision log</p>
          <div className="decision-list">
            {decisions.map((event) => (
              <div className="decision-row" key={`${event.action}-${event.occurred_at}`}>
                <strong>{readableKind(event.action)}</strong>
                <span>{formatPublicDate(event.occurred_at)}</span>
                <p>{event.rationale_md}</p>
              </div>
            ))}
          </div>
        </article>
      </section>

      <section className="reference-dossier">
        <div className="section-heading layered-heading" data-layer="REFERENCES">
          <p className="section-kicker">Reference dossier</p>
          <h2>Compartmentalized evidence, identifiers, and provenance.</h2>
          <p>
            The short citation labels above are expanded here into source compartments.
            Each record carries the claim it supports, identifiers, provenance, and duplicate
            handling so the public page can be audited without decoding internal refs.
          </p>
        </div>

        <div className="reference-groups">
          {referenceGroups.map((group) => (
            <section className="reference-group" key={group}>
              <div className="reference-group-heading">
                <span>{group}</span>
                <strong>{literature.filter((item) => referenceGroup(item) === group).length}</strong>
              </div>
              <div className="reference-card-stack">
                {literature
                  .filter((item) => referenceGroup(item) === group)
                  .map((item) => (
                    <article className="reference-card" key={`${group}-${item.ref}`}>
                      <div className="reference-card-head">
                        <strong>{item.ref}</strong>
                        <span>{item.publication_year ?? formatPublicDate(item.published_at)}</span>
                      </div>
                      <h3>
                        {item.source_url ? (
                          <a href={item.source_url} target="_blank" rel="noopener noreferrer">
                            {item.title}
                          </a>
                        ) : (
                          item.title
                        )}
                      </h3>
                      <p>{item.supports}</p>
                      <div className="reference-meta-grid">
                        <span>Source / {readableKind(item.source_key)}</span>
                        <span>Kind / {readableKind(item.evidence_kind)}</span>
                        <span>Resolved / {item.resolved ? 'yes' : 'no'}</span>
                        <span>Duplicates / {item.dedupe?.duplicate_count ?? 0}</span>
                      </div>
                      <div className="reference-identifiers">
                        <span>{identifiersLine(item) || 'No public identifiers recorded'}</span>
                        <span>
                          Objects / {(item.provenance?.research_object_ids ?? []).length} · Chunks /{' '}
                          {(item.provenance?.chunk_ids ?? []).length}
                        </span>
                      </div>
                    </article>
                  ))}
              </div>
            </section>
          ))}
        </div>
      </section>

      <CandidateContributionPanel
        candidateId={candidate.candidate_id}
        displayId={displayId}
        payloadPath={payloadPath}
        snapshotHash={shortHash(candidate.content_hash ?? snapshot?.content_hash)}
      />

      <section className="record-panel method-callout">
        <p className="section-kicker">Reproducibility</p>
        <h2>Snapshot generated with {payload?.reproducibility?.pipeline_version ?? snapshot?.pipeline_version ?? 'record method'}</h2>
        <p>
          This public record is an inspection artifact, not a clinical recommendation.
          The method page explains how candidate snapshots, citation refs, decision logs,
          content hashes, and public JSON payloads are produced.
        </p>
        <div className="payload-receipt">
          <span>Public payload</span>
          <code>{payloadPath}</code>
          <p>
            This endpoint returns the candidate metadata, latest snapshot, expanded
            references, decision events, and reproducibility fields used to render this page.
          </p>
        </div>
        <div className="method-actions">
          <Link href="/methods/candidate-record-v1" className="record-link">
            Read method
          </Link>
          <a href={payloadPath} className="record-link">
            Open JSON payload
          </a>
        </div>
      </section>
    </div>
  );
}
