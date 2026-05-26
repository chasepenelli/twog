'use client';

import { FormEvent, useEffect, useMemo, useState } from 'react';
import {
  CANDIDATE_CONTRIBUTIONS_PAUSED,
  CANDIDATE_CONTRIBUTIONS_PAUSED_MESSAGE,
} from '@/lib/public-contribution-status';

const CONTRIBUTION_TYPES = [
  'evidence_addition',
  'citation_repair',
  'claim_critique',
  'replication_result',
  'compute_artifact',
  'omics_note',
  'validation_proposal',
  'safety_or_translation_note',
  'candidate_demotion_case',
] as const;
const RELATIONS = ['supports', 'challenges', 'extends', 'corrects', 'requests_validation', 'requests_compute'] as const;
const ACTIONS = [
  'evidence_review',
  'citation_repair',
  'validation_packet',
  'omics_readout',
  'docking_or_md_review',
  'no_action',
] as const;

interface CandidateContributionPanelProps {
  candidateId: string;
  displayId: string;
  payloadPath: string;
  evidenceBundlePath: string;
  snapshotHash: string;
}

interface IntakeStatus {
  storage_configured?: boolean;
  contribution_id?: string;
  contribution_content_hash?: string;
  status_url?: string;
  status?: string;
  error?: string;
  message?: string;
  details?: string[];
}

export function CandidateContributionPanel({
  candidateId,
  displayId,
  payloadPath,
  evidenceBundlePath,
  snapshotHash,
}: CandidateContributionPanelProps) {
  const submissionUrl = `/api/public-candidates/${candidateId}/contributions`;
  const templateUrl = `/api/public-candidates/${candidateId}/contribution-template`;
  const [storageConfigured, setStorageConfigured] = useState<boolean | null>(null);
  const [contributionType, setContributionType] = useState<(typeof CONTRIBUTION_TYPES)[number]>('evidence_addition');
  const [relation, setRelation] = useState<(typeof RELATIONS)[number]>('extends');
  const [requestedAction, setRequestedAction] = useState<(typeof ACTIONS)[number]>('evidence_review');
  const [name, setName] = useState('');
  const [handle, setHandle] = useState('');
  const [contact, setContact] = useState('');
  const [title, setTitle] = useState('');
  const [summary, setSummary] = useState('');
  const [claim, setClaim] = useState('');
  const [targetedSection, setTargetedSection] = useState('');
  const [methodNotes, setMethodNotes] = useState('');
  const [evidenceRefs, setEvidenceRefs] = useState('');
  const [artifactRefs, setArtifactRefs] = useState('');
  const [evidenceUrl, setEvidenceUrl] = useState('');
  const [evidenceTitle, setEvidenceTitle] = useState('');
  const [limitations, setLimitations] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [result, setResult] = useState<IntakeStatus | null>(null);

  useEffect(() => {
    if (CANDIDATE_CONTRIBUTIONS_PAUSED) {
      setStorageConfigured(false);
      return;
    }

    let cancelled = false;
    fetch(submissionUrl)
      .then((response) => response.json())
      .then((payload: IntakeStatus) => {
        if (!cancelled) setStorageConfigured(Boolean(payload.storage_configured));
      })
      .catch(() => {
        if (!cancelled) setStorageConfigured(false);
      });
    return () => {
      cancelled = true;
    };
  }, [submissionUrl]);

  const evidence = useMemo(() => {
    if (!evidenceUrl.trim() && !evidenceTitle.trim()) return [];
    return [
      {
        title: evidenceTitle.trim(),
        url: evidenceUrl.trim(),
        source_type: 'paper | dataset | method | compute_artifact | clinical_record | other',
        supported_claim: claim.trim(),
        notes: '',
      },
    ];
  }, [claim, evidenceTitle, evidenceUrl]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setResult(null);

    if (CANDIDATE_CONTRIBUTIONS_PAUSED) {
      setResult({
        error: 'candidate_contribution_intake_paused',
        message: CANDIDATE_CONTRIBUTIONS_PAUSED_MESSAGE,
      });
      return;
    }

    if (!contact.trim() || !title.trim() || !summary.trim() || !claim.trim() || !methodNotes.trim()) {
      setResult({
        error: 'missing_required_fields',
        message: 'Contact, title, summary, claim/question, and method notes are required.',
      });
      return;
    }

    setIsSubmitting(true);
    try {
      const response = await fetch(submissionUrl, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          contribution_type: contributionType,
          contributor: {
            name,
            handle,
            contact,
          },
          title,
          summary,
          claim_or_question: claim,
          targeted_claim_or_section: targetedSection || claim,
          method_notes: methodNotes,
          relation_to_current_record: relation,
          evidence_refs: evidenceRefs
            .split(',')
            .map((value) => value.trim())
            .filter(Boolean),
          evidence,
          artifact_refs: artifactRefs
            .split(',')
            .map((value) => value.trim())
            .filter(Boolean),
          artifacts: [],
          requested_system_action: requestedAction,
          conflicts_or_limitations: limitations,
        }),
      });
      const payload = (await response.json()) as IntakeStatus;
      setResult(payload);
      if (response.ok) {
        setTitle('');
        setSummary('');
        setClaim('');
        setTargetedSection('');
        setMethodNotes('');
        setEvidenceRefs('');
        setArtifactRefs('');
        setEvidenceUrl('');
        setEvidenceTitle('');
        setLimitations('');
      }
    } catch {
      setResult({
        error: 'submission_failed',
        message: 'The browser could not submit this packet. Use the JSON template or email fallback.',
      });
    } finally {
      setIsSubmitting(false);
    }
  }

  return (
    <section className="candidate-contribution-panel" aria-label="Contribute to this candidate record">
      <div className="contribution-intro">
        <p className="section-kicker">Contribute to this record</p>
        <h2>Check out the public payload. Check in better evidence.</h2>
        <p>
          Use the payload when you want the exact machine-readable snapshot behind this page. Use the form or template
          when you have a critique, citation repair, replication note, artifact, or validation proposal to check back in.
          Contributions enter a gated intake queue; they never edit the public record directly.
        </p>
        <div className="contribution-receipt">
          <span>{displayId}</span>
          <code>{snapshotHash}</code>
          <strong>
            {CANDIDATE_CONTRIBUTIONS_PAUSED
              ? 'intake paused'
              : storageConfigured === null
                ? 'checking intake'
                : storageConfigured
                  ? 'intake online'
                  : 'email fallback'}
          </strong>
        </div>
        <div className="method-actions">
          <a href={payloadPath} className="record-link" target="_blank" rel="noreferrer">
            Check out record
          </a>
          <a href={evidenceBundlePath} className="record-link" target="_blank" rel="noreferrer">
            Download evidence bundle
          </a>
          <a href={templateUrl} className="record-link" target="_blank" rel="noreferrer">
            Open template
          </a>
        </div>
      </div>

      <form
        className="candidate-contribution-form"
        data-paused={CANDIDATE_CONTRIBUTIONS_PAUSED}
        onSubmit={handleSubmit}
      >
        {CANDIDATE_CONTRIBUTIONS_PAUSED && (
          <div className="contribution-paused-note" role="status">
            <strong>Check-in paused</strong>
            <span>{CANDIDATE_CONTRIBUTIONS_PAUSED_MESSAGE}</span>
          </div>
        )}

        <fieldset className="contribution-form-fieldset" disabled={CANDIDATE_CONTRIBUTIONS_PAUSED || isSubmitting}>
          <div className="contribution-form-grid">
            <label>
              <span>Contribution</span>
              <select value={contributionType} onChange={(event) => setContributionType(event.target.value as typeof contributionType)}>
                {CONTRIBUTION_TYPES.map((value) => (
                  <option key={value} value={value}>
                    {value.replaceAll('_', ' ')}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span>Relation</span>
              <select value={relation} onChange={(event) => setRelation(event.target.value as typeof relation)}>
                {RELATIONS.map((value) => (
                  <option key={value} value={value}>
                    {value.replaceAll('_', ' ')}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span>Route</span>
              <select value={requestedAction} onChange={(event) => setRequestedAction(event.target.value as typeof requestedAction)}>
                {ACTIONS.map((value) => (
                  <option key={value} value={value}>
                    {value.replaceAll('_', ' ')}
                  </option>
                ))}
              </select>
            </label>
          </div>

          <div className="contribution-form-grid two">
            <label>
              <span>Name</span>
              <input value={name} onChange={(event) => setName(event.target.value)} placeholder="Your name or group" />
            </label>
            <label>
              <span>Handle</span>
              <input value={handle} onChange={(event) => setHandle(event.target.value)} placeholder="@handle, ORCID, or lab" />
            </label>
          </div>

          <div className="contribution-form-grid two">
            <label>
              <span>Contact</span>
              <input
                value={contact}
                onChange={(event) => setContact(event.target.value)}
                placeholder="email or durable contact"
                required
              />
            </label>
          </div>

          <label>
            <span>Title</span>
            <input value={title} onChange={(event) => setTitle(event.target.value)} placeholder="Short label for the contribution" required />
          </label>
          <label>
            <span>Summary</span>
            <textarea
              value={summary}
              onChange={(event) => setSummary(event.target.value)}
              placeholder="What should TWOG review?"
              required
              rows={3}
            />
          </label>
          <label>
            <span>Claim or question</span>
            <textarea
              value={claim}
              onChange={(event) => setClaim(event.target.value)}
              placeholder="What claim does this support, challenge, correct, or extend?"
              required
              rows={3}
            />
          </label>
          <label>
            <span>Targeted claim or section</span>
            <input
              value={targetedSection}
              onChange={(event) => setTargetedSection(event.target.value)}
              placeholder="Example: rationale paragraph, C12 citation, MD setup, risk note"
            />
          </label>
          <label>
            <span>Method notes</span>
            <textarea
              value={methodNotes}
              onChange={(event) => setMethodNotes(event.target.value)}
              placeholder="How did you produce or evaluate this contribution?"
              required
              rows={3}
            />
          </label>

          <div className="contribution-form-grid two">
            <label>
              <span>Evidence refs</span>
              <input
                value={evidenceRefs}
                onChange={(event) => setEvidenceRefs(event.target.value)}
                placeholder="C1, C20, chunk IDs, DOI refs"
              />
            </label>
            <label>
              <span>Artifact refs</span>
              <input
                value={artifactRefs}
                onChange={(event) => setArtifactRefs(event.target.value)}
                placeholder="CID, hash, URL, run ID"
              />
            </label>
          </div>

          <div className="contribution-form-grid two">
            <label>
              <span>Evidence title</span>
              <input value={evidenceTitle} onChange={(event) => setEvidenceTitle(event.target.value)} placeholder="Paper, dataset, artifact" />
            </label>
            <label>
              <span>Evidence URL</span>
              <input value={evidenceUrl} onChange={(event) => setEvidenceUrl(event.target.value)} placeholder="https://..." type="url" />
            </label>
          </div>

          <label>
            <span>Limits or conflicts</span>
            <textarea
              value={limitations}
              onChange={(event) => setLimitations(event.target.value)}
              placeholder="Known caveats, uncertainty, conflicts, or replication limits"
              rows={2}
            />
          </label>
        </fieldset>

        <button type="submit" disabled={CANDIDATE_CONTRIBUTIONS_PAUSED || isSubmitting}>
          {CANDIDATE_CONTRIBUTIONS_PAUSED ? 'Intake paused' : isSubmitting ? 'Submitting...' : 'Submit to intake queue'}
        </button>
        {result && (
          <div className={result.contribution_id ? 'contribution-result success' : 'contribution-result error'}>
            {result.contribution_id ? (
              <>
                <strong>Queued: {result.contribution_id}</strong>
                <span>{result.status}</span>
                {result.contribution_content_hash && <code>{result.contribution_content_hash.slice(0, 16)}</code>}
                {result.status_url && (
                  <a href={result.status_url} target="_blank" rel="noreferrer">
                    Track contribution
                  </a>
                )}
              </>
            ) : (
              <>
                <strong>{result.error}</strong>
                <span>{result.message ?? result.details?.join(' ')}</span>
              </>
            )}
          </div>
        )}
      </form>
    </section>
  );
}
