'use client';

import { FormEvent, useEffect, useMemo, useState } from 'react';

const CONTRIBUTION_TYPES = [
  'evidence',
  'critique',
  'replication',
  'artifact',
  'validation_proposal',
  'compute_result',
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
  snapshotHash: string;
}

interface IntakeStatus {
  storage_configured?: boolean;
  contribution_id?: string;
  status?: string;
  error?: string;
  message?: string;
  details?: string[];
}

export function CandidateContributionPanel({
  candidateId,
  displayId,
  payloadPath,
  snapshotHash,
}: CandidateContributionPanelProps) {
  const submissionUrl = `/api/public-candidates/${candidateId}/contributions`;
  const templateUrl = `/api/public-candidates/${candidateId}/contribution-template`;
  const [storageConfigured, setStorageConfigured] = useState<boolean | null>(null);
  const [contributionType, setContributionType] = useState<(typeof CONTRIBUTION_TYPES)[number]>('evidence');
  const [relation, setRelation] = useState<(typeof RELATIONS)[number]>('extends');
  const [requestedAction, setRequestedAction] = useState<(typeof ACTIONS)[number]>('evidence_review');
  const [name, setName] = useState('');
  const [contact, setContact] = useState('');
  const [title, setTitle] = useState('');
  const [summary, setSummary] = useState('');
  const [claim, setClaim] = useState('');
  const [evidenceUrl, setEvidenceUrl] = useState('');
  const [evidenceTitle, setEvidenceTitle] = useState('');
  const [limitations, setLimitations] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [result, setResult] = useState<IntakeStatus | null>(null);

  useEffect(() => {
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
    if (!contact.trim() || !title.trim() || !summary.trim() || !claim.trim()) {
      setResult({
        error: 'missing_required_fields',
        message: 'Contact, title, summary, and claim/question are required.',
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
            contact,
          },
          title,
          summary,
          claim_or_question: claim,
          relation_to_current_record: relation,
          evidence,
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
          Contributions do not edit the public record directly. They enter a TWOG intake queue for provenance review,
          citation repair, and routing into evidence review, validation planning, or compute review.
        </p>
        <div className="contribution-receipt">
          <span>{displayId}</span>
          <code>{snapshotHash}</code>
          <strong>{storageConfigured === null ? 'checking intake' : storageConfigured ? 'intake online' : 'email fallback'}</strong>
        </div>
        <div className="method-actions">
          <a href={payloadPath} className="record-link">
            Open payload
          </a>
          <a href={templateUrl} className="record-link">
            Open template
          </a>
        </div>
      </div>

      <form className="candidate-contribution-form" onSubmit={handleSubmit}>
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

        <button type="submit" disabled={isSubmitting}>
          {isSubmitting ? 'Submitting...' : 'Submit to intake queue'}
        </button>
        {result && (
          <div className={result.contribution_id ? 'contribution-result success' : 'contribution-result error'}>
            {result.contribution_id ? (
              <>
                <strong>Queued: {result.contribution_id}</strong>
                <span>{result.status}</span>
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
