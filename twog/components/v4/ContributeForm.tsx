'use client';

// "Contribute to this idea" — the hands-on Contribute lane. Submits a bounded, structured contribution
// packet (evidence / critique / replication / …) to /api/public-candidates/[id]/contributions, which
// queues it in Neon candidate_contribution_intake for human triage. Nothing here changes the public
// record directly. Intake is env-gated (TWOG_CONTRIBUTIONS_OPEN): when paused, we show an honest notice
// and route people to the lighter Suggest lane instead.

import { useEffect, useState } from 'react';
import Link from 'next/link';

const TYPES: [string, string][] = [
  ['evidence', 'New evidence'],
  ['critique', 'Critique / challenge'],
  ['replication', 'Replication'],
  ['artifact', 'Artifact / data'],
  ['validation_proposal', 'Validation proposal'],
  ['compute_result', 'Compute result'],
];
const RELATIONS: [string, string][] = [
  ['supports', 'supports it'],
  ['challenges', 'challenges it'],
  ['extends', 'extends it'],
  ['corrects', 'corrects it'],
  ['requests_validation', 'asks to validate it'],
  ['requests_compute', 'asks to run compute'],
];
const ACTIONS: [string, string][] = [
  ['evidence_review', 'Review my evidence'],
  ['citation_repair', 'Repair / resolve a citation'],
  ['validation_packet', 'Build a validation packet'],
  ['omics_readout', 'Run an omics readout'],
  ['docking_or_md_review', 'Review docking / MD'],
  ['no_action', 'No action — just logging it'],
];

type Status = 'loading' | 'paused' | 'unconfigured' | 'open' | 'submitting' | 'done' | 'error';

export function ContributeForm({ candidateId }: { candidateId: string }) {
  const [status, setStatus] = useState<Status>('loading');
  const [errors, setErrors] = useState<string[]>([]);
  const [contributionId, setContributionId] = useState<string | null>(null);
  const [f, setF] = useState({
    contribution_type: 'evidence',
    relation_to_current_record: 'supports',
    requested_system_action: 'evidence_review',
    title: '',
    summary: '',
    claim_or_question: '',
    contact: '',
    name: '',
    evidence_url: '',
    evidence_notes: '',
    conflicts_or_limitations: '',
  });
  const set = (k: keyof typeof f) => (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) =>
    setF((prev) => ({ ...prev, [k]: e.target.value }));

  useEffect(() => {
    let live = true;
    (async () => {
      try {
        const r = await fetch(`/api/public-candidates/${candidateId}/contributions`);
        const j = await r.json();
        if (!live) return;
        if (j?.intake_paused) setStatus('paused');
        else if (j?.storage_configured === false) setStatus('unconfigured');
        else setStatus('open');
      } catch {
        if (live) setStatus('unconfigured');
      }
    })();
    return () => {
      live = false;
    };
  }, [candidateId]);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrors([]);
    setStatus('submitting');
    const packet = {
      contribution_type: f.contribution_type,
      contributor: { contact: f.contact, name: f.name || undefined },
      title: f.title,
      summary: f.summary,
      claim_or_question: f.claim_or_question,
      relation_to_current_record: f.relation_to_current_record,
      requested_system_action: f.requested_system_action,
      evidence: f.evidence_url
        ? [{ title: f.title, url: f.evidence_url, notes: f.evidence_notes || undefined }]
        : [],
      artifacts: [],
      conflicts_or_limitations: f.conflicts_or_limitations || undefined,
    };
    try {
      const r = await fetch(`/api/public-candidates/${candidateId}/contributions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(packet),
      });
      const j = await r.json().catch(() => ({}));
      if (r.status === 503) {
        setStatus('paused');
        return;
      }
      if (!r.ok) {
        setErrors(Array.isArray(j?.details) && j.details.length ? j.details : [j?.message ?? 'Submission failed — try again.']);
        setStatus('error');
        return;
      }
      setContributionId(j?.contribution_id ?? null);
      setStatus('done');
    } catch {
      setErrors(['Something went wrong — try again.']);
      setStatus('error');
    }
  };

  if (status === 'loading') {
    return <p className="v4-note">Checking whether check-in is open…</p>;
  }

  if (status === 'paused' || status === 'unconfigured') {
    return (
      <div className="v4-trust">
        <strong>Check-in is paused right now.</strong>
        <p>
          Structured contributions to this record aren’t open yet — we’re tightening the review path first.
          In the meantime you can <Link href="/involved#suggest" style={{ color: 'var(--bone)' }}>suggest a
          drug, target, or question →</Link> and we’ll put it to the test in public.
        </p>
      </div>
    );
  }

  if (status === 'done') {
    return (
      <div className="v4-trust">
        <strong>Logged — thank you.</strong>
        <p>
          Queued for TWOG intake: provenance review, then routing to evidence review, validation, or compute.
          It does not change the public record on its own — a human reviews it first.
          {contributionId ? <> <span className="v4-mono">ref {contributionId.slice(0, 8)}</span></> : null}
        </p>
      </div>
    );
  }

  const busy = status === 'submitting';
  return (
    <form className="v4-form" onSubmit={submit}>
      <div className="v4-form__grid">
        <label className="v4-form__row">
          <span>Kind of contribution</span>
          <select value={f.contribution_type} onChange={set('contribution_type')} disabled={busy}>
            {TYPES.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
          </select>
        </label>
        <label className="v4-form__row">
          <span>It…</span>
          <select value={f.relation_to_current_record} onChange={set('relation_to_current_record')} disabled={busy}>
            {RELATIONS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
          </select>
        </label>
        <label className="v4-form__row">
          <span>What should we do?</span>
          <select value={f.requested_system_action} onChange={set('requested_system_action')} disabled={busy}>
            {ACTIONS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
          </select>
        </label>
      </div>

      <label className="v4-form__row">
        <span>Title</span>
        <input value={f.title} onChange={set('title')} disabled={busy} required minLength={6}
          placeholder="A short, specific title (≥ 6 chars)" />
      </label>

      <label className="v4-form__row">
        <span>Summary</span>
        <textarea value={f.summary} onChange={set('summary')} disabled={busy} required minLength={20} rows={3}
          placeholder="What are you contributing, and why does it matter here? (≥ 20 chars)" />
      </label>

      <label className="v4-form__row">
        <span>The claim or question</span>
        <textarea value={f.claim_or_question} onChange={set('claim_or_question')} disabled={busy} required minLength={12} rows={2}
          placeholder="The specific claim you’re supporting/challenging, or the question you’re asking (≥ 12 chars)" />
      </label>

      <div className="v4-form__grid">
        <label className="v4-form__row">
          <span>Source URL (optional)</span>
          <input value={f.evidence_url} onChange={set('evidence_url')} disabled={busy} type="url"
            placeholder="link to a paper, dataset, or artifact" />
        </label>
        <label className="v4-form__row">
          <span>Note on the source (optional)</span>
          <input value={f.evidence_notes} onChange={set('evidence_notes')} disabled={busy}
            placeholder="what it shows" />
        </label>
      </div>

      <label className="v4-form__row">
        <span>Conflicts / limitations (optional)</span>
        <input value={f.conflicts_or_limitations} onChange={set('conflicts_or_limitations')} disabled={busy}
          placeholder="anything that would change how to read this" />
      </label>

      <div className="v4-form__grid">
        <label className="v4-form__row">
          <span>Your name (optional)</span>
          <input value={f.name} onChange={set('name')} disabled={busy} placeholder="name or handle" />
        </label>
        <label className="v4-form__row">
          <span>Contact</span>
          <input value={f.contact} onChange={set('contact')} disabled={busy} required minLength={6}
            placeholder="email — so we can follow up" />
        </label>
      </div>

      {errors.length ? (
        <ul className="v4-form__err">{errors.map((x) => <li key={x}>{x}</li>)}</ul>
      ) : null}

      <div className="v4-form__actions">
        <button type="submit" disabled={busy}>{busy ? 'Sending…' : 'Submit for review →'}</button>
        <span className="v4-note" style={{ margin: 0 }}>
          Queued for human triage — it never changes the record on its own.
        </span>
      </div>
    </form>
  );
}
