'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';

const ACTIONS: [string, string][] = [
  ['start_triage', 'Start triage'],
  ['accept_for_evidence_review', 'Accept → evidence review'],
  ['accept_for_validation_queue', 'Accept → validation queue'],
  ['accept_for_compute_review', 'Accept → compute review'],
  ['request_more_information', 'Needs more info'],
  ['reject', 'Reject'],
  ['archive', 'Archive'],
];

export function TriageActions({ contributionId, defaultAction }: { contributionId: string; defaultAction: string }) {
  const router = useRouter();
  const [action, setAction] = useState(defaultAction);
  const [notes, setNotes] = useState('');
  const [op, setOp] = useState('');
  const [state, setState] = useState<'idle' | 'busy' | 'error'>('idle');
  const [msg, setMsg] = useState('');

  const apply = async () => {
    setState('busy');
    setMsg('');
    try {
      const r = await fetch(`/api/operator/contributions/${contributionId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, review_notes: notes, operator: op }),
      });
      if (r.ok) {
        router.refresh();
        return;
      }
      const j = await r.json().catch(() => ({}));
      setMsg(j?.error ?? 'Failed — try again.');
      setState('error');
    } catch {
      setMsg('Failed — try again.');
      setState('error');
    }
  };

  return (
    <div className="v4-triage__act">
      <select value={action} onChange={(e) => setAction(e.target.value)} disabled={state === 'busy'} aria-label="Triage action">
        {ACTIONS.map(([v, l]) => (
          <option key={v} value={v}>{l}</option>
        ))}
      </select>
      <input value={op} onChange={(e) => setOp(e.target.value)} placeholder="initials" disabled={state === 'busy'} aria-label="Operator" />
      <input value={notes} onChange={(e) => setNotes(e.target.value)} placeholder="review note" disabled={state === 'busy'} aria-label="Review note" />
      <button onClick={apply} disabled={state === 'busy'}>{state === 'busy' ? '…' : 'Apply →'}</button>
      {state === 'error' ? <span className="v4-gate__err" style={{ marginTop: 0 }}>{msg}</span> : null}
    </div>
  );
}
