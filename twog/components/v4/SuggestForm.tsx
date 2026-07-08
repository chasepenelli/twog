'use client';

// "Suggest an experiment" — name a drug, target, or question; optional email so we can reach out.
// Posts to /api/suggestions. Public, no account. We log every suggestion; results show up on the ledger.

import { useState } from 'react';

export function SuggestForm() {
  const [idea, setIdea] = useState('');
  const [email, setEmail] = useState('');
  const [state, setState] = useState<'idle' | 'busy' | 'done' | 'error' | 'rate'>('idle');
  const [ref, setRef] = useState<string | null>(null);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setState('busy');
    try {
      const r = await fetch('/api/suggestions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ idea, email }),
      });
      if (r.status === 429) {
        setState('rate');
        return;
      }
      const j = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error('failed');
      setRef(typeof j?.id === 'string' ? j.id : null);
      setState('done');
    } catch {
      setState('error');
    }
  };

  if (state === 'done') {
    return (
      <div className="v4-suggest__done">
        Logged — thank you. We read every suggestion and put the strongest to the test in public; watch the
        ledger to see what runs. {ref ? <span className="v4-mono">ref {ref.slice(0, 8)}</span> : null}
      </div>
    );
  }

  return (
    <form className="v4-suggest" onSubmit={submit}>
      <textarea
        required
        rows={3}
        placeholder="Name a drug, a target, or a question — e.g. “does propranolol engage VEGFR2 in canine HSA?”"
        value={idea}
        onChange={(e) => setIdea(e.target.value)}
        disabled={state === 'busy'}
        aria-label="Your suggestion"
      />
      <div className="v4-suggest__row">
        <input
          type="email"
          placeholder="email (optional — so we can follow up)"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          disabled={state === 'busy'}
          aria-label="Email (optional)"
        />
        <button type="submit" disabled={state === 'busy'}>
          {state === 'busy' ? 'Sending…' : 'Put it to the test →'}
        </button>
      </div>
      {state === 'error' && <p className="v4-gate__err">Something went wrong — try again.</p>}
      {state === 'rate' && (
        <p className="v4-gate__err">You’ve sent a few already — give it a few minutes, then try again.</p>
      )}
    </form>
  );
}
