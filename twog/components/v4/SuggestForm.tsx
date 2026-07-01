'use client';

// "Suggest an experiment" — name a drug, target, or question; optional email to hear the result.
// Posts to /api/suggestions. Public, no account.

import { useState } from 'react';

export function SuggestForm() {
  const [idea, setIdea] = useState('');
  const [email, setEmail] = useState('');
  const [state, setState] = useState<'idle' | 'busy' | 'done' | 'error'>('idle');

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setState('busy');
    try {
      const r = await fetch('/api/suggestions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ idea, email }),
      });
      if (!r.ok) throw new Error('failed');
      setState('done');
    } catch {
      setState('error');
    }
  };

  if (state === 'done') {
    return (
      <div className="v4-suggest__done">
        Got it — thank you. If you left an email, we’ll tell you when it goes to the test.
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
          placeholder="email (optional — to hear the result)"
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
    </form>
  );
}
