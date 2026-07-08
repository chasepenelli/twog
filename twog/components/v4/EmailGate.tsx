'use client';

// The honest "gate": a newsletter / early-access email capture shown in place of the deep experiment
// tier. On submit it POSTs to /api/subscribe (stores the email + sets the twog_email cookie), then
// router.refresh() so the server re-reads the cookie and renders the deep tier. Not a login — a list.

import { useState } from 'react';
import { useRouter } from 'next/navigation';

export function EmailGate({
  source = 'evidence',
  heading,
  blurb,
  mode = 'gate',
}: {
  source?: string;
  heading?: string;
  blurb?: string;
  /** 'gate' reveals a deep tier on success (router.refresh); 'newsletter' shows a thank-you. */
  mode?: 'gate' | 'newsletter';
}) {
  const router = useRouter();
  const [email, setEmail] = useState('');
  const [state, setState] = useState<'idle' | 'busy' | 'done' | 'error'>('idle');

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setState('busy');
    try {
      const r = await fetch('/api/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, source }),
      });
      if (!r.ok) throw new Error('subscribe failed');
      if (mode === 'newsletter') setState('done');
      else router.refresh(); // server re-reads the cookie → deep tier renders in place
    } catch {
      setState('error');
    }
  };

  if (state === 'done') {
    return (
      <div className="v4-gate">
        <div className="v4-gate__kicker">YOU’RE ON THE LIST</div>
        <h3 className="v4-gate__h">Thanks — we’ll be in touch.</h3>
        <p className="v4-gate__p">You’ll get each verdict as it lands, and an invite to sign in when it’s ready.</p>
      </div>
    );
  }

  return (
    <div className="v4-gate">
      <div className="v4-gate__kicker">EARLY ACCESS · NO ACCOUNT NEEDED</div>
      <h3 className="v4-gate__h">{heading ?? 'See the full experiment record.'}</h3>
      <p className="v4-gate__p">
        {blurb ??
          'The pre-registered kill-criterion, the raw readout, and the signed provenance you can verify yourself — plus every new verdict as it lands. Drop your email and we’ll open it up.'}
      </p>
      <form className="v4-gate__form" onSubmit={submit}>
        <input
          type="email"
          required
          placeholder="you@lab.org"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          disabled={state === 'busy'}
          aria-label="Email address"
        />
        <button type="submit" disabled={state === 'busy'}>
          {state === 'busy' ? 'Opening…' : 'Show me →'}
        </button>
      </form>
      {state === 'error' && <p className="v4-gate__err">Something went wrong — try again.</p>}
      <p className="v4-gate__fine">
        A mailing list for early access, not a login. We’ll email you to set up real sign-in once things
        are polished.
      </p>
    </div>
  );
}
