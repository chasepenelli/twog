'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';

export function OperatorLogin({ configured }: { configured: boolean }) {
  const router = useRouter();
  const [token, setToken] = useState('');
  const [state, setState] = useState<'idle' | 'busy' | 'error'>('idle');
  const [msg, setMsg] = useState('');

  if (!configured) {
    return (
      <div className="v4-trust">
        <strong>Operator access isn’t set up.</strong>
        <p>Set <code className="v4-mono">TWOG_OPERATOR_TOKEN</code> to enable the triage board.</p>
      </div>
    );
  }

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setState('busy');
    setMsg('');
    try {
      const r = await fetch('/api/operator/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token }),
      });
      if (r.ok) {
        router.refresh();
        return;
      }
      const j = await r.json().catch(() => ({}));
      setMsg(r.status === 401 ? 'Wrong passphrase.' : j?.message ?? 'Login failed.');
      setState('error');
    } catch {
      setMsg('Something went wrong — try again.');
      setState('error');
    }
  };

  return (
    <form className="v4-form" onSubmit={submit} style={{ maxWidth: 420 }}>
      <label className="v4-form__row">
        <span>Operator passphrase</span>
        <input
          type="password"
          value={token}
          onChange={(e) => setToken(e.target.value)}
          disabled={state === 'busy'}
          autoFocus
          placeholder="passphrase"
        />
      </label>
      {state === 'error' ? <p className="v4-gate__err">{msg}</p> : null}
      <div className="v4-form__actions">
        <button type="submit" disabled={state === 'busy' || !token}>
          {state === 'busy' ? 'Checking…' : 'Enter →'}
        </button>
      </div>
    </form>
  );
}

export function OperatorLogout() {
  const router = useRouter();
  const [busy, setBusy] = useState(false);
  return (
    <button
      className="v4-triage__logout"
      disabled={busy}
      onClick={async () => {
        setBusy(true);
        await fetch('/api/operator/logout', { method: 'POST' }).catch(() => {});
        router.refresh();
      }}
    >
      {busy ? '…' : 'sign out'}
    </button>
  );
}
