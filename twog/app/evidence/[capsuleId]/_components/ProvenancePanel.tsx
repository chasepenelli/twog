// The deep tier (email-gated): the provenance bundle — content hash, signing status, lineage — the
// "verify it yourself" anchor. Content-addressed so anyone can re-run the inputs and check the hash.

import type { Capsule, Provenance } from '@/lib/types/public-detail';

export function ProvenancePanel({ prov, capsule }: { prov: Provenance | null; capsule: Capsule }) {
  const hash = prov?.content_hash ?? capsule.content_hash ?? '—';
  const signed = prov?.signed ?? false;
  return (
    <div className="v4-plan">
      <div className="v4-dh2" style={{ marginBottom: 0 }}>Provenance — verify / re-derive</div>
      <dl className="v4-kv">
        <dt>content hash</dt>
        <dd className="v4-mono">{hash}</dd>
        <dt>signing</dt>
        <dd>
          {signed
            ? `signed — ${prov?.signer ?? 'twog'}`
            : 'custodial — content-addressed by hash (not yet Ed25519-signed)'}
        </dd>
        <dt>lineage index</dt>
        <dd>{prov?.lineage_index ?? capsule.lineage_index ?? 0}</dd>
        <dt>produced by</dt>
        <dd>{capsule.produced_by ?? '—'}</dd>
        <dt>submitted by</dt>
        <dd>{capsule.submitted_by ?? '—'}</dd>
      </dl>
      <p className="v4-note">
        The content hash pins this exact result: re-run the same inputs and you should get the same hash.
        Clearing the checks is necessary, not sufficient — a human holds the terminal decision, and
        nothing is ever auto-promoted.
      </p>
    </div>
  );
}
