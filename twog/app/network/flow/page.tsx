import Link from 'next/link';
import {
  getProofFlowSnapshot,
  isProofFlowStorageConfigured,
  type ProofFlowSnapshot,
} from '@/lib/proof-flow';
import { ProofFlowDiagram } from '@/components/proof-network/ProofFlowDiagram';

export const dynamic = 'force-dynamic';
export const revalidate = 30;

export const metadata = {
  title: 'Proof flow — TWOG Proof Network',
  description:
    'A live Sankey-style view of how work moves through the TWOG Proof Network — from open packets, through review, to a terminal verdict.',
};

function emptySnapshotShell(): ProofFlowSnapshot {
  return {
    schema_version: 'twog-proof-flow-v1',
    generated_at: new Date().toISOString(),
    storage_configured: false,
    work_packets_open: 0,
    work_packets_in_progress: 0,
    capsules_submitted: 0,
    capsules_in_review: 0,
    capsules_needs_changes: 0,
    capsules_accepted: 0,
    capsules_routed_to_validation: 0,
    capsules_routed_to_compute_review: 0,
    capsules_rejected: 0,
    capsules_archived: 0,
    capsules_total: 0,
    packets_total: 0,
  };
}

async function load(): Promise<{ snapshot: ProofFlowSnapshot; error?: string }> {
  if (!isProofFlowStorageConfigured()) {
    return { snapshot: emptySnapshotShell() };
  }
  try {
    return { snapshot: await getProofFlowSnapshot() };
  } catch (error) {
    console.error('proof flow snapshot failed', error);
    return {
      snapshot: emptySnapshotShell(),
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

export default async function ProofFlowPage() {
  const { snapshot, error } = await load();

  return (
    <div className="site-shell page-shell">
      <section className="network-hero">
        <div className="network-hero-copy">
          <p className="section-kicker">TWOG / Proof Network / Flow</p>
          <h1>How work flows through the proof network.</h1>
          <p>
            Every accepted capsule starts as a bounded research packet on a
            candidate record. This diagram tracks the path: contributors check
            out open packets, submit proof capsules, an operator picks each one
            up for review, and the capsule lands on a terminal verdict. Width
            of each band is proportional to the number of items currently in
            that bucket — a glance tells you where the pipeline is loaded.
          </p>
          <div className="network-hero-actions">
            <Link href="/network" className="network-cta primary">
              Back to mission board
            </Link>
            <Link href="/leaderboard" className="network-cta">
              Leaderboard
            </Link>
            <Link href="/api/network/flow" className="network-cta">
              Raw JSON
            </Link>
          </div>
        </div>
      </section>

      {!snapshot.storage_configured ? (
        <section className="network-storage-banner">
          <p>
            The hosted Neon/Postgres database is not configured for this
            environment. The flow diagram will populate once
            {' '}
            <code>NEON_DATABASE_URL</code> (or an alias) is set and real
            packets and capsules exist.
          </p>
        </section>
      ) : null}

      {error ? (
        <section className="network-storage-banner">
          <p>
            Failed to load the proof flow snapshot: <code>{error}</code>. The
            page will refresh on the next request.
          </p>
        </section>
      ) : null}

      <ProofFlowDiagram snapshot={snapshot} />

      <section className="network-agents" aria-label="Read the data">
        <div className="network-section-heading">
          <p className="section-kicker">Read the data</p>
          <h2>The same snapshot, as JSON</h2>
          <p>
            The diagram above renders directly from this object. Agents can
            fetch the same payload from <code>/api/network/flow</code> and
            build their own dashboards without scraping HTML.
          </p>
        </div>
        <details
          style={{
            border: '1px solid #d8d6cf',
            background: '#f7f5ee',
            padding: '12px 16px',
            borderRadius: 2,
          }}
        >
          <summary
            style={{
              cursor: 'pointer',
              fontFamily: "'IBM Plex Mono', monospace",
              fontSize: '0.85rem',
              letterSpacing: '0.04em',
              textTransform: 'uppercase',
            }}
          >
            Show raw snapshot
          </summary>
          <pre
            style={{
              background: '#0a0a0a',
              color: '#f4f3ee',
              padding: '20px',
              marginTop: 12,
              overflowX: 'auto',
              fontFamily: "'IBM Plex Mono', monospace",
              fontSize: '0.78rem',
              lineHeight: 1.55,
              border: '1px solid #0a0a0a',
            }}
          >
            {JSON.stringify(snapshot, null, 2)}
          </pre>
        </details>
      </section>
    </div>
  );
}
