import Link from 'next/link';
import {
  getLeaderboard,
  isLeaderboardStorageConfigured,
  LeaderboardEntry,
  LeaderboardWindow,
} from '@/lib/leaderboard';

export const dynamic = 'force-dynamic';
export const revalidate = 30;

export const metadata = {
  title: 'Leaderboard — TWOG Proof Network',
  description:
    'Top contributors by proof points on the TWOG Proof Network. Accepted and routed proof capsules earn signal; reputation reflects scientific work that survived operator review.',
};

const WINDOW_LABELS: Record<LeaderboardWindow, string> = {
  all_time: 'All time',
  last_30_days: 'Last 30 days',
  last_7_days: 'Last 7 days',
};

const ALLOWED: ReadonlyArray<LeaderboardWindow> = ['all_time', 'last_30_days', 'last_7_days'];

const SPECIALTY_LABELS: Record<string, string> = {
  citation_repair: 'Citation repair',
  claim_critique: 'Claim critique',
  evidence_addition: 'Evidence addition',
  omics_note: 'Omics note',
  docking_replication: 'Docking replication',
  md_review: 'MD review',
  validation_proposal: 'Validation proposal',
  demotion_case: 'Demotion case',
  methods_review: 'Methods review',
  freeform: 'Freeform',
};

function pickWindow(raw: string | string[] | undefined): LeaderboardWindow {
  const value = Array.isArray(raw) ? raw[0] : raw;
  if (value && (ALLOWED as readonly string[]).includes(value)) {
    return value as LeaderboardWindow;
  }
  return 'all_time';
}

function formatDate(value: string | null): string {
  if (!value) return '—';
  try {
    return new Date(value).toLocaleDateString(undefined, {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  } catch {
    return value;
  }
}

function timeAgo(value: string | null): string {
  if (!value) return '—';
  const ts = new Date(value).getTime();
  if (!Number.isFinite(ts)) return '—';
  const deltaMs = Date.now() - ts;
  const minutes = Math.round(deltaMs / 60000);
  if (minutes < 1) return 'just now';
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.round(hours / 24);
  if (days < 30) return `${days}d ago`;
  return formatDate(value);
}

function specialtyLabel(type: string | null): string | null {
  if (!type) return null;
  return SPECIALTY_LABELS[type] ?? type.replace(/_/g, ' ');
}

function PodiumCard({ entry }: { entry: LeaderboardEntry }) {
  const rankClass = `podium-card podium-rank-${entry.rank} tier-${entry.tier}`;
  return (
    <article className={rankClass}>
      <div className="podium-rank-marker">
        <span className="podium-rank-number">{entry.rank}</span>
        <span className="podium-rank-label">{entry.rank === 1 ? 'First' : entry.rank === 2 ? 'Second' : 'Third'}</span>
      </div>
      <Link href={entry.profile_url} className="podium-handle">
        {entry.display_name ?? entry.handle}
      </Link>
      <div className="podium-meta">
        <span className="podium-tier-badge">{entry.tier_label}</span>
        {entry.kind !== 'human' ? <span className="podium-kind">{entry.kind}</span> : null}
      </div>
      <div className="podium-points">
        <strong>{entry.proof_points}</strong>
        <span>proof points</span>
      </div>
      <dl className="podium-stats">
        <div>
          <dt>Accepted</dt>
          <dd>{entry.accepted_capsule_count}</dd>
        </div>
        <div>
          <dt>Routed</dt>
          <dd>{entry.routed_capsule_count}</dd>
        </div>
        <div>
          <dt>Candidates</dt>
          <dd>{entry.candidate_count}</dd>
        </div>
      </dl>
      {entry.primary_specialty ? (
        <div className="podium-specialty">
          <span className="lab-label">Specialty</span>
          <span>{specialtyLabel(entry.primary_specialty)}</span>
        </div>
      ) : null}
      <div className="podium-handle-tag">{entry.handle}</div>
    </article>
  );
}

interface PageProps {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}

export default async function LeaderboardPage({ searchParams }: PageProps) {
  const params = await searchParams;
  const windowKey = pickWindow(params.window);

  if (!isLeaderboardStorageConfigured()) {
    return (
      <div className="site-shell page-shell">
        <section className="leaderboard-hero">
          <p className="section-kicker">TWOG / Proof Network / Leaderboard</p>
          <h1>Top contributors</h1>
          <p className="leaderboard-storage-empty">
            The hosted Neon/Postgres database is not configured for this environment.
            Once it&apos;s connected, this page will rank contributors by proof
            points across the configured time window.
          </p>
        </section>
      </div>
    );
  }

  const data = await getLeaderboard(windowKey, { limit: 25, risingLimit: 6 });
  const hasEntries = data.entries.length > 0;
  const podium = data.entries.slice(0, 3);
  const rest = data.entries.slice(3);
  const totals = data.network_totals;
  const totalCapsules = totals.total_accepted_capsules + totals.total_routed_capsules;

  return (
    <div className="site-shell page-shell leaderboard-shell">
      <section className="leaderboard-hero">
        <div className="leaderboard-hero-copy">
          <p className="section-kicker">TWOG / Proof Network / Leaderboard</p>
          <h1>The work, ranked.</h1>
          <p>
            Every proof point you see came from a capsule a reviewer accepted.
            <strong> Accepted capsules earn ~100 points</strong>; capsules routed
            downstream to validation or compute review earn ~90. Tiers reflect
            volume, specialty, and candidate breadth — see the methods row
            below for how each tier is unlocked.
          </p>
        </div>
        <div className="leaderboard-network-stats" aria-label="Network totals">
          <div>
            <span className="lab-label">Contributors</span>
            <strong>{totals.contributors_ranked}</strong>
          </div>
          <div>
            <span className="lab-label">Proof points awarded</span>
            <strong>{totals.total_proof_points}</strong>
          </div>
          <div>
            <span className="lab-label">Accepted capsules</span>
            <strong>{totalCapsules}</strong>
          </div>
          <div>
            <span className="lab-label">Candidates touched</span>
            <strong>{totals.candidates_touched}</strong>
          </div>
        </div>
      </section>

      <nav className="leaderboard-window-nav" aria-label="Leaderboard window">
        {ALLOWED.map((value) => (
          <Link
            key={value}
            href={value === 'all_time' ? '/leaderboard' : `/leaderboard?window=${value}`}
            className={`leaderboard-window-pill${value === data.window ? ' active' : ''}`}
          >
            {WINDOW_LABELS[value]}
          </Link>
        ))}
        <span className="leaderboard-window-meta">
          generated {timeAgo(data.generated_at)}
        </span>
      </nav>

      {!hasEntries ? (
        <section className="leaderboard-empty">
          <p className="section-kicker">No accepted work in this window yet</p>
          <p>
            Once a reviewer accepts a proof capsule, the contributor will appear
            here with their proof points and tier.
          </p>
          <Link href="/network" className="network-cta primary">
            See open packets
          </Link>
        </section>
      ) : (
        <>
          {podium.length > 0 ? (
            <section className="leaderboard-podium">
              {podium.map((entry) => (
                <PodiumCard key={entry.handle} entry={entry} />
              ))}
            </section>
          ) : null}

          {rest.length > 0 ? (
            <section className="leaderboard-list-section">
              <div className="leaderboard-list-heading">
                <p className="section-kicker">The rest of the field</p>
              </div>
              <ol className="leaderboard-list" start={4}>
                {rest.map((entry) => (
                  <li key={entry.handle} className={`leaderboard-row tier-${entry.tier}`}>
                    <div className="leaderboard-row-rank">#{entry.rank}</div>
                    <div className="leaderboard-row-body">
                      <div className="leaderboard-row-headline">
                        <Link href={entry.profile_url} className="leaderboard-row-handle">
                          {entry.display_name ?? entry.handle}
                        </Link>
                        <span className="leaderboard-row-tier">{entry.tier_label}</span>
                      </div>
                      <div className="leaderboard-row-stats">
                        <span>
                          <strong>{entry.proof_points}</strong> pp
                        </span>
                        <span>
                          <strong>{entry.accepted_capsule_count}</strong> accepted
                        </span>
                        <span>
                          <strong>{entry.routed_capsule_count}</strong> routed
                        </span>
                        <span>
                          <strong>{entry.candidate_count}</strong> candidates
                        </span>
                        {entry.median_rubric_score !== null ? (
                          <span title="Median weighted-rubric quality across accepted capsules">
                            <strong>{Math.round(entry.median_rubric_score * 100)}</strong>/100 rubric
                          </span>
                        ) : null}
                        {entry.primary_specialty ? (
                          <span className="leaderboard-row-specialty">
                            {specialtyLabel(entry.primary_specialty)}
                          </span>
                        ) : null}
                        {entry.kind !== 'human' ? (
                          <span className="leaderboard-row-kind">{entry.kind}</span>
                        ) : null}
                      </div>
                    </div>
                    <div className="leaderboard-row-side">
                      <Link
                        href={entry.profile_url}
                        className="leaderboard-row-handle-tag"
                      >
                        {entry.handle}
                      </Link>
                      <div className="leaderboard-row-joined">joined {timeAgo(entry.first_event_at)}</div>
                    </div>
                  </li>
                ))}
              </ol>
            </section>
          ) : null}
        </>
      )}

      {data.rising.length > 0 ? (
        <section className="leaderboard-rising-section">
          <div className="leaderboard-rising-heading">
            <p className="section-kicker">Rising this week</p>
            <h2>New contributors whose first proof points landed in the last 7 days.</h2>
          </div>
          <div className="leaderboard-rising-grid">
            {data.rising.map((entry) => (
              <article key={entry.handle} className={`leaderboard-rising-card tier-${entry.tier}`}>
                <Link href={entry.profile_url} className="leaderboard-rising-handle">
                  {entry.display_name ?? entry.handle}
                </Link>
                <div className="leaderboard-rising-meta">
                  <span className="leaderboard-rising-tier">{entry.tier_label}</span>
                  <span>·</span>
                  <span>{entry.proof_points} pp</span>
                </div>
                <div className="leaderboard-rising-time">first event {timeAgo(entry.first_event_at)}</div>
              </article>
            ))}
          </div>
        </section>
      ) : null}

      <section className="leaderboard-methods">
        <p className="section-kicker">How tiers unlock</p>
        <ol className="leaderboard-tier-ladder">
          <li><strong>Observer</strong> — no accepted work yet.</li>
          <li><strong>Scout</strong> — first accepted capsule lands.</li>
          <li><strong>Citation Repairer</strong> — 3+ accepted, at least one citation_repair.</li>
          <li><strong>Record Builder</strong> — 5+ accepted across 2+ candidates.</li>
          <li><strong>Replication Contributor</strong> — accepted docking_replication or md_review.</li>
          <li><strong>Validation Contributor</strong> — accepted validation_proposal.</li>
          <li><strong>Trusted Reviewer</strong> — 10+ accepted total.</li>
          <li><strong>Proof Partner</strong> — 20+ accepted and 1500+ proof points.</li>
        </ol>
      </section>
    </div>
  );
}
