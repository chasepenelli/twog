/**
 * /tracks/early-detection — placeholder page for the Early Detection track.
 *
 * Full standing track page ships later. For now, a short orientation block.
 */

export const metadata = {
  title: 'Early Detection — TWOG',
  description:
    'Finding canine hemangiosarcoma before it finds you. The Early Detection track of the TWOG research journal.',
};

export default function EarlyDetectionPage() {
  return (
    <div
      style={{
        paddingTop: 120,
        paddingBottom: 120,
        paddingLeft: 24,
        paddingRight: 24,
      }}
    >
      <div style={{ maxWidth: 720, margin: '0 auto' }}>
        <div
          style={{
            fontFamily: 'var(--font-jetbrains-mono), monospace',
            fontSize: '0.68rem',
            letterSpacing: '0.26em',
            textTransform: 'uppercase',
            color: 'var(--gray-500)',
            marginBottom: 20,
          }}
        >
          Track &middot; 02 of 03
        </div>

        <h1
          style={{
            fontFamily: 'var(--font-space-mono), monospace',
            fontWeight: 700,
            fontSize: 'clamp(2.5rem, 6vw, 4.5rem)',
            lineHeight: 0.94,
            letterSpacing: '-0.035em',
            margin: 0,
            color: 'var(--foreground)',
          }}
        >
          Early Detection
        </h1>

        <p
          style={{
            fontFamily: 'var(--font-space-mono), monospace',
            fontSize: '1.15rem',
            lineHeight: 1.55,
            color: 'var(--gray-700)',
            marginTop: 20,
          }}
        >
          Finding it before it finds you.
        </p>

        <p
          style={{
            fontFamily: 'var(--font-space-mono), monospace',
            fontSize: '1rem',
            lineHeight: 1.8,
            color: 'var(--gray-700)',
            marginTop: 32,
          }}
        >
          Most families meet hemangiosarcoma in an emergency &mdash; after a
          splenic mass has already ruptured. The Early Detection track asks a
          different question: what would it take to catch this disease
          months before that moment, so that every treatment option stays on
          the table instead of collapsing to surgery in the middle of the
          night?
        </p>

        <p
          style={{
            fontFamily: 'var(--font-space-mono), monospace',
            fontSize: '1rem',
            lineHeight: 1.8,
            color: 'var(--gray-700)',
            marginTop: 20,
          }}
        >
          The work here centers on cell-free DNA signatures, breed-specific
          risk stratification, and scheduled imaging protocols built around
          the timescales this disease actually runs on. The standing track
          page &mdash; with the running list of the research, the open
          questions, and what the pipeline is screening &mdash; is drafting
          and will land on this URL shortly.
        </p>
      </div>
    </div>
  );
}
