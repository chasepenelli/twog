import PipelineSchematic from '@/components/schematic/PipelineSchematic';

export const metadata = {
  title: 'Pipeline — TWOG',
  description: 'How the TWOG research machine works, end to end.',
};

export default function SchematicPage() {
  return (
    <div
      style={{
        paddingTop: 120,
        paddingBottom: 100,
        paddingLeft: 24,
        paddingRight: 24,
      }}
    >
      <div style={{ maxWidth: 720, margin: '0 auto 40px' }}>
        <p
          style={{
            fontFamily: 'var(--font-jetbrains-mono), monospace',
            fontSize: '0.68rem',
            letterSpacing: '0.26em',
            textTransform: 'uppercase',
            color: 'var(--gray-500)',
            margin: 0,
          }}
        >
          The machine
        </p>
        <h1
          style={{
            fontFamily: 'var(--font-space-mono), monospace',
            fontWeight: 700,
            fontSize: 'clamp(2.5rem, 6vw, 4.5rem)',
            lineHeight: 0.94,
            letterSpacing: '-0.035em',
            textTransform: 'uppercase',
            color: 'var(--foreground)',
            margin: '12px 0 0',
          }}
        >
          Pipeline
        </h1>
        <p
          style={{
            fontFamily: 'var(--font-space-mono), monospace',
            fontWeight: 400,
            fontSize: '1.05rem',
            lineHeight: 1.55,
            color: 'var(--gray-700)',
            marginTop: 20,
          }}
        >
          A research lab that never sleeps. It reads every new paper the
          moment it&apos;s published, sketches new molecules on the fly, runs
          each one past a panel of eight specialists, and simulates the
          winners atom by atom.
        </p>
      </div>

      <div style={{ width: '100%', margin: '0 auto' }}>
        <PipelineSchematic />
      </div>
    </div>
  );
}
