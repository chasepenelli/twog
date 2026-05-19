/**
 * /treatments — placeholder page for the Treatment track.
 *
 * Retires the prior actionable-treatments catalog. Full standing track
 * page ships later. For now, a short orientation block.
 */

export const metadata = {
  title: 'Treatment — TWOG',
  description:
    'The Treatment track of the TWOG research journal. What you do when your dog is diagnosed with hemangiosarcoma.',
};

export default function TreatmentsPage() {
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
          Track &middot; 01 of 03 &middot; In focus this issue
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
          Treatment
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
          What you do when your dog is diagnosed.
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
          The standard of care for canine hemangiosarcoma has been roughly
          unchanged for thirty years &mdash; splenectomy to stop the
          bleeding, followed by a chemotherapy drug called doxorubicin.
          Median survival for the most common presentation sits between 140
          and 180 days. For a disease that kills one in five golden
          retrievers and shows up in corgis, German shepherds, and labs at
          rates close behind, that is an answer that has been due for an
          update for a long time.
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
          The Treatment track is where TWOG pressure-tests new combinations
          against that baseline. The pipeline reads every paper on the
          disease, runs molecular simulations against the targets the
          literature keeps circling back to, and puts every direction in
          front of an eight-persona specialist panel before a dollar gets
          spent. The first issue of the journal is a feature on this
          track &mdash; three oral drugs, stacked on purpose, that could
          let a dog stay home instead of going through surgery and IV
          chemo. It releases on Friday.
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
          The full standing track page &mdash; running list of the
          research, the open questions, what the pipeline is screening,
          what the specialist panel has rated go versus not &mdash; is
          drafting and will land on this URL shortly.
        </p>
      </div>
    </div>
  );
}
