import Link from 'next/link';
import styles from './ProofFlowDiagram.module.css';
import type { ProofFlowSnapshot } from '@/lib/proof-flow';

interface Props {
  snapshot: ProofFlowSnapshot;
}

// CSS color tokens come straight from globals.css — we read them through
// var(--token) so the Sankey re-themes when the design system changes.
const COLOR = {
  positive: 'var(--green)',
  inflight: 'var(--accent-ink)',
  warn: 'var(--amber)',
  cool: 'var(--gray-400)',
  source: 'var(--gray-600)',
} as const;

type BandColor = keyof typeof COLOR;

interface Band {
  id: string;
  label: string;
  count: number;
  color: BandColor;
}

// Layout constants. Diagram lives in a 1000x600 viewBox so the SVG scales
// with the wrapper width.
const VB_WIDTH = 1000;
const VB_HEIGHT = 600;

const COLUMN_LABEL_Y = 38;
const COLUMN_TOP = 80;
const COLUMN_BOTTOM = VB_HEIGHT - 40;
const COLUMN_HEIGHT = COLUMN_BOTTOM - COLUMN_TOP;

const SOURCE_X = 80;
const SOURCE_W = 180;
const MIDDLE_X = 460;
const MIDDLE_W = 100;
const VERDICT_X = 760;
const VERDICT_W = 200;

const MIN_BAND_HEIGHT = 22;
const BAND_GAP = 14;

interface PositionedBand extends Band {
  x: number;
  width: number;
  y: number;
  height: number;
}

// Lay out a column of bands so the *total* count is what scales — but
// every band, even one with zero hits, still appears with a minimum
// height. That way the pipeline is legible even before traffic exists.
function layoutColumn(bands: Band[], x: number, width: number): PositionedBand[] {
  const usableHeight = COLUMN_HEIGHT - BAND_GAP * (bands.length - 1);

  // If every band is zero, distribute evenly with min heights.
  const positives = bands.map((b) => Math.max(b.count, 0));
  const sumPositives = positives.reduce((a, b) => a + b, 0);

  // Reserve min height for each band, then distribute the remainder by count.
  const minTotal = MIN_BAND_HEIGHT * bands.length;
  const remainder = Math.max(0, usableHeight - minTotal);

  const heights = bands.map((band, idx) => {
    if (sumPositives === 0) return Math.max(MIN_BAND_HEIGHT, usableHeight / bands.length);
    return MIN_BAND_HEIGHT + (positives[idx] / sumPositives) * remainder;
  });

  let cursor = COLUMN_TOP;
  return bands.map((band, idx) => {
    const height = heights[idx];
    const positioned: PositionedBand = {
      ...band,
      x,
      width,
      y: cursor,
      height,
    };
    cursor += height + BAND_GAP;
    return positioned;
  });
}

// Build a smooth Sankey ribbon between (x0, y0) and (x1, y1) with the
// given source/target widths. The ribbon is two cubic Bezier edges —
// one along the top, one along the bottom — closed into a single path.
function ribbonPath(
  x0: number,
  y0: number,
  h0: number,
  x1: number,
  y1: number,
  h1: number
): string {
  const cx0 = x0 + (x1 - x0) * 0.5;
  const cx1 = x0 + (x1 - x0) * 0.5;
  // Top edge from (x0, y0) to (x1, y1)
  const top = `M ${x0} ${y0} C ${cx0} ${y0}, ${cx1} ${y1}, ${x1} ${y1}`;
  // Bottom edge from (x1, y1+h1) back to (x0, y0+h0)
  const bottom = `L ${x1} ${y1 + h1} C ${cx1} ${y1 + h1}, ${cx0} ${y0 + h0}, ${x0} ${y0 + h0} Z`;
  return `${top} ${bottom}`;
}

// Allocate a vertical slice within a source band proportional to a count.
// Returns the y offset and height for the ribbon's source-side anchor.
function sliceWithin(
  parent: { y: number; height: number },
  cursor: number,
  count: number,
  totalCount: number
): { y: number; height: number } {
  if (totalCount <= 0) {
    return { y: parent.y + cursor, height: 0 };
  }
  const fraction = Math.max(0, count) / totalCount;
  const height = parent.height * fraction;
  return { y: parent.y + cursor, height };
}

export function ProofFlowDiagram({ snapshot }: Props) {
  const empty =
    snapshot.packets_total === 0 && snapshot.capsules_total === 0;

  if (empty) {
    return (
      <section className={styles.empty} aria-label="Proof flow empty state">
        <span className={styles.emptyKicker}>Proof flow</span>
        <h2 className={styles.emptyTitle}>
          No capsules submitted yet. The pipeline is waiting for its first checkout.
        </h2>
        <p className={styles.emptyBody}>
          Once contributors pick up open packets and submit proof capsules, this
          diagram will show how each capsule moves through review and lands on
          one of the terminal verdicts. Pick up a packet on the network to
          start the flow.
        </p>
        <Link href="/network" className={styles.emptyCta}>
          See open packets →
        </Link>
      </section>
    );
  }

  const sourceBands: Band[] = [
    {
      id: 'work_packets_open',
      label: 'Open packets',
      count: snapshot.work_packets_open,
      color: 'source',
    },
    {
      id: 'work_packets_in_progress',
      label: 'In-progress packets',
      count: snapshot.work_packets_in_progress,
      color: 'inflight',
    },
  ];

  const middleBands: Band[] = [
    {
      id: 'capsules_submitted',
      label: 'Submitted',
      count: snapshot.capsules_submitted,
      color: 'inflight',
    },
    {
      id: 'capsules_in_review',
      label: 'In review',
      count: snapshot.capsules_in_review,
      color: 'inflight',
    },
    {
      id: 'capsules_needs_changes',
      label: 'Needs changes',
      count: snapshot.capsules_needs_changes,
      color: 'warn',
    },
  ];

  const verdictBands: Band[] = [
    {
      id: 'capsules_accepted',
      label: 'Accepted',
      count: snapshot.capsules_accepted,
      color: 'positive',
    },
    {
      id: 'capsules_routed_to_validation',
      label: 'Routed → validation',
      count: snapshot.capsules_routed_to_validation,
      color: 'positive',
    },
    {
      id: 'capsules_routed_to_compute_review',
      label: 'Routed → compute review',
      count: snapshot.capsules_routed_to_compute_review,
      color: 'positive',
    },
    {
      id: 'capsules_rejected',
      label: 'Rejected',
      count: snapshot.capsules_rejected,
      color: 'cool',
    },
    {
      id: 'capsules_archived',
      label: 'Archived',
      count: snapshot.capsules_archived,
      color: 'cool',
    },
  ];

  const sourceCol = layoutColumn(sourceBands, SOURCE_X, SOURCE_W);
  const middleCol = layoutColumn(middleBands, MIDDLE_X, MIDDLE_W);
  const verdictCol = layoutColumn(verdictBands, VERDICT_X, VERDICT_W);

  const submitted = middleCol[0];
  const inReview = middleCol[1];

  // Source -> Submitted ribbons. The submitted band is the target for
  // both source bands; we split it vertically by source counts.
  const sourceTotal =
    snapshot.work_packets_open + snapshot.work_packets_in_progress;
  let submittedCursor = 0;
  const sourceRibbons = sourceCol.map((src) => {
    const slice = sliceWithin(
      submitted,
      submittedCursor,
      src.count,
      sourceTotal
    );
    submittedCursor += slice.height;
    return {
      id: `r-${src.id}`,
      color: src.color,
      // Source band right edge -> middle band left edge
      path: ribbonPath(
        src.x + src.width,
        src.y,
        src.height,
        submitted.x,
        slice.y,
        slice.height
      ),
    };
  });

  // Submitted and in_review live in the same column; the visual flow
  // between them is implied by vertical stacking + a chevron, not by a
  // horizontal ribbon (a same-column ribbon would degenerate to a flat
  // strip and add visual noise).
  void submitted;

  // In review -> verdicts. Split inReview vertically into 5 slices,
  // proportional to each verdict count (excluding needs_changes which is
  // terminal in the middle column).
  const verdictTotal =
    snapshot.capsules_accepted +
    snapshot.capsules_routed_to_validation +
    snapshot.capsules_routed_to_compute_review +
    snapshot.capsules_rejected +
    snapshot.capsules_archived;
  let verdictCursor = 0;
  const verdictRibbons = verdictCol.map((v) => {
    const slice = sliceWithin(inReview, verdictCursor, v.count, verdictTotal);
    verdictCursor += slice.height;
    return {
      id: `r-${v.id}`,
      color: v.color,
      path: ribbonPath(
        inReview.x + inReview.width,
        slice.y,
        slice.height,
        v.x,
        v.y,
        v.height
      ),
    };
  });

  // Tiny horizontal connector between submitted and in_review (since both
  // sit in the same column) is already drawn by submittedToReview above —
  // it'll be a near-flat ribbon because cx0/cx1 are at the midpoint.

  const allBands = [...sourceCol, ...middleCol, ...verdictCol];

  return (
    <section className={styles.wrap} aria-label="Proof network flow diagram">
      <header className={styles.header}>
        <div className={styles.headerCopy}>
          <p className={styles.headerNote}>Pipeline · generated {snapshot.generated_at.slice(0, 19).replace('T', ' ')}Z</p>
          <h2 className={styles.headerTitle}>
            Width of each band reflects the number of packets or capsules
            currently in that bucket.
          </h2>
        </div>
        <div className={styles.legend} aria-label="Color legend">
          <span className={styles.legendItem}>
            <span className={styles.legendSwatch} style={{ background: COLOR.inflight }} />
            in flight
          </span>
          <span className={styles.legendItem}>
            <span className={styles.legendSwatch} style={{ background: COLOR.positive }} />
            positive verdict
          </span>
          <span className={styles.legendItem}>
            <span className={styles.legendSwatch} style={{ background: COLOR.warn }} />
            needs changes
          </span>
          <span className={styles.legendItem}>
            <span className={styles.legendSwatch} style={{ background: COLOR.cool }} />
            archived / rejected
          </span>
        </div>
      </header>

      <div className={styles.diagramFrame}>
        <svg
          className={styles.svg}
          viewBox={`0 0 ${VB_WIDTH} ${VB_HEIGHT}`}
          role="img"
          aria-label="Sankey diagram of the proof network pipeline"
        >
          {/* Column headers */}
          <text x={SOURCE_X} y={COLUMN_LABEL_Y} className={styles.columnLabel}>
            Source · work packets
          </text>
          <text x={MIDDLE_X} y={COLUMN_LABEL_Y} className={styles.columnLabel}>
            Capsule review
          </text>
          <text x={VERDICT_X} y={COLUMN_LABEL_Y} className={styles.columnLabel}>
            Terminal verdicts
          </text>

          {/* Ribbons render BEFORE bands so the bands cap the ribbon ends */}
          <g aria-hidden="true">
            {sourceRibbons.map((r) => (
              <path
                key={r.id}
                d={r.path}
                fill={COLOR[r.color]}
                fillOpacity={0.18}
                stroke={COLOR[r.color]}
                strokeOpacity={0.35}
                strokeWidth={0.5}
              />
            ))}
            {verdictRibbons.map((r) => (
              <path
                key={r.id}
                d={r.path}
                fill={COLOR[r.color]}
                fillOpacity={0.22}
                stroke={COLOR[r.color]}
                strokeOpacity={0.4}
                strokeWidth={0.5}
              />
            ))}
          </g>

          {/* Bands */}
          <g>
            {allBands.map((band) => {
              const color = COLOR[band.color];
              const textInside = band.width >= 140;
              const labelX = textInside ? band.x + 12 : band.x + band.width + 10;
              const labelAnchor = textInside ? 'start' : 'start';
              const showCountInside = band.height >= 32 && band.width >= 80;
              return (
                <g key={band.id}>
                  <rect
                    x={band.x}
                    y={band.y}
                    width={band.width}
                    height={band.height}
                    fill={color}
                    fillOpacity={0.85}
                    stroke={color}
                    strokeWidth={1}
                  />
                  <text
                    x={labelX}
                    y={band.y + 14}
                    textAnchor={labelAnchor}
                    className={styles.bandLabel}
                    style={textInside ? { fill: '#fff' } : undefined}
                  >
                    {band.label}
                  </text>
                  {showCountInside ? (
                    <text
                      x={band.x + band.width - 10}
                      y={band.y + band.height - 8}
                      textAnchor="end"
                      className={styles.bandCount}
                      style={{ fill: '#fff' }}
                    >
                      {band.count}
                    </text>
                  ) : (
                    <text
                      x={labelX}
                      y={band.y + 28}
                      textAnchor={labelAnchor}
                      className={styles.bandCount}
                    >
                      {band.count}
                    </text>
                  )}
                </g>
              );
            })}
          </g>
        </svg>
      </div>

      <div className={styles.totalsRow}>
        <div className={styles.totalCell}>
          <span className={styles.totalLabel}>Packets in source</span>
          <span className={styles.totalValue}>{snapshot.packets_total}</span>
        </div>
        <div className={styles.totalCell}>
          <span className={styles.totalLabel}>Capsules in flight</span>
          <span className={styles.totalValue}>
            {snapshot.capsules_submitted +
              snapshot.capsules_in_review}
          </span>
        </div>
        <div className={styles.totalCell}>
          <span className={styles.totalLabel}>Positive verdicts</span>
          <span className={styles.totalValue}>
            {snapshot.capsules_accepted +
              snapshot.capsules_routed_to_validation +
              snapshot.capsules_routed_to_compute_review}
          </span>
        </div>
        <div className={styles.totalCell}>
          <span className={styles.totalLabel}>Needs changes</span>
          <span className={styles.totalValue}>{snapshot.capsules_needs_changes}</span>
        </div>
        <div className={styles.totalCell}>
          <span className={styles.totalLabel}>Archived / rejected</span>
          <span className={styles.totalValue}>
            {snapshot.capsules_rejected + snapshot.capsules_archived}
          </span>
        </div>
      </div>
    </section>
  );
}
