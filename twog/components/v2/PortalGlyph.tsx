// Concentric nested-triangle "portal" glyph (echoes The Nifty Portal's HUD vector). Stroke = currentColor.
export function PortalGlyph({ className }: { className?: string }) {
  const base: [number, number][] = [
    [50, 8],
    [90, 84],
    [10, 84],
  ];
  const c: [number, number] = [50, 58.7];
  const scales = [1, 0.8, 0.6, 0.4, 0.2];
  return (
    <svg viewBox="0 0 100 100" className={className} aria-hidden>
      {scales.map((k, i) => {
        const pts = base
          .map(([x, y]) => `${(c[0] + (x - c[0]) * k).toFixed(2)},${(c[1] + (y - c[1]) * k).toFixed(2)}`)
          .join(' ');
        return <polygon key={i} points={pts} fill="none" stroke="currentColor" strokeWidth={0.5} />;
      })}
      <circle cx={50} cy={58.7} r={1.2} fill="currentColor" />
    </svg>
  );
}
