// Basic asset placeholder — a clearly-marked slot where a Nano Banana Pro asset drops in later.
// Swap for <img src="/v3/aX.png" .../> once generated. Shows the asset id, label, and aspect ratio.

export function Placeholder({
  id,
  ratio,
  label,
  className,
}: {
  id: string;
  ratio: string; // CSS aspect-ratio, e.g. "3 / 4"
  label: string;
  className?: string;
}) {
  return (
    <div className={`v3-ph ${className ?? ''}`} style={{ aspectRatio: ratio }} data-asset={id}>
      <span className="v3-ph__id">{id}</span>
      <span className="v3-ph__label">{label}</span>
      <span className="v3-ph__ratio">{ratio.replace(/\s/g, '')}</span>
    </div>
  );
}
