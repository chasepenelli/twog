/* FieldPoster — the static SVG twin of the WebGL field: same candidates, same positions, same colors.
   Serves as the reduced-motion / no-WebGL / loading fallback so those users see a faithful still, not a
   blank. aria-hidden (the count + legend beside it carry the meaning in text). */

import { STATE_HEX, discXY, type FieldPoint } from "./field-shared";

export function FieldPoster({ points = [] }: { points?: FieldPoint[] }) {
  const W = 620;
  const H = 460;
  const R = 258;
  return (
    <svg viewBox={`0 0 ${W} ${H}`} width="100%" height="100%" aria-hidden style={{ display: "block" }}>
      {points.map((p) => {
        const { x, y } = discXY(p.id, R);
        const ruled = p.state === "ruled";
        return (
          <circle
            key={p.id}
            cx={W / 2 + x}
            cy={H / 2 + y}
            r={ruled ? 1.7 : 2.6}
            fill={STATE_HEX[p.state]}
            opacity={ruled ? 0.4 : 0.88}
          />
        );
      })}
    </svg>
  );
}
