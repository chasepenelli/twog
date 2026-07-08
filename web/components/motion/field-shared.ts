/* Shared data for the "field of ideas" hero — the WebGL field and its SVG fallback render the SAME
   real candidates at the SAME deterministic positions, so the fallback is a faithful still of the field.
   A point is a real candidate; its state is its real verdict. No decorative/filler points (honesty). */

export type FieldState = "held" | "testing" | "ruled";
export type FieldPoint = { id: string; name: string; state: FieldState };

/** verdict → point state. standing = held (green), underpowered = still testing (cobalt), refuted = ruled out (ember). */
export function stateForStatus(status: string): FieldState {
  if (status === "standing") return "held";
  if (status === "refuted") return "ruled";
  return "testing";
}

export const STATE_RGB: Record<FieldState, [number, number, number]> = {
  held: [0.24, 0.77, 0.48],
  testing: [0.3, 0.49, 1.0],
  ruled: [1.0, 0.44, 0.39],
};
export const STATE_HEX: Record<FieldState, string> = {
  held: "#3ec27a",
  testing: "#4d7cff",
  ruled: "#ff6f63",
};

/** Stable per-id pseudo-random in [0,1) (FNV-1a). Same id → same position every render (not random). */
export function hashUnit(str: string, salt: number): number {
  let h = (2166136261 ^ salt) >>> 0;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return ((h >>> 0) % 100000) / 100000;
}

/** Deterministic disc position (flattened vertically), shared by the WebGL field and the SVG poster. */
export function discXY(id: string, radius: number): { x: number; y: number } {
  const a = hashUnit(id, 1) * Math.PI * 2;
  const r = Math.sqrt(hashUnit(id, 2)) * radius;
  return { x: Math.cos(a) * r, y: Math.sin(a) * r * 0.62 };
}
