"use client";

/* ScrollRail — a 2px cobalt reading-progress line pinned to the very top of the viewport. Decorative
   (aria-hidden); reads native scroll via useScrollProgress, so it stays accurate under Lenis. */

import { useScrollProgress } from "@/components/motion";

export function ScrollRail() {
  const p = useScrollProgress();
  return (
    <div
      aria-hidden
      style={{ position: "fixed", top: 0, left: 0, right: 0, height: 2, zIndex: 200, pointerEvents: "none", background: "transparent" }}
    >
      <div style={{ height: "100%", width: `${(p * 100).toFixed(2)}%`, background: "var(--accent)", transformOrigin: "left" }} />
    </div>
  );
}
