"use client";

import { useEffect, useState } from "react";

/** A never-frozen "M:SS ago" counter — keeps the page feeling live. Resets on a cadence as if a
 * fresh verdict just landed. */
export function LiveAgo({ seedSeconds = 14, resetEvery = 0 }: { seedSeconds?: number; resetEvery?: number }) {
  const [s, setS] = useState(seedSeconds);
  useEffect(() => {
    const id = setInterval(() => {
      setS((x) => (resetEvery && x >= resetEvery ? 0 : x + 1));
    }, 1000);
    return () => clearInterval(id);
  }, [resetEvery]);
  const mm = Math.floor(s / 60);
  const ss = s % 60;
  return <span>{mm}:{String(ss).padStart(2, "0")} ago</span>;
}
