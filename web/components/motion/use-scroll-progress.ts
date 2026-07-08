"use client";

/* useScrollProgress — 0..1 document scroll progress for the nav rail + chapter markers. Reads native
   scroll (Lenis drives native scroll, so this stays accurate under smooth scroll too). */

import { useEffect, useState } from "react";

export function useScrollProgress(): number {
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    const compute = () => {
      const el = document.documentElement;
      const max = el.scrollHeight - el.clientHeight;
      setProgress(max > 0 ? Math.min(1, Math.max(0, el.scrollTop / max)) : 0);
    };
    compute();
    window.addEventListener("scroll", compute, { passive: true });
    window.addEventListener("resize", compute);
    return () => {
      window.removeEventListener("scroll", compute);
      window.removeEventListener("resize", compute);
    };
  }, []);

  return progress;
}
