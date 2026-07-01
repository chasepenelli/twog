"use client";

import { useEffect, useRef, useState } from "react";

/* A tiny count-up. Animates 0 → `to` on first paint, or when scrolled into view (`onScroll`). No deps.
   (There is no Counter primitive in web/ to reuse — this is the one.) Respects prefers-reduced-motion. */

export function CountUp({
  to,
  duration = 900,
  onScroll = false,
  className,
  style,
}: {
  to: number;
  duration?: number;
  onScroll?: boolean;
  className?: string;
  style?: React.CSSProperties;
}) {
  const ref = useRef<HTMLSpanElement>(null);
  const [n, setN] = useState(0);
  const [armed, setArmed] = useState(!onScroll);

  useEffect(() => {
    if (!onScroll || armed) return;
    const el = ref.current;
    if (!el) return;
    const io = new IntersectionObserver(
      (entries) => {
        if (entries[0]?.isIntersecting) {
          setArmed(true);
          io.disconnect();
        }
      },
      { threshold: 0.4 },
    );
    io.observe(el);
    return () => io.disconnect();
  }, [onScroll, armed]);

  useEffect(() => {
    if (!armed) return;
    if (typeof window !== "undefined" && window.matchMedia?.("(prefers-reduced-motion: reduce)").matches) {
      setN(to);
      return;
    }
    let raf = 0;
    const start = performance.now();
    const tick = (t: number) => {
      const p = Math.min(1, (t - start) / duration);
      const eased = 1 - Math.pow(1 - p, 3); // ease-out cubic
      setN(Math.round(to * eased));
      if (p < 1) raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [armed, to, duration]);

  return (
    <span ref={ref} className={className} style={style}>
      {n}
    </span>
  );
}
