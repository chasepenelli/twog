"use client";

import { useEffect, useRef, useState } from "react";

/* A tiny count-up. The server renders the REAL final value (so no-JS / crawlers always see the true
   number — motion never carries the number). On the client, when motion is allowed, it drops to 0 and
   animates up: on first paint (default) or when scrolled into view (`onScroll`). Respects
   prefers-reduced-motion (stays on the real value, no animation). `delay` lets a later counter land
   after an earlier one. This is the one numeric-animation primitive in web/. */

export function CountUp({
  to,
  duration = 900,
  delay = 0,
  onScroll = false,
  className,
  style,
}: {
  to: number;
  duration?: number;
  delay?: number;
  onScroll?: boolean;
  className?: string;
  style?: React.CSSProperties;
}) {
  const ref = useRef<HTMLSpanElement>(null);
  const [n, setN] = useState(to); // SSR + no-JS show the real value
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
    const reduce =
      typeof window !== "undefined" &&
      window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;
    if (reduce) {
      setN(to); // hold on the real value
      return;
    }
    if (!armed) {
      setN(0); // waiting to be scrolled into view — hold at 0 (off-screen), ready to count
      return;
    }
    setN(0);
    let raf = 0;
    let start = 0;
    const tick = (t: number) => {
      if (!start) start = t;
      const p = Math.min(1, (t - start) / duration);
      const eased = 1 - Math.pow(1 - p, 3); // ease-out cubic
      setN(Math.round(to * eased));
      if (p < 1) raf = requestAnimationFrame(tick);
    };
    const timer = window.setTimeout(() => {
      raf = requestAnimationFrame(tick);
    }, delay);
    return () => {
      window.clearTimeout(timer);
      cancelAnimationFrame(raf);
    };
  }, [armed, to, duration, delay]);

  return (
    <span ref={ref} className={className} style={style}>
      {n}
    </span>
  );
}
