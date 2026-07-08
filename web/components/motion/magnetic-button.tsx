"use client";

/* MagneticButton — the pill/link drifts a few px toward the cursor within its bounds and springs back.
   Pure hover polish (no ScrollTrigger). Disabled on touch pointers and under reduced motion. Wraps its
   child in an inline-flex span and moves that, so the inner <Link>/<button> stays fully clickable. */

import { useRef } from "react";
import gsap from "gsap";
import { useGSAP } from "@gsap/react";
import { useReducedMotion } from "./motion-provider";

export function MagneticButton({
  children,
  strength = 0.35,
  className,
  style,
}: {
  children: React.ReactNode;
  strength?: number;
  className?: string;
  style?: React.CSSProperties;
}) {
  const ref = useRef<HTMLSpanElement>(null);
  const reduced = useReducedMotion();

  useGSAP(
    () => {
      const el = ref.current;
      if (!el || reduced) return;
      if (window.matchMedia("(pointer: coarse)").matches) return;
      const xTo = gsap.quickTo(el, "x", { duration: 0.5, ease: "power3.out" });
      const yTo = gsap.quickTo(el, "y", { duration: 0.5, ease: "power3.out" });
      const onMove = (e: PointerEvent) => {
        const r = el.getBoundingClientRect();
        xTo((e.clientX - (r.left + r.width / 2)) * strength);
        yTo((e.clientY - (r.top + r.height / 2)) * strength);
      };
      const onLeave = () => {
        xTo(0);
        yTo(0);
      };
      el.addEventListener("pointermove", onMove);
      el.addEventListener("pointerleave", onLeave);
      return () => {
        el.removeEventListener("pointermove", onMove);
        el.removeEventListener("pointerleave", onLeave);
      };
    },
    { dependencies: [reduced], scope: ref },
  );

  return (
    <span ref={ref} className={className} style={{ display: "inline-flex", ...style }}>
      {children}
    </span>
  );
}
