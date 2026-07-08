"use client";

/* Reveal — the core "motion island": a dumb client wrapper that fades/translates its SERVER-rendered
   children in on scroll. Data is never lifted to the client — the server page renders the markup and
   hands it here as children; this only animates the DOM node via a ref.

   FOUC/CLS-safe: the element carries `reveal-init`, which is hidden ONLY under `.js-ready` (set by
   MotionProvider). So no-JS / slow-JS / reduced-motion users see content immediately. GSAP's fromTo
   sets the from-state in a layout effect (before paint) and clears props on completion. Never wrap the
   LCP <h1> in this. */

import { useRef } from "react";
import gsap from "gsap";
import { useGSAP } from "@gsap/react";
import { useReducedMotion } from "./motion-provider";

type Tag = keyof JSX.IntrinsicElements;

export function Reveal({
  children,
  as = "div",
  y = 24,
  delay = 0,
  duration = 0.8,
  start = "top 85%",
  once = true,
  className,
  style,
}: {
  children: React.ReactNode;
  as?: Tag;
  y?: number;
  delay?: number;
  duration?: number;
  start?: string;
  once?: boolean;
  className?: string;
  style?: React.CSSProperties;
}) {
  const ref = useRef<HTMLElement>(null);
  const reduced = useReducedMotion();

  useGSAP(
    () => {
      const el = ref.current;
      if (!el) return;
      if (reduced) {
        gsap.set(el, { autoAlpha: 1, y: 0, clearProps: "transform" });
        return;
      }
      gsap.fromTo(
        el,
        { autoAlpha: 0, y },
        {
          autoAlpha: 1,
          y: 0,
          duration,
          delay,
          ease: "power3.out",
          scrollTrigger: { trigger: el, start, once },
        },
      );
    },
    { dependencies: [reduced], scope: ref },
  );

  const Component = as as React.ElementType;
  return (
    <Component ref={ref} className={["reveal-init", className].filter(Boolean).join(" ")} style={style}>
      {children}
    </Component>
  );
}
