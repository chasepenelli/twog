"use client";

/* Parallax — scrub-driven vertical drift as the element passes through the viewport. Desktop-only
   (>=861px, matching the site's layout breakpoints) and disabled under reduced motion. */

import { useRef } from "react";
import gsap from "gsap";
import { useGSAP } from "@gsap/react";
import { useReducedMotion } from "./motion-provider";

type Tag = keyof JSX.IntrinsicElements;

export function Parallax({
  children,
  speed = 0.18,
  as = "div",
  className,
  style,
}: {
  children: React.ReactNode;
  speed?: number;
  as?: Tag;
  className?: string;
  style?: React.CSSProperties;
}) {
  const ref = useRef<HTMLElement>(null);
  const reduced = useReducedMotion();

  useGSAP(
    () => {
      const el = ref.current;
      if (!el || reduced) return;
      const mm = gsap.matchMedia();
      mm.add("(min-width: 861px)", () => {
        gsap.to(el, {
          yPercent: speed * 100,
          ease: "none",
          scrollTrigger: { trigger: el, start: "top bottom", end: "bottom top", scrub: true },
        });
      });
      return () => mm.revert();
    },
    { dependencies: [reduced], scope: ref },
  );

  const Component = as as React.ElementType;
  return (
    <Component ref={ref} className={className} style={style}>
      {children}
    </Component>
  );
}
