"use client";

/* StaggerGroup — reveals its direct children in sequence on scroll-into-view. Server children in,
   client stagger out. Use below the fold (off-screen at load = no flash). Reduced-motion renders all
   children in place. */

import { useRef } from "react";
import gsap from "gsap";
import { useGSAP } from "@gsap/react";
import { useReducedMotion } from "./motion-provider";

type Tag = keyof JSX.IntrinsicElements;

export function StaggerGroup({
  children,
  as = "div",
  y = 22,
  stagger = 0.09,
  duration = 0.7,
  start = "top 82%",
  selector = ":scope > *",
  className,
  style,
}: {
  children: React.ReactNode;
  as?: Tag;
  y?: number;
  stagger?: number;
  duration?: number;
  start?: string;
  selector?: string;
  className?: string;
  style?: React.CSSProperties;
}) {
  const ref = useRef<HTMLElement>(null);
  const reduced = useReducedMotion();

  useGSAP(
    () => {
      const el = ref.current;
      if (!el) return;
      const items = Array.from(el.querySelectorAll<HTMLElement>(selector));
      if (!items.length) return;
      if (reduced) {
        gsap.set(items, { autoAlpha: 1, y: 0 });
        return;
      }
      gsap.fromTo(
        items,
        { autoAlpha: 0, y },
        {
          autoAlpha: 1,
          y: 0,
          duration,
          ease: "power3.out",
          stagger,
          scrollTrigger: { trigger: el, start, once: true },
        },
      );
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
