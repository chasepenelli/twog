"use client";

/* TextReveal — masked line-by-line rise for a headline. The text stays in the server DOM (LCP-safe,
   readable, selectable); SplitText only re-wraps it on the client to animate. Plays on load by default
   (the hero) or on scroll. Reduced-motion / no-JS: the plain heading renders, untouched. */

import { useRef } from "react";
import gsap from "gsap";
import { SplitText } from "gsap/SplitText";
import { useGSAP } from "@gsap/react";
import { useReducedMotion } from "./motion-provider";

if (typeof window !== "undefined") {
  gsap.registerPlugin(SplitText);
}

type Tag = keyof JSX.IntrinsicElements;

export function TextReveal({
  children,
  as = "h1",
  trigger = "load",
  delay = 0.05,
  stagger = 0.09,
  duration = 0.9,
  className,
  style,
}: {
  children: React.ReactNode;
  as?: Tag;
  trigger?: "load" | "scroll";
  delay?: number;
  stagger?: number;
  duration?: number;
  className?: string;
  style?: React.CSSProperties;
}) {
  const ref = useRef<HTMLElement>(null);
  const reduced = useReducedMotion();

  useGSAP(
    () => {
      const el = ref.current;
      if (!el || reduced) return;
      // Defensive: if SplitText ever throws, the plain (already-visible) heading stays — text is
      // never hidden behind a failed animation.
      let split: SplitText | null = null;
      let inner: SplitText | null = null;
      let tween: gsap.core.Tween | null = null;
      try {
        split = new SplitText(el, { type: "lines", linesClass: "tr-line" });
        split.lines.forEach((line) => {
          (line as HTMLElement).style.overflow = "hidden"; // clip mask so lines rise from behind
        });
        inner = new SplitText(split.lines, { type: "lines", linesClass: "tr-line-inner" });
        tween = gsap.fromTo(
          inner.lines,
          { yPercent: 115 },
          {
            yPercent: 0,
            duration,
            delay: trigger === "load" ? delay : 0,
            ease: "power4.out",
            stagger,
            scrollTrigger:
              trigger === "scroll" ? { trigger: el, start: "top 85%", once: true } : undefined,
          },
        );
      } catch {
        inner?.revert();
        split?.revert();
        return;
      }
      return () => {
        tween?.scrollTrigger?.kill();
        inner?.revert();
        split?.revert();
      };
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
