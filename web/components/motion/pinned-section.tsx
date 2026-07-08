"use client";

/* PinnedSection — pins its content while a scroll span passes (the set-piece beats: THIS WEEK, the
   5.91 A refusal monument). Desktop-only; under reduced motion or on mobile it's a normal static block.
   An optional onProgress reports 0..1 through the pin so children can drive count-ups / crossfades. */

import { useRef } from "react";
import gsap from "gsap";
import { ScrollTrigger } from "gsap/ScrollTrigger";
import { useGSAP } from "@gsap/react";
import { useReducedMotion } from "./motion-provider";

export function PinnedSection({
  children,
  end = "+=100%",
  onProgress,
  className,
  style,
}: {
  children: React.ReactNode;
  end?: string;
  onProgress?: (p: number) => void;
  className?: string;
  style?: React.CSSProperties;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const reduced = useReducedMotion();

  useGSAP(
    () => {
      const el = ref.current;
      if (!el || reduced) return;
      const mm = gsap.matchMedia();
      mm.add("(min-width: 861px)", () => {
        ScrollTrigger.create({
          trigger: el,
          start: "top top",
          end,
          pin: true,
          pinSpacing: true,
          onUpdate: onProgress ? (self) => onProgress(self.progress) : undefined,
        });
      });
      return () => mm.revert();
    },
    { dependencies: [reduced], scope: ref },
  );

  return (
    <div ref={ref} className={className} style={style}>
      {children}
    </div>
  );
}
