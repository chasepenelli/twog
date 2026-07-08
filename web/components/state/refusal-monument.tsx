"use client";

/* RefusalMonument — the signature beat, in a COMPACT two-column layout so it fills the dark panel
   instead of leaving a tall void. A real structure showed up for docking; its own physics check put the
   redock pose at 5.91 A, past the <=2.0 A gate, so the engine refused it before spending any compute.

   Honesty: 5.91 and the 2.0 gate are the real stored values, shown statically. The motion is on the
   reveal + the gate slamming — it never animates a measurement climbing through fabricated values.
   Triggers early (top 82%) so the words appear as the panel enters view, not after an empty gap. */

import { useRef } from "react";
import gsap from "gsap";
import { useGSAP } from "@gsap/react";
import { useReducedMotion } from "@/components/motion";

export function RefusalMonument({ rmsd = 5.91, gate = 2.0 }: { rmsd?: number; gate?: number }) {
  const ref = useRef<HTMLDivElement>(null);
  const reduced = useReducedMotion();

  useGSAP(
    () => {
      const el = ref.current;
      if (!el) return;
      const q = gsap.utils.selector(el);
      if (reduced) {
        gsap.set(q("[data-fig], [data-refused], [data-cap]"), { autoAlpha: 1, y: 0 });
        gsap.set(q("[data-gate]"), { scaleX: 1 });
        gsap.set(q("[data-fig]"), { color: "#ff6f63" });
        return;
      }
      const tl = gsap.timeline({ scrollTrigger: { trigger: el, start: "top 82%", once: true } });
      tl.from(q("[data-fig]"), { autoAlpha: 0, y: 14, duration: 0.5, ease: "power3.out" })
        .fromTo(q("[data-gate]"), { scaleX: 0 }, { scaleX: 1, duration: 0.45, ease: "power4.inOut" }, "+=0.1")
        .to(q("[data-fig]"), { color: "#ff6f63", duration: 0.3 }, "<0.05")
        .from(q("[data-refused]"), { autoAlpha: 0, y: 12, duration: 0.55, ease: "power3.out" }, "+=0.02")
        .from(q("[data-cap]"), { autoAlpha: 0, duration: 0.4 }, "-=0.2");
    },
    { dependencies: [reduced], scope: ref },
  );

  return (
    <div ref={ref} className="refusal-grid">
      <div>
        <div className="mono" style={{ fontSize: 11, letterSpacing: "0.16em", color: "#8a8f98" }}>THE REFUSAL · INPUT QUALITY GATE</div>
        <div style={{ marginTop: 12 }}>
          <span data-fig style={{ fontSize: "clamp(50px,7.5vw,88px)", fontWeight: 700, letterSpacing: "-0.05em", lineHeight: 0.9, color: "#e9eaec", display: "inline-block" }}>
            {rmsd.toFixed(2)}
            <span className="mono" style={{ fontSize: "0.3em", fontWeight: 500, marginLeft: 8, letterSpacing: 0, verticalAlign: "baseline" }}>Å</span>
          </span>
        </div>
        <div data-gate aria-hidden style={{ height: 3, background: "var(--accent)", transformOrigin: "left", margin: "14px 0 0", maxWidth: 320 }} />
        <div data-refused className="serif" style={{ fontStyle: "italic", fontSize: "clamp(38px,5.5vw,68px)", fontWeight: 500, letterSpacing: "-0.02em", lineHeight: 0.95, margin: "16px 0 0", color: "#fff" }}>
          refused.
        </div>
      </div>
      <div>
        <p style={{ fontSize: "clamp(15px,1.5vw,18px)", color: "#c4c6ca", margin: 0, lineHeight: 1.55, maxWidth: "36ch" }}>
          A structure showed up for docking. Its own physics check put the redock pose almost three times
          worse than the {gate.toFixed(1)} Å the gate allows — so the engine turned it away before spending a
          cent of compute.
        </p>
        <p data-cap className="mono" style={{ fontSize: 12, letterSpacing: "0.05em", color: "#6f7480", margin: "18px 0 0", lineHeight: 1.7, maxWidth: "40ch" }}>
          No money spent. No result faked. The flashiest thing this engine does is say no to a bad input.
        </p>
      </div>
    </div>
  );
}
