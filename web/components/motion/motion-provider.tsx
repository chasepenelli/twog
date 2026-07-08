"use client";

/* MotionProvider — the single client boundary that owns page-wide motion.
   - Mounts Lenis (smooth scroll) on the document root and drives it off GSAP's single ticker,
     so ScrollTrigger and Lenis share one rAF clock (no scrub jitter, no competing loops).
   - Exposes ONE reduced-motion switch via context; every motion primitive reads it and degrades
     to the final state. Under reduced-motion we skip Lenis entirely (native scroll stays instant).
   - Sets `.js-ready` on <html> so CSS can safely hide reveal targets only once JS is confirmed live
     (no-JS / slow-JS users always see content — motion never carries a claim).
   - Refreshes ScrollTrigger on route change and after fonts settle (IBM Plex `display:swap` shifts
     metrics and would otherwise leave triggers at stale positions).
   Public pages stay server components; this only wraps their already-rendered children. */

import {
  createContext,
  useContext,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { usePathname } from "next/navigation";
import { ReactLenis, type LenisRef } from "lenis/react";
import gsap from "gsap";
import { ScrollTrigger } from "gsap/ScrollTrigger";

if (typeof window !== "undefined") {
  gsap.registerPlugin(ScrollTrigger);
}

const ReducedMotionContext = createContext(false);
/** True when the user prefers reduced motion. Every motion primitive must honor this. */
export const useReducedMotion = () => useContext(ReducedMotionContext);

// useLayoutEffect warns during SSR; fall back to useEffect on the server pass.
const useIsomorphicLayoutEffect = typeof window !== "undefined" ? useLayoutEffect : useEffect;

export function MotionProvider({ children }: { children: React.ReactNode }) {
  const lenisRef = useRef<LenisRef>(null);
  const pathname = usePathname();
  const [reduced, setReduced] = useState(false);

  // Reduced-motion: single live source of truth.
  useEffect(() => {
    const mq = window.matchMedia("(prefers-reduced-motion: reduce)");
    const apply = () => setReduced(mq.matches);
    apply();
    mq.addEventListener("change", apply);
    return () => mq.removeEventListener("change", apply);
  }, []);

  // Mark JS ready so `.js-ready .reveal-init { opacity: 0 }` can apply (FOUC-safe: hidden state
  // only exists once JS is live; without JS the content is simply visible).
  useEffect(() => {
    const root = document.documentElement;
    root.classList.add("js-ready");
    return () => root.classList.remove("js-ready");
  }, []);

  // Wire Lenis -> GSAP ticker + ScrollTrigger (one rAF loop). Retries until the Lenis instance
  // exists so a frozen-scroll race is impossible (Lenis runs with autoRaf:false).
  useEffect(() => {
    if (reduced) return; // no Lenis under reduced motion — native scroll stays instant
    let tickerFn: ((time: number) => void) | null = null;
    let frame = 0;
    const wire = () => {
      const lenis = lenisRef.current?.lenis;
      if (!lenis) {
        frame = requestAnimationFrame(wire);
        return;
      }
      lenis.on("scroll", ScrollTrigger.update);
      tickerFn = (time: number) => lenis.raf(time * 1000);
      gsap.ticker.add(tickerFn);
      gsap.ticker.lagSmoothing(0);
    };
    wire();
    return () => {
      cancelAnimationFrame(frame);
      const lenis = lenisRef.current?.lenis;
      if (lenis) lenis.off("scroll", ScrollTrigger.update);
      if (tickerFn) gsap.ticker.remove(tickerFn);
    };
  }, [reduced]);

  // Route change: jump to top and recompute trigger positions once the new page has committed.
  useIsomorphicLayoutEffect(() => {
    lenisRef.current?.lenis?.scrollTo(0, { immediate: true });
    const id = requestAnimationFrame(() => ScrollTrigger.refresh());
    return () => cancelAnimationFrame(id);
  }, [pathname]);

  // Fonts change metrics after triggers compute — refresh when they settle.
  useEffect(() => {
    if (!document.fonts?.ready) return;
    let alive = true;
    document.fonts.ready.then(() => {
      if (alive) ScrollTrigger.refresh();
    });
    return () => {
      alive = false;
    };
  }, []);

  const content = (
    <ReducedMotionContext.Provider value={reduced}>{children}</ReducedMotionContext.Provider>
  );

  // Under reduced motion, render children with native scroll (no Lenis instance at all).
  if (reduced) return content;

  return (
    <ReactLenis
      root
      ref={lenisRef}
      options={{ autoRaf: false, duration: 1.05, smoothWheel: true, wheelMultiplier: 1 }}
    >
      {content}
    </ReactLenis>
  );
}
