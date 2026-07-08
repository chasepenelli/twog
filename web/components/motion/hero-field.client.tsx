"use client";

/* HeroFieldClient — mounts the WebGL field, with the SVG poster as a faithful fallback beneath it.
   The poster shows until the canvas signals ready (then the canvas fades in over it), and stays if the
   user prefers reduced motion or if WebGL fails to initialize. Server pages import THIS; the canvas is
   dynamically imported ssr:false so three/WebGL never touch the server. */

import { Component, useState, type ReactNode } from "react";
import dynamic from "next/dynamic";

import { useReducedMotion } from "./motion-provider";
import { FieldPoster } from "./field-poster";
import type { FieldPoint } from "./field-shared";

const HeroField = dynamic(() => import("./hero-field"), { ssr: false, loading: () => null });

class Boundary extends Component<{ onError: () => void; children: ReactNode }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError() {
    return { failed: true };
  }
  componentDidCatch() {
    this.props.onError();
  }
  render() {
    return this.state.failed ? null : this.props.children;
  }
}

export function HeroFieldClient({ points }: { points: FieldPoint[] }) {
  const reduced = useReducedMotion();
  const [ready, setReady] = useState(false);
  const [failed, setFailed] = useState(false);
  const showPoster = reduced || failed || !ready;

  return (
    <div style={{ position: "relative", width: "100%", height: "100%" }}>
      {showPoster ? (
        <div style={{ position: "absolute", inset: 0 }}>
          <FieldPoster points={points} />
        </div>
      ) : null}
      {!reduced && !failed ? (
        <div style={{ position: "absolute", inset: 0, opacity: ready ? 1 : 0, transition: "opacity 0.7s ease" }}>
          <Boundary onError={() => setFailed(true)}>
            <HeroField points={points} onReady={() => setReady(true)} />
          </Boundary>
        </div>
      ) : null}
    </div>
  );
}
