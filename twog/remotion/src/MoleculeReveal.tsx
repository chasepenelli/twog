import React, { useRef, useEffect } from "react";
import {
  AbsoluteFill,
  useCurrentFrame,
  useVideoConfig,
  interpolate,
  spring,
} from "remotion";

/*
 * MoleculeReveal — 3D molecular render + particle system
 *
 * 1280x720 @ 30fps = 5 seconds (150 frames)
 *
 * Phases:
 *   0-40:   Particles swirl inward from edges, converging to center
 *   20-50:  Molecule SVG fades in with isometric 3D tilt + dramatic lighting
 *   40-120: Molecule rotates slowly, particles orbit, compound name types in
 *   120-150: Score badges rise in, particles settle into orbital rings
 */

export interface MoleculeRevealProps {
  compoundName?: string;
  targetGene?: string;
  compositeScore?: number;
  qedScore?: number;
  moleculeSvg?: string;
}

/* Simple seeded random for deterministic particle positions */
function seeded(seed: number) {
  let s = seed;
  return () => {
    s = (s * 16807 + 0) % 2147483647;
    return (s - 1) / 2147483646;
  };
}

interface Particle {
  x0: number; y0: number; // start position (edge)
  x1: number; y1: number; // converged position (near center)
  speed: number;
  size: number;
  orbit: number; // orbital radius once converged
  orbitSpeed: number;
  orbitPhase: number;
  hue: number; // 0 = white, 1 = green
  trail: number; // trail length factor
}

function initParticles(count: number, w: number, h: number): Particle[] {
  const rng = seeded(42);
  const particles: Particle[] = [];
  for (let i = 0; i < count; i++) {
    const angle = rng() * Math.PI * 2;
    const edgeDist = 400 + rng() * 300;
    particles.push({
      x0: w / 2 + Math.cos(angle) * edgeDist,
      y0: h / 2 + Math.sin(angle) * edgeDist,
      x1: w / 2 + (rng() - 0.5) * 80,
      y1: h / 2 + (rng() - 0.5) * 80,
      speed: 0.6 + rng() * 0.4,
      size: 1.5 + rng() * 2.5,
      orbit: 100 + rng() * 160,
      orbitSpeed: 0.008 + rng() * 0.015,
      orbitPhase: rng() * Math.PI * 2,
      hue: rng() > 0.6 ? 1 : 0,
      trail: 0.3 + rng() * 0.7,
    });
  }
  return particles;
}

const PARTICLES = initParticles(120, 1280, 720);

export const MoleculeReveal: React.FC<MoleculeRevealProps> = ({
  compoundName = "TWOG-001",
  targetGene = "cKDR",
  compositeScore = 0.85,
  qedScore = 0.72,
  moleculeSvg = "",
}) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const W = 1280, H = 720;

  /* ── Particle canvas ── */
  useEffect(() => {
    const ctx = canvasRef.current?.getContext("2d");
    if (!ctx) return;
    ctx.clearRect(0, 0, W, H);

    const convergence = interpolate(frame, [0, 45], [0, 1], {
      extrapolateLeft: "clamp", extrapolateRight: "clamp",
    });
    const globalAlpha = interpolate(frame, [0, 10, 140, 150], [0, 0.9, 0.9, 0.4], {
      extrapolateLeft: "clamp", extrapolateRight: "clamp",
    });

    for (const p of PARTICLES) {
      const t = Math.min(1, convergence * p.speed);
      // Ease in cubic
      const ease = t * t * (3 - 2 * t);

      let x: number, y: number;
      if (frame < 45) {
        // Converging phase
        x = p.x0 + (p.x1 - p.x0) * ease;
        y = p.y0 + (p.y1 - p.y0) * ease;
      } else {
        // Orbital phase
        const orbitFrame = frame - 45;
        const angle = p.orbitPhase + orbitFrame * p.orbitSpeed;
        x = W / 2 + Math.cos(angle) * p.orbit;
        y = H / 2 + Math.sin(angle) * p.orbit * 0.55; // squashed for isometric
      }

      const alpha = globalAlpha * (p.hue === 1 ? 0.8 : 0.5);
      const color = p.hue === 1
        ? `rgba(34, 197, 94, ${alpha})`
        : `rgba(200, 200, 200, ${alpha})`;

      // Trail
      if (frame < 45 && p.trail > 0.5) {
        const prevT = Math.max(0, t - 0.08);
        const prevEase = prevT * prevT * (3 - 2 * prevT);
        const px = p.x0 + (p.x1 - p.x0) * prevEase;
        const py = p.y0 + (p.y1 - p.y0) * prevEase;
        ctx.beginPath();
        ctx.moveTo(px, py);
        ctx.lineTo(x, y);
        ctx.strokeStyle = p.hue === 1
          ? `rgba(34, 197, 94, ${alpha * 0.3})`
          : `rgba(200, 200, 200, ${alpha * 0.2})`;
        ctx.lineWidth = p.size * 0.5;
        ctx.stroke();
      }

      // Particle dot
      ctx.beginPath();
      ctx.arc(x, y, p.size, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();

      // Glow for green particles
      if (p.hue === 1 && p.size > 2) {
        ctx.beginPath();
        ctx.arc(x, y, p.size * 3, 0, Math.PI * 2);
        ctx.fillStyle = `rgba(34, 197, 94, ${alpha * 0.08})`;
        ctx.fill();
      }
    }

    // Connection lines between nearby orbital particles
    if (frame >= 50) {
      ctx.globalAlpha = 0.06;
      for (let i = 0; i < PARTICLES.length; i += 3) {
        for (let j = i + 3; j < PARTICLES.length; j += 5) {
          const pi = PARTICLES[i], pj = PARTICLES[j];
          const orbitFrame = frame - 45;
          const ai = pi.orbitPhase + orbitFrame * pi.orbitSpeed;
          const aj = pj.orbitPhase + orbitFrame * pj.orbitSpeed;
          const xi = W / 2 + Math.cos(ai) * pi.orbit;
          const yi = H / 2 + Math.sin(ai) * pi.orbit * 0.55;
          const xj = W / 2 + Math.cos(aj) * pj.orbit;
          const yj = H / 2 + Math.sin(aj) * pj.orbit * 0.55;
          const dist = Math.hypot(xi - xj, yi - yj);
          if (dist < 120) {
            ctx.beginPath();
            ctx.moveTo(xi, yi);
            ctx.lineTo(xj, yj);
            ctx.strokeStyle = "rgba(34, 197, 94, 0.4)";
            ctx.lineWidth = 0.5;
            ctx.stroke();
          }
        }
      }
      ctx.globalAlpha = 1;
    }
  }, [frame]);

  /* ── Molecule transform ── */
  const molOpacity = interpolate(frame, [20, 50], [0, 1], {
    extrapolateLeft: "clamp", extrapolateRight: "clamp",
  });
  const molScale = spring({
    frame: Math.max(0, frame - 20),
    fps,
    config: { damping: 18, stiffness: 80, mass: 0.8 },
  });
  const rotY = interpolate(frame, [20, 150], [-15, 15], {
    extrapolateLeft: "clamp", extrapolateRight: "clamp",
  });
  const rotX = interpolate(frame, [20, 150], [12, 8], {
    extrapolateLeft: "clamp", extrapolateRight: "clamp",
  });

  /* ── Name typing ── */
  const charsToShow = Math.min(
    compoundName.length,
    Math.floor(interpolate(frame, [40, 70], [0, compoundName.length], {
      extrapolateRight: "clamp",
    }))
  );
  const nameOpacity = interpolate(frame, [40, 50], [0, 1], {
    extrapolateLeft: "clamp", extrapolateRight: "clamp",
  });
  const cursorOn = frame >= 40 && frame < 80 && Math.floor(frame / 4) % 2 === 0;

  /* ── Score badges ── */
  const badges = [
    { label: "Composite", value: compositeScore.toFixed(4), color: "#fff", delay: 118 },
    { label: "Target", value: targetGene, color: "#22C55E", delay: 126 },
    { label: "QED", value: qedScore.toFixed(2), color: "#fff", delay: 134 },
  ];

  return (
    <AbsoluteFill style={{
      background: "radial-gradient(ellipse at 50% 45%, #0f0f0f 0%, #0a0a0a 50%, #050505 100%)",
      overflow: "hidden",
    }}>
      {/* Particle layer */}
      <canvas
        ref={canvasRef}
        width={W}
        height={H}
        style={{ position: "absolute", inset: 0, width: "100%", height: "100%" }}
      />

      {/* Molecule SVG — isometric 3D tilt */}
      <div style={{
        position: "absolute",
        top: "50%",
        left: "50%",
        transform: `translate(-50%, -55%) perspective(800px) rotateX(${rotX}deg) rotateY(${rotY}deg) scale(${molScale})`,
        opacity: molOpacity,
        width: 420,
        height: 420,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        filter: `drop-shadow(0 8px 40px rgba(0,0,0,0.6)) drop-shadow(0 0 60px rgba(34,197,94,0.12))`,
      }}>
        {moleculeSvg ? (
          <div dangerouslySetInnerHTML={{ __html: moleculeSvg }} />
        ) : (
          <div style={{
            width: 300, height: 300,
            borderRadius: "50%",
            border: "1.5px solid rgba(34,197,94,0.25)",
            boxShadow: "0 0 60px rgba(34,197,94,0.1), inset 0 0 40px rgba(34,197,94,0.05)",
          }} />
        )}
      </div>

      {/* Compound name */}
      <div style={{
        position: "absolute",
        top: 52,
        left: 0,
        right: 0,
        textAlign: "center",
        opacity: nameOpacity,
      }}>
        <span style={{
          fontFamily: "'Space Mono', monospace",
          fontSize: 64,
          fontWeight: 700,
          color: "#fff",
          letterSpacing: "0.05em",
          textShadow: "0 2px 30px rgba(0,0,0,0.7)",
        }}>
          {compoundName.slice(0, charsToShow)}
          {cursorOn && <span style={{ color: "#22C55E" }}>|</span>}
        </span>
      </div>

      {/* Score badges */}
      <div style={{
        position: "absolute",
        bottom: 52,
        left: 0,
        right: 0,
        display: "flex",
        justifyContent: "center",
        gap: 64,
      }}>
        {badges.map(({ label, value, color, delay }) => {
          const op = interpolate(frame, [delay, delay + 10], [0, 1], {
            extrapolateLeft: "clamp", extrapolateRight: "clamp",
          });
          const y = interpolate(frame, [delay, delay + 10], [20, 0], {
            extrapolateLeft: "clamp", extrapolateRight: "clamp",
          });
          return (
            <div key={label} style={{ opacity: op, transform: `translateY(${y}px)`, textAlign: "center" }}>
              <div style={{
                fontFamily: "'JetBrains Mono', monospace",
                fontSize: 48,
                fontWeight: 700,
                color,
                textShadow: color === "#22C55E"
                  ? "0 0 25px rgba(34,197,94,0.5)"
                  : "0 2px 15px rgba(0,0,0,0.5)",
              }}>
                {value}
              </div>
              <div style={{
                fontFamily: "'Space Mono', monospace",
                fontSize: 15,
                color: "#aaa",
                letterSpacing: "0.2em",
                textTransform: "uppercase",
                marginTop: 6,
              }}>
                {label}
              </div>
            </div>
          );
        })}
      </div>
    </AbsoluteFill>
  );
};
