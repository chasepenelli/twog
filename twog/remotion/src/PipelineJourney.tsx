import React, { useRef, useEffect } from "react";
import {
  AbsoluteFill,
  useCurrentFrame,
  useVideoConfig,
  interpolate,
  spring,
} from "remotion";

/*
 * TWOG Pipeline Journey — Particle flow visualization
 *
 * 1920x1080 @ 30fps = 15 seconds (450 frames)
 *
 * Concept: Hundreds of particles stream left-to-right through 6 pipeline
 * nodes. At each node they cluster, change color, and accelerate onward.
 * Stage labels + numbers appear as the wave reaches each node.
 *
 * Phases:
 *   0-50:    Title card fades, particle emitter starts
 *   50-320:  Camera follows the particle wave through stages
 *   320-400: Zoom out to see full pipeline with all particles flowing
 *   400-450: Final badge + tagline
 */

export interface StageData {
  label: string;
  number: string;
  unit: string;
  description: string;
  color: string;
}

export interface PipelineJourneyProps {
  stages?: StageData[];
}

const DEFAULT_STAGES: StageData[] = [
  { label: "READ", number: "1,531", unit: "papers", description: "Every cancer paper. Scanned daily.", color: "#fff" },
  { label: "DESIGN", number: "84,000", unit: "molecules", description: "Generated. Filtered. Optimized.", color: "#fff" },
  { label: "DOCK", number: "20,650", unit: "simulations", description: "Binding tested against canine targets.", color: "#fff" },
  { label: "SCORE", number: "13", unit: "profiled", description: "ADMET. Toxicity. Absorption. Metabolism.", color: "#fff" },
  { label: "VALIDATE", number: "5", unit: "tested", description: "Molecular dynamics simulation completed.", color: "#22C55E" },
  { label: "REVIEW", number: "5", unit: "profiled", description: "Full safety profile. Awaiting wet lab.", color: "#22C55E" },
];

function seeded(seed: number) {
  let s = seed;
  return () => { s = (s * 16807 + 0) % 2147483647; return (s - 1) / 2147483646; };
}

interface FlowParticle {
  baseY: number;
  speed: number;
  size: number;
  yWave: number;
  waveSpeed: number;
  phase: number;
  startFrame: number;
  green: boolean;
}

function initFlowParticles(count: number, h: number): FlowParticle[] {
  const rng = seeded(777);
  const particles: FlowParticle[] = [];
  for (let i = 0; i < count; i++) {
    particles.push({
      baseY: h * 0.25 + rng() * h * 0.5,
      speed: 3 + rng() * 5,
      size: 1.2 + rng() * 2.8,
      yWave: 15 + rng() * 40,
      waveSpeed: 0.02 + rng() * 0.04,
      phase: rng() * Math.PI * 2,
      startFrame: Math.floor(rng() * 60),
      green: rng() > 0.55,
    });
  }
  return particles;
}

const FLOW_PARTICLES = initFlowParticles(200, 1080);
const W = 1920, H = 1080;

// Node positions across the width
const NODE_POSITIONS = [0.1, 0.25, 0.42, 0.58, 0.75, 0.9];

export const PipelineJourney: React.FC<PipelineJourneyProps> = ({ stages }) => {
  const STAGES = stages ?? DEFAULT_STAGES;
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const canvasRef = useRef<HTMLCanvasElement>(null);

  const isTitlePhase = frame < 50;
  const isOverviewPhase = frame >= 320;

  /* ── Particle canvas ── */
  useEffect(() => {
    const ctx = canvasRef.current?.getContext("2d");
    if (!ctx) return;
    ctx.clearRect(0, 0, W, H);

    if (frame < 20) return;

    const flowFrame = frame - 20;
    const globalAlpha = interpolate(frame, [20, 40, 420, 450], [0, 1, 1, 0.3], {
      extrapolateLeft: "clamp", extrapolateRight: "clamp",
    });

    // Camera pan — during flow phase, viewport shifts right to follow particles
    let cameraX = 0;
    if (frame >= 50 && frame < 320) {
      cameraX = interpolate(frame, [50, 300], [0, W * 0.6], {
        extrapolateLeft: "clamp", extrapolateRight: "clamp",
      });
    } else if (frame >= 320) {
      // Zoom out: camera returns to center
      cameraX = interpolate(frame, [320, 370], [W * 0.6, 0], {
        extrapolateLeft: "clamp", extrapolateRight: "clamp",
      });
    }

    // Draw node columns (vertical glow lines at each pipeline stage)
    for (let i = 0; i < NODE_POSITIONS.length; i++) {
      const nx = NODE_POSITIONS[i] * W * 1.6 - cameraX;
      if (nx < -100 || nx > W + 100) continue;

      const isGreen = i >= 4;
      const nodeAlpha = interpolate(frame, [50 + i * 40, 70 + i * 40], [0, 0.15], {
        extrapolateLeft: "clamp", extrapolateRight: "clamp",
      });

      // Vertical glow line
      const grad = ctx.createLinearGradient(nx, H * 0.1, nx, H * 0.9);
      const c = isGreen ? "34, 197, 94" : "255, 255, 255";
      grad.addColorStop(0, `rgba(${c}, 0)`);
      grad.addColorStop(0.3, `rgba(${c}, ${nodeAlpha})`);
      grad.addColorStop(0.5, `rgba(${c}, ${nodeAlpha * 2})`);
      grad.addColorStop(0.7, `rgba(${c}, ${nodeAlpha})`);
      grad.addColorStop(1, `rgba(${c}, 0)`);

      ctx.beginPath();
      ctx.moveTo(nx, H * 0.1);
      ctx.lineTo(nx, H * 0.9);
      ctx.strokeStyle = grad;
      ctx.lineWidth = isGreen ? 3 : 2;
      ctx.stroke();

      // Node circle
      ctx.beginPath();
      ctx.arc(nx, H * 0.5, isGreen ? 6 : 4, 0, Math.PI * 2);
      ctx.fillStyle = isGreen
        ? `rgba(34, 197, 94, ${nodeAlpha * 4})`
        : `rgba(255, 255, 255, ${nodeAlpha * 3})`;
      ctx.fill();

      // Node glow
      if (isGreen) {
        ctx.beginPath();
        ctx.arc(nx, H * 0.5, 20, 0, Math.PI * 2);
        ctx.fillStyle = `rgba(34, 197, 94, ${nodeAlpha * 0.5})`;
        ctx.fill();
      }
    }

    // Draw flowing particles
    for (const p of FLOW_PARTICLES) {
      const pFrame = flowFrame - p.startFrame;
      if (pFrame < 0) continue;

      const rawX = pFrame * p.speed;
      const worldX = rawX - cameraX;

      // Particles get pulled toward nearest node
      let pullY = 0;
      for (let i = 0; i < NODE_POSITIONS.length; i++) {
        const nodeWorldX = NODE_POSITIONS[i] * W * 1.6;
        const dist = Math.abs(rawX - nodeWorldX);
        if (dist < 80) {
          const pull = 1 - dist / 80;
          pullY += (H * 0.5 - p.baseY) * pull * 0.4;
        }
      }

      const y = p.baseY + Math.sin(pFrame * p.waveSpeed + p.phase) * p.yWave + pullY;
      const x = worldX;

      if (x < -50 || x > W + 50) continue;

      // Color shifts to green as particles pass later nodes
      const progressRatio = rawX / (W * 1.6);
      const isNowGreen = p.green || progressRatio > 0.65;
      const alpha = globalAlpha * (isNowGreen ? 0.7 : 0.45);

      // Particle
      ctx.beginPath();
      ctx.arc(x, y, p.size, 0, Math.PI * 2);
      ctx.fillStyle = isNowGreen
        ? `rgba(34, 197, 94, ${alpha})`
        : `rgba(220, 220, 220, ${alpha})`;
      ctx.fill();

      // Trail line
      const trailX = x - p.speed * 3;
      ctx.beginPath();
      ctx.moveTo(trailX, y);
      ctx.lineTo(x, y);
      ctx.strokeStyle = isNowGreen
        ? `rgba(34, 197, 94, ${alpha * 0.2})`
        : `rgba(200, 200, 200, ${alpha * 0.1})`;
      ctx.lineWidth = p.size * 0.4;
      ctx.stroke();

      // Glow for large green particles
      if (isNowGreen && p.size > 2.5) {
        ctx.beginPath();
        ctx.arc(x, y, p.size * 4, 0, Math.PI * 2);
        ctx.fillStyle = `rgba(34, 197, 94, ${alpha * 0.06})`;
        ctx.fill();
      }
    }
  }, [frame]);

  /* ── Stage labels overlay ── */
  // During flow phase, show current stage label
  const stageFrameLen = 45;
  const currentStageIdx = frame >= 50 && frame < 320
    ? Math.min(STAGES.length - 1, Math.floor((frame - 50) / stageFrameLen))
    : -1;
  const stageLocal = frame >= 50 && frame < 320 ? (frame - 50) % stageFrameLen : 0;

  return (
    <AbsoluteFill style={{
      background: "#050505",
      overflow: "hidden",
    }}>
      {/* Subtle radial ambient */}
      <div style={{
        position: "absolute", inset: 0,
        background: "radial-gradient(ellipse at 50% 50%, rgba(34,197,94,0.03) 0%, transparent 60%)",
      }} />

      {/* Particle canvas */}
      <canvas
        ref={canvasRef}
        width={W}
        height={H}
        style={{ position: "absolute", inset: 0, width: "100%", height: "100%" }}
      />

      {/* ── TITLE CARD ── */}
      {isTitlePhase && (() => {
        const titleOp = interpolate(frame, [0, 15], [0, 1], { extrapolateRight: "clamp" });
        const subOp = interpolate(frame, [15, 30], [0, 1], { extrapolateRight: "clamp" });
        const fadeOut = interpolate(frame, [38, 50], [1, 0], { extrapolateLeft: "clamp", extrapolateRight: "clamp" });
        return (
          <AbsoluteFill style={{
            display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
            opacity: fadeOut, zIndex: 10,
          }}>
            <div style={{
              fontFamily: "'Space Mono', monospace", fontSize: 160, fontWeight: 700,
              color: "#fff", letterSpacing: "0.08em", opacity: titleOp,
              textShadow: "0 4px 60px rgba(0,0,0,0.8), 0 0 120px rgba(34,197,94,0.08)",
            }}>TWOG</div>
            <div style={{
              fontFamily: "'Space Mono', monospace", fontSize: 24,
              color: "#888", letterSpacing: "0.3em", textTransform: "uppercase" as const,
              marginTop: 20, opacity: subOp,
            }}>From paper to molecule in minutes</div>
          </AbsoluteFill>
        );
      })()}

      {/* ── STAGE LABELS (during flow) ── */}
      {currentStageIdx >= 0 && (() => {
        const stage = STAGES[currentStageIdx];
        const isGreen = stage.color === "#22C55E";

        const numScale = spring({
          frame: stageLocal, fps,
          config: { damping: 14, stiffness: 160, mass: 0.5 },
        });
        const labelOp = interpolate(stageLocal, [5, 14], [0, 1], {
          extrapolateLeft: "clamp", extrapolateRight: "clamp",
        });
        const fadeOut = interpolate(stageLocal, [36, 44], [1, 0], {
          extrapolateLeft: "clamp", extrapolateRight: "clamp",
        });

        return (
          <AbsoluteFill style={{
            display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
            opacity: fadeOut, zIndex: 10,
          }}>
            {/* Counter */}
            <div style={{
              position: "absolute", top: 48, right: 72,
              fontFamily: "'JetBrains Mono', monospace", fontSize: 18,
              color: "#555", letterSpacing: "0.1em",
            }}>
              {String(currentStageIdx + 1).padStart(2, "0")} / {String(STAGES.length).padStart(2, "0")}
            </div>

            {/* Big number */}
            <div style={{
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: stage.label === "GO" ? 240 : 160,
              fontWeight: 700, color: stage.color, lineHeight: 1,
              transform: `scale(${numScale})`,
              textShadow: isGreen
                ? "0 0 80px rgba(34,197,94,0.3), 0 4px 30px rgba(0,0,0,0.6)"
                : "0 4px 40px rgba(0,0,0,0.6)",
            }}>{stage.number}</div>

            {/* Unit */}
            <div style={{
              fontFamily: "'Space Mono', monospace", fontSize: 30,
              color: "#aaa", letterSpacing: "0.25em", textTransform: "uppercase" as const,
              marginTop: 10, opacity: labelOp,
            }}>{stage.unit}</div>

            {/* Label */}
            <div style={{
              fontFamily: "'Space Mono', monospace", fontSize: 56, fontWeight: 700,
              color: stage.color, letterSpacing: "0.15em", textTransform: "uppercase" as const,
              marginTop: 36, opacity: labelOp,
              textShadow: isGreen ? "0 0 40px rgba(34,197,94,0.3)" : "none",
            }}>{stage.label}</div>

            {/* Description */}
            <div style={{
              fontFamily: "'Space Mono', monospace", fontSize: 20,
              color: "#888", marginTop: 14, opacity: labelOp, letterSpacing: "0.05em",
            }}>{stage.description}</div>
          </AbsoluteFill>
        );
      })()}

      {/* ── OVERVIEW + END CARD ── */}
      {isOverviewPhase && (() => {
        const overviewOp = interpolate(frame, [330, 360], [0, 1], {
          extrapolateLeft: "clamp", extrapolateRight: "clamp",
        });

        return (
          <AbsoluteFill style={{
            display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
            opacity: overviewOp, zIndex: 10,
          }}>
            {/* Pipeline nodes row */}
            <div style={{ display: "flex", alignItems: "center", gap: 24 }}>
              {STAGES.map((stage, i) => {
                const nodeDelay = 340 + i * 10;
                const nodeOp = interpolate(frame, [nodeDelay, nodeDelay + 15], [0, 1], {
                  extrapolateLeft: "clamp", extrapolateRight: "clamp",
                });
                const isGo = stage.label === "REVIEW";
                const isGreen = stage.color === "#22C55E";

                return (
                  <React.Fragment key={stage.label}>
                    {i > 0 && (
                      <div style={{
                        width: 48, height: 2, opacity: nodeOp,
                        background: "linear-gradient(90deg, rgba(80,80,80,0), rgba(80,80,80,1), rgba(80,80,80,0))",
                      }} />
                    )}
                    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", opacity: nodeOp }}>
                      <div style={{
                        width: isGo ? 76 : 60, height: isGo ? 76 : 60, borderRadius: "50%",
                        border: `2px solid ${isGreen ? "rgba(34,197,94,0.7)" : "rgba(150,150,150,0.4)"}`,
                        background: isGreen
                          ? "radial-gradient(circle, rgba(34,197,94,0.15), transparent)"
                          : "radial-gradient(circle, rgba(255,255,255,0.04), transparent)",
                        display: "flex", alignItems: "center", justifyContent: "center",
                        boxShadow: isGreen ? "0 0 30px rgba(34,197,94,0.25)" : "0 4px 20px rgba(0,0,0,0.4)",
                      }}>
                        <span style={{
                          fontFamily: "'JetBrains Mono', monospace",
                          fontSize: isGo ? 26 : 16, fontWeight: 700,
                          color: isGreen ? "#22C55E" : "#ccc",
                        }}>{isGo ? "R" : String(i + 1).padStart(2, "0")}</span>
                      </div>
                      <span style={{
                        fontFamily: "'Space Mono', monospace", fontSize: 15, fontWeight: 700,
                        color: isGreen ? "#22C55E" : "#fff", letterSpacing: "0.1em",
                        textTransform: "uppercase" as const, marginTop: 10,
                      }}>{stage.label}</span>
                      <span style={{
                        fontFamily: "'JetBrains Mono', monospace", fontSize: 13,
                        color: "#999", marginTop: 4,
                      }}>{stage.number} {stage.unit}</span>
                    </div>
                  </React.Fragment>
                );
              })}
            </div>

            {/* Tagline */}
            {(() => {
              const tagOp = interpolate(frame, [400, 420], [0, 1], {
                extrapolateLeft: "clamp", extrapolateRight: "clamp",
              });
              return (
                <div style={{ marginTop: 64, opacity: tagOp, textAlign: "center" }}>
                  <div style={{
                    fontFamily: "'Space Mono', monospace", fontSize: 48, fontWeight: 700,
                    color: "#fff", letterSpacing: "0.08em",
                    textShadow: "0 4px 40px rgba(0,0,0,0.6)",
                  }}>TWOG</div>
                  <div style={{
                    fontFamily: "'Space Mono', monospace", fontSize: 18,
                    color: "#888", letterSpacing: "0.25em", textTransform: "uppercase" as const,
                    marginTop: 12,
                  }}>Autonomous drug discovery for canine cancer</div>
                  <div style={{
                    fontFamily: "'Space Mono', monospace", fontSize: 20,
                    color: "#22C55E", letterSpacing: "0.2em", marginTop: 18,
                    textShadow: "0 0 25px rgba(34,197,94,0.3)",
                  }}>twog.bio</div>
                </div>
              );
            })()}
          </AbsoluteFill>
        );
      })()}
    </AbsoluteFill>
  );
};
