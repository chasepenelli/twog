"use client";

/* HeroField — the "field of ideas": a slow-drifting point-field where every point is a REAL candidate.
   Green = still standing, cobalt = still being tested, ember = ruled out (dim, cold). Living points
   twinkle; embers don't. The whole field drifts and leans gently toward the cursor. One THREE.Points
   draw call + a small shader — cheap. Positions are a deterministic hash of the candidate id (stable),
   and the color counts equal the real verdict counts — the field IS the scoreboard, not decoration. */

import { useMemo, useRef } from "react";
import { Canvas, useFrame } from "@react-three/fiber";
import * as THREE from "three";

import { STATE_RGB, hashUnit, type FieldPoint } from "./field-shared";

const VERT = /* glsl */ `
  uniform float uTime;
  uniform float uSize;
  uniform float uDpr;
  attribute vec3 aColor;
  attribute vec3 aData; // x=phase, y=twinkle(1/0), z=baseBrightness
  varying vec3 vColor;
  varying float vBright;
  void main() {
    vColor = aColor;
    float living = aData.y;
    float tw = mix(1.0, 0.74 + 0.26 * sin(uTime * 1.5 + aData.x), living);
    vBright = aData.z * mix(0.8, 0.84 + 0.28 * sin(uTime * 1.5 + aData.x), living);
    vec4 mv = modelViewMatrix * vec4(position, 1.0);
    gl_Position = projectionMatrix * mv;
    gl_PointSize = uSize * uDpr * tw * (1.0 / -mv.z);
  }
`;

const FRAG = /* glsl */ `
  precision mediump float;
  varying vec3 vColor;
  varying float vBright;
  void main() {
    vec2 c = gl_PointCoord - 0.5;
    float d = length(c);
    if (d > 0.5) discard;
    float core = smoothstep(0.5, 0.04, d);
    float halo = smoothstep(0.5, 0.0, d);
    // bright core + soft halo; additive blending makes the cores read as glowing dots
    vec3 col = vColor * (0.5 + 1.35 * core) * vBright;
    gl_FragColor = vec4(col, (0.28 * halo + 0.72 * core) * min(1.0, vBright * 1.25));
  }
`;

function Field({ points, mouse }: { points: FieldPoint[]; mouse: React.MutableRefObject<{ x: number; y: number }> }) {
  const matRef = useRef<THREE.ShaderMaterial>(null);
  const groupRef = useRef<THREE.Group>(null);

  const { pointGeo, lineGeo } = useMemo(() => {
    const n = points.length;
    const pos = new Float32Array(n * 3);
    const col = new Float32Array(n * 3);
    const dat = new Float32Array(n * 3);
    const R = 3.6;
    const DEPTH = 1.7;
    points.forEach((p, i) => {
      const a = hashUnit(p.id, 1) * Math.PI * 2;
      const r = Math.sqrt(hashUnit(p.id, 2)) * R;
      pos[i * 3] = Math.cos(a) * r;
      pos[i * 3 + 1] = Math.sin(a) * r * 0.62;
      pos[i * 3 + 2] = (hashUnit(p.id, 3) - 0.5) * DEPTH;
      const c = STATE_RGB[p.state];
      col[i * 3] = c[0];
      col[i * 3 + 1] = c[1];
      col[i * 3 + 2] = c[2];
      const ruled = p.state === "ruled";
      dat[i * 3] = hashUnit(p.id, 4) * Math.PI * 2; // phase
      dat[i * 3 + 1] = ruled ? 0 : 1; // twinkle only for living
      dat[i * 3 + 2] = ruled ? 0.5 : 1.0; // embers dimmer
    });
    const g = new THREE.BufferGeometry();
    g.setAttribute("position", new THREE.BufferAttribute(pos, 3));
    g.setAttribute("aColor", new THREE.BufferAttribute(col, 3));
    g.setAttribute("aData", new THREE.BufferAttribute(dat, 3));

    // constellation: connect each point to its 2 nearest neighbors (dedup edges) — the network web
    const seg: number[] = [];
    const edge = new Set<string>();
    for (let i = 0; i < n; i++) {
      const d: Array<[number, number]> = [];
      for (let j = 0; j < n; j++) {
        if (i === j) continue;
        const dx = pos[i * 3] - pos[j * 3];
        const dy = pos[i * 3 + 1] - pos[j * 3 + 1];
        const dz = pos[i * 3 + 2] - pos[j * 3 + 2];
        d.push([dx * dx + dy * dy + dz * dz, j]);
      }
      d.sort((a2, b2) => a2[0] - b2[0]);
      for (let k = 0; k < Math.min(2, d.length); k++) {
        const j = d[k][1];
        const key = i < j ? `${i}_${j}` : `${j}_${i}`;
        if (edge.has(key)) continue;
        edge.add(key);
        seg.push(pos[i * 3], pos[i * 3 + 1], pos[i * 3 + 2], pos[j * 3], pos[j * 3 + 1], pos[j * 3 + 2]);
      }
    }
    const lg = new THREE.BufferGeometry();
    lg.setAttribute("position", new THREE.BufferAttribute(new Float32Array(seg), 3));

    return { pointGeo: g, lineGeo: lg };
  }, [points]);

  const uniforms = useMemo(
    () => ({
      uTime: { value: 0 },
      uSize: { value: 62 },
      uDpr: { value: Math.min(typeof window !== "undefined" ? window.devicePixelRatio : 1, 1.5) },
    }),
    [],
  );

  useFrame((_, delta) => {
    const d = Math.min(delta, 0.05);
    if (matRef.current) matRef.current.uniforms.uTime.value += d;
    const g = groupRef.current;
    if (g) {
      const tx = mouse.current.x * 0.22;
      const ty = mouse.current.y * 0.14;
      g.rotation.y += (tx - g.rotation.y) * 0.045 + d * 0.05;
      g.rotation.x += (ty * -1 - g.rotation.x) * 0.045;
    }
  });

  return (
    <group ref={groupRef}>
      <lineSegments geometry={lineGeo}>
        <lineBasicMaterial color="#33538f" transparent opacity={0.22} depthWrite={false} blending={THREE.AdditiveBlending} />
      </lineSegments>
      <points geometry={pointGeo}>
        <shaderMaterial
          ref={matRef}
          uniforms={uniforms}
          vertexShader={VERT}
          fragmentShader={FRAG}
          transparent
          depthWrite={false}
          depthTest={false}
          blending={THREE.AdditiveBlending}
        />
      </points>
    </group>
  );
}

export default function HeroField({ points, onReady }: { points: FieldPoint[]; onReady?: () => void }) {
  const mouse = useRef({ x: 0, y: 0 });
  const onMove = (e: React.PointerEvent) => {
    const r = (e.currentTarget as HTMLElement).getBoundingClientRect();
    mouse.current.x = ((e.clientX - r.left) / r.width - 0.5) * 2;
    mouse.current.y = ((e.clientY - r.top) / r.height - 0.5) * 2;
  };
  const reset = () => {
    mouse.current.x = 0;
    mouse.current.y = 0;
  };
  return (
    <div onPointerMove={onMove} onPointerLeave={reset} style={{ width: "100%", height: "100%", cursor: "crosshair" }}>
      <Canvas
        dpr={[1, 1.5]}
        camera={{ position: [0, 0, 5.0], fov: 52 }}
        gl={{ antialias: true, alpha: true, powerPreference: "high-performance" }}
        style={{ width: "100%", height: "100%" }}
        onCreated={() => onReady?.()}
      >
        <Field points={points} mouse={mouse} />
      </Canvas>
    </div>
  );
}
