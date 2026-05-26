'use client';

import { useEffect, useRef, useState } from 'react';

interface Molecule3DProps {
  compoundName: string;
  size?: number;
}

const STORAGE_BASE = 'https://ktkvqoaskukndgxhutzg.supabase.co/storage/v1/object/public/videos/molecules';

type MoleculeViewer = {
  addModel: (data: string, format: string) => void;
  setStyle: (selection: Record<string, never>, style: Record<string, unknown>) => void;
  zoomTo: () => void;
  zoom: (factor: number) => void;
  render: () => void;
  spin: (axis: string, speed: number) => void;
};

type ThreeDmolModule = {
  createViewer: (element: HTMLDivElement, config: Record<string, unknown>) => MoleculeViewer;
};

export default function Molecule3D({ compoundName, size = 280 }: Molecule3DProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const viewerRef = useRef<HTMLDivElement>(null);
  const [loaded, setLoaded] = useState(false);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    if (!viewerRef.current || typeof window === 'undefined') return;
    let cancelled = false;

    async function init() {
      const $3Dmol = (await import('3dmol')) as unknown as ThreeDmolModule;
      if (cancelled || !viewerRef.current) return;

      // Create viewer in the inner square div (no border-radius on the canvas container)
      const viewer = $3Dmol.createViewer(viewerRef.current, {
        backgroundColor: 'rgba(0,0,0,0)',
        antialias: true,
        disableFog: true,
        disableMouse: true,
      });

      try {
        const resp = await fetch(`${STORAGE_BASE}/${compoundName}_3d.mol`);
        if (!resp.ok) throw new Error(`${resp.status}`);
        const molData = await resp.text();
        if (cancelled) return;

        viewer.addModel(molData, 'mol');
        viewer.setStyle({}, {
          stick: {
            radius: 0.14,
            colorscheme: { prop: 'elem', map: {
              C: 0xffffff, N: 0x22C55E, O: 0xff6b6b, S: 0xffd700,
              F: 0x66d9ef, Cl: 0x66d9ef, Br: 0xcc6633, H: 0x666666,
            }},
          },
          sphere: {
            scale: 0.28,
            colorscheme: { prop: 'elem', map: {
              C: 0xffffff, N: 0x22C55E, O: 0xff6b6b, S: 0xffd700,
              F: 0x66d9ef, Cl: 0x66d9ef, Br: 0xcc6633, H: 0x666666,
            }},
          },
        });
        viewer.zoomTo();
        viewer.zoom(0.8);
        viewer.render();
        viewer.spin('y', 0.8);
        setLoaded(true);
      } catch {
        if (!cancelled) setFailed(true);
      }
    }

    init();
    return () => { cancelled = true; };
  }, [compoundName]);

  return (
    <div
      ref={containerRef}
      style={{
        width: size,
        height: size,
        position: 'relative',
        borderRadius: '50%',
        overflow: 'hidden',
        filter: loaded
          ? 'drop-shadow(0 0 30px rgba(34,197,94,0.1)) drop-shadow(0 8px 20px rgba(0,0,0,0.3))'
          : 'none',
      }}
    >
      {/* 3Dmol canvas container — no border-radius so WebGL renders clean */}
      <div
        ref={viewerRef}
        style={{
          width: '100%',
          height: '100%',
          pointerEvents: 'none',
        }}
      />

      {/* Fallback: compound name initial when no 3D available */}
      {(failed || (!loaded && !viewerRef.current)) && (
        <div
          style={{
            position: 'absolute',
            inset: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            border: '1.5px solid rgba(34,197,94,0.2)',
            borderRadius: '50%',
            background: 'radial-gradient(circle, rgba(34,197,94,0.04), transparent)',
          }}
        >
          <span style={{
            fontFamily: "'JetBrains Mono', monospace",
            fontSize: size * 0.15,
            color: 'rgba(34,197,94,0.4)',
            fontWeight: 700,
          }}>
            {compoundName.replace('GRF-DL-', '')}
          </span>
        </div>
      )}

      {/* Loading pulse */}
      {!loaded && !failed && (
        <div
          style={{
            position: 'absolute',
            inset: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            animation: 'pulse 2s ease-in-out infinite',
          }}
        >
          <div style={{
            width: '60%',
            height: '60%',
            borderRadius: '50%',
            border: '1px solid rgba(34,197,94,0.15)',
          }} />
        </div>
      )}
    </div>
  );
}
