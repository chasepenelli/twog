'use client';

import { useEffect, useMemo, useRef, useState } from 'react';

type Record = {
  id: number;
  pmid: string | null;
  doi: string | null;
  title: string;
  journal: string | null;
  year: number | null;
  x: number;
  y: number;
  cluster: number;
  track: string;
};

type TopicMap = {
  meta: {
    n_papers: number;
    n_clusters: number;
    n_noise: number;
    tracks: string[];
    track_counts: { [k: string]: number };
    cluster_counts: { [k: string]: number };
    umap_params: { n_neighbors: number; min_dist: number };
    hdbscan_params: { min_cluster_size: number };
  };
  records: Record[];
};

type ClusterLabels = {
  [cluster_id: string]: {
    name: string;
    top_terms: string[];
    n_papers: number;
    track_mix: { [k: string]: number };
  };
};

const TRACK_COLORS: { [k: string]: string } = {
  treatment: '#ef4444',
  early_detection: '#3b82f6',
  supplements: '#10b981',
  breed_screening: '#f59e0b',
  splenic_hsa: '#06b6d4',
  cross_cutting: '#8b5cf6',
  untagged: '#9ca3af',
};

const TRACK_ORDER = ['treatment', 'early_detection', 'supplements', 'breed_screening', 'splenic_hsa', 'cross_cutting', 'untagged'];

export default function CorpusTopicMap() {
  const [data, setData] = useState<TopicMap | null>(null);
  const [labels, setLabels] = useState<ClusterLabels | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [activeTracks, setActiveTracks] = useState<Set<string>>(new Set(TRACK_ORDER));
  const [search, setSearch] = useState('');
  const [selectedCluster, setSelectedCluster] = useState<number | null>(null);
  const [hoveredPaper, setHoveredPaper] = useState<Record | null>(null);

  const plotRef = useRef<HTMLDivElement | null>(null);
  const plotlyRef = useRef<any>(null);

  // Fetch data once
  useEffect(() => {
    let alive = true;
    Promise.all([
      fetch('/data/topic_map_v2.json').then(r => r.json()),
      fetch('/data/cluster_labels_v2.json').then(r => r.json()),
    ])
      .then(([tm, cl]) => {
        if (!alive) return;
        setData(tm);
        setLabels(cl);
      })
      .catch(e => {
        if (alive) setError(String(e));
      });
    return () => {
      alive = false;
    };
  }, []);

  // Lazy-load plotly on mount (client only)
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const mod: any = await import('plotly.js-dist-min');
      if (!cancelled) plotlyRef.current = mod.default || mod;
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const traces = useMemo(() => {
    if (!data) return [];
    const query = search.trim().toLowerCase();
    const matchesSearch = (r: Record) =>
      !query || r.title.toLowerCase().includes(query) || (r.journal || '').toLowerCase().includes(query);
    const inCluster = (r: Record) => selectedCluster === null || r.cluster === selectedCluster;

    return TRACK_ORDER.filter(t => activeTracks.has(t)).map(track => {
      const filtered = data.records.filter(r => r.track === track && matchesSearch(r) && inCluster(r));
      return {
        type: 'scattergl',
        mode: 'markers',
        name: track,
        x: filtered.map(r => r.x),
        y: filtered.map(r => r.y),
        text: filtered.map(r => r.id.toString()),
        hovertemplate: filtered
          .map(
            r =>
              `<b>${escapeHtml(r.title).slice(0, 120)}</b><br>` +
              `${r.journal ? escapeHtml(r.journal).slice(0, 60) + ' · ' : ''}${r.year ?? ''}<br>` +
              `cluster: ${escapeHtml(labels?.[String(r.cluster)]?.name ?? String(r.cluster))}<br>` +
              `${r.pmid ? `PMID ${r.pmid}` : r.track}<extra></extra>`,
          )
          .slice(0, 1)[0] || '%{text}<extra></extra>',
        // Per-point hovertext
        hovertext: filtered.map(
          r =>
            `<b>${escapeHtml(r.title).slice(0, 120)}</b><br>` +
            `${r.journal ? escapeHtml(r.journal).slice(0, 60) + ' · ' : ''}${r.year ?? ''}<br>` +
            `cluster: ${escapeHtml(labels?.[String(r.cluster)]?.name ?? String(r.cluster))}<br>` +
            `${r.pmid ? `PMID ${r.pmid}` : r.track}`,
        ),
        customdata: filtered.map(r => r.id),
        marker: {
          size: query || selectedCluster !== null ? 6 : 4,
          color: TRACK_COLORS[track],
          opacity: track === 'untagged' ? 0.4 : 0.7,
          line: { width: 0 },
        },
      };
    });
  }, [data, labels, activeTracks, search, selectedCluster]);

  // Render / update plot whenever traces change
  useEffect(() => {
    const Plotly = plotlyRef.current;
    if (!Plotly || !plotRef.current || !data) return;

    const layout = {
      autosize: true,
      margin: { l: 12, r: 12, t: 12, b: 12 },
      xaxis: { showgrid: false, zeroline: false, showticklabels: false, title: '', fixedrange: false },
      yaxis: {
        showgrid: false,
        zeroline: false,
        showticklabels: false,
        title: '',
        scaleanchor: 'x',
      },
      showlegend: false,
      hovermode: 'closest' as const,
      dragmode: 'pan' as const,
      plot_bgcolor: '#fdfcfa',
      paper_bgcolor: '#fdfcfa',
      hoverlabel: {
        bgcolor: 'white',
        bordercolor: '#e5e5e5',
        font: { size: 11.5, family: 'Georgia, serif', color: '#0a0a0a' },
        align: 'left' as const,
      },
    };

    const config = {
      displaylogo: false,
      scrollZoom: true,
      responsive: true,
      modeBarButtonsToRemove: ['lasso2d', 'select2d', 'autoScale2d', 'toggleSpikelines'],
      // Use hovermode 'closest' to avoid flicker on scattergl
      doubleClick: 'reset' as const,
    };

    // Use react() for efficient updates after initial plot
    const fn = (plotRef.current as any)._hasPlot ? Plotly.react : Plotly.newPlot;
    fn(plotRef.current, traces, layout, config).then(() => {
      if (plotRef.current) (plotRef.current as any)._hasPlot = true;
    });

    // Wire hover / click once
    const div = plotRef.current as any;
    if (!div._handlersBound) {
      div.on('plotly_hover', (ev: any) => {
        const pt = ev.points?.[0];
        if (!pt) return;
        const id = pt.customdata;
        const rec = data.records.find(r => r.id === id);
        if (rec) setHoveredPaper(rec);
      });
      div.on('plotly_click', (ev: any) => {
        const pt = ev.points?.[0];
        const id = pt?.customdata;
        const rec = data.records.find(r => r.id === id);
        if (!rec) return;
        if (rec.pmid) window.open(`https://pubmed.ncbi.nlm.nih.gov/${rec.pmid}/`, '_blank');
        else if (rec.doi) window.open(`https://doi.org/${rec.doi}`, '_blank');
      });
      div._handlersBound = true;
    }

    // Cleanup on unmount only
    return undefined;
  }, [traces, data]);

  // Full cleanup on unmount
  useEffect(() => {
    return () => {
      const Plotly = plotlyRef.current;
      if (Plotly && plotRef.current) {
        try {
          Plotly.purge(plotRef.current);
        } catch {
          /* noop */
        }
      }
    };
  }, []);

  const clusterList = useMemo(() => {
    if (!labels) return [];
    return Object.entries(labels)
      .map(([id, v]) => ({ id: parseInt(id, 10), ...v }))
      .filter(c => c.id !== -1)
      .sort((a, b) => b.n_papers - a.n_papers);
  }, [labels]);

  if (error) {
    return <div className="p-6 text-sm font-mono text-red-600">Failed to load topic map: {error}</div>;
  }

  if (!data || !labels) {
    return (
      <div className="flex h-[60vh] items-center justify-center text-xs font-mono text-[var(--gray-400)]">
        <div className="flex flex-col items-center gap-3">
          <div className="flex items-center gap-2">
            <span className="inline-block w-1.5 h-1.5 rounded-full bg-[var(--gray-400)] animate-pulse" />
            <span>loading 16,193 papers · {data ? 'preparing plot engine' : 'fetching corpus'}…</span>
          </div>
          <div className="text-[0.6rem] text-[var(--gray-400)]">First load: ~4 MB of coordinates + 1 MB plot library. Should take 2-5 seconds on most connections.</div>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col lg:flex-row gap-4 w-full">
      {/* Plot */}
      <div className="flex-1 min-w-0">
        <div className="mb-3 flex flex-wrap gap-2 items-center">
          <input
            type="text"
            placeholder="search titles & journals…"
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="flex-1 min-w-[200px] px-3 py-1.5 text-xs border border-[var(--gray-200)] rounded-lg font-mono focus:outline-none focus:border-[var(--gray-400)]"
          />
          {selectedCluster !== null && (
            <button
              onClick={() => setSelectedCluster(null)}
              className="px-3 py-1.5 text-[0.7rem] font-mono border border-[var(--gray-200)] rounded-lg hover:bg-[var(--gray-100)]"
            >
              clear cluster filter
            </button>
          )}
        </div>

        <div className="flex flex-wrap gap-2 mb-3 text-[0.72rem]">
          <span className="text-[0.65rem] font-mono text-[var(--gray-400)] self-center mr-1">toggle:</span>
          {TRACK_ORDER.map(track => {
            const active = activeTracks.has(track);
            return (
              <button
                key={track}
                onClick={() => {
                  const next = new Set(activeTracks);
                  if (active) next.delete(track);
                  else next.add(track);
                  setActiveTracks(next);
                }}
                className="flex items-center gap-1.5 px-2.5 py-1 rounded-full border transition-all"
                style={{
                  borderColor: active ? TRACK_COLORS[track] : 'var(--gray-200)',
                  background: active ? `${TRACK_COLORS[track]}10` : 'transparent',
                  color: active ? TRACK_COLORS[track] : 'var(--gray-400)',
                }}
              >
                <span
                  className="inline-block w-2 h-2 rounded-full"
                  style={{ background: active ? TRACK_COLORS[track] : 'var(--gray-300)' }}
                />
                {track.replace('_', ' ')}
              </button>
            );
          })}
        </div>

        <div
          className="border border-[var(--gray-200)] rounded-2xl overflow-hidden shadow-sm"
          style={{ height: '70vh', minHeight: 520, background: '#fdfcfa' }}
        >
          <div ref={plotRef} style={{ width: '100%', height: '100%' }} />
        </div>

        <div className="mt-2 text-[0.68rem] text-[var(--gray-400)] italic" style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}>
          {data.meta.n_papers.toLocaleString()} papers, grouped into {data.meta.n_clusters} neighborhoods.
          Drag to pan, scroll to zoom, click a dot to open it on PubMed.
        </div>

        {hoveredPaper && (
          <div className="mt-3 p-3 border border-[var(--gray-200)] rounded-lg bg-[var(--gray-100)]">
            <p className="text-[0.75rem] font-semibold leading-snug">{hoveredPaper.title}</p>
            <p className="text-[0.65rem] font-mono text-[var(--gray-400)] mt-1">
              {hoveredPaper.journal && <>{hoveredPaper.journal} · </>}
              {hoveredPaper.year}
              {hoveredPaper.pmid && (
                <>
                  {' · '}
                  <a
                    href={`https://pubmed.ncbi.nlm.nih.gov/${hoveredPaper.pmid}/`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="underline hover:text-[var(--foreground)]"
                  >
                    PMID {hoveredPaper.pmid}
                  </a>
                </>
              )}
            </p>
            <p className="text-[0.6rem] font-mono text-[var(--gray-500)] mt-1">
              cluster {hoveredPaper.cluster}: {labels[String(hoveredPaper.cluster)]?.name || '—'} · track{' '}
              {hoveredPaper.track}
            </p>
          </div>
        )}
      </div>

      {/* Cluster panel */}
      <aside className="w-full lg:w-80 shrink-0">
        <div className="border border-[var(--gray-200)] rounded-xl bg-white">
          <div className="px-3 py-2 border-b border-[var(--gray-200)]">
            <p className="text-[0.7rem] font-mono uppercase tracking-wider text-[var(--gray-400)]">
              Clusters · {clusterList.length}
            </p>
          </div>
          <div className="max-h-[70vh] overflow-y-auto">
            {clusterList.map(c => {
              const dominantTrack = Object.entries(c.track_mix).sort((a, b) => b[1] - a[1])[0]?.[0] || 'untagged';
              const color = TRACK_COLORS[dominantTrack] || '#9ca3af';
              const isSelected = selectedCluster === c.id;
              return (
                <button
                  key={c.id}
                  onClick={() => setSelectedCluster(isSelected ? null : c.id)}
                  className={`w-full text-left px-3 py-2 border-b border-[var(--gray-100)] hover:bg-[var(--gray-100)] transition ${
                    isSelected ? 'bg-[var(--gray-100)]' : ''
                  }`}
                >
                  <div className="flex items-center gap-2">
                    <span className="inline-block w-2 h-2 rounded-full shrink-0" style={{ background: color }} />
                    <p className="text-[0.75rem] font-semibold leading-tight flex-1">{c.name}</p>
                    <span className="text-[0.6rem] font-mono text-[var(--gray-400)] shrink-0">{c.n_papers}</span>
                  </div>
                  <p className="text-[0.6rem] font-mono text-[var(--gray-400)] mt-1 pl-4 truncate">
                    {c.top_terms.slice(0, 5).join(' · ')}
                  </p>
                </button>
              );
            })}
          </div>
        </div>
      </aside>
    </div>
  );
}

function escapeHtml(s: string): string {
  return s.replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c] || c));
}
