'use client';

import { useEffect, useState, useCallback } from 'react';

export interface CompoundWithMD {
  id: number;
  name: string;
  smiles: string;
  target_gene: string;
  composite_score: number;
  qed_score: number;
  sa_score: number | null;
  docking_confidence: number | null;
  status: string;
  admet_summary: Record<string, number> | null;
  created_at: string;
  md: {
    stability_classification: string;
    ligand_rmsd_mean: number;
    protein_rmsd_mean: number;
    simulation_length_ns: number;
    protocol_version: string | null;
    hbond_occupancy: number | null;
    contact_residues: unknown | null;
    created_at: string;
  } | null;
}

export interface CompoundsFilter {
  target?: string;
  mdStatus?: string;
  minScore?: number;
  sort?: string;
  order?: 'asc' | 'desc';
}

export function useDesignLabCompounds(initialFilter?: CompoundsFilter) {
  const [compounds, setCompounds] = useState<CompoundWithMD[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [filter, setFilter] = useState<CompoundsFilter>(initialFilter ?? {});
  const [loading, setLoading] = useState(true);
  const limit = 50;

  const fetchCompounds = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      params.set('offset', String(offset));
      params.set('limit', String(limit));
      if (filter.sort) params.set('sort', filter.sort);
      if (filter.order) params.set('order', filter.order);
      if (filter.target) params.set('target', filter.target);
      if (filter.mdStatus) params.set('mdStatus', filter.mdStatus);
      if (filter.minScore != null) params.set('minScore', String(filter.minScore));

      const res = await fetch(`/api/design-lab/compounds?${params}`);
      const data = await res.json();
      setCompounds(data.compounds ?? []);
      setTotal(data.total ?? 0);
    } catch (e) {
      console.error('Failed to fetch compounds:', e);
    } finally {
      setLoading(false);
    }
  }, [offset, filter]);

  useEffect(() => {
    fetchCompounds();
  }, [fetchCompounds]);

  return {
    compounds,
    total,
    offset,
    limit,
    loading,
    filter,
    setFilter: (f: CompoundsFilter) => { setOffset(0); setFilter(f); },
    nextPage: () => setOffset((o) => Math.min(o + limit, total - 1)),
    prevPage: () => setOffset((o) => Math.max(o - limit, 0)),
    refresh: fetchCompounds,
  };
}
