'use client';

import { useEffect, useState } from 'react';
import { useSupabase } from './useSupabase';

export interface TopCompound {
  id: number;
  name: string;
  composite_score: number;
  target_gene: string | null;
  qed_score: number | null;
  smiles: string | null;
}

/* Short descriptions keyed by target gene — biology is stable even if compounds change */
const TARGET_INFO: Record<string, string> = {
  cPIK3CA: 'PI3K inhibitor. Blocks the survival signal active in 100% of HSA tumors.',
  JAK1: 'JAK1 inhibitor. Disrupts the inflammatory signaling that drives tumor growth.',
  JAK2: 'JAK2 inhibitor. Targets the signaling cascade behind uncontrolled proliferation.',
  CDK6: 'CDK6 inhibitor. Halts the cell division cycle cancer cells exploit to spread.',
  CDK4: 'CDK4 inhibitor. Arrests the checkpoint that lets cancer cells keep dividing.',
  cKDR: 'VEGFR2 inhibitor. Cuts off the blood supply that feeds the tumor.',
  cFLT4: 'VEGFR3 inhibitor. Blocks the lymphatic vessels tumors use to metastasize.',
  PIK3CA: 'PI3K inhibitor. Targets the same survival pathway from a different angle.',
  BRAF: 'BRAF inhibitor. Shuts down the MAPK growth signal driving proliferation.',
  MAP2K2: 'MEK2 inhibitor. Blocks the signaling relay downstream of RAS mutations.',
  MAP2K1: 'MEK1 inhibitor. Interrupts the growth signal cascade at a critical junction.',
  cMTOR: 'mTOR inhibitor. Starves the metabolic engine that fuels tumor growth.',
  EGFR: 'EGFR inhibitor. Blocks the growth factor receptor overexpressed in aggressive tumors.',
  cEGFR: 'EGFR inhibitor. Targets the canine-specific growth receptor driving HSA.',
  MET: 'MET inhibitor. Disrupts the receptor that promotes invasion and metastasis.',
  PDGFRA: 'PDGFRA inhibitor. Targets the stromal growth factor feeding the tumor microenvironment.',
  KDR: 'VEGFR2 inhibitor. Starves the tumor of its blood supply.',
};

export function getTargetDescription(gene: string | null): string {
  if (!gene) return 'Multi-target agent designed to disrupt multiple cancer pathways simultaneously.';
  return TARGET_INFO[gene] ?? `Targets ${gene}. Designed to disrupt a key driver of canine hemangiosarcoma.`;
}

const POLL_INTERVAL = 300_000; // refresh every 5 minutes

export function useTopCompounds() {
  const sb = useSupabase();
  const [lead, setLead] = useState<TopCompound | null>(null);
  const [alternatives, setAlternatives] = useState<TopCompound[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!sb) return;

    async function fetchTop() {
      /* Fetch overall lead — exclude invalidated/failed compounds */
      const { data: topData } = await sb!.from('designed_compounds')
        .select('id, name, composite_score, target_gene, qed_score, smiles')
        .not('composite_score', 'is', null)
        .not('status', 'in', '("invalidated","failed","rejected")')
        .order('composite_score', { ascending: false })
        .limit(1);

      if (!topData || topData.length === 0) {
        setLoading(false);
        return;
      }

      const top = topData[0] as TopCompound;
      setLead(top);

      /* Fetch best compound per target using RPC-style query
         Get a larger set and dedupe — need enough rows to find diverse targets */
      const { data: allData } = await sb!.from('designed_compounds')
        .select('id, name, composite_score, target_gene, qed_score, smiles')
        .not('composite_score', 'is', null)
        .not('target_gene', 'is', null)
        .not('status', 'in', '("invalidated","failed","rejected")')
        .order('composite_score', { ascending: false })
        .limit(500);

      const seen = new Set<string | null>([top.target_gene]);
      const alts: TopCompound[] = [];
      if (allData) {
        for (const c of allData) {
          if (alts.length >= 10) break;
          if (!seen.has(c.target_gene)) {
            seen.add(c.target_gene);
            alts.push(c as TopCompound);
          }
        }
      }
      setAlternatives(alts);
      setLoading(false);
    }

    fetchTop();
    const id = setInterval(fetchTop, POLL_INTERVAL);

    /* Realtime: re-fetch when new high-scoring compounds appear */
    const channel = sb.channel('top_compounds_watch')
      .on('postgres_changes', {
        event: 'INSERT',
        schema: 'public',
        table: 'designed_compounds',
      }, () => {
        fetchTop();
      })
      .subscribe();

    return () => {
      clearInterval(id);
      sb.removeChannel(channel);
    };
  }, [sb]);

  return { lead, alternatives, loading };
}
