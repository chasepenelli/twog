'use client';

import { useEffect, useState } from 'react';
import { useSupabase } from './useSupabase';

export interface ValidationCompound {
  id: number;
  name: string;
  smiles: string;
  target_gene: string;
  composite_score: number;
  md?: MdValidation;
  selectivity?: SelectivitySummary;
  safety?: SafetyProfile;
  synergy?: SynergyModel;
  ip?: IpReport;
  literature?: LiteratureValidation;
  offtargets?: OfftargetProfile[];
  analogs?: AnalogCompound[];
}

export interface MdValidation {
  rmsd_mean: number | null;
  rmsd_std: number | null;
  binding_free_energy: number | null;
  stable_binding: boolean | null;
  stability_classification: string | null;
  simulation_length_ns: number | null;
  compute_time_hours: number | null;
}

export interface SelectivitySummary {
  pi3ka_selectivity_index: number | null;
  herg_risk: boolean | null;
  cyp_inhibition_risk: boolean | null;
  beneficial_offtargets: string | null;
}

export interface SafetyProfile {
  metabolite_count: number | null;
  metabolic_stability: string | null;
  primary_cyp_enzyme: string | null;
  herg_risk_score: number | null;
  herg_risk_level: string | null;
  herg_flagged_feature: string | null;
  aqueous_solubility_mg_ml: number | null;
  caco2_permeability: number | null;
  plasma_protein_binding_pct: number | null;
  oral_feasibility: string | null;
  recommended_formulation: string | null;
  overall_safety_score: number | null;
}

export interface SynergyModel {
  synergy_score: number | null;
  synergy_classification: string | null;
  synergy_mechanism: string | null;
  pathway_nodes_affected: string | null;
  resistance_predictions: string | null;
  drugs: string | null;
}

export interface IpReport {
  patent_hits: string | null;
  freedom_to_operate: string | null;
  ip_risk_summary: string | null;
}

export interface LiteratureValidation {
  similar_compounds: string | null;
  published_ic50_proxy: number | null;
  sar_agreement: string | null;
  relevant_publications: string | null;
}

export interface OfftargetProfile {
  target_name: string;
  target_pdb_id: string;
  docking_score: number | null;
  selectivity_ratio: number | null;
  classification: string | null;
  risk_level: string | null;
}

export interface AnalogCompound {
  smiles: string;
  modification_description: string | null;
  modification_type: string | null;
  composite_score: number | null;
  docking_score: number | null;
  sa_score: number | null;
  rank: number | null;
}

export interface CampaignInfo {
  name: string;
  status: string;
  total_compounds: number;
  completed_compounds: number;
  created_at: string;
}

export function useValidationData() {
  const sb = useSupabase();
  const [compounds, setCompounds] = useState<ValidationCompound[]>([]);
  const [campaign, setCampaign] = useState<CampaignInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!sb) return;

    async function load() {
      try {
        // Get latest completed campaign
        const { data: campaigns, error: campErr } = await sb!.from('validation_campaigns')
          .select('name, status, total_compounds, completed_compounds, created_at')
          .eq('status', 'completed')
          .order('created_at', { ascending: false })
          .limit(1);
        if (campErr) console.error('[Validation] Campaign query error:', campErr);
        if (campaigns?.[0]) setCampaign(campaigns[0]);

        // Get compound IDs that have validation data (safety profiles OR md_validations)
        const { data: safetyRows, error: safetyErr } = await sb!.from('safety_profiles')
          .select('compound_id')
          .limit(100);
        if (safetyErr) console.error('[Validation] Safety profiles query error:', safetyErr);

        const { data: mdRows, error: mdErr } = await sb!.from('md_validations')
          .select('compound_id')
          .limit(100);
        if (mdErr) console.error('[Validation] MD validations query error:', mdErr);

        // Merge IDs from both sources
        const allIds = [
          ...(safetyRows ?? []).map(r => r.compound_id),
          ...(mdRows ?? []).map(r => r.compound_id),
        ];
        const validatedIds = [...new Set(allIds)].filter(Boolean);

        if (validatedIds.length === 0) {
          console.warn('[Validation] No validated compound IDs found');
          setLoading(false);
          return;
        }

        // Fetch compounds
        const { data: compoundRows, error: compErr } = await sb!.from('designed_compounds')
          .select('id, name, smiles, target_gene, composite_score')
          .in('id', validatedIds)
          .order('composite_score', { ascending: false });
        if (compErr) console.error('[Validation] Compounds query error:', compErr);

        if (!compoundRows?.length) {
          console.warn('[Validation] No compound rows matched validated IDs:', validatedIds);
          setLoading(false);
          return;
        }

        // Fetch all validation data in parallel
        const ids = compoundRows.map(c => c.id);
        const [md, sel, safety, synergy, ip, lit, offtarget, analogs] = await Promise.all([
          sb!.from('md_validations').select('*').in('compound_id', ids).order('compute_time_hours', { ascending: false }),
          sb!.from('selectivity_summaries').select('*').in('compound_id', ids),
          sb!.from('safety_profiles').select('*').in('compound_id', ids),
          sb!.from('synergy_models').select('*').in('compound_id', ids).order('created_at', { ascending: false }).limit(20),
          sb!.from('ip_reports').select('*').in('compound_id', ids),
          sb!.from('literature_validations').select('*').in('compound_id', ids),
          sb!.from('offtarget_profiles').select('*').in('compound_id', ids).order('selectivity_ratio', { ascending: true }),
          sb!.from('analog_compounds').select('*').in('parent_compound_id', ids).order('rank', { ascending: true }),
        ]);

        // Index by compound_id
        const byId = <T extends { compound_id?: number; parent_compound_id?: number }>(
          rows: T[] | null, key: 'compound_id' | 'parent_compound_id' = 'compound_id'
        ) => {
          const map: Record<number, T[]> = {};
          for (const r of rows ?? []) {
            const id = r[key] as number;
            if (!map[id]) map[id] = [];
            map[id].push(r);
          }
          return map;
        };

        const mdMap = byId(md.data);
        const selMap = byId(sel.data);
        const safetyMap = byId(safety.data);
        const synergyMap = byId(synergy.data);
        const ipMap = byId(ip.data);
        const litMap = byId(lit.data);
        const otMap = byId(offtarget.data);
        const analogMap = byId(analogs.data, 'parent_compound_id');

        const result: ValidationCompound[] = compoundRows.map(c => ({
          id: c.id,
          name: c.name,
          smiles: c.smiles,
          target_gene: c.target_gene,
          composite_score: c.composite_score,
          md: mdMap[c.id]?.[0],
          selectivity: selMap[c.id]?.[0],
          safety: safetyMap[c.id]?.[0],
          synergy: synergyMap[c.id]?.[0],
          ip: ipMap[c.id]?.[0],
          literature: litMap[c.id]?.[0],
          offtargets: otMap[c.id] ?? [],
          analogs: analogMap[c.id] ?? [],
        }));

        setCompounds(result);
      } catch (err) {
        console.error('[Validation] Unexpected error:', err);
        setError(String(err));
      } finally {
        setLoading(false);
      }
    }

    load();
  }, [sb]);

  return { compounds, campaign, loading, error };
}
