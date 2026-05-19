'use client';

import { useEffect, useState } from 'react';
import { useSupabase } from './useSupabase';

export interface ActionableTreatment {
  id: number;
  name: string;
  category: string;
  description: string | null;
  availability: string | null;
  estimated_cost: string | null;
  canine_safety: string;
  contraindications: string[];
  drug_interactions: string[];
  dosing_dogs: string | null;
  evidence_level: string;
  paper_count: number;
  key_findings: string | null;
  actionability_score: number;
  evidence_score: number;
  safety_score: number;
  mechanisms: string[];
  pathways_targeted: string[];
}

const POLL_INTERVAL = 600_000; // 10 minutes

export function useActionableTreatments() {
  const sb = useSupabase();
  const [treatments, setTreatments] = useState<ActionableTreatment[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!sb) return;

    async function load() {
      const { data } = await sb!
        .from('actionable_treatments')
        .select('*')
        .eq('approved', true)
        .order('actionability_score', { ascending: false });

      if (data) {
        setTreatments(data as ActionableTreatment[]);
      }
      setLoading(false);
    }

    load();
    const id = setInterval(load, POLL_INTERVAL);
    return () => clearInterval(id);
  }, [sb]);

  return { treatments, loading };
}
