import { getServerSupabase } from '@/lib/supabase-server';

export async function GET() {
  const sb = getServerSupabase();

  // Group by parent_smiles as scaffold proxy
  const { data: compounds } = await sb.from('designed_compounds')
    .select('id, parent_smiles, target_gene, composite_score, smiles');

  if (!compounds?.length) return Response.json([]);

  // Get all MD validations
  const { data: mdData } = await sb.from('md_validations')
    .select('compound_id, stability_classification');

  const mdMap: Record<number, string> = {};
  for (const m of mdData ?? []) {
    // Keep the best classification per compound
    const existing = mdMap[m.compound_id];
    if (!existing || m.stability_classification === 'stable' ||
        (m.stability_classification === 'marginal' && existing === 'unstable')) {
      mdMap[m.compound_id] = m.stability_classification;
    }
  }

  // Group by parent_smiles
  const families: Record<string, {
    parentSmiles: string;
    count: number;
    targets: Set<string>;
    mdTested: number;
    mdStable: number;
    mdMarginal: number;
    bestComposite: number;
    bestSmiles: string;
  }> = {};

  for (const c of compounds) {
    const key = c.parent_smiles ?? 'unknown';
    if (!families[key]) {
      families[key] = {
        parentSmiles: key,
        count: 0,
        targets: new Set(),
        mdTested: 0,
        mdStable: 0,
        mdMarginal: 0,
        bestComposite: 0,
        bestSmiles: '',
      };
    }
    const f = families[key];
    f.count++;
    if (c.target_gene) f.targets.add(c.target_gene);
    if (c.composite_score > f.bestComposite) {
      f.bestComposite = c.composite_score;
      f.bestSmiles = c.smiles;
    }

    const mdStatus = mdMap[c.id];
    if (mdStatus) {
      f.mdTested++;
      if (mdStatus === 'stable') f.mdStable++;
      if (mdStatus === 'marginal') f.mdMarginal++;
    }
  }

  // Sort by compound count, return top 30
  const result = Object.values(families)
    .map((f) => ({
      ...f,
      targets: Array.from(f.targets),
      mdPassRate: f.mdTested > 0 ? f.mdStable / f.mdTested : 0,
    }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 30);

  return Response.json(result);
}
