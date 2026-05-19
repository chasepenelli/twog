import { getServerSupabase } from '@/lib/supabase-server';
import { readFileSync } from 'fs';
import { join } from 'path';

export async function GET() {
  const sb = getServerSupabase();

  // Fetch protein targets
  const { data: targets } = await sb.from('protein_targets')
    .select('gene_symbol, pdb_id, uniprot_id, pocket_center_x, pocket_center_y, pocket_center_z');

  // Fetch compound counts per target
  const { data: compounds } = await sb.from('designed_compounds')
    .select('target_gene');

  // Fetch MD validation counts per target (join via compound)
  const { data: mdResults } = await sb.from('md_validations')
    .select('compound_id, stability_classification')
    .or('invalidated.is.null,invalidated.eq.false');

  const { data: mdCompounds } = await sb.from('designed_compounds')
    .select('id, target_gene');

  // Build lookup maps
  const compoundCounts: Record<string, number> = {};
  for (const c of compounds ?? []) {
    const g = c.target_gene;
    if (g) compoundCounts[g] = (compoundCounts[g] ?? 0) + 1;
  }

  const compoundTargetMap: Record<number, string> = {};
  for (const c of mdCompounds ?? []) {
    compoundTargetMap[c.id] = c.target_gene;
  }

  const mdCounts: Record<string, number> = {};
  const stableCounts: Record<string, number> = {};
  for (const m of mdResults ?? []) {
    const gene = compoundTargetMap[m.compound_id];
    if (!gene) continue;
    mdCounts[gene] = (mdCounts[gene] ?? 0) + 1;
    if (m.stability_classification === 'stable') {
      stableCounts[gene] = (stableCounts[gene] ?? 0) + 1;
    }
  }

  // Load directive for seeds and ordering
  let directive: Record<string, unknown> = {};
  try {
    const path = join(process.cwd(), '..', 'committee', 'directive.json');
    directive = JSON.parse(readFileSync(path, 'utf-8'));
  } catch { /* no directive */ }

  const trackB = (directive.track_b ?? directive) as Record<string, unknown>;
  const targetOrder = (trackB.target_order ?? []) as string[];
  const roundsPerTarget = (trackB.rounds_per_target ?? {}) as Record<string, number>;
  const seedCompounds = (trackB.seed_compounds ?? {}) as Record<string, string[]>;

  const result = (targets ?? []).map((t) => {
    const gene = t.gene_symbol;
    const mdCount = mdCounts[gene] ?? 0;
    const stableCount = stableCounts[gene] ?? 0;
    return {
      gene,
      pdbId: t.pdb_id,
      uniprotId: t.uniprot_id,
      pocket: t.pocket_center_x != null
        ? [t.pocket_center_x, t.pocket_center_y, t.pocket_center_z]
        : null,
      compoundCount: compoundCounts[gene] ?? 0,
      mdCount,
      stableCount,
      mdPassRate: mdCount > 0 ? stableCount / mdCount : 0,
      seeds: seedCompounds[gene] ?? [],
      directiveRounds: roundsPerTarget[gene] ?? 0,
      directiveOrder: targetOrder.indexOf(gene),
    };
  }).sort((a, b) => {
    if (a.directiveOrder >= 0 && b.directiveOrder >= 0) return a.directiveOrder - b.directiveOrder;
    if (a.directiveOrder >= 0) return -1;
    if (b.directiveOrder >= 0) return 1;
    return b.compoundCount - a.compoundCount;
  });

  return Response.json(result);
}
