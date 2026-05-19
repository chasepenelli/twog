import { getServerSupabase } from '@/lib/supabase-server';
import { readFileSync, writeFileSync } from 'fs';
import { join } from 'path';

const DIRECTIVE_PATH = join(process.cwd(), '..', 'committee', 'directive.json');

function loadDirective() {
  return JSON.parse(readFileSync(DIRECTIVE_PATH, 'utf-8'));
}

function saveDirective(directive: Record<string, unknown>) {
  writeFileSync(DIRECTIVE_PATH, JSON.stringify(directive, null, 2));
}

export async function POST(request: Request) {
  const { action, target_gene, smiles } = await request.json();

  if (!target_gene) return Response.json({ error: 'target_gene required' }, { status: 400 });

  const directive = loadDirective();
  const trackB = directive.track_b ?? directive;
  const seeds: Record<string, string[]> = trackB.seed_compounds ?? {};

  if (action === 'add' && smiles) {
    if (!seeds[target_gene]) seeds[target_gene] = [];
    if (!seeds[target_gene].includes(smiles)) {
      seeds[target_gene].push(smiles);
    }
    trackB.seed_compounds = seeds;
    saveDirective(directive);
    return Response.json({ seeds: seeds[target_gene] });

  } else if (action === 'remove' && smiles) {
    if (seeds[target_gene]) {
      seeds[target_gene] = seeds[target_gene].filter((s: string) => s !== smiles);
    }
    trackB.seed_compounds = seeds;
    saveDirective(directive);
    return Response.json({ seeds: seeds[target_gene] ?? [] });

  } else if (action === 'promote_stable') {
    // Find MD-stable compounds for this target
    const sb = getServerSupabase();
    const { data: stableValidations } = await sb.from('md_validations')
      .select('compound_id')
      .eq('stability_classification', 'stable');

    if (!stableValidations?.length) {
      return Response.json({ seeds: seeds[target_gene] ?? [], promoted: 0 });
    }

    const compoundIds = stableValidations.map((v) => v.compound_id);
    const { data: stableCompounds } = await sb.from('designed_compounds')
      .select('smiles')
      .in('id', compoundIds)
      .eq('target_gene', target_gene);

    if (!seeds[target_gene]) seeds[target_gene] = [];
    let promoted = 0;
    for (const c of stableCompounds ?? []) {
      if (c.smiles && !seeds[target_gene].includes(c.smiles)) {
        seeds[target_gene].push(c.smiles);
        promoted++;
      }
    }
    trackB.seed_compounds = seeds;
    saveDirective(directive);
    return Response.json({ seeds: seeds[target_gene], promoted });

  }

  return Response.json({ error: 'Invalid action' }, { status: 400 });
}
