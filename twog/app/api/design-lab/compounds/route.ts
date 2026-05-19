import { getServerSupabase } from '@/lib/supabase-server';
import type { NextRequest } from 'next/server';

export async function GET(request: NextRequest) {
  const sb = getServerSupabase();
  const params = request.nextUrl.searchParams;

  const offset = Number(params.get('offset') ?? '0');
  const limit = Math.min(Number(params.get('limit') ?? '50'), 100);
  const sort = params.get('sort') ?? 'composite_score';
  const order = params.get('order') ?? 'desc';
  const target = params.get('target');
  const mdStatus = params.get('mdStatus');
  const minScore = params.get('minScore') ? Number(params.get('minScore')) : null;

  // Build compound query
  let query = sb.from('designed_compounds')
    .select('id, name, smiles, target_gene, composite_score, qed_score, sa_score, docking_confidence, status, admet_summary, created_at', { count: 'exact' });

  if (target) query = query.eq('target_gene', target);
  if (minScore != null) query = query.gte('composite_score', minScore);

  query = query.order(sort, { ascending: order === 'asc' })
    .range(offset, offset + limit - 1);

  const { data: compounds, count, error } = await query;

  if (error) return Response.json({ error: error.message }, { status: 500 });
  if (!compounds?.length) return Response.json({ compounds: [], total: 0, offset });

  // Batch fetch MD validations for these compound IDs
  const compoundIds = compounds.map((c) => c.id);
  const { data: mdData } = await sb.from('md_validations')
    .select('compound_id, stability_classification, ligand_rmsd_mean, protein_rmsd_mean, simulation_length_ns, protocol_version, hbond_occupancy, contact_residues, created_at')
    .in('compound_id', compoundIds)
    .or('invalidated.is.null,invalidated.eq.false')
    .order('simulation_length_ns', { ascending: false });

  // Build best-MD-per-compound map (longest simulation, most recent)
  const mdMap: Record<number, typeof mdData extends (infer T)[] | null ? T : never> = {};
  for (const m of mdData ?? []) {
    const existing = mdMap[m.compound_id];
    if (!existing ||
        m.simulation_length_ns > existing.simulation_length_ns ||
        (m.simulation_length_ns === existing.simulation_length_ns && m.created_at > existing.created_at)) {
      mdMap[m.compound_id] = m;
    }
  }

  // Merge and filter by MD status if requested
  let merged = compounds.map((c) => ({
    ...c,
    md: mdMap[c.id] ?? null,
  }));

  if (mdStatus === 'not_tested') {
    merged = merged.filter((c) => !c.md);
  } else if (mdStatus && mdStatus !== 'any') {
    merged = merged.filter((c) => c.md?.stability_classification === mdStatus);
  }

  return Response.json({
    compounds: merged,
    total: count ?? 0,
    offset,
  });
}
