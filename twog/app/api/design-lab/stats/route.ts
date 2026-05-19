import { getServerSupabase } from '@/lib/supabase-server';

export async function GET() {
  const sb = getServerSupabase();

  const [designed, docked, mdScreened, mdStable, v3Stable] = await Promise.all([
    sb.from('designed_compounds').select('*', { count: 'exact', head: true }),
    sb.from('designed_compounds').select('*', { count: 'exact', head: true })
      .not('docking_confidence', 'is', null),
    sb.from('md_validations').select('compound_id', { count: 'exact', head: true })
      .or('invalidated.is.null,invalidated.eq.false'),
    sb.from('md_validations').select('*', { count: 'exact', head: true })
      .eq('stability_classification', 'stable')
      .or('invalidated.is.null,invalidated.eq.false'),
    sb.from('md_validations').select('*', { count: 'exact', head: true })
      .eq('protocol_version', 'v3')
      .eq('stability_classification', 'stable')
      .or('invalidated.is.null,invalidated.eq.false'),
  ]);

  return Response.json({
    designed: designed.count ?? 0,
    docked: docked.count ?? 0,
    mdScreened: mdScreened.count ?? 0,
    mdStable: mdStable.count ?? 0,
    v3Validated: v3Stable.count ?? 0,
  });
}
