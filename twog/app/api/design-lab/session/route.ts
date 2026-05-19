import { getServerSupabase } from '@/lib/supabase-server';

export async function GET() {
  const sb = getServerSupabase();

  const { data, error } = await sb.from('design_sessions')
    .select('*')
    .order('created_at', { ascending: false })
    .limit(20);

  if (error) return Response.json({ error: error.message }, { status: 500 });

  return Response.json(data ?? []);
}
