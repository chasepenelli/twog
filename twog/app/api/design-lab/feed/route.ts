import { getServerSupabase } from '@/lib/supabase-server';
import type { NextRequest } from 'next/server';

export async function GET(request: NextRequest) {
  const sb = getServerSupabase();
  const limit = Number(request.nextUrl.searchParams.get('limit') ?? '100');

  const { data, error } = await sb.from('agent_logs')
    .select('id, level, message, created_at')
    .order('created_at', { ascending: false })
    .limit(limit);

  if (error) return Response.json({ error: error.message }, { status: 500 });

  // Filter to design-related logs client-side (Supabase text search is limited)
  const designKeywords = ['design_loop', 'molgen', 'md_agent', 'md_runner', 'v3_screen', 'design_lab', 'MDAgent'];
  const filtered = (data ?? []).filter((log) =>
    designKeywords.some((kw) => log.message?.includes(kw))
  );

  return Response.json(filtered);
}
