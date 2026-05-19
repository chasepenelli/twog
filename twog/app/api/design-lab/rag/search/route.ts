import { getServerSupabase } from '@/lib/supabase-server';
import { getEmbedding } from '@/lib/openrouter';

const SOURCE_RPC_MAP: Record<string, string> = {
  papers: 'match_papers',
  hypotheses: 'match_hypotheses',
  discoveries: 'match_discoveries',
  trials: 'match_trials',
};

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const {
      query,
      sources,
      threshold = 0.45,
      limit = 10,
    }: {
      query: string;
      sources?: string[];
      threshold?: number;
      limit?: number;
    } = body;

    if (!query || typeof query !== 'string') {
      return Response.json({ error: 'query is required' }, { status: 400 });
    }

    const embedding = await getEmbedding(query);
    const embeddingStr = JSON.stringify(embedding);
    const sb = getServerSupabase();

    let allResults: Record<string, unknown>[] = [];

    // Use specific source RPCs if requested, otherwise match_all_sources
    if (sources && sources.length > 0 && sources.every((s) => s in SOURCE_RPC_MAP)) {
      for (const source of sources) {
        const rpcName = SOURCE_RPC_MAP[source];
        const { data, error } = await sb.rpc(rpcName, {
          query_embedding: embeddingStr,
          match_threshold: threshold,
          match_count: limit,
        });

        if (error) {
          console.error(`RPC ${rpcName} failed:`, error.message);
          continue;
        }

        if (data) {
          for (const row of data as Record<string, unknown>[]) {
            allResults.push({ ...row, source_table: source });
          }
        }
      }
    } else {
      const { data, error } = await sb.rpc('match_all_sources', {
        query_embedding: embeddingStr,
        match_threshold: threshold,
        match_count: limit,
      });

      if (error) {
        return Response.json({ error: error.message }, { status: 500 });
      }

      allResults = (data as Record<string, unknown>[]) || [];
    }

    // Sort by similarity descending, cap at limit
    allResults.sort(
      (a, b) => (b.similarity as number) - (a.similarity as number),
    );
    const results = allResults.slice(0, limit);

    return Response.json({ results, count: results.length });
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Unknown error';
    console.error('RAG search error:', message);
    return Response.json({ error: message }, { status: 500 });
  }
}
