import { getServerSupabase } from '@/lib/supabase-server';
import { writeFileSync } from 'fs';
import { join } from 'path';

export async function POST(request: Request) {
  const body = await request.json();
  const { mode, targets, seeds, num_molecules, similarity_threshold, diversity_penalty, rounds_per_target } = body;

  if (!mode || !['exploit', 'explore', 'moonshot'].includes(mode)) {
    return Response.json({ error: 'Invalid mode' }, { status: 400 });
  }
  if (!targets?.length) {
    return Response.json({ error: 'At least one target required' }, { status: 400 });
  }

  const sb = getServerSupabase();

  // Create session record
  const { data: session, error } = await sb.from('design_sessions').insert({
    mode,
    targets,
    seeds: seeds ?? {},
    parameters: {
      num_molecules: num_molecules ?? 15,
      similarity_threshold: similarity_threshold ?? 0.3,
      diversity_penalty: diversity_penalty ?? 0.25,
      rounds_per_target: rounds_per_target ?? {},
    },
    status: 'pending',
  }).select().single();

  if (error) return Response.json({ error: error.message }, { status: 500 });

  // Write config file for the Python design loop runner
  const config = {
    session_id: session.id,
    mode,
    targets,
    seeds: seeds ?? {},
    num_molecules: num_molecules ?? 15,
    similarity_threshold: similarity_threshold ?? 0.3,
    diversity_penalty: diversity_penalty ?? 0.25,
    rounds_per_target: rounds_per_target ?? {},
    created_at: new Date().toISOString(),
  };

  try {
    const configPath = join(process.cwd(), '..', 'config', 'design_lab_session.json');
    writeFileSync(configPath, JSON.stringify(config, null, 2));
  } catch (e) {
    // Non-fatal — session is created in DB even if file write fails
    console.error('Failed to write session config file:', e);
  }

  return Response.json({ sessionId: session.id, status: 'pending' });
}
