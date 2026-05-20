import { createClient, type SupabaseClient } from '@supabase/supabase-js';

declare global {
  var twogServerSupabaseClient: SupabaseClient | undefined;
}

function supabaseUrl(): string | undefined {
  return process.env.SUPABASE_URL ?? process.env.NEXT_PUBLIC_SUPABASE_URL;
}

function supabaseKey(): string | undefined {
  return (
    process.env.SUPABASE_SERVICE_ROLE_KEY ??
    process.env.SUPABASE_ANON_KEY ??
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY
  );
}

export function isServerSupabaseConfigured(): boolean {
  return Boolean(supabaseUrl() && supabaseKey());
}

export function getServerSupabase(): SupabaseClient {
  const url = supabaseUrl();
  const key = supabaseKey();

  if (!url || !key) {
    throw new Error(
      'Supabase is not configured. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY, or NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY.'
    );
  }

  if (!globalThis.twogServerSupabaseClient) {
    globalThis.twogServerSupabaseClient = createClient(url, key, {
      auth: {
        persistSession: false,
        autoRefreshToken: false,
      },
    });
  }

  return globalThis.twogServerSupabaseClient;
}
