import { createClient, type SupabaseClient } from '@supabase/supabase-js';

let browserClient: SupabaseClient | null | undefined;

function envConfig() {
  return {
    url: process.env.NEXT_PUBLIC_SUPABASE_URL,
    key: process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY,
  };
}

async function remoteConfig(): Promise<{ url?: string; key?: string }> {
  if (typeof window === 'undefined') return {};

  try {
    const response = await fetch('/api/config', { cache: 'no-store' });
    if (!response.ok) return {};
    const payload = (await response.json()) as { supabaseUrl?: string; supabaseKey?: string };
    return {
      url: payload.supabaseUrl,
      key: payload.supabaseKey,
    };
  } catch {
    return {};
  }
}

export async function getSupabase(): Promise<SupabaseClient | null> {
  if (browserClient !== undefined) return browserClient;

  const fromEnv = envConfig();
  const fromApi = fromEnv.url && fromEnv.key ? {} : await remoteConfig();
  const url = fromEnv.url ?? fromApi.url;
  const key = fromEnv.key ?? fromApi.key;

  if (!url || !key) {
    browserClient = null;
    return browserClient;
  }

  browserClient = createClient(url, key);
  return browserClient;
}
