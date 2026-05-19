'use client';

import { useEffect, useState } from 'react';
import { SupabaseClient } from '@supabase/supabase-js';
import { getSupabase } from '@/lib/supabase';

export function useSupabase() {
  const [sb, setSb] = useState<SupabaseClient | null>(null);

  useEffect(() => {
    getSupabase().then(setSb);
  }, []);

  return sb;
}
