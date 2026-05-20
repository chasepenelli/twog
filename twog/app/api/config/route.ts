import { NextResponse } from 'next/server';

export async function GET() {
  return NextResponse.json(
    {
      supabaseUrl: process.env.NEXT_PUBLIC_SUPABASE_URL ?? process.env.SUPABASE_URL,
      supabaseKey: process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? process.env.SUPABASE_ANON_KEY,
    },
    {
      headers: {
        'Cache-Control': 's-maxage=300, stale-while-revalidate',
      },
    }
  );
}
