import { NextResponse } from 'next/server';
import {
  getLeaderboard,
  isLeaderboardStorageConfigured,
  LeaderboardWindow,
} from '@/lib/leaderboard';
import { publicOptionsResponse, publicReadHeaders } from '@/lib/api-cors';

export const runtime = 'nodejs';

export const OPTIONS = async () => publicOptionsResponse();

const ALLOWED_WINDOWS: ReadonlySet<LeaderboardWindow> = new Set([
  'all_time',
  'last_30_days',
  'last_7_days',
]);

export async function GET(request: Request) {
  if (!isLeaderboardStorageConfigured()) {
    return NextResponse.json(
      {
        error: 'leaderboard_storage_not_configured',
        message: 'Leaderboard requires Neon/Postgres storage.',
      },
      { status: 503, headers: publicReadHeaders() },
    );
  }

  const url = new URL(request.url);
  const requestedWindow = (url.searchParams.get('window') ?? 'all_time') as LeaderboardWindow;
  const windowKey: LeaderboardWindow = ALLOWED_WINDOWS.has(requestedWindow)
    ? requestedWindow
    : 'all_time';
  const limitParam = url.searchParams.get('limit');
  const risingLimitParam = url.searchParams.get('rising_limit');

  try {
    const result = await getLeaderboard(windowKey, {
      limit: limitParam ? Number(limitParam) : undefined,
      risingLimit: risingLimitParam ? Number(risingLimitParam) : undefined,
    });
    return NextResponse.json(
      {
        ...result,
        public_boundary:
          'Leaderboard derives rankings from accepted/routed proof capsules and reward events. Sensitive contributor fields (contact, agent_id, website) never appear in this response.',
      },
      {
        headers: publicReadHeaders({ cacheControl: 's-maxage=60, stale-while-revalidate' }),
      },
    );
  } catch (error) {
    console.error('leaderboard query failed', error);
    return NextResponse.json(
      { error: 'leaderboard_failed', message: String(error) },
      { status: 500, headers: publicReadHeaders() },
    );
  }
}
