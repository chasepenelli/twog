import { NextResponse } from 'next/server';
import {
  getContributorProfileByHandle,
  isContributorProfileStorageConfigured,
} from '@/lib/contributor-profiles';
import { publicOptionsResponse, publicReadHeaders } from '@/lib/api-cors';

export const runtime = 'nodejs';

export const OPTIONS = async () => publicOptionsResponse();

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ handle: string }> }
) {
  const { handle } = await params;
  const decoded = decodeURIComponent(handle ?? '');

  if (!decoded.trim()) {
    return NextResponse.json(
      { error: 'contributor_handle_required' },
      { status: 400 }
    );
  }

  if (!isContributorProfileStorageConfigured()) {
    return NextResponse.json(
      {
        error: 'contributor_profile_storage_not_configured',
        handle: decoded,
        message: 'Contributor profile requires Neon/Postgres storage.',
      },
      { status: 503 }
    );
  }

  try {
    const profile = await getContributorProfileByHandle(decoded);
    return NextResponse.json(
      {
        ...profile,
        public_boundary:
          'Contributor profiles are derived from accepted proof capsules and reward events keyed by handle. Sensitive contributor fields (contact, agent_id, website) never appear in this response.',
      },
      {
        headers: publicReadHeaders({ cacheControl: 's-maxage=60, stale-while-revalidate' }),
      }
    );
  } catch (error) {
    console.error('contributor profile lookup failed', error);
    return NextResponse.json(
      { error: 'contributor_profile_failed', message: String(error) },
      { status: 500, headers: publicReadHeaders() }
    );
  }
}
