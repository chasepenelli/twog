// GET /api/contributions-status — a tiny public surface telling callers whether structured candidate
// check-in is currently open or paused, without having to load a candidate page and probe the form. Used
// by the /involved status note; also handy for any external tooling.

import { NextResponse } from 'next/server';
import {
  CANDIDATE_CONTRIBUTIONS_PAUSED,
  CANDIDATE_CONTRIBUTIONS_PAUSED_MESSAGE,
} from '@/lib/public-contribution-status';

export const runtime = 'nodejs';

export async function GET() {
  return NextResponse.json({
    open: !CANDIDATE_CONTRIBUTIONS_PAUSED,
    status: CANDIDATE_CONTRIBUTIONS_PAUSED ? 'paused' : 'open',
    message: CANDIDATE_CONTRIBUTIONS_PAUSED
      ? CANDIDATE_CONTRIBUTIONS_PAUSED_MESSAGE
      : 'Structured candidate check-in is open. Submit a bounded packet on any candidate page — it’s queued for human review and never changes the record on its own.',
    // Suggestions are always open (lighter-weight lane); contribution intake is the gated one.
    suggest_open: true,
  });
}
