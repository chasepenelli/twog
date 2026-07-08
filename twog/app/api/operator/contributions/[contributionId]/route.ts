import { NextResponse } from 'next/server';
import { isOperator } from '@/lib/operator-gate';
import { isTriageAction, triageContribution } from '@/lib/contribution-triage';

// PATCH one contribution's triage state. Operator-gated (the whole point).
export const runtime = 'nodejs';

export async function PATCH(request: Request, { params }: { params: Promise<{ contributionId: string }> }) {
  if (!(await isOperator())) {
    return NextResponse.json({ error: 'operator_required' }, { status: 401 });
  }
  const { contributionId } = await params;

  let body: any;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: 'invalid_json_body' }, { status: 400 });
  }
  if (!isTriageAction(body?.action)) {
    return NextResponse.json({ error: 'invalid_action' }, { status: 400 });
  }
  const reviewNotes = typeof body?.review_notes === 'string' ? body.review_notes.slice(0, 4000) : undefined;
  const operator = typeof body?.operator === 'string' ? body.operator.slice(0, 80) : undefined;

  try {
    const result = await triageContribution({
      contributionId,
      action: body.action,
      operator,
      reviewNotes,
      nowIso: new Date().toISOString(),
    });
    if (!result.ok) {
      return NextResponse.json(result, { status: result.error === 'not_found' ? 404 : 500 });
    }
    return NextResponse.json(result);
  } catch {
    return NextResponse.json({ error: 'triage_failed' }, { status: 500 });
  }
}
