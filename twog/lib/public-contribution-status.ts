// Whether public candidate contribution intake accepts writes. Env-gated so enabling it in prod is a
// config decision, not a code edit: set TWOG_CONTRIBUTIONS_OPEN=1 (or "true") to open intake. Default
// stays PAUSED so public writes to Neon are never on by accident.
const OPEN = ['1', 'true', 'yes'].includes((process.env.TWOG_CONTRIBUTIONS_OPEN ?? '').toLowerCase());

export const CANDIDATE_CONTRIBUTIONS_PAUSED = !OPEN;

export const CANDIDATE_CONTRIBUTIONS_PAUSED_MESSAGE =
  'Public candidate check-in is temporarily paused while TWOG tightens the intake review path. Payloads and templates remain available for inspection.';
