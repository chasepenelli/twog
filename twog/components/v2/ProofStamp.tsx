// The signature TWOG mark: a slightly-rotated mono "rubber stamp" verdict. Recurs in the hero, every
// ledger decision, and the footer. Seeing a rotated mono stamp anywhere = TWOG.
import type { Verdict } from './ledgerEvents';

const FACE: Record<Verdict, { glyph: string; word: string; mod: string }> = {
  survived: { glyph: '✓', word: 'SURVIVED', mod: 'ok' },
  supports: { glyph: '✓', word: 'SUPPORTS', mod: 'ok' },
  killed: { glyph: '✗', word: 'KILLED', mod: 'ko' },
  running: { glyph: '◷', word: 'RUNNING', mod: 'run' },
};

export function ProofStamp({ verdict, code, size = 'sm' }: { verdict: Verdict; code?: string; size?: 'sm' | 'lg' }) {
  const f = FACE[verdict];
  return (
    <span className={`v2-stamp v2-stamp--${f.mod} v2-stamp--${size}`} aria-label={`${f.word}${code ? ` ${code}` : ''}`}>
      <span className="v2-stamp__face">
        <span aria-hidden>{f.glyph}</span> {f.word}
      </span>
      {code && <span className="v2-stamp__code">{code}</span>}
    </span>
  );
}
