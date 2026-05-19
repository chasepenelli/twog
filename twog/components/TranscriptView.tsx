'use client';

import { useState } from 'react';

interface TranscriptViewProps {
  transcript: string;
}

/**
 * Renders transcript markdown with speaker names bolded.
 * Expects lines like "**Dr. Chase:** Some text here..." or
 * plain paragraphs separated by newlines.
 */
function renderTranscript(raw: string): string {
  return raw
    /* Bold speaker names — "Speaker Name:" at start of line */
    .replace(/^([A-Z][A-Za-z .]+):/gm, '<strong>$1:</strong>')
    /* Double newlines → paragraph breaks */
    .replace(/\n\n/g, '</p><p>')
    /* Single newlines → line breaks */
    .replace(/\n/g, '<br />')
    /* Wrap in paragraph */
    .replace(/^/, '<p>')
    .replace(/$/, '</p>');
}

export default function TranscriptView({ transcript }: TranscriptViewProps) {
  const [open, setOpen] = useState(false);

  return (
    <div className="border-t border-[var(--gray-200)] mt-8">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between py-4 text-left group"
      >
        <span className="text-[0.6rem] uppercase tracking-[0.2em] text-[var(--gray-400)] group-hover:text-[var(--foreground)] transition-colors mono">
          Transcript
        </span>
        <span
          className="text-[0.7rem] text-[var(--gray-400)] group-hover:text-[var(--foreground)] transition-all duration-300 mono"
          style={{ transform: open ? 'rotate(180deg)' : 'rotate(0deg)', display: 'inline-block' }}
        >
          v
        </span>
      </button>

      {open && (
        <div className="pb-8">
          <div
            className="digest-article"
            dangerouslySetInnerHTML={{ __html: renderTranscript(transcript) }}
          />
        </div>
      )}
    </div>
  );
}
