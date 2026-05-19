'use client';

import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface FieldGuideSynthesisProps {
  markdown: string;
}

// Convert [PMID 12345678] tokens into markdown links before rendering.
// The underlying synthesis text contains bracketed PMID citations the data
// scientist asked us to render as PubMed anchors.
function linkPmids(src: string): string {
  return src.replace(
    /\[PMID\s+(\d+)\]/g,
    (_, pmid: string) =>
      `[PMID ${pmid}](https://pubmed.ncbi.nlm.nih.gov/${pmid}/)`,
  );
}

export default function FieldGuideSynthesis({ markdown }: FieldGuideSynthesisProps) {
  const withLinks = linkPmids(markdown);

  return (
    <div
      className="field-guide-synthesis"
      style={{ fontFamily: 'var(--font-crimson), Georgia, serif' }}
    >
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          p: ({ children }) => (
            <p className="text-[1.05rem] leading-[1.85] mb-5 text-[var(--gray-600)]">
              {children}
            </p>
          ),
          a: ({ children, href }) => (
            <a
              href={href}
              target="_blank"
              rel="noopener noreferrer"
              className="font-mono text-[0.78rem] px-1.5 py-[1px] rounded-md align-[1px] text-[var(--gray-500)] bg-[var(--gray-100)] hover:bg-[var(--gray-200)] hover:text-[var(--foreground)] transition-colors no-underline"
            >
              {children}
            </a>
          ),
          strong: ({ children }) => (
            <strong className="text-[var(--foreground)] font-semibold">
              {children}
            </strong>
          ),
          em: ({ children }) => <em className="italic">{children}</em>,
        }}
      >
        {withLinks}
      </ReactMarkdown>
    </div>
  );
}
