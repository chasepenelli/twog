'use client';

import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface DigestArticleProps {
  content: string;
}

export default function DigestArticle({ content }: DigestArticleProps) {
  return (
    <div className="digest-article editorial">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          h1: ({ children }) => (
            <h2 className="text-[1.8rem] md:text-[2.2rem] font-bold leading-[1.2] mt-16 mb-6 tracking-tight">
              {children}
            </h2>
          ),
          h2: ({ children }) => (
            <h3 className="text-[1.3rem] md:text-[1.5rem] font-bold leading-[1.3] mt-12 mb-4 tracking-tight">
              {children}
            </h3>
          ),
          h3: ({ children }) => (
            <h4 className="text-[1rem] md:text-[1.15rem] font-bold leading-[1.4] mt-8 mb-3">
              {children}
            </h4>
          ),
          p: ({ children }) => (
            <p className="text-[0.9rem] md:text-[0.95rem] leading-[1.9] mb-6 text-[var(--gray-600)]">
              {children}
            </p>
          ),
          ul: ({ children }) => (
            <ul className="list-disc pl-6 mb-6 space-y-2">
              {children}
            </ul>
          ),
          ol: ({ children }) => (
            <ol className="list-decimal pl-6 mb-6 space-y-2">
              {children}
            </ol>
          ),
          li: ({ children }) => (
            <li className="text-[0.88rem] leading-[1.8] text-[var(--gray-600)]">
              {children}
            </li>
          ),
          strong: ({ children }) => (
            <strong className="text-[var(--foreground)] font-bold">
              {children}
            </strong>
          ),
          hr: () => (
            <hr className="my-12 border-[var(--gray-200)]" />
          ),
          blockquote: ({ children }) => (
            <blockquote className="border-l-2 border-[#22C55E] pl-6 my-8 italic text-[var(--gray-500)]">
              {children}
            </blockquote>
          ),
          code: ({ children, className }) => {
            if (className) {
              // Code block
              return (
                <code className="block bg-[var(--black)] text-[0.8rem] p-4 rounded-lg my-6 overflow-x-auto mono text-[var(--gray-500)]">
                  {children}
                </code>
              );
            }
            // Inline code
            return (
              <code className="bg-[var(--black)] text-[0.82rem] px-1.5 py-0.5 rounded mono text-[#22C55E]">
                {children}
              </code>
            );
          },
        }}
      >
        {content}
      </ReactMarkdown>
    </div>
  );
}
