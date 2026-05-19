'use client';

import { useEffect, useRef, useState, useCallback } from 'react';

interface TerminalFeedProps {
  lines: string[];
  typingSpeed?: number;
  lineDelay?: number;
  className?: string;
  title?: string;
}

export default function TerminalFeed({
  lines,
  typingSpeed = 25,
  lineDelay = 350,
  className = '',
  title = 'twog pipeline',
}: TerminalFeedProps) {
  const [displayedLines, setDisplayedLines] = useState<string[]>([]);
  const [currentLine, setCurrentLine] = useState('');
  const [lineIndex, setLineIndex] = useState(0);
  const [charIndex, setCharIndex] = useState(0);
  const [cursorVisible, setCursorVisible] = useState(true);
  const [isVisible, setIsVisible] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const bodyRef = useRef<HTMLDivElement>(null);

  // Start only when scrolled into view
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const obs = new IntersectionObserver(
      ([entry]) => { if (entry.isIntersecting) setIsVisible(true); },
      { threshold: 0.3 },
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, []);

  // Cursor blink
  useEffect(() => {
    const id = setInterval(() => setCursorVisible((v) => !v), 530);
    return () => clearInterval(id);
  }, []);

  // Typing engine
  useEffect(() => {
    if (!isVisible) return;

    if (lineIndex >= lines.length) {
      const id = setTimeout(() => {
        setDisplayedLines([]);
        setCurrentLine('');
        setLineIndex(0);
        setCharIndex(0);
      }, 4000);
      return () => clearTimeout(id);
    }

    const line = lines[lineIndex];

    if (charIndex < line.length) {
      const speed = line.startsWith('$') ? typingSpeed * 2.5 : typingSpeed;
      const id = setTimeout(() => {
        setCurrentLine(line.slice(0, charIndex + 1));
        setCharIndex((c) => c + 1);
      }, speed);
      return () => clearTimeout(id);
    }

    // Line finished — commit and advance
    const pause = line.includes('✓') ? lineDelay * 1.5 : lineDelay;
    const id = setTimeout(() => {
      setDisplayedLines((prev) => [...prev, line]);
      setCurrentLine('');
      setLineIndex((l) => l + 1);
      setCharIndex(0);
    }, pause);
    return () => clearTimeout(id);
  }, [isVisible, lineIndex, charIndex, lines, typingSpeed, lineDelay]);

  // Auto-scroll body
  useEffect(() => {
    if (bodyRef.current) bodyRef.current.scrollTop = bodyRef.current.scrollHeight;
  }, [displayedLines, currentLine]);

  return (
    <div
      ref={containerRef}
      className={`rounded-lg overflow-hidden ${className}`}
      style={{
        border: '1px solid rgba(255,255,255,0.08)',
        background: 'rgba(0,0,0,0.5)',
        backdropFilter: 'blur(8px)',
      }}
    >
      {/* Chrome */}
      <div className="flex items-center gap-1.5 px-4 py-2.5" style={{ borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
        <span className="w-[9px] h-[9px] rounded-full bg-[#ff5f57]" />
        <span className="w-[9px] h-[9px] rounded-full bg-[#febc2e]" />
        <span className="w-[9px] h-[9px] rounded-full bg-[#28c840]" />
        <span className="ml-3 text-[0.5rem] text-[var(--gray-500)] uppercase tracking-[0.15em] mono">{title}</span>
      </div>

      {/* Body */}
      <div ref={bodyRef} className="px-4 py-3 overflow-hidden mono" style={{ height: 260 }}>
        {displayedLines.map((line, i) => (
          <Line key={i} text={line} />
        ))}
        {lineIndex < lines.length && (
          <div className="flex">
            <span className="flex-1">
              <LineContent text={currentLine} />
            </span>
            <span
              className="inline-block w-[6px] self-stretch ml-px"
              style={{
                background: cursorVisible ? 'var(--green)' : 'transparent',
                transition: 'background 0.08s',
              }}
            />
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Rendering helpers ── */

function Line({ text }: { text: string }) {
  return (
    <div className="text-[0.65rem] leading-[1.9] whitespace-nowrap">
      <LineContent text={text} />
    </div>
  );
}

function LineContent({ text }: { text: string }) {
  if (text.startsWith('$')) {
    return (
      <span className="text-[0.65rem] leading-[1.9]">
        <span className="text-[var(--green)]">$ </span>
        <span className="text-white">{text.slice(2)}</span>
      </span>
    );
  }

  // Lines with bracket tags: [agent]  message
  const tagMatch = text.match(/^(\[[\w:.-]+\])\s*(.*)/);
  if (tagMatch) {
    const isSuccess = tagMatch[2].includes('✓');
    return (
      <span className="text-[0.65rem] leading-[1.9]">
        <span className="text-[var(--gray-500)]">{tagMatch[1]}</span>
        <span className={isSuccess ? 'text-[var(--green)]' : 'text-[var(--gray-300)]'}>
          {'  '}{tagMatch[2]}
        </span>
      </span>
    );
  }

  // Status key: value lines
  const kvMatch = text.match(/^(\s*[\w\s]+?):(\s+.+)/);
  if (kvMatch) {
    return (
      <span className="text-[0.65rem] leading-[1.9]">
        <span className="text-[var(--gray-500)]">{kvMatch[1]}:</span>
        <span className="text-[var(--gray-300)]">{kvMatch[2]}</span>
      </span>
    );
  }

  return (
    <span className="text-[0.65rem] leading-[1.9] text-[var(--gray-400)]">{text}</span>
  );
}
