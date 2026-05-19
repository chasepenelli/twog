'use client';

import { useState, useRef, useEffect, type KeyboardEvent } from 'react';
import { useRAGChat, type ChatMessage, type RAGCitation, type RAGStep } from '@/hooks/useRAGChat';

const SUGGESTED_QUERIES = [
  'What targets show the most promise?',
  'What do we know about sorafenib?',
  'Recent trial results for VEGFR inhibitors',
  'Top hypotheses for HSA treatment',
];

function formatTimestamp(date: Date): string {
  return date.toLocaleTimeString('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
}

function sourceLabel(table: string): string {
  if (table.includes('paper')) return 'Paper';
  if (table.includes('trial')) return 'Trial';
  if (table.includes('compound')) return 'Compound';
  if (table.includes('hypothesis') || table.includes('hypothes')) return 'Hypothesis';
  if (table.includes('target')) return 'Target';
  return table.replace(/_/g, ' ');
}

const STEP_LABELS: Record<RAGStep['type'], string> = {
  classifying: 'Classifying query',
  searching: 'Searching sources',
  evaluating: 'Evaluating results',
  refining: 'Refining context',
  synthesizing: 'Synthesizing answer',
};

// ─── Citation Chip ───────────────────────────────────────────────────
function CitationChip({ citation }: { citation: RAGCitation }) {
  return (
    <span
      title={`${sourceLabel(citation.source_table)}: ${citation.title} (${Math.round(citation.similarity * 100)}% match)`}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: '0.25rem',
        padding: '0.1rem 0.4rem',
        background: '#58a6ff15',
        border: '1px solid #58a6ff33',
        borderRadius: '3px',
        fontSize: '0.7rem',
        fontFamily: 'var(--font-mono)',
        color: '#58a6ff',
        cursor: 'default',
        verticalAlign: 'middle',
        lineHeight: 1.4,
      }}
    >
      <span style={{ opacity: 0.6, fontSize: '0.65rem' }}>{sourceLabel(citation.source_table)}</span>
      <span style={{ maxWidth: '14rem', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {citation.title}
      </span>
    </span>
  );
}

// ─── Render message content with inline citation references ──────────
function MessageContent({ content, citations }: { content: string; citations?: RAGCitation[] }) {
  if (!citations || citations.length === 0) {
    return <span>{content}</span>;
  }

  // Split on [N] patterns and interleave citation chips
  const parts = content.split(/(\[\d+\])/g);
  return (
    <span>
      {parts.map((part, i) => {
        const match = part.match(/^\[(\d+)\]$/);
        if (match) {
          const idx = parseInt(match[1], 10);
          const citation = citations.find((c) => c.index === idx);
          if (citation) {
            return (
              <span key={i} style={{ margin: '0 0.15rem' }}>
                <span
                  style={{
                    display: 'inline-block',
                    padding: '0 0.3rem',
                    background: '#58a6ff20',
                    border: '1px solid #58a6ff44',
                    borderRadius: '3px',
                    fontSize: '0.65rem',
                    fontFamily: 'var(--font-mono)',
                    color: '#58a6ff',
                    cursor: 'default',
                    verticalAlign: 'middle',
                    lineHeight: 1.5,
                  }}
                  title={`${sourceLabel(citation.source_table)}: ${citation.title}`}
                >
                  {idx}
                </span>
              </span>
            );
          }
        }
        return <span key={i}>{part}</span>;
      })}
    </span>
  );
}

// ─── Single Message ──────────────────────────────────────────────────
function Message({ message }: { message: ChatMessage }) {
  const isUser = message.role === 'user';

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: isUser ? 'flex-end' : 'flex-start',
        padding: '0.4rem 0',
      }}
    >
      <div
        style={{
          maxWidth: '88%',
          padding: '0.6rem 0.8rem',
          borderRadius: '6px',
          background: isUser ? '#22C55E15' : '#1a1a1a',
          border: isUser ? '1px solid #22C55E25' : '1px solid #222',
          fontFamily: 'var(--font-mono)',
          fontSize: '0.8rem',
          lineHeight: 1.55,
          color: isUser ? '#c8f7d5' : '#ccc',
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
        }}
      >
        <MessageContent content={message.content} citations={message.citations} />
      </div>

      {/* Citation list beneath assistant messages */}
      {!isUser && message.citations && message.citations.length > 0 && (
        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: '0.3rem',
            marginTop: '0.35rem',
            maxWidth: '88%',
          }}
        >
          {message.citations.map((c) => (
            <CitationChip key={`${c.source_table}-${c.source_id}`} citation={c} />
          ))}
        </div>
      )}

      <span
        style={{
          fontSize: '0.65rem',
          fontFamily: 'var(--font-mono)',
          color: '#444',
          marginTop: '0.2rem',
          paddingLeft: isUser ? 0 : '0.2rem',
          paddingRight: isUser ? '0.2rem' : 0,
        }}
      >
        {formatTimestamp(message.timestamp)}
      </span>
    </div>
  );
}

// ─── Step Indicator Bar ──────────────────────────────────────────────
function StepIndicator({ steps }: { steps: RAGStep[] }) {
  const latest = steps[steps.length - 1];
  if (!latest) return null;

  const label = latest.detail || STEP_LABELS[latest.type] || latest.type;

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: '0.5rem',
        padding: '0.35rem 1rem',
        borderBottom: '1px solid #222',
        background: '#111',
        fontSize: '0.7rem',
        fontFamily: 'var(--font-mono)',
        color: '#888',
      }}
    >
      <span
        style={{
          width: '6px',
          height: '6px',
          borderRadius: '50%',
          background: '#22C55E',
          animation: 'pulse 1s ease-in-out infinite',
          flexShrink: 0,
        }}
      />
      <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {label}...
      </span>
    </div>
  );
}

// ─── Empty State ─────────────────────────────────────────────────────
function EmptyState({ onSelect }: { onSelect: (q: string) => void }) {
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        height: '100%',
        padding: '2rem 1.5rem',
        textAlign: 'center',
      }}
    >
      <p
        style={{
          fontFamily: 'var(--font-mono)',
          fontSize: '0.8rem',
          color: '#666',
          maxWidth: '24rem',
          lineHeight: 1.6,
          marginBottom: '1.25rem',
        }}
      >
        Ask questions about papers, compounds, trials, and hypotheses in the research database.
      </p>
      <div
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          gap: '0.4rem',
          justifyContent: 'center',
          maxWidth: '22rem',
        }}
      >
        {SUGGESTED_QUERIES.map((q) => (
          <button
            key={q}
            onClick={() => onSelect(q)}
            style={{
              padding: '0.35rem 0.65rem',
              background: '#1a1a1a',
              border: '1px solid #333',
              borderRadius: '4px',
              fontFamily: 'var(--font-mono)',
              fontSize: '0.7rem',
              color: '#888',
              cursor: 'pointer',
              transition: 'border-color 0.15s, color 0.15s',
              textAlign: 'left',
              lineHeight: 1.4,
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.borderColor = '#58a6ff55';
              e.currentTarget.style.color = '#aaa';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.borderColor = '#333';
              e.currentTarget.style.color = '#888';
            }}
          >
            {q}
          </button>
        ))}
      </div>
    </div>
  );
}

// ─── ChatPanel (main export) ─────────────────────────────────────────
export default function ChatPanel() {
  const { messages, isStreaming, currentSteps, sendMessage, clearHistory } = useRAGChat();
  const [input, setInput] = useState('');
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Auto-scroll on new messages or streaming content
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, messages[messages.length - 1]?.content]);

  const handleSend = () => {
    const text = input.trim();
    if (!text || isStreaming) return;
    setInput('');
    sendMessage(text);
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleSuggestedQuery = (q: string) => {
    sendMessage(q);
    inputRef.current?.focus();
  };

  return (
    <div className="flex flex-col h-full" style={{ background: '#111' }}>
      {/* Header */}
      <div
        className="shrink-0 px-4 py-2 flex items-center justify-between"
        style={{
          borderBottom: '1px solid #222',
          fontSize: '0.7rem',
          fontFamily: 'var(--font-mono)',
          color: '#888',
          textTransform: 'uppercase',
          letterSpacing: '0.1em',
        }}
      >
        <span>Research Chat</span>
        {messages.length > 0 && (
          <button
            onClick={clearHistory}
            style={{
              background: 'none',
              border: 'none',
              fontFamily: 'var(--font-mono)',
              fontSize: '0.65rem',
              color: '#555',
              cursor: 'pointer',
              padding: '0.1rem 0.3rem',
              textTransform: 'uppercase',
              letterSpacing: '0.05em',
            }}
            onMouseEnter={(e) => { e.currentTarget.style.color = '#888'; }}
            onMouseLeave={(e) => { e.currentTarget.style.color = '#555'; }}
          >
            Clear
          </button>
        )}
      </div>

      {/* Step indicator (visible during streaming) */}
      {isStreaming && currentSteps.length > 0 && <StepIndicator steps={currentSteps} />}

      {/* Messages area */}
      <div
        className="flex-1 overflow-y-auto px-4 py-2"
        style={{ scrollbarWidth: 'thin', scrollbarColor: '#333 transparent' }}
      >
        {messages.length === 0 ? (
          <EmptyState onSelect={handleSuggestedQuery} />
        ) : (
          <>
            {messages.map((m) => (
              <Message key={m.id} message={m} />
            ))}
            <div ref={messagesEndRef} />
          </>
        )}
      </div>

      {/* Input area */}
      <div
        className="shrink-0 flex items-center gap-2 px-3 py-2"
        style={{ borderTop: '1px solid #222' }}
      >
        <input
          ref={inputRef}
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask about research..."
          disabled={isStreaming}
          style={{
            flex: 1,
            background: '#1a1a1a',
            border: '1px solid #333',
            borderRadius: '4px',
            padding: '0.45rem 0.65rem',
            fontFamily: 'var(--font-mono)',
            fontSize: '0.8rem',
            color: '#fff',
            outline: 'none',
            opacity: isStreaming ? 0.5 : 1,
          }}
        />
        <button
          onClick={handleSend}
          disabled={!input.trim() || isStreaming}
          style={{
            padding: '0.45rem 0.75rem',
            background: !input.trim() || isStreaming ? '#1a1a1a' : '#22C55E',
            border: '1px solid',
            borderColor: !input.trim() || isStreaming ? '#333' : '#22C55E',
            borderRadius: '4px',
            fontFamily: 'var(--font-mono)',
            fontSize: '0.7rem',
            fontWeight: 600,
            color: !input.trim() || isStreaming ? '#555' : '#000',
            cursor: !input.trim() || isStreaming ? 'default' : 'pointer',
            textTransform: 'uppercase',
            letterSpacing: '0.05em',
            transition: 'background 0.15s, border-color 0.15s, color 0.15s',
          }}
        >
          Send
        </button>
      </div>
    </div>
  );
}
