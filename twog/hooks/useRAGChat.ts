'use client';

import { useState, useCallback, useRef } from 'react';

export interface RAGCitation {
  index: number;
  source_table: string;
  source_id: number;
  title: string;
  similarity: number;
  metadata: Record<string, unknown>;
}

export interface RAGStep {
  type: 'classifying' | 'searching' | 'evaluating' | 'refining' | 'synthesizing';
  detail: string;
}

export interface ChatMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  citations?: RAGCitation[];
  steps?: RAGStep[];
  timestamp: Date;
}

export function useRAGChat() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [isStreaming, setIsStreaming] = useState(false);
  const [currentSteps, setCurrentSteps] = useState<RAGStep[]>([]);
  const abortRef = useRef<AbortController | null>(null);

  const sendMessage = useCallback(async (text: string) => {
    const trimmed = text.trim();
    if (!trimmed || isStreaming) return;

    const userMsg: ChatMessage = {
      id: crypto.randomUUID(),
      role: 'user',
      content: trimmed,
      timestamp: new Date(),
    };

    const assistantId = crypto.randomUUID();
    let assistantContent = '';
    let assistantCitations: RAGCitation[] | undefined;
    let assistantSteps: RAGStep[] = [];

    setMessages((prev) => [...prev, userMsg]);
    setIsStreaming(true);
    setCurrentSteps([]);

    // Build message history for the API
    const history = [...messages, userMsg].map((m) => ({
      role: m.role,
      content: m.content,
    }));

    abortRef.current = new AbortController();

    try {
      const response = await fetch('/api/design-lab/rag/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ messages: history }),
        signal: abortRef.current.signal,
      });

      if (!response.ok || !response.body) {
        throw new Error(`Chat request failed: ${response.status}`);
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let currentEventType = '';

      // Add placeholder assistant message
      setMessages((prev) => [
        ...prev,
        {
          id: assistantId,
          role: 'assistant',
          content: '',
          timestamp: new Date(),
        },
      ]);

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        // Keep the last incomplete line in the buffer
        buffer = lines.pop() ?? '';

        for (const line of lines) {
          if (line.startsWith('event:')) {
            currentEventType = line.slice(6).trim();
          } else if (line.startsWith('data:')) {
            const raw = line.slice(5).trim();
            if (!raw) continue;

            try {
              const payload = JSON.parse(raw);

              switch (currentEventType) {
                case 'step': {
                  const step = payload as RAGStep;
                  assistantSteps = [...assistantSteps, step];
                  setCurrentSteps([...assistantSteps]);
                  break;
                }
                case 'citations': {
                  assistantCitations = payload as RAGCitation[];
                  break;
                }
                case 'token': {
                  const token = typeof payload === 'string' ? payload : payload.text ?? '';
                  assistantContent += token;
                  setMessages((prev) =>
                    prev.map((m) =>
                      m.id === assistantId ? { ...m, content: assistantContent } : m
                    )
                  );
                  break;
                }
                case 'done': {
                  // Finalize
                  break;
                }
              }
            } catch {
              // Malformed JSON line, skip
            }
          }
          // Empty lines reset event type
          if (line === '') {
            currentEventType = '';
          }
        }
      }

      // Finalize the assistant message with citations and steps
      setMessages((prev) =>
        prev.map((m) =>
          m.id === assistantId
            ? { ...m, content: assistantContent, citations: assistantCitations, steps: assistantSteps }
            : m
        )
      );
    } catch (err: unknown) {
      if (err instanceof DOMException && err.name === 'AbortError') {
        // User cancelled, leave partial message
      } else {
        console.error('RAG chat error:', err);
        // Add error as assistant message content
        setMessages((prev) =>
          prev.map((m) =>
            m.id === assistantId
              ? { ...m, content: assistantContent || 'Failed to get a response. Please try again.' }
              : m
          )
        );
      }
    } finally {
      setIsStreaming(false);
      setCurrentSteps([]);
      abortRef.current = null;
    }
  }, [messages, isStreaming]);

  const clearHistory = useCallback(() => {
    if (abortRef.current) {
      abortRef.current.abort();
    }
    setMessages([]);
    setIsStreaming(false);
    setCurrentSteps([]);
  }, []);

  return { messages, isStreaming, currentSteps, sendMessage, clearHistory };
}
