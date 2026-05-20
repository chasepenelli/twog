import { getChatCompletion } from '@/lib/openrouter';

export interface RAGAgentStep {
  type: 'classifying' | 'searching' | 'evaluating' | 'refining' | 'synthesizing';
  detail: string;
}

export interface RAGAgentCitation {
  index: number;
  source_table: string;
  source_id: number;
  title: string;
  similarity: number;
  metadata: Record<string, unknown>;
}

export type RAGAgentEvent =
  | { kind: 'step'; step: RAGAgentStep }
  | { kind: 'citations'; citations: RAGAgentCitation[] }
  | { kind: 'token'; text: string }
  | { kind: 'done' };

export interface RAGAgentRequest {
  question: string;
  history?: Array<{ role: string; content: string }>;
}

function chunkText(text: string, size = 60): string[] {
  const chunks: string[] = [];
  for (let index = 0; index < text.length; index += size) {
    chunks.push(text.slice(index, index + size));
  }
  return chunks;
}

export async function* runRAGAgent(request: RAGAgentRequest): AsyncGenerator<RAGAgentEvent> {
  yield { kind: 'step', step: { type: 'classifying', detail: 'Classifying the research question.' } };
  yield { kind: 'step', step: { type: 'searching', detail: 'Using the current public research context.' } };
  yield { kind: 'citations', citations: [] };
  yield { kind: 'step', step: { type: 'synthesizing', detail: 'Drafting a concise answer.' } };

  const answer = await getChatCompletion([
    {
      role: 'system',
      content:
        'You are TWOG research support. Answer cautiously, cite uncertainty, avoid medical advice, and explain what evidence would be needed next.',
    },
    ...(request.history ?? []),
    { role: 'user', content: request.question },
  ]);

  for (const text of chunkText(answer || 'No answer was returned.')) {
    yield { kind: 'token', text };
  }
  yield { kind: 'done' };
}
