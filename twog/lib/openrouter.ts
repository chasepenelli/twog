const OPENROUTER_URL = 'https://openrouter.ai/api/v1';
const DEFAULT_EMBEDDING_MODEL = 'openai/text-embedding-3-large';

function apiKey(): string | undefined {
  return process.env.OPENROUTER_API_KEY;
}

export function isOpenRouterConfigured(): boolean {
  return Boolean(apiKey());
}

async function openRouterPost<T>(path: string, body: Record<string, unknown>): Promise<T> {
  const key = apiKey();
  if (!key) {
    throw new Error('OpenRouter is not configured. Set OPENROUTER_API_KEY.');
  }

  const response = await fetch(`${OPENROUTER_URL}${path}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${key}`,
      'Content-Type': 'application/json',
      'HTTP-Referer': process.env.NEXT_PUBLIC_SITE_URL ?? 'https://twog.bio',
      'X-Title': 'TWOG',
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(`OpenRouter request failed: ${response.status} ${detail.slice(0, 500)}`);
  }

  return (await response.json()) as T;
}

export async function getEmbedding(input: string): Promise<number[]> {
  const payload = await openRouterPost<{
    data?: Array<{ embedding?: number[] }>;
  }>('/embeddings', {
    model: process.env.OPENROUTER_EMBEDDING_MODEL ?? DEFAULT_EMBEDDING_MODEL,
    input,
  });

  const embedding = payload.data?.[0]?.embedding;
  if (!embedding?.length) {
    throw new Error('OpenRouter embedding response did not include an embedding.');
  }
  return embedding;
}

export async function getChatCompletion(messages: Array<{ role: string; content: string }>): Promise<string> {
  const payload = await openRouterPost<{
    choices?: Array<{ message?: { content?: string } }>;
  }>('/chat/completions', {
    model: process.env.OPENROUTER_MODEL ?? 'anthropic/claude-sonnet-4.6',
    messages,
    temperature: 0.2,
  });

  return payload.choices?.[0]?.message?.content ?? '';
}
