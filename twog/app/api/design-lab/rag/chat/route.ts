import { runRAGAgent } from '@/lib/rag-agent';

export const maxDuration = 120;

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const {
      messages,
    }: {
      messages: { role: string; content: string }[];
      context?: { target?: string };
    } = body;

    if (!messages || !Array.isArray(messages) || messages.length === 0) {
      return Response.json({ error: 'messages array is required' }, { status: 400 });
    }

    // Extract the latest user message as the question
    const lastUserMsg = [...messages].reverse().find((m) => m.role === 'user');
    if (!lastUserMsg) {
      return Response.json({ error: 'No user message found' }, { status: 400 });
    }

    // Build conversation history (everything except the latest user message)
    const history = messages.slice(0, -1);

    const encoder = new TextEncoder();
    const generator = runRAGAgent({
      question: lastUserMsg.content,
      history: history.length > 0 ? history : undefined,
    });

    const stream = new ReadableStream({
      async start(controller) {
        try {
          for await (const event of generator) {
            let sseData: string;

            switch (event.kind) {
              case 'step':
                sseData = `event: step\ndata: ${JSON.stringify(event.step)}\n\n`;
                break;
              case 'citations':
                sseData = `event: citations\ndata: ${JSON.stringify(event.citations)}\n\n`;
                break;
              case 'token':
                sseData = `event: token\ndata: ${JSON.stringify({ text: event.text })}\n\n`;
                break;
              case 'done':
                sseData = `event: done\ndata: {}\n\n`;
                break;
            }

            controller.enqueue(encoder.encode(sseData));
          }
        } catch (err) {
          const message = err instanceof Error ? err.message : 'Unknown error';
          console.error('RAG chat stream error:', message);
          const errorEvent = `event: error\ndata: ${JSON.stringify({ error: message })}\n\n`;
          controller.enqueue(encoder.encode(errorEvent));
        } finally {
          controller.close();
        }
      },
    });

    return new Response(stream, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
      },
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Unknown error';
    console.error('RAG chat error:', message);
    return Response.json({ error: message }, { status: 500 });
  }
}
