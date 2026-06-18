/**
 * API client configuration + transport helpers.
 *
 * The whole client layer is built around a single switch: NEXT_PUBLIC_USE_MOCKS.
 * When on (the v1 default — there is no live backend yet) every call resolves
 * bundled fixtures. When off, calls hit NEXT_PUBLIC_API_BASE_URL via `request`.
 *
 * API-CLIENT owns lib/api/*.
 */

/** True when the client should resolve bundled mock fixtures. Default: true. */
export const USE_MOCKS =
  (process.env.NEXT_PUBLIC_USE_MOCKS ?? "true").toLowerCase() !== "false";

/** Base URL of the real backend (used only when USE_MOCKS is false). */
export const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

/** Simulated network latency (ms) for mock calls, to exercise loading states. */
const MOCK_LATENCY_MS = 280;

/** Error thrown by the client. Carries an HTTP-ish status for UI branching. */
export class ApiError extends Error {
  status: number;
  constructor(message: string, status = 0) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

/**
 * Resolve a mock value after a short simulated delay. Returns a deep clone so
 * callers (and React state) can never mutate the shared fixtures.
 */
export async function mock<T>(value: T): Promise<T> {
  await new Promise((r) => setTimeout(r, MOCK_LATENCY_MS));
  return structuredClone(value);
}

type RequestOptions = {
  method?: "GET" | "POST" | "PATCH" | "DELETE";
  body?: unknown;
  signal?: AbortSignal;
};

/**
 * Real-backend transport. STUBBED for v1: the path is fully wired (URL, JSON
 * body, error mapping) so flipping NEXT_PUBLIC_USE_MOCKS=false targets a real
 * service without touching call sites — but no backend exists yet, so this only
 * runs when mocks are explicitly disabled.
 */
export async function request<T>(
  path: string,
  { method = "GET", body, signal }: RequestOptions = {},
): Promise<T> {
  const url = `${API_BASE_URL}${path}`;
  let res: Response;
  try {
    res = await fetch(url, {
      method,
      headers: body ? { "Content-Type": "application/json" } : undefined,
      body: body ? JSON.stringify(body) : undefined,
      signal,
    });
  } catch (err) {
    throw new ApiError(
      `Network request failed: ${(err as Error).message}`,
      0,
    );
  }

  if (!res.ok) {
    let detail = res.statusText;
    try {
      const j = (await res.json()) as { detail?: string; message?: string };
      detail = j.detail ?? j.message ?? detail;
    } catch {
      /* non-JSON error body */
    }
    throw new ApiError(detail || `Request failed (${res.status})`, res.status);
  }

  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}
