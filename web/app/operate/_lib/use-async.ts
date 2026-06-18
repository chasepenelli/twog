"use client";

/**
 * use-async — tiny data-fetching primitive for the OPERATE surface.
 *
 * Every operate list/page renders loading -> empty/error -> data. This hook
 * standardizes that lifecycle on top of the mock-backed api client. It tracks
 * { data, loading, error } and exposes `reload()` so mutation handlers can
 * refresh after an accept/promote/revoke. AbortSignal is threaded through so a
 * fast unmount or a re-run cancels the in-flight (simulated) request.
 *
 * OPERATE owns app/operate/*.
 */

import * as React from "react";
import { ApiError } from "@/lib/api";

export interface AsyncState<T> {
  data: T | null;
  loading: boolean;
  error: ApiError | Error | null;
  /** Re-run the fetcher (e.g. after a mutation). */
  reload: () => void;
}

export function useAsync<T>(
  fetcher: (signal: AbortSignal) => Promise<T>,
  deps: React.DependencyList = [],
): AsyncState<T> {
  const [data, setData] = React.useState<T | null>(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<ApiError | Error | null>(null);
  const [nonce, setNonce] = React.useState(0);

  // The fetcher is intentionally not a dep — callers pass an inline closure and
  // express real dependencies via `deps`.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const memoFetcher = React.useCallback(fetcher, deps);

  React.useEffect(() => {
    const controller = new AbortController();
    let active = true;
    setLoading(true);
    setError(null);
    memoFetcher(controller.signal)
      .then((value) => {
        if (active) setData(value);
      })
      .catch((err) => {
        if (!active) return;
        if (err instanceof DOMException && err.name === "AbortError") return;
        setError(err as Error);
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
      controller.abort();
    };
  }, [memoFetcher, nonce]);

  const reload = React.useCallback(() => setNonce((n) => n + 1), []);

  return { data, loading, error, reload };
}
