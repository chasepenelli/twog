"use client";

/**
 * AsyncSection — renders the loading -> error -> empty -> data lifecycle that
 * every operate list/page is required to show. Pass the {@link AsyncState} from
 * useAsync plus an `isEmpty` predicate and a `render` for the data branch.
 *
 * OPERATE owns app/operate/*.
 */

import * as React from "react";
import { AlertTriangle } from "lucide-react";

import { ApiError } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { EmptyState } from "@/components/ui/empty-state";
import { Skeleton } from "@/components/ui/skeleton";
import type { AsyncState } from "@/app/operate/_lib/use-async";

interface AsyncSectionProps<T> {
  state: AsyncState<T>;
  /** True when `data` should be treated as "nothing to show". */
  isEmpty?: (data: T) => boolean;
  /** Empty-state copy. */
  empty?: { title: string; description?: React.ReactNode; icon?: React.ComponentProps<typeof EmptyState>["icon"] };
  /** Custom skeleton; defaults to a few rows. */
  loading?: React.ReactNode;
  render: (data: T) => React.ReactNode;
}

function DefaultSkeleton() {
  return (
    <div className="space-y-2" aria-busy="true" aria-label="Loading">
      {Array.from({ length: 4 }).map((_, i) => (
        <Skeleton key={i} className="h-12 w-full" />
      ))}
    </div>
  );
}

export function AsyncSection<T>({
  state,
  isEmpty,
  empty,
  loading,
  render,
}: AsyncSectionProps<T>) {
  const { data, loading: isLoading, error, reload } = state;

  if (isLoading && data === null) {
    return <>{loading ?? <DefaultSkeleton />}</>;
  }

  if (error) {
    const status = error instanceof ApiError ? error.status : undefined;
    return (
      <EmptyState
        variant="error"
        icon={AlertTriangle}
        title="Could not load this view"
        description={
          <>
            {status ? `(${status}) ` : ""}
            {error.message || "An unexpected error occurred."}
          </>
        }
        action={
          <Button variant="outline" size="sm" onClick={reload}>
            Try again
          </Button>
        }
      />
    );
  }

  if (data === null || (isEmpty && isEmpty(data))) {
    return (
      <EmptyState
        title={empty?.title ?? "Nothing here yet"}
        description={empty?.description}
        icon={empty?.icon}
      />
    );
  }

  return <>{render(data)}</>;
}
