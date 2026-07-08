import * as React from "react";

import { cn } from "@/lib/utils";

/**
 * Loading placeholder. Used by every list/page's loading state.
 */
function Skeleton({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn(
        "relative overflow-hidden rounded-md bg-muted",
        "before:absolute before:inset-0 before:-translate-x-full before:animate-shimmer before:bg-gradient-to-r before:from-transparent before:via-background/40 before:to-transparent",
        className,
      )}
      {...props}
    />
  );
}

export { Skeleton };
