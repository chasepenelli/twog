import * as React from "react";
import { Inbox, type LucideIcon } from "lucide-react";

import { cn } from "@/lib/utils";

export interface EmptyStateProps
  extends React.HTMLAttributes<HTMLDivElement> {
  /** Leading icon. Defaults to an inbox. */
  icon?: LucideIcon;
  title: string;
  description?: React.ReactNode;
  /** Optional action node (e.g. a Button). */
  action?: React.ReactNode;
  /** Render as an error rather than a benign empty list. */
  variant?: "empty" | "error";
}

/**
 * Shared empty / error state for lists and pages. Every list surface is
 * expected to render loading -> empty/error -> data; this covers the middle.
 */
function EmptyState({
  className,
  icon: Icon = Inbox,
  title,
  description,
  action,
  variant = "empty",
  ...props
}: EmptyStateProps) {
  return (
    <div
      role={variant === "error" ? "alert" : undefined}
      className={cn(
        "flex flex-col items-center justify-center rounded-lg border border-dashed border-border px-6 py-12 text-center",
        variant === "error" && "border-danger/40 bg-danger-subtle/40",
        className,
      )}
      {...props}
    >
      <div
        className={cn(
          "mb-3 flex size-10 items-center justify-center rounded-full",
          variant === "error"
            ? "bg-danger-subtle text-danger"
            : "bg-muted text-muted-foreground",
        )}
      >
        <Icon className="size-5" aria-hidden />
      </div>
      <p className="text-sm font-semibold text-foreground">{title}</p>
      {description && (
        <p className="mt-1 max-w-sm text-sm text-muted-foreground">
          {description}
        </p>
      )}
      {action && <div className="mt-4">{action}</div>}
    </div>
  );
}

export { EmptyState };
