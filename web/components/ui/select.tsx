import * as React from "react";
import { ChevronDown } from "lucide-react";

import { cn } from "@/lib/utils";

/**
 * Styled NATIVE select.
 *
 * We deliberately wrap a native <select> rather than a Radix listbox: the
 * scaffold's dependency set does not include @radix-ui/react-select, and the
 * forms in this v1 (set-scopes, key-mode toggle, signal pickers) are simple
 * single-choice inputs where the native control is accessible and zero-dep.
 * The visual treatment matches Input so forms read consistently.
 */
export interface SelectProps
  extends React.SelectHTMLAttributes<HTMLSelectElement> {}

const Select = React.forwardRef<HTMLSelectElement, SelectProps>(
  ({ className, children, ...props }, ref) => {
    return (
      <div className="relative">
        <select
          ref={ref}
          className={cn(
            "flex h-9 w-full appearance-none rounded-md border border-input bg-background px-3 py-1 pr-9 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1 focus-visible:ring-offset-background disabled:cursor-not-allowed disabled:opacity-50",
            className,
          )}
          {...props}
        >
          {children}
        </select>
        <ChevronDown className="pointer-events-none absolute right-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
      </div>
    );
  },
);
Select.displayName = "Select";

/** Convenience re-export so call sites can stay declarative. */
const SelectOption = (
  props: React.OptionHTMLAttributes<HTMLOptionElement>,
) => <option {...props} />;

export { Select, SelectOption };
