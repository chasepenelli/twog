"use client";

/**
 * ScopePicker — checkbox group for granting scopes when approving an applicant.
 *
 * Operator-only scopes (accept_capsule, promote_candidate) are shown but
 * disabled for collaborator applicants: those gate the terminal human decision
 * and may never be delegated to an external collaborator. This mirrors the
 * backend's OPERATOR_ONLY_SCOPES invariant in the UI.
 *
 * OPERATE owns app/operate/*.
 */

import * as React from "react";
import { Lock } from "lucide-react";

import {
  OPERATOR_ONLY_SCOPES,
  type Scope,
} from "@/lib/types/domain";
import { cn } from "@/lib/utils";

const ALL_SCOPES: { scope: Scope; label: string; help: string }[] = [
  {
    scope: "lease_workspace",
    label: "Lease workspace",
    help: "Open a sandbox to produce artifacts.",
  },
  {
    scope: "submit_compute",
    label: "Submit compute",
    help: "Run validation jobs on the sandbox runner.",
  },
  {
    scope: "submit_capsule",
    label: "Submit capsule",
    help: "Submit evidence capsules for review at the gate.",
  },
  {
    scope: "accept_capsule",
    label: "Accept capsule",
    help: "Operator-only — accept reviewed evidence.",
  },
  {
    scope: "promote_candidate",
    label: "Promote candidate",
    help: "Operator-only — the terminal human gate.",
  },
];

const OPERATOR_ONLY = new Set<Scope>(OPERATOR_ONLY_SCOPES);

export function ScopePicker({
  value,
  onChange,
}: {
  value: Scope[];
  onChange: (next: Scope[]) => void;
}) {
  const selected = new Set(value);

  function toggle(scope: Scope) {
    const next = new Set(selected);
    if (next.has(scope)) next.delete(scope);
    else next.add(scope);
    onChange(ALL_SCOPES.map((s) => s.scope).filter((s) => next.has(s)));
  }

  return (
    <fieldset className="space-y-2">
      <legend className="sr-only">Scopes to grant</legend>
      {ALL_SCOPES.map(({ scope, label, help }) => {
        const operatorOnly = OPERATOR_ONLY.has(scope);
        const checked = selected.has(scope);
        return (
          <label
            key={scope}
            className={cn(
              "flex cursor-pointer items-start gap-3 rounded-md border border-border p-3 text-sm transition-colors",
              checked && "border-primary/50 bg-secondary/40",
              operatorOnly && "cursor-not-allowed opacity-60",
            )}
          >
            <input
              type="checkbox"
              className="mt-0.5 size-4 accent-[hsl(var(--primary))]"
              checked={checked}
              disabled={operatorOnly}
              onChange={() => toggle(scope)}
            />
            <span className="space-y-0.5">
              <span className="flex items-center gap-1.5 font-medium text-foreground">
                {label}
                {operatorOnly && (
                  <Lock className="size-3 text-muted-foreground" aria-hidden />
                )}
              </span>
              <span className="block text-xs text-muted-foreground">{help}</span>
            </span>
          </label>
        );
      })}
      <p className="text-xs text-muted-foreground">
        Operator-only scopes cannot be granted to a collaborator.
      </p>
    </fieldset>
  );
}
