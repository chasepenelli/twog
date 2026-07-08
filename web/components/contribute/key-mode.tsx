"use client";

/**
 * Key-mode: how a collaborator's contributions are signed.
 *
 *  - custodial: the platform holds the signing key and signs on the
 *    collaborator's behalf. Lowest friction; the platform is the custodian.
 *  - self_held: the collaborator holds their own key and signs locally; the
 *    signature is attached to the capsule and verified at the provenance gate.
 *    Strongest provenance; the platform never sees the private key.
 *
 * The chosen mode is a local UX preference (persisted to localStorage) that
 * drives the signing path in the Submit flow. It maps directly onto the api
 * client's SigningMode.
 *
 * CONTRIBUTE specialist owns components/contribute/*.
 */

import * as React from "react";
import { KeyRound, ServerCog, Check } from "lucide-react";

import type { SigningMode } from "@/lib/api";
import { cn } from "@/lib/utils";

const STORAGE_KEY = "twog.contribute.keyMode";

/** Persisted toggle. Defaults to custodial (lowest friction) for v1. */
export function useKeyMode(): [SigningMode, (m: SigningMode) => void] {
  const [mode, setMode] = React.useState<SigningMode>("custodial");

  React.useEffect(() => {
    try {
      const stored = window.localStorage.getItem(STORAGE_KEY);
      if (stored === "custodial" || stored === "self_held") setMode(stored);
    } catch {
      /* localStorage unavailable — keep default */
    }
  }, []);

  const update = React.useCallback((m: SigningMode) => {
    setMode(m);
    try {
      window.localStorage.setItem(STORAGE_KEY, m);
    } catch {
      /* ignore */
    }
  }, []);

  return [mode, update];
}

interface ModeSpec {
  mode: SigningMode;
  label: string;
  blurb: string;
  detail: string;
  icon: typeof KeyRound;
}

const MODES: ModeSpec[] = [
  {
    mode: "custodial",
    label: "Custodial",
    blurb: "The platform signs on your behalf.",
    detail:
      "Lowest friction. The platform is the custodian of the signing key and stamps each contribution for you. Choose this if you do not manage your own keys.",
    icon: ServerCog,
  },
  {
    mode: "self_held",
    label: "Self-held",
    blurb: "You sign locally with your own key.",
    detail:
      "Strongest provenance. You sign each contribution with your private key and attach the signature; the platform never holds your key. Requires a registered public key.",
    icon: KeyRound,
  },
];

export function KeyModeToggle({
  value,
  onChange,
  publicKey,
  className,
}: {
  value: SigningMode;
  onChange: (m: SigningMode) => void;
  /** The collaborator's registered public key, when self-held. */
  publicKey?: string;
  className?: string;
}) {
  return (
    <div
      role="radiogroup"
      aria-label="Signing key mode"
      className={cn("grid gap-3 sm:grid-cols-2", className)}
    >
      {MODES.map((spec) => {
        const selected = value === spec.mode;
        const Icon = spec.icon;
        return (
          <button
            key={spec.mode}
            type="button"
            role="radio"
            aria-checked={selected}
            onClick={() => onChange(spec.mode)}
            className={cn(
              "group relative flex flex-col gap-2 rounded-lg border p-4 text-left transition-colors",
              selected
                ? "border-primary bg-primary/5 ring-1 ring-primary"
                : "border-border hover:border-input hover:bg-secondary/40",
            )}
          >
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Icon
                  className={cn(
                    "size-4",
                    selected ? "text-primary" : "text-muted-foreground",
                  )}
                  aria-hidden
                />
                <span className="text-sm font-semibold text-foreground">
                  {spec.label}
                </span>
              </div>
              {selected && (
                <span className="flex size-5 items-center justify-center rounded-full bg-primary text-primary-foreground">
                  <Check className="size-3" aria-hidden />
                </span>
              )}
            </div>
            <p className="text-sm font-medium text-foreground">{spec.blurb}</p>
            <p className="text-xs text-muted-foreground">{spec.detail}</p>
            {spec.mode === "self_held" && selected && (
              <p className="mt-1 truncate font-mono text-[11px] text-muted-foreground">
                {publicKey
                  ? `key: ${publicKey}`
                  : "no public key on file"}
              </p>
            )}
          </button>
        );
      })}
    </div>
  );
}
