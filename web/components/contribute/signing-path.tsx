"use client";

/**
 * Signing path for a contribution form. Shows the chosen signing mode (custodial
 * vs self-held) and, when self-held, collects the detached signature that will
 * be attached to the artifact and checked at the provenance gate.
 *
 * Reuses the KeyModeToggle so the form's signing choice and the dashboard
 * preference share one control + one explanation of each mode.
 *
 * CONTRIBUTE specialist owns components/contribute/*.
 */

import * as React from "react";
import { PenLine, ServerCog } from "lucide-react";

import type { SigningMode } from "@/lib/api";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { KeyModeToggle } from "@/components/contribute/key-mode";

export function SigningPath({
  mode,
  onModeChange,
  signature,
  onSignatureChange,
  publicKey,
}: {
  mode: SigningMode;
  onModeChange: (m: SigningMode) => void;
  signature: string;
  onSignatureChange: (s: string) => void;
  publicKey?: string;
}) {
  return (
    <div className="space-y-3 rounded-lg border border-border bg-muted/30 p-4">
      <div className="flex items-center gap-2">
        <PenLine className="size-4 text-muted-foreground" aria-hidden />
        <p className="text-sm font-medium text-foreground">Signing path</p>
      </div>

      <KeyModeToggle
        value={mode}
        onChange={onModeChange}
        publicKey={publicKey}
      />

      {mode === "custodial" ? (
        <p className="flex items-start gap-2 text-xs text-muted-foreground">
          <ServerCog className="mt-0.5 size-3.5 shrink-0" aria-hidden />
          The platform will sign this contribution on your behalf. No signature
          is required from you.
        </p>
      ) : (
        <div className="space-y-2 pt-1">
          <Label htmlFor="signing-signature">Detached signature</Label>
          <Textarea
            id="signing-signature"
            value={signature}
            onChange={(e) => onSignatureChange(e.target.value)}
            placeholder="ed25519:…  — sign the artifact locally and paste the signature"
            className="font-mono text-xs"
            rows={2}
          />
          <p className="text-xs text-muted-foreground">
            Sign the artifact with your private key and attach the signature. It
            is verified against your registered public key at the provenance gate.
          </p>
        </div>
      )}
    </div>
  );
}

/** Validate that a self-held submission carries a signature. */
export function signingValid(mode: SigningMode, signature: string): boolean {
  return mode === "custodial" || signature.trim().length > 0;
}
