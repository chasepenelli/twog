"use client";

/**
 * Candidate-proposal submission form. Proposes a new hypothesis (title +
 * targets) to enter the falsification queue. Lands in operator triage — it is
 * not validated or promoted automatically.
 *
 * Targets are entered as a small tag editor (comma / Enter to add). The proposal
 * is signed custodially or with a self-held key, same as a capsule.
 *
 * CONTRIBUTE specialist owns components/contribute/*.
 */

import * as React from "react";
import { AlertTriangle, Loader2, X } from "lucide-react";

import { api, ApiError } from "@/lib/api";
import type { SigningMode, Contribution } from "@/lib/api";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import {
  SigningPath,
  signingValid,
} from "@/components/contribute/signing-path";

export function CandidateProposalForm({
  keyMode,
  onKeyModeChange,
  publicKey,
  onSubmitted,
}: {
  keyMode: SigningMode;
  onKeyModeChange: (m: SigningMode) => void;
  publicKey?: string;
  onSubmitted: (c: Contribution) => void;
}) {
  const [title, setTitle] = React.useState("");
  const [targets, setTargets] = React.useState<string[]>([]);
  const [targetDraft, setTargetDraft] = React.useState("");
  const [signature, setSignature] = React.useState("");
  const [submitting, setSubmitting] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const canSubmit =
    title.trim().length > 4 &&
    targets.length > 0 &&
    signingValid(keyMode, signature) &&
    !submitting;

  function addTarget(raw: string) {
    const next = raw.trim().toUpperCase();
    if (!next) return;
    setTargets((prev) => (prev.includes(next) ? prev : [...prev, next]));
    setTargetDraft("");
  }

  function onTargetKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Enter" || e.key === ",") {
      e.preventDefault();
      addTarget(targetDraft);
    } else if (e.key === "Backspace" && !targetDraft && targets.length) {
      setTargets((prev) => prev.slice(0, -1));
    }
  }

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    // Fold a half-typed target in before validating.
    const pending = targetDraft.trim().toUpperCase();
    const finalTargets =
      pending && !targets.includes(pending) ? [...targets, pending] : targets;
    if (
      title.trim().length <= 4 ||
      finalTargets.length === 0 ||
      !signingValid(keyMode, signature)
    ) {
      setTargets(finalTargets);
      setTargetDraft("");
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      const result = await api.contributions.submit({
        mode: "candidate_proposal",
        title: title.trim(),
        targets: finalTargets,
        signing_mode: keyMode,
        signature: keyMode === "self_held" ? signature.trim() : undefined,
      });
      onSubmitted(result);
    } catch (err) {
      setError(
        err instanceof ApiError
          ? err.message
          : "Could not submit the proposal. Please try again.",
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Card>
      <form onSubmit={onSubmit} noValidate>
        <CardHeader>
          <CardTitle>Candidate proposal</CardTitle>
          <CardDescription>
            Propose a new hypothesis for the falsification queue. It enters
            operator triage — it is not validated or promoted automatically.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-5">
          {error && (
            <div
              role="alert"
              className="rounded-md border border-danger/40 bg-danger-subtle/50 px-3 py-2 text-sm text-danger"
            >
              {error}
            </div>
          )}

          <div className="space-y-2">
            <Label htmlFor="prop-title">Hypothesis title</Label>
            <Input
              id="prop-title"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              placeholder="KDR inhibition slows canine hemangiosarcoma progression"
            />
            <p className="text-xs text-muted-foreground">
              State a falsifiable claim — something the platform can try to
              disprove.
            </p>
          </div>

          <div className="space-y-2">
            <Label htmlFor="prop-targets">Targets</Label>
            <div className="flex min-h-9 flex-wrap items-center gap-1.5 rounded-md border border-input bg-background px-2 py-1.5 focus-within:ring-2 focus-within:ring-ring focus-within:ring-offset-1 focus-within:ring-offset-background">
              {targets.map((t) => (
                <Badge key={t} variant="secondary" className="gap-1">
                  <span className="font-mono">{t}</span>
                  <button
                    type="button"
                    aria-label={`Remove ${t}`}
                    onClick={() =>
                      setTargets((prev) => prev.filter((x) => x !== t))
                    }
                    className="rounded-sm text-muted-foreground hover:text-foreground"
                  >
                    <X className="size-3" aria-hidden />
                  </button>
                </Badge>
              ))}
              <input
                id="prop-targets"
                value={targetDraft}
                onChange={(e) => setTargetDraft(e.target.value)}
                onKeyDown={onTargetKeyDown}
                onBlur={() => addTarget(targetDraft)}
                placeholder={targets.length ? "" : "PIK3CA, MTOR…"}
                className="min-w-[8ch] flex-1 bg-transparent font-mono text-xs outline-none placeholder:text-muted-foreground placeholder:font-sans"
              />
            </div>
            <p className="text-xs text-muted-foreground">
              Gene / molecular targets. Press Enter or comma to add each.
            </p>
          </div>

          <SigningPath
            mode={keyMode}
            onModeChange={onKeyModeChange}
            signature={signature}
            onSignatureChange={setSignature}
            publicKey={publicKey}
          />

          {keyMode === "self_held" && !signingValid(keyMode, signature) && (
            <p className="flex items-center gap-1.5 text-xs text-warning">
              <AlertTriangle className="size-3.5" aria-hidden />
              A signature is required for self-held submissions.
            </p>
          )}
        </CardContent>
        <CardFooter className="justify-end">
          <Button type="submit" disabled={!canSubmit}>
            {submitting && (
              <Loader2 className="size-4 animate-spin" aria-hidden />
            )}
            Submit proposal
          </Button>
        </CardFooter>
      </form>
    </Card>
  );
}
