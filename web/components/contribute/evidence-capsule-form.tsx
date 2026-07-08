"use client";

/**
 * Evidence-capsule submission form. A capsule attaches a signed signal
 * (supports / refutes / neutral) to an existing candidate, names its validation
 * type, and is signed either custodially or with a self-held key. On submit it
 * lands in the confound + provenance gates.
 *
 * Loads candidate targets to attach to (loading / empty / error states).
 *
 * CONTRIBUTE specialist owns components/contribute/*.
 */

import * as React from "react";
import Link from "next/link";
import { AlertTriangle, Loader2, RefreshCw } from "lucide-react";

import { api, ApiError } from "@/lib/api";
import type { SigningMode, Contribution } from "@/lib/api";
import type { Candidate, CapsuleSignal } from "@/lib/types/domain";
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
import { Select, SelectOption } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { EmptyState } from "@/components/ui/empty-state";
import { StatusPill } from "@/components/ui/status-pill";
import { useAsync } from "@/components/contribute/use-async";
import {
  SigningPath,
  signingValid,
} from "@/components/contribute/signing-path";

const SIGNALS: { value: CapsuleSignal; label: string; hint: string }[] = [
  { value: "supports", label: "Supports", hint: "evidence for the hypothesis" },
  { value: "refutes", label: "Refutes", hint: "evidence against — a real result" },
  { value: "neutral", label: "Neutral", hint: "inconclusive" },
];

const VALIDATION_TYPES = [
  "cohort_association",
  "docking_falsification",
  "expression_panel",
  "variant_call",
  "survival_analysis",
];

export function EvidenceCapsuleForm({
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
  const candidates = useAsync<Candidate[]>(
    (signal) => api.contributions.listCandidateTargets(signal),
    [],
  );

  const [candidateId, setCandidateId] = React.useState("");
  const [signal, setSignal] = React.useState<CapsuleSignal>("refutes");
  const [validationType, setValidationType] = React.useState(
    VALIDATION_TYPES[1],
  );
  const [signature, setSignature] = React.useState("");
  const [submitting, setSubmitting] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  // Default the candidate select to the first option once loaded.
  React.useEffect(() => {
    if (!candidateId && candidates.data && candidates.data.length > 0) {
      setCandidateId(candidates.data[0].candidate_id);
    }
  }, [candidates.data, candidateId]);

  const selected = candidates.data?.find(
    (c) => c.candidate_id === candidateId,
  );

  const canSubmit =
    !!candidateId &&
    validationType.trim().length > 0 &&
    signingValid(keyMode, signature) &&
    !submitting;

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!canSubmit) return;
    setSubmitting(true);
    setError(null);
    try {
      const result = await api.contributions.submit({
        mode: "evidence_capsule",
        candidate_id: candidateId,
        signal,
        validation_type: validationType.trim(),
        signing_mode: keyMode,
        signature: keyMode === "self_held" ? signature.trim() : undefined,
      });
      onSubmitted(result);
    } catch (err) {
      setError(
        err instanceof ApiError
          ? err.message
          : "Could not submit the capsule. Please try again.",
      );
    } finally {
      setSubmitting(false);
    }
  }

  if (candidates.loading) {
    return (
      <Card>
        <CardHeader>
          <Skeleton className="h-5 w-44" />
          <Skeleton className="h-3 w-64" />
        </CardHeader>
        <CardContent className="space-y-4">
          <Skeleton className="h-9 w-full" />
          <Skeleton className="h-9 w-full" />
          <Skeleton className="h-24 w-full" />
        </CardContent>
      </Card>
    );
  }

  if (candidates.error) {
    return (
      <EmptyState
        variant="error"
        title="Could not load candidates"
        description={candidates.error}
        action={
          <Button variant="outline" onClick={candidates.reload}>
            <RefreshCw className="size-4" aria-hidden />
            Retry
          </Button>
        }
      />
    );
  }

  if (!candidates.data || candidates.data.length === 0) {
    return (
      <EmptyState
        title="No candidates to attach evidence to"
        description="There are no candidate hypotheses in the queue yet. Propose one first, then attach evidence."
        action={
          <Button asChild variant="outline">
            <Link href="/contribute/submit?mode=candidate_proposal">
              Propose a candidate
            </Link>
          </Button>
        }
      />
    );
  }

  return (
    <Card>
      <form onSubmit={onSubmit} noValidate>
        <CardHeader>
          <CardTitle>Evidence capsule</CardTitle>
          <CardDescription>
            Attach a signed signal to a candidate. It enters the confound +
            provenance gates before any operator sees it.
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
            <Label htmlFor="cap-candidate">Candidate</Label>
            <Select
              id="cap-candidate"
              value={candidateId}
              onChange={(e) => setCandidateId(e.target.value)}
            >
              {candidates.data.map((c) => (
                <SelectOption key={c.candidate_id} value={c.candidate_id}>
                  {c.title}
                </SelectOption>
              ))}
            </Select>
            {selected && (
              <div className="flex items-center gap-2 pt-0.5 text-xs text-muted-foreground">
                <StatusPill kind="hypothesis" value={selected.public_status} />
                <span className="font-mono">
                  {selected.targets.join(", ") || "no targets"}
                </span>
              </div>
            )}
          </div>

          <div className="space-y-2">
            <Label>Signal</Label>
            <div className="grid gap-2 sm:grid-cols-3">
              {SIGNALS.map((s) => {
                const active = signal === s.value;
                return (
                  <button
                    key={s.value}
                    type="button"
                    aria-pressed={active}
                    onClick={() => setSignal(s.value)}
                    className={
                      "flex flex-col items-start gap-1 rounded-md border p-3 text-left transition-colors " +
                      (active
                        ? "border-primary bg-primary/5 ring-1 ring-primary"
                        : "border-border hover:bg-secondary/40")
                    }
                  >
                    <StatusPill kind="signal" value={s.value} />
                    <span className="text-xs text-muted-foreground">
                      {s.hint}
                    </span>
                  </button>
                );
              })}
            </div>
          </div>

          <div className="space-y-2">
            <Label htmlFor="cap-validation">Validation type</Label>
            <Input
              id="cap-validation"
              list="cap-validation-options"
              value={validationType}
              onChange={(e) => setValidationType(e.target.value)}
              placeholder="docking_falsification"
              className="font-mono text-xs"
            />
            <datalist id="cap-validation-options">
              {VALIDATION_TYPES.map((t) => (
                <option key={t} value={t} />
              ))}
            </datalist>
            <p className="text-xs text-muted-foreground">
              The method that produced this evidence (e.g. how the hypothesis was
              tested for falsification).
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
            Submit capsule
          </Button>
        </CardFooter>
      </form>
    </Card>
  );
}
