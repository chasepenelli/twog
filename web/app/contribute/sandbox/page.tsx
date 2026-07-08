"use client";

/**
 * Sandbox: lease a workspace and inspect the contribution bundle.
 *
 * "Opening" a sandbox leases a workspace and returns a bundle describing the
 * leased environment (the sandbox manifest) plus the contribution modes it
 * accepts. Each mode card states what it produces and the admission gate the
 * artifact must pass. Modes the Submit flow can produce in-app link straight
 * there; others surface their CLI entry point.
 *
 * Loading -> error (retry) -> opened. We don't auto-open: leasing is an explicit
 * action, so the default state invites the collaborator to open one.
 *
 * CONTRIBUTE specialist owns app/contribute/*.
 */

import * as React from "react";
import Link from "next/link";
import {
  Boxes,
  Clock,
  FlaskConical,
  Loader2,
  RefreshCw,
  ShieldCheck,
} from "lucide-react";

import { api, ApiError } from "@/lib/api";
import type { SandboxBundle } from "@/lib/types/domain";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { EmptyState } from "@/components/ui/empty-state";
import { SectionHeader } from "@/components/contribute/section";
import {
  ContributionModeCard,
  SUBMITTABLE_MODES,
} from "@/components/contribute/contribution-mode-card";

type ViewState =
  | { phase: "idle" }
  | { phase: "opening" }
  | { phase: "open"; bundle: SandboxBundle }
  | { phase: "error"; message: string };

export default function SandboxPage() {
  const [state, setState] = React.useState<ViewState>({ phase: "idle" });

  const openSandbox = React.useCallback(async () => {
    setState({ phase: "opening" });
    try {
      const bundle = await api.sandbox.open();
      setState({ phase: "open", bundle });
    } catch (err) {
      const message =
        err instanceof ApiError
          ? err.message
          : "Could not lease a sandbox. Please try again.";
      setState({ phase: "error", message });
    }
  }, []);

  return (
    <div className="space-y-8">
      <SectionHeader
        title="Sandbox"
        description="Lease a workspace to produce artifacts. The bundle describes your leased environment and every way you may contribute — each with the gate its artifact must pass."
        action={
          state.phase === "open" ? (
            <Button variant="outline" onClick={openSandbox}>
              <RefreshCw className="size-4" aria-hidden />
              Re-lease
            </Button>
          ) : undefined
        }
      />

      {state.phase === "idle" && (
        <EmptyState
          icon={FlaskConical}
          title="No active sandbox"
          description="Open a sandbox to lease a runner and see the contribution modes available to you. Leasing requires the lease_workspace scope."
          action={
            <Button onClick={openSandbox}>
              <FlaskConical className="size-4" aria-hidden />
              Open a sandbox
            </Button>
          }
        />
      )}

      {state.phase === "opening" && <SandboxSkeleton />}

      {state.phase === "error" && (
        <EmptyState
          variant="error"
          title="Could not open a sandbox"
          description={state.message}
          action={
            <Button onClick={openSandbox} variant="outline">
              <RefreshCw className="size-4" aria-hidden />
              Try again
            </Button>
          }
        />
      )}

      {state.phase === "open" && <SandboxView bundle={state.bundle} />}
    </div>
  );
}

function SandboxView({ bundle }: { bundle: SandboxBundle }) {
  const lease = formatLease(bundle.lease_expires_at);
  return (
    <div className="space-y-8">
      {/* Manifest */}
      <Card>
        <CardHeader>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="flex items-center gap-2">
              <Boxes className="size-4 text-muted-foreground" aria-hidden />
              <CardTitle>Sandbox manifest</CardTitle>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="success">
                <ShieldCheck className="size-3" aria-hidden />
                {humanize(bundle.gate_policy)}
              </Badge>
              <Badge variant={lease.expired ? "danger" : "neutral"}>
                <Clock className="size-3" aria-hidden />
                {lease.label}
              </Badge>
            </div>
          </div>
          <CardDescription className="font-mono text-xs">
            {bundle.workspace_id}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <ManifestGrid manifest={bundle.sandbox} />
        </CardContent>
      </Card>

      {/* Contribution modes */}
      <div className="space-y-4">
        <div>
          <h2 className="text-base font-semibold text-foreground">
            Contribution modes
          </h2>
          <p className="text-sm text-muted-foreground">
            Each mode produces a different artifact and must clear its admission
            gate. Nothing is auto-admitted.
          </p>
        </div>
        <div className="grid gap-4 md:grid-cols-2">
          {bundle.contribution_modes.map((spec) => (
            <ContributionModeCard
              key={spec.mode}
              spec={spec}
              action={
                SUBMITTABLE_MODES.includes(spec.mode) ? (
                  <Button asChild variant="outline" size="sm" className="w-full">
                    <Link href={`/contribute/submit?mode=${spec.mode}`}>
                      Submit this in-app
                    </Link>
                  </Button>
                ) : undefined
              }
            />
          ))}
        </div>
      </div>
    </div>
  );
}

function ManifestGrid({ manifest }: { manifest: Record<string, unknown> }) {
  const entries = Object.entries(manifest);
  if (entries.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">
        The manifest is empty.
      </p>
    );
  }
  return (
    <dl className="grid gap-x-6 gap-y-3 sm:grid-cols-2 lg:grid-cols-3">
      {entries.map(([key, value]) => (
        <div key={key} className="space-y-0.5">
          <dt className="text-xs uppercase tracking-wide text-muted-foreground">
            {humanize(key)}
          </dt>
          <dd className="font-mono text-sm text-foreground">
            {formatValue(value)}
          </dd>
        </div>
      ))}
    </dl>
  );
}

function SandboxSkeleton() {
  return (
    <div className="space-y-8" aria-busy="true" aria-live="polite">
      <Card>
        <CardHeader>
          <Skeleton className="h-5 w-40" />
          <Skeleton className="h-3 w-56" />
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {Array.from({ length: 6 }).map((_, i) => (
              <div key={i} className="space-y-2">
                <Skeleton className="h-3 w-16" />
                <Skeleton className="h-4 w-24" />
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
      <div className="grid gap-4 md:grid-cols-2">
        {Array.from({ length: 4 }).map((_, i) => (
          <Card key={i}>
            <CardHeader className="space-y-2">
              <Skeleton className="h-5 w-40" />
              <Skeleton className="h-3 w-full" />
            </CardHeader>
            <CardContent className="space-y-2">
              <Skeleton className="h-3 w-full" />
              <Skeleton className="h-3 w-3/4" />
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}

// --- formatting helpers ---------------------------------------------------

function humanize(s: string) {
  return s.replace(/[_-]+/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatValue(value: unknown): string {
  if (Array.isArray(value)) return value.join(", ");
  if (value === null || value === undefined) return "—";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function formatLease(iso: string): { label: string; expired: boolean } {
  const expires = new Date(iso);
  if (Number.isNaN(expires.getTime())) {
    return { label: "lease unknown", expired: false };
  }
  const expired = expires.getTime() <= Date.now();
  const date = expires.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
  const time = expires.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
  });
  return {
    label: expired
      ? `Lease expired ${date}`
      : `Lease expires ${date}, ${time}`,
    expired,
  };
}
