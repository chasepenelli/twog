"use client";

/**
 * Contribution tracker: follow each submitted capsule through the confound +
 * provenance gates to the operator's terminal decision.
 *
 * Falsification-first framing: a capsule that REFUTES is a real scientific
 * result; a capsule never auto-promotes; the operator stage is always the human
 * terminal gate. The pipeline visualizes exactly where each capsule sits and why
 * it is (or is not) eligible for an operator decision.
 *
 * Loading / empty / error states throughout.
 *
 * CONTRIBUTE specialist owns app/contribute/*.
 */

import * as React from "react";
import Link from "next/link";
import { RefreshCw, Route, Send } from "lucide-react";

import { api } from "@/lib/api";
import type { ProofCapsule } from "@/lib/types/domain";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { EmptyState } from "@/components/ui/empty-state";
import { StatusPill } from "@/components/ui/status-pill";
import { SectionHeader } from "@/components/contribute/section";
import { useAsync } from "@/components/contribute/use-async";
import {
  GatePipeline,
  GateVerdicts,
} from "@/components/contribute/gate-pipeline";

export default function TrackerPage() {
  const capsules = useAsync<ProofCapsule[]>(
    (signal) => api.contributions.listMine(signal),
    [],
  );

  return (
    <div className="space-y-8">
      <SectionHeader
        title="Contribution tracker"
        description="Every capsule you submit moves through the confound and provenance gates before it can reach the operator. Nothing is promoted automatically — a human holds the terminal accept gate."
        action={
          capsules.data && capsules.data.length > 0 ? (
            <Button variant="outline" onClick={capsules.reload}>
              <RefreshCw className="size-4" aria-hidden />
              Refresh
            </Button>
          ) : undefined
        }
      />

      {capsules.loading && <TrackerSkeleton />}

      {capsules.error && (
        <EmptyState
          variant="error"
          title="Could not load your contributions"
          description={capsules.error}
          action={
            <Button variant="outline" onClick={capsules.reload}>
              <RefreshCw className="size-4" aria-hidden />
              Retry
            </Button>
          }
        />
      )}

      {!capsules.loading &&
        !capsules.error &&
        (!capsules.data || capsules.data.length === 0) && (
          <EmptyState
            icon={Route}
            title="No contributions yet"
            description="Once you submit an evidence capsule, you can follow it through the gates here."
            action={
              <Button asChild>
                <Link href="/contribute/submit">
                  <Send className="size-4" aria-hidden />
                  Submit a contribution
                </Link>
              </Button>
            }
          />
        )}

      {!capsules.loading && !capsules.error && capsules.data && (
        <div className="space-y-4">
          {capsules.data.map((capsule) => (
            <CapsuleCard key={capsule.capsule_id} capsule={capsule} />
          ))}
        </div>
      )}
    </div>
  );
}

function CapsuleCard({ capsule }: { capsule: ProofCapsule }) {
  return (
    <Card>
      <CardHeader className="space-y-3">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="space-y-1">
            <CardTitle className="font-mono text-sm">
              {capsule.capsule_id}
            </CardTitle>
            <p className="text-xs text-muted-foreground">
              candidate{" "}
              <span className="font-mono">{capsule.candidate_id}</span> ·{" "}
              {capsule.validation_type}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <StatusPill kind="signal" value={capsule.signal} />
            {capsule.signature ? (
              <span
                className="font-mono text-[11px] text-muted-foreground"
                title="Self-held signature"
              >
                signed: {capsule.signature}
              </span>
            ) : (
              <span className="text-[11px] text-muted-foreground">
                custodial signature
              </span>
            )}
          </div>
        </div>
        <GateVerdicts capsule={capsule} />
      </CardHeader>
      <CardContent>
        <GatePipeline capsule={capsule} />
      </CardContent>
    </Card>
  );
}

function TrackerSkeleton() {
  return (
    <div className="space-y-4" aria-busy="true" aria-live="polite">
      {Array.from({ length: 3 }).map((_, i) => (
        <Card key={i}>
          <CardHeader className="space-y-3">
            <div className="flex items-center justify-between">
              <Skeleton className="h-4 w-40" />
              <Skeleton className="h-5 w-24" />
            </div>
            <Skeleton className="h-4 w-56" />
          </CardHeader>
          <CardContent>
            <div className="flex gap-2">
              <Skeleton className="h-16 flex-1" />
              <Skeleton className="h-16 flex-1" />
              <Skeleton className="h-16 flex-1" />
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
