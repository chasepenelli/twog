"use client";

/**
 * Operator overview. A calm, data-dense entry point that surfaces the work
 * waiting on the operator (pending applicants, capsules at the write gate) and
 * restates the platform invariant up top: nothing auto-promotes.
 *
 * OPERATE owns app/operate/*.
 */

import * as React from "react";
import Link from "next/link";
import {
  Users,
  FlaskConical,
  ScrollText,
  ShieldCheck,
  Target,
  ArrowRight,
  type LucideIcon,
} from "lucide-react";

import { api } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useAsync } from "./_lib/use-async";
import { PageHeader } from "./_components/page-header";

interface Counts {
  pendingApplicants: number;
  activeCollaborators: number;
  candidates: number;
  campaigns: number;
  pendingCapsules: number;
  refusedTargets: number;
}

export default function OperateOverviewPage() {
  const state = useAsync<Counts>(async (signal) => {
    const [collaborators, candidates, campaigns, pending, targets] =
      await Promise.all([
        api.collaborators.list(signal),
        api.candidates.list(signal),
        api.campaigns.list(signal),
        api.capsules.listPending(signal),
        api.targetLibrary.list(signal),
      ]);
    return {
      pendingApplicants: collaborators.filter((c) => c.status === "pending")
        .length,
      activeCollaborators: collaborators.filter((c) => c.status === "active")
        .length,
      candidates: candidates.length,
      campaigns: campaigns.length,
      pendingCapsules: pending.length,
      refusedTargets: targets.filter((t) => t.verdict === "refused").length,
    };
  });

  const { data, loading } = state;

  return (
    <div className="space-y-6">
      <PageHeader
        title="Operator console"
        description="You hold the terminal accept and promote gate. The platform tries to disprove hypotheses — nothing is ever auto-promoted."
      />

      <Card className="border-info/30 bg-info-subtle/30">
        <CardContent className="flex items-start gap-3 p-4 text-sm">
          <ShieldCheck className="mt-0.5 size-5 shrink-0 text-info" aria-hidden />
          <p className="text-foreground/90">
            <span className="font-semibold">Nothing auto-promotes.</span> Every
            candidate that reaches the write gate is held for an explicit human
            decision. Evidence that <span className="font-medium">refutes</span>{" "}
            a hypothesis is a result, not a failure.
          </p>
        </CardContent>
      </Card>

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        <StatLink
          href="/operate/applicants"
          icon={Users}
          label="Pending applicants"
          value={data?.pendingApplicants}
          loading={loading}
          emphasis={(data?.pendingApplicants ?? 0) > 0}
        />
        <StatLink
          href="/operate/write-gate"
          icon={ShieldCheck}
          label="Capsules at write gate"
          value={data?.pendingCapsules}
          loading={loading}
          emphasis={(data?.pendingCapsules ?? 0) > 0}
        />
        <StatLink
          href="/operate/collaborators"
          icon={Users}
          label="Active collaborators"
          value={data?.activeCollaborators}
          loading={loading}
        />
        <StatLink
          href="/operate/candidates"
          icon={FlaskConical}
          label="Candidates under test"
          value={data?.candidates}
          loading={loading}
        />
        <StatLink
          href="/operate/campaigns"
          icon={ScrollText}
          label="Falsification campaigns"
          value={data?.campaigns}
          loading={loading}
        />
        <StatLink
          href="/operate/targets"
          icon={Target}
          label="Refused targets"
          value={data?.refusedTargets}
          loading={loading}
        />
      </div>
    </div>
  );
}

function StatLink({
  href,
  icon: Icon,
  label,
  value,
  loading,
  emphasis,
}: {
  href: string;
  icon: LucideIcon;
  label: string;
  value: number | undefined;
  loading: boolean;
  emphasis?: boolean;
}) {
  return (
    <Link href={href} className="group block focus:outline-none">
      <Card className="transition-colors group-hover:border-foreground/20 group-focus-visible:ring-2 group-focus-visible:ring-ring">
        <CardHeader className="flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="flex items-center gap-2 text-sm font-medium text-muted-foreground">
            <Icon className="size-4" aria-hidden />
            {label}
          </CardTitle>
          <ArrowRight className="size-4 text-muted-foreground opacity-0 transition-opacity group-hover:opacity-100" />
        </CardHeader>
        <CardContent>
          {loading && value === undefined ? (
            <Skeleton className="h-8 w-12" />
          ) : (
            <span
              className={
                emphasis
                  ? "text-3xl font-semibold tabular-nums text-warning"
                  : "text-3xl font-semibold tabular-nums"
              }
            >
              {value ?? 0}
            </span>
          )}
        </CardContent>
      </Card>
    </Link>
  );
}
