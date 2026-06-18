"use client";

/**
 * Campaign report. The headline is the trust signal: any_promoted is FALSE —
 * shown prominently, because the platform never auto-promotes. Below it, the
 * rollup (terminal reasons, leading hypothesis statuses, cost vs budget) and a
 * per-candidate table: each candidate's terminal reason, hypothesis status,
 * cost, and the capsules it produced.
 *
 * OPERATE owns app/operate/*.
 */

import * as React from "react";
import Link from "next/link";
import { ArrowLeft, ShieldCheck } from "lucide-react";

import { api } from "@/lib/api";
import type { RunManifest } from "@/lib/types/domain";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { StatusPill } from "@/components/ui/status-pill";
import { usd, humanize } from "../../_lib/format";
import { useAsync } from "../../_lib/use-async";
import { AsyncSection } from "../../_components/async-section";

export default function CampaignReportPage({
  params,
}: {
  params: { manifestId: string };
}) {
  const { manifestId } = params;
  const state = useAsync<RunManifest>(
    (signal) => api.campaigns.get(manifestId, signal),
    [manifestId],
  );

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-2 w-fit">
        <Link href="/operate/campaigns">
          <ArrowLeft className="size-4" />
          Campaigns
        </Link>
      </Button>

      <AsyncSection
        state={state}
        empty={{ title: "Campaign not found" }}
        render={(m) => (
          <div className="space-y-6">
            <div className="space-y-1">
              <h1 className="font-mono text-xl font-semibold tracking-tight">
                {m.manifest_id}
              </h1>
              <p className="text-sm text-muted-foreground">
                Runner: {humanize(m.runner_kind)}
              </p>
            </div>

            {/* Trust signal — any_promoted is the invariant headline. */}
            <Card className="border-success/40 bg-success-subtle/40">
              <CardContent className="flex items-start gap-3 p-5">
                <ShieldCheck
                  className="mt-0.5 size-6 shrink-0 text-success"
                  aria-hidden
                />
                <div className="space-y-1">
                  <p className="text-base font-semibold">
                    Nothing was auto-promoted in this campaign.
                  </p>
                  <p className="text-sm text-muted-foreground">
                    <span className="font-mono">any_promoted = false</span>.
                    Every candidate that survived remains standing for an
                    explicit human decision at the write gate. The platform’s
                    job here was to try to disprove — not to advance.
                  </p>
                </div>
              </CardContent>
            </Card>

            {/* Rollup */}
            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
              <RollupStat
                label="Candidates"
                value={`${m.rollup.candidates_processed}/${m.rollup.candidates_selected}`}
                sub="processed / selected"
              />
              <RollupStat
                label="Estimated cost"
                value={usd(m.rollup.total_est_cost_usd)}
                sub={m.rollup.budget_exhausted ? "budget exhausted" : "within budget"}
                tone={m.rollup.budget_exhausted ? "warning" : "default"}
              />
              <RollupStat label="Promoted" value="0" sub="invariant" tone="success" />
              <RollupStat
                label="Refuted"
                value={String(m.rollup.leading_hypothesis_status.refuted ?? 0)}
                sub="hypotheses falsified"
              />
            </div>

            <div className="grid gap-4 lg:grid-cols-2">
              <Card>
                <CardHeader>
                  <CardTitle>Leading hypothesis status</CardTitle>
                  <CardDescription>
                    Where each candidate’s hypothesis stands after the run.
                  </CardDescription>
                </CardHeader>
                <CardContent className="flex flex-wrap gap-2">
                  {Object.entries(m.rollup.leading_hypothesis_status).map(
                    ([status, count]) => (
                      <div
                        key={status}
                        className="flex items-center gap-1.5 rounded-md border border-border px-2.5 py-1 text-sm"
                      >
                        <StatusPill kind="hypothesis" value={status} hideIcon />
                        <span className="tabular-nums text-muted-foreground">
                          ×{count}
                        </span>
                      </div>
                    ),
                  )}
                </CardContent>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle>Terminal reasons</CardTitle>
                  <CardDescription>
                    Why each candidate’s processing stopped.
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-1.5">
                  {Object.entries(m.rollup.terminal_reasons).map(
                    ([reason, count]) => (
                      <div
                        key={reason}
                        className="flex items-center justify-between text-sm"
                      >
                        <span>{humanize(reason)}</span>
                        <span className="tabular-nums text-muted-foreground">
                          {count}
                        </span>
                      </div>
                    ),
                  )}
                </CardContent>
              </Card>
            </div>

            {/* Per-candidate rows */}
            <Card>
              <CardHeader>
                <CardTitle>Per-candidate outcomes</CardTitle>
                <CardDescription>
                  Terminal reason, hypothesis status, and cost for each
                  candidate in the run.
                </CardDescription>
              </CardHeader>
              <CardContent className="p-0">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Candidate</TableHead>
                      <TableHead>Hypothesis</TableHead>
                      <TableHead>Terminal reason</TableHead>
                      <TableHead>Capsules</TableHead>
                      <TableHead className="text-right">Cost</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {m.rows.map((row) => (
                      <TableRow key={row.candidate_id}>
                        <TableCell>
                          <Link
                            href={`/operate/candidates/${row.candidate_id}`}
                            className="font-mono text-xs hover:underline"
                          >
                            {row.candidate_id}
                          </Link>
                        </TableCell>
                        <TableCell>
                          <StatusPill
                            kind="hypothesis"
                            value={row.leading_hypothesis_status}
                          />
                        </TableCell>
                        <TableCell className="text-sm">
                          {humanize(row.terminal_reason)}
                        </TableCell>
                        <TableCell>
                          {row.capsule_ids.length === 0 ? (
                            <span className="text-xs text-muted-foreground">
                              —
                            </span>
                          ) : (
                            <Badge variant="secondary">
                              {row.capsule_ids.length}
                            </Badge>
                          )}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {usd(row.total_est_cost_usd)}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          </div>
        )}
      />
    </div>
  );
}

function RollupStat({
  label,
  value,
  sub,
  tone = "default",
}: {
  label: string;
  value: string;
  sub?: string;
  tone?: "default" | "success" | "warning";
}) {
  const valueClass =
    tone === "success"
      ? "text-success"
      : tone === "warning"
        ? "text-warning"
        : "";
  return (
    <Card>
      <CardContent className="space-y-1 p-4">
        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          {label}
        </p>
        <p className={`text-2xl font-semibold tabular-nums ${valueClass}`}>
          {value}
        </p>
        {sub && <p className="text-xs text-muted-foreground">{sub}</p>}
      </CardContent>
    </Card>
  );
}
