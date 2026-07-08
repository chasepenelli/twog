"use client";

/**
 * Campaigns list — falsification runs (RunManifests). Each row summarizes the
 * runner, how many candidates were processed, total estimated cost, and — as a
 * standing trust signal — that nothing was promoted.
 *
 * OPERATE owns app/operate/*.
 */

import * as React from "react";
import Link from "next/link";
import { ScrollText, ChevronRight, ShieldCheck } from "lucide-react";

import { api } from "@/lib/api";
import type { RunManifest } from "@/lib/types/domain";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { humanize } from "../_lib/format";
import { useAsync } from "../_lib/use-async";
import { PageHeader } from "../_components/page-header";
import { AsyncSection } from "../_components/async-section";

export default function CampaignsPage() {
  const state = useAsync<RunManifest[]>((signal) => api.campaigns.list(signal));

  return (
    <div className="space-y-6">
      <PageHeader
        title="Falsification campaigns"
        description="Runs over selected candidates. Every campaign rolls up to the same invariant: nothing was auto-promoted."
      />

      <AsyncSection
        state={state}
        isEmpty={(rows) => rows.length === 0}
        empty={{
          icon: ScrollText,
          title: "No campaigns yet",
          description: "Completed falsification runs will appear here.",
        }}
        render={(rows) => (
          <div className="rounded-lg border border-border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Campaign</TableHead>
                  <TableHead>Runner</TableHead>
                  <TableHead>Processed</TableHead>
                  <TableHead>Promoted</TableHead>
                  <TableHead className="w-8" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((m) => (
                  <TableRow key={m.manifest_id}>
                    <TableCell>
                      <Link
                        href={`/operate/campaigns/${m.manifest_id}`}
                        className="font-mono text-sm font-medium hover:underline"
                      >
                        {m.manifest_id}
                      </Link>
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {humanize(m.runner_kind)}
                    </TableCell>
                    <TableCell className="tabular-nums">
                      {m.rollup.candidates_processed}/
                      {m.rollup.candidates_selected}
                    </TableCell>
                    <TableCell>
                      <Badge variant="success" className="gap-1">
                        <ShieldCheck className="size-3" aria-hidden />
                        None
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Link
                        href={`/operate/campaigns/${m.manifest_id}`}
                        aria-label={`Open ${m.manifest_id}`}
                        className="inline-flex text-muted-foreground hover:text-foreground"
                      >
                        <ChevronRight className="size-4" />
                      </Link>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      />
    </div>
  );
}
