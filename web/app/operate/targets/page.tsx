"use client";

/**
 * Verified-target catalog. Targets that survived structural + expression
 * verification are "verified"; those rejected at the gate are "refused"
 * (falsification voice carries here too — e.g. MTOR, refused on a bad pose).
 * Refused entries are shown alongside verified ones, not hidden: a refusal is a
 * result. The verification rationale (redock / PoseBusters note) is surfaced
 * verbatim from the catalog entry.
 *
 * OPERATE owns app/operate/*.
 */

import * as React from "react";
import { Target, CircleCheck, CircleX } from "lucide-react";

import { api, type TargetLibraryEntry } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { useAsync } from "../_lib/use-async";
import { PageHeader } from "../_components/page-header";
import { AsyncSection } from "../_components/async-section";

export default function TargetsPage() {
  const state = useAsync<TargetLibraryEntry[]>((signal) =>
    api.targetLibrary.list(signal),
  );

  return (
    <div className="space-y-6">
      <PageHeader
        title="Verified-target catalog"
        description="Targets that survived structural + expression verification, shown next to those refused at the gate. The redock / pose-check rationale is recorded for every verdict."
      />

      <AsyncSection
        state={state}
        isEmpty={(rows) => rows.length === 0}
        empty={{
          icon: Target,
          title: "No targets catalogued",
          description: "Proposed target-library entries land here after the gate.",
        }}
        render={(rows) => {
          const verified = rows.filter((t) => t.verdict === "verified");
          const refused = rows.filter((t) => t.verdict === "refused");
          return (
            <div className="space-y-6">
              <div className="grid gap-4 sm:grid-cols-2">
                <SummaryStat
                  label="Verified"
                  count={verified.length}
                  tone="success"
                />
                <SummaryStat
                  label="Refused"
                  count={refused.length}
                  tone="danger"
                />
              </div>

              <div className="grid gap-3 lg:grid-cols-2">
                {rows.map((t) => (
                  <TargetCard key={t.target_id} entry={t} />
                ))}
              </div>
            </div>
          );
        }}
      />
    </div>
  );
}

/**
 * Extract a redock RMSD figure from the verdict note when present (the catalog
 * records the rationale as free text, e.g. "redock RMSD 1.2 Å"). Falls back to
 * null so we never fabricate a number the entry does not carry.
 */
function extractRmsd(note: string): string | null {
  const m = note.match(/rmsd[^0-9]*([0-9]+(?:\.[0-9]+)?)\s*(Å|a|ang)?/i);
  return m ? `${m[1]} Å` : null;
}

function TargetCard({ entry }: { entry: TargetLibraryEntry }) {
  const verified = entry.verdict === "verified";
  const rmsd = extractRmsd(entry.note);
  return (
    <Card className={verified ? "" : "border-danger/30"}>
      <CardHeader className="flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="flex items-center gap-2 text-base">
          {entry.gene}
          <span className="font-mono text-xs font-normal text-muted-foreground">
            {entry.target_id}
          </span>
        </CardTitle>
        <Badge variant={verified ? "success" : "danger"} className="gap-1">
          {verified ? (
            <CircleCheck className="size-3" aria-hidden />
          ) : (
            <CircleX className="size-3" aria-hidden />
          )}
          {verified ? "Verified" : "Refused"}
        </Badge>
      </CardHeader>
      <CardContent className="space-y-3 text-sm">
        <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-xs">
          <dt className="text-muted-foreground">Structure</dt>
          <dd className="font-mono">
            {entry.structure_ref ?? <span className="text-muted-foreground">—</span>}
          </dd>
          <dt className="text-muted-foreground">Redock RMSD</dt>
          <dd className="font-mono">
            {rmsd ?? (
              <span className="text-muted-foreground">
                {verified ? "within tolerance" : "failed pose check"}
              </span>
            )}
          </dd>
          <dt className="text-muted-foreground">Candidates</dt>
          <dd className="font-mono">{entry.candidate_refs.length}</dd>
        </dl>
        <p className="text-muted-foreground">{entry.note}</p>
      </CardContent>
    </Card>
  );
}

function SummaryStat({
  label,
  count,
  tone,
}: {
  label: string;
  count: number;
  tone: "success" | "danger";
}) {
  return (
    <Card>
      <CardContent className="flex items-center justify-between p-4">
        <span className="text-sm font-medium text-muted-foreground">
          {label}
        </span>
        <span
          className={
            tone === "success"
              ? "text-2xl font-semibold tabular-nums text-success"
              : "text-2xl font-semibold tabular-nums text-danger"
          }
        >
          {count}
        </span>
      </CardContent>
    </Card>
  );
}
