"use client";

/**
 * Candidate library — every hypothesis under test, with its falsification
 * status, validation-readiness, and target list. Click through to a candidate
 * for its evidence and capsules.
 *
 * OPERATE owns app/operate/*.
 */

import * as React from "react";
import Link from "next/link";
import { FlaskConical, ChevronRight } from "lucide-react";

import { api } from "@/lib/api";
import type { Candidate } from "@/lib/types/domain";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { StatusPill } from "@/components/ui/status-pill";
import { useAsync } from "../_lib/use-async";
import { PageHeader } from "../_components/page-header";
import { AsyncSection } from "../_components/async-section";

export default function CandidatesPage() {
  const state = useAsync<Candidate[]>((signal) => api.candidates.list(signal));

  return (
    <div className="space-y-6">
      <PageHeader
        title="Candidate library"
        description="Hypotheses under active falsification. A candidate is only validation-ready once it carries enough admitted evidence — nothing promotes on its own."
      />

      <AsyncSection
        state={state}
        isEmpty={(rows) => rows.length === 0}
        empty={{
          icon: FlaskConical,
          title: "No candidates",
          description: "Proposed candidates that pass operator triage land here.",
        }}
        render={(rows) => (
          <div className="rounded-lg border border-border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Candidate</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Validation</TableHead>
                  <TableHead>Targets</TableHead>
                  <TableHead>Evidence</TableHead>
                  <TableHead className="w-8" />
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((c) => (
                  <TableRow key={c.candidate_id} className="group">
                    <TableCell className="max-w-md">
                      <Link
                        href={`/operate/candidates/${c.candidate_id}`}
                        className="font-medium hover:underline"
                      >
                        {c.title}
                      </Link>
                      <div className="font-mono text-xs text-muted-foreground">
                        {c.candidate_id}
                      </div>
                    </TableCell>
                    <TableCell>
                      <StatusPill kind="hypothesis" value={c.public_status} />
                    </TableCell>
                    <TableCell>
                      {c.validation_ready ? (
                        <Badge variant="success">Ready</Badge>
                      ) : (
                        <Badge variant="neutral">Not ready</Badge>
                      )}
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        {c.targets.length === 0 ? (
                          <span className="text-xs text-muted-foreground">—</span>
                        ) : (
                          c.targets.map((t) => (
                            <Badge key={t} variant="outline">
                              {t}
                            </Badge>
                          ))
                        )}
                      </div>
                    </TableCell>
                    <TableCell className="tabular-nums text-muted-foreground">
                      {c.evidence_refs.length}
                    </TableCell>
                    <TableCell>
                      <Link
                        href={`/operate/candidates/${c.candidate_id}`}
                        aria-label={`Open ${c.title}`}
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
