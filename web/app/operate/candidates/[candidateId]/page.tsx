"use client";

/**
 * Candidate detail — the full falsification picture for one hypothesis:
 * current status, validation-readiness, targets, the evidence refs, and every
 * proof capsule attached to it (with confound + provenance verdicts and signal
 * direction). Refuting capsules are shown plainly as results.
 *
 * OPERATE owns app/operate/*.
 */

import * as React from "react";
import Link from "next/link";
import { ArrowLeft, FileText, ExternalLink } from "lucide-react";

import { api } from "@/lib/api";
import type { Candidate, ProofCapsule } from "@/lib/types/domain";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { StatusPill } from "@/components/ui/status-pill";
import { humanize } from "../../_lib/format";
import { useAsync } from "../../_lib/use-async";
import { PageHeader } from "../../_components/page-header";
import { AsyncSection } from "../../_components/async-section";

interface CandidateDetail {
  candidate: Candidate;
  capsules: ProofCapsule[];
}

export default function CandidateDetailPage({
  params,
}: {
  params: { candidateId: string };
}) {
  const { candidateId } = params;
  const state = useAsync<CandidateDetail>(
    async (signal) => {
      const [candidate, allCapsules] = await Promise.all([
        api.candidates.get(candidateId, signal),
        api.capsules.list(signal),
      ]);
      return {
        candidate,
        capsules: allCapsules.filter((c) => c.candidate_id === candidateId),
      };
    },
    [candidateId],
  );

  return (
    <div className="space-y-6">
      <Button asChild variant="ghost" size="sm" className="-ml-2 w-fit">
        <Link href="/operate/candidates">
          <ArrowLeft className="size-4" />
          Candidate library
        </Link>
      </Button>

      <AsyncSection
        state={state}
        empty={{ title: "Candidate not found" }}
        render={({ candidate, capsules }) => (
          <div className="space-y-6">
            <PageHeader
              title={candidate.title}
              description={
                <span className="font-mono text-xs">
                  {candidate.candidate_id}
                </span>
              }
              actions={
                <StatusPill kind="hypothesis" value={candidate.public_status} />
              }
            />

            <div className="grid gap-4 sm:grid-cols-3">
              <MetaCard label="Falsification status">
                <StatusPill kind="hypothesis" value={candidate.public_status} />
              </MetaCard>
              <MetaCard label="Validation">
                {candidate.validation_ready ? (
                  <Badge variant="success">Ready for validation</Badge>
                ) : (
                  <Badge variant="neutral">Not yet validation-ready</Badge>
                )}
              </MetaCard>
              <MetaCard label="Targets">
                <div className="flex flex-wrap gap-1">
                  {candidate.targets.length === 0 ? (
                    <span className="text-sm text-muted-foreground">None</span>
                  ) : (
                    candidate.targets.map((t) => (
                      <Badge key={t} variant="outline">
                        {t}
                      </Badge>
                    ))
                  )}
                </div>
              </MetaCard>
            </div>

            <Card>
              <CardHeader>
                <CardTitle>Proof capsules ({capsules.length})</CardTitle>
              </CardHeader>
              <CardContent>
                {capsules.length === 0 ? (
                  <p className="text-sm text-muted-foreground">
                    No capsules attached yet. This hypothesis has not been
                    tested with a submitted unit of evidence.
                  </p>
                ) : (
                  <ul className="divide-y divide-border">
                    {capsules.map((cap) => (
                      <li
                        key={cap.capsule_id}
                        className="flex flex-col gap-2 py-3 first:pt-0 last:pb-0 sm:flex-row sm:items-center sm:justify-between"
                      >
                        <div className="min-w-0 space-y-1">
                          <div className="flex items-center gap-2">
                            <span className="font-mono text-xs text-muted-foreground">
                              {cap.capsule_id}
                            </span>
                            <Badge variant="outline">
                              {humanize(cap.validation_type)}
                            </Badge>
                          </div>
                          <div className="text-xs text-muted-foreground">
                            Status: {humanize(cap.status)}
                            {cap.signature ? " · self-signed" : " · custodial"}
                          </div>
                        </div>
                        <div className="flex flex-wrap items-center gap-2">
                          <StatusPill kind="signal" value={cap.signal} />
                          {cap.confound_verdict && (
                            <span className="flex items-center gap-1 text-xs text-muted-foreground">
                              confound
                              <StatusPill
                                kind="gate"
                                value={cap.confound_verdict}
                                hideIcon
                              />
                            </span>
                          )}
                          {cap.provenance_verdict && (
                            <span className="flex items-center gap-1 text-xs text-muted-foreground">
                              provenance
                              <StatusPill
                                kind="gate"
                                value={cap.provenance_verdict}
                                hideIcon
                              />
                            </span>
                          )}
                        </div>
                      </li>
                    ))}
                  </ul>
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Evidence references</CardTitle>
              </CardHeader>
              <CardContent>
                {candidate.evidence_refs.length === 0 ? (
                  <p className="text-sm text-muted-foreground">
                    No evidence references recorded.
                  </p>
                ) : (
                  <ul className="space-y-1">
                    {candidate.evidence_refs.map((ref) => {
                      const isCapsule = ref.startsWith("cap_");
                      return (
                        <li
                          key={ref}
                          className="flex items-center gap-2 text-sm"
                        >
                          {isCapsule ? (
                            <ExternalLink
                              className="size-3.5 text-muted-foreground"
                              aria-hidden
                            />
                          ) : (
                            <FileText
                              className="size-3.5 text-muted-foreground"
                              aria-hidden
                            />
                          )}
                          <span className="font-mono text-xs">{ref}</span>
                        </li>
                      );
                    })}
                  </ul>
                )}
              </CardContent>
            </Card>
          </div>
        )}
      />
    </div>
  );
}

function MetaCard({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <Card>
      <CardContent className="space-y-2 p-4">
        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          {label}
        </p>
        <div>{children}</div>
      </CardContent>
    </Card>
  );
}
