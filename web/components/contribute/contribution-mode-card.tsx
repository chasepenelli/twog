import * as React from "react";
import {
  FlaskConical,
  Library,
  Lightbulb,
  Workflow,
  ArrowRight,
  type LucideIcon,
} from "lucide-react";

import type { ContributionMode, ContributionModeSpec } from "@/lib/types/domain";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";

/**
 * One contribution-mode card: what the mode produces, where it runs, and the
 * admission gate the artifact must pass. Used on the sandbox page.
 *
 * CONTRIBUTE specialist owns components/contribute/*.
 */

const MODE_ICON: Record<ContributionMode, LucideIcon> = {
  evidence_capsule: FlaskConical,
  target_library_entry: Library,
  candidate_proposal: Lightbulb,
  new_lane: Workflow,
};

const MODE_LABEL: Record<ContributionMode, string> = {
  evidence_capsule: "Evidence capsule",
  target_library_entry: "Target library entry",
  candidate_proposal: "Candidate proposal",
  new_lane: "New lane",
};

/** Which modes the v1 Submit flow can produce directly in-app. */
const SUBMITTABLE: ContributionMode[] = [
  "evidence_capsule",
  "candidate_proposal",
];

export function ContributionModeCard({
  spec,
  action,
}: {
  spec: ContributionModeSpec;
  /** Optional action slot (e.g. a "Submit" link for submittable modes). */
  action?: React.ReactNode;
}) {
  const Icon = MODE_ICON[spec.mode];
  const submittable = SUBMITTABLE.includes(spec.mode);
  return (
    <Card className="flex flex-col">
      <CardHeader className="space-y-2">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <span className="flex size-8 items-center justify-center rounded-md bg-muted text-muted-foreground">
              <Icon className="size-4" aria-hidden />
            </span>
            <CardTitle>{MODE_LABEL[spec.mode]}</CardTitle>
          </div>
          {submittable ? (
            <Badge variant="info">In-app</Badge>
          ) : (
            <Badge variant="neutral">CLI / off-platform</Badge>
          )}
        </div>
        <CardDescription>{spec.what}</CardDescription>
      </CardHeader>
      <CardContent className="mt-auto space-y-3 text-sm">
        <dl className="space-y-2">
          <Meta label="Artifact" value={spec.artifact} />
          <Meta label="Produced on" value={spec.produced_on} />
          <div className="flex items-start justify-between gap-4">
            <dt className="text-muted-foreground">Admission gate</dt>
            <dd className="text-right">
              <Badge variant="warning">{spec.admission_gate}</Badge>
            </dd>
          </div>
        </dl>
        <div className="flex items-center gap-2 rounded-md bg-muted/60 px-3 py-2 font-mono text-xs text-muted-foreground">
          <ArrowRight className="size-3.5 shrink-0" aria-hidden />
          <span className="truncate">{spec.entry_point}</span>
        </div>
        {action && <div className="pt-1">{action}</div>}
      </CardContent>
    </Card>
  );
}

function Meta({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-4">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="text-right font-medium text-foreground">{value}</dd>
    </div>
  );
}

export { SUBMITTABLE as SUBMITTABLE_MODES };
