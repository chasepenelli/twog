import * as React from "react";
import {
  ShieldCheck,
  ShieldAlert,
  ShieldQuestion,
  Gavel,
  ChevronRight,
  type LucideIcon,
} from "lucide-react";

import type { GateVerdict, ProofCapsule } from "@/lib/types/domain";
import { cn } from "@/lib/utils";
import { StatusPill } from "@/components/ui/status-pill";

/**
 * Gate pipeline: visualizes a capsule's journey through the two automated gates
 * (confound, provenance) to the terminal operator decision. Falsification-first:
 * a refuting capsule that clears its gates is a valid result, and NOTHING is
 * auto-promoted — the operator stage is always the human terminal gate.
 *
 * CONTRIBUTE specialist owns components/contribute/*.
 */

type StageTone = "pass" | "fail" | "pending" | "idle";

interface Stage {
  key: string;
  label: string;
  icon: LucideIcon;
  tone: StageTone;
  detail: string;
}

const TONE_CLASS: Record<StageTone, string> = {
  pass: "border-success/40 bg-success-subtle/40 text-success",
  fail: "border-danger/40 bg-danger-subtle/40 text-danger",
  pending: "border-warning/40 bg-warning-subtle/40 text-warning",
  idle: "border-border bg-muted/40 text-muted-foreground",
};

function verdictTone(v: GateVerdict | undefined): StageTone {
  switch (v) {
    case "pass":
      return "pass";
    case "fail":
      return "fail";
    case "pending":
      return "pending";
    default:
      return "idle";
  }
}

function gateIcon(v: GateVerdict | undefined): LucideIcon {
  if (v === "pass") return ShieldCheck;
  if (v === "fail") return ShieldAlert;
  return ShieldQuestion;
}

/**
 * Derive the operator-stage state. The operator can only act once BOTH gates
 * pass; a failed gate blocks the capsule (it never reaches the human gate); an
 * accepted/promoted status reflects an operator decision already taken.
 */
function operatorStage(capsule: ProofCapsule): Stage {
  const status = capsule.status.toLowerCase();
  const bothPass =
    capsule.confound_verdict === "pass" &&
    capsule.provenance_verdict === "pass";
  const anyFail =
    capsule.confound_verdict === "fail" ||
    capsule.provenance_verdict === "fail";

  if (status.includes("accept") || status.includes("promot")) {
    return {
      key: "operator",
      label: "Operator decision",
      icon: Gavel,
      tone: "pass",
      detail: "Accepted at the terminal human gate.",
    };
  }
  if (anyFail) {
    return {
      key: "operator",
      label: "Operator decision",
      icon: Gavel,
      tone: "idle",
      detail: "Blocked — a gate failed; never reaches the operator.",
    };
  }
  if (bothPass) {
    return {
      key: "operator",
      label: "Operator decision",
      icon: Gavel,
      tone: "pending",
      detail: "Awaiting the operator's accept decision. Nothing auto-promotes.",
    };
  }
  return {
    key: "operator",
    label: "Operator decision",
    icon: Gavel,
    tone: "idle",
    detail: "Pending — gates must clear first.",
  };
}

export function GatePipeline({ capsule }: { capsule: ProofCapsule }) {
  const stages: Stage[] = [
    {
      key: "confound",
      label: "Confound gate",
      icon: gateIcon(capsule.confound_verdict),
      tone: verdictTone(capsule.confound_verdict),
      detail: detailFor("confound", capsule.confound_verdict),
    },
    {
      key: "provenance",
      label: "Provenance gate",
      icon: gateIcon(capsule.provenance_verdict),
      tone: verdictTone(capsule.provenance_verdict),
      detail: detailFor("provenance", capsule.provenance_verdict),
    },
    operatorStage(capsule),
  ];

  return (
    <ol className="flex flex-col gap-2 sm:flex-row sm:items-stretch sm:gap-0">
      {stages.map((stage, i) => {
        const Icon = stage.icon;
        return (
          <React.Fragment key={stage.key}>
            <li
              className={cn(
                "flex flex-1 flex-col gap-1.5 rounded-lg border p-3",
                TONE_CLASS[stage.tone],
              )}
            >
              <div className="flex items-center gap-2">
                <Icon className="size-4" aria-hidden />
                <span className="text-sm font-semibold">{stage.label}</span>
              </div>
              <p className="text-xs text-current/80">{stage.detail}</p>
            </li>
            {i < stages.length - 1 && (
              <div
                aria-hidden
                className="flex items-center justify-center px-1 text-muted-foreground"
              >
                <ChevronRight className="hidden size-4 sm:block" />
                <span className="block h-3 w-px bg-border sm:hidden" />
              </div>
            )}
          </React.Fragment>
        );
      })}
    </ol>
  );
}

function detailFor(
  gate: "confound" | "provenance",
  v: GateVerdict | undefined,
): string {
  const subject =
    gate === "confound"
      ? "Confound controls"
      : "Provenance + signature";
  switch (v) {
    case "pass":
      return `${subject} cleared.`;
    case "fail":
      return `${subject} failed — capsule blocked here.`;
    case "pending":
      return `${subject} check running.`;
    default:
      return `${subject} not yet evaluated.`;
  }
}

/** Compact verdict line used in card headers. */
export function GateVerdicts({ capsule }: { capsule: ProofCapsule }) {
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      <span className="text-xs text-muted-foreground">confound</span>
      <StatusPill kind="gate" value={capsule.confound_verdict ?? "unknown"} />
      <span className="text-xs text-muted-foreground">provenance</span>
      <StatusPill kind="gate" value={capsule.provenance_verdict ?? "unknown"} />
    </div>
  );
}
