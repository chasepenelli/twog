import * as React from "react";
import {
  CircleDot,
  CircleCheck,
  CircleSlash,
  CircleX,
  CircleHelp,
  ShieldCheck,
  ShieldAlert,
  ShieldQuestion,
  type LucideIcon,
} from "lucide-react";

import { cn } from "@/lib/utils";
import { Badge, type BadgeProps } from "@/components/ui/badge";
import type {
  CollaboratorStatus,
  CapsuleSignal,
  GateVerdict,
} from "@/lib/types/domain";

type Tone = NonNullable<BadgeProps["variant"]>;

interface PillSpec {
  label: string;
  tone: Tone;
  icon: LucideIcon;
}

/**
 * Collaborator lifecycle status.
 *   pending  -> awaiting operator decision (warning)
 *   active   -> approved + holding scopes (success)
 *   revoked  -> access withdrawn (neutral)
 */
const COLLABORATOR_STATUS: Record<CollaboratorStatus, PillSpec> = {
  pending: { label: "Pending", tone: "warning", icon: CircleDot },
  active: { label: "Active", tone: "success", icon: CircleCheck },
  revoked: { label: "Revoked", tone: "neutral", icon: CircleSlash },
};

/**
 * Capsule evidence signal. Falsification voice: a capsule that REFUTES is a
 * meaningful scientific result, not an error — styled serious, not alarming.
 *   supports -> evidence for the hypothesis (success)
 *   refutes  -> evidence against (danger)
 *   neutral  -> inconclusive (neutral)
 */
const CAPSULE_SIGNAL: Record<CapsuleSignal, PillSpec> = {
  supports: { label: "Supports", tone: "success", icon: CircleCheck },
  refutes: { label: "Refutes", tone: "danger", icon: CircleX },
  neutral: { label: "Neutral", tone: "neutral", icon: CircleSlash },
};

/**
 * Automated gate verdict (confound / provenance).
 */
const GATE_VERDICT: Record<GateVerdict, PillSpec> = {
  pass: { label: "Pass", tone: "success", icon: ShieldCheck },
  fail: { label: "Fail", tone: "danger", icon: ShieldAlert },
  pending: { label: "Pending", tone: "warning", icon: ShieldQuestion },
  unknown: { label: "Unknown", tone: "neutral", icon: ShieldQuestion },
};

/**
 * Hypothesis / candidate public status. Free-form on the backend, so we map by
 * keyword and fall back to a neutral pill that preserves the raw label.
 * Falsification voice: "standing" / "survived" are live, "refuted" / "falsified"
 * are conclusive negatives.
 */
function hypothesisSpec(raw: string): PillSpec {
  const k = raw.trim().toLowerCase();
  if (/(refut|falsif|disproven|rejected)/.test(k))
    return { label: titleCase(raw), tone: "danger", icon: CircleX };
  if (/(surviv|support|promoted|confirmed)/.test(k))
    return { label: titleCase(raw), tone: "success", icon: CircleCheck };
  if (/(standing|under|testing|active|open)/.test(k))
    return { label: titleCase(raw), tone: "info", icon: CircleDot };
  if (/(pending|queued|await)/.test(k))
    return { label: titleCase(raw), tone: "warning", icon: CircleDot };
  if (/(neutral|inconclusive|unknown)/.test(k))
    return { label: titleCase(raw), tone: "neutral", icon: CircleHelp };
  return { label: titleCase(raw), tone: "neutral", icon: CircleHelp };
}

function titleCase(s: string) {
  return s
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .trim();
}

type StatusPillProps = {
  className?: string;
  /** Hide the leading icon. */
  hideIcon?: boolean;
} & (
  | { kind: "collaborator"; value: CollaboratorStatus }
  | { kind: "signal"; value: CapsuleSignal }
  | { kind: "gate"; value: GateVerdict }
  | { kind: "hypothesis"; value: string }
);

/**
 * Domain-aware status pill. One component renders collaborator lifecycle,
 * capsule signal, automated gate verdict, and free-form hypothesis status with
 * consistent color semantics across the whole app.
 */
function StatusPill(props: StatusPillProps) {
  const { className, hideIcon } = props;
  let spec: PillSpec;
  switch (props.kind) {
    case "collaborator":
      spec = COLLABORATOR_STATUS[props.value];
      break;
    case "signal":
      spec = CAPSULE_SIGNAL[props.value];
      break;
    case "gate":
      spec = GATE_VERDICT[props.value];
      break;
    case "hypothesis":
      spec = hypothesisSpec(props.value);
      break;
  }

  const Icon = spec.icon;
  return (
    <Badge variant={spec.tone} className={cn("font-medium", className)}>
      {!hideIcon && <Icon aria-hidden className="size-3" />}
      {spec.label}
    </Badge>
  );
}

export { StatusPill };
export type { StatusPillProps };
