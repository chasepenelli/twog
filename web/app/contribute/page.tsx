"use client";

/**
 * Contribute dashboard: the collaborator's home.
 *  - profile (name, principal, role, status)
 *  - the scopes they hold (and what each unlocks)
 *  - a key-mode toggle: custodial vs self-held signing, with each explained
 *
 * Identity + authorization come from useSession() (resolved server-side, passed
 * down by SessionProvider — wired into the root layout by POLISH). The key-mode
 * choice is a local UX preference for v1: it changes how the Submit flow signs.
 *
 * CONTRIBUTE specialist owns app/contribute/*.
 */

import Link from "next/link";
import {
  KeyRound,
  ShieldCheck,
  UserCircle,
  Send,
  FlaskConical,
  Info,
} from "lucide-react";

import { useSession } from "@/lib/auth/use-session";
import type { Scope } from "@/lib/types/domain";
import { cn } from "@/lib/utils";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { StatusPill } from "@/components/ui/status-pill";
import { EmptyState } from "@/components/ui/empty-state";
import { SectionHeader } from "@/components/contribute/section";
import {
  useKeyMode,
  KeyModeToggle,
} from "@/components/contribute/key-mode";

/** Human description of what each scope unlocks. */
const SCOPE_COPY: Record<Scope, { label: string; what: string }> = {
  lease_workspace: {
    label: "Lease workspace",
    what: "Open a sandbox and lease a leased runner to produce artifacts.",
  },
  submit_compute: {
    label: "Submit compute",
    what: "Dispatch compute jobs (e.g. docking) inside a leased sandbox.",
  },
  submit_capsule: {
    label: "Submit capsule",
    what: "Submit an evidence capsule into the confound + provenance gates.",
  },
  accept_capsule: {
    label: "Accept capsule",
    what: "Operator-only. Accept a capsule at the terminal human gate.",
  },
  promote_candidate: {
    label: "Promote candidate",
    what: "Operator-only. Promote a candidate — nothing here auto-promotes.",
  },
};

export default function ContributeDashboardPage() {
  const { principal, isOperator } = useSession();
  const [keyMode, setKeyMode] = useKeyMode();

  // Signed in but not yet a collaborator: nudge them to apply.
  if (!principal || !principal.collaborator) {
    return (
      <div className="space-y-8">
        <SectionHeader
          title="Your contributor dashboard"
          description="Collaborators contribute evidence to a comparative-oncology falsification platform. We try to disprove hypotheses; nothing is ever auto-promoted."
        />
        <EmptyState
          icon={UserCircle}
          title="You are not a collaborator yet"
          description="Apply for collaborator access. An operator reviews each application and grants scopes on approval."
          action={
            <Button asChild>
              <Link href="/contribute/apply">Apply for access</Link>
            </Button>
          }
        />
      </div>
    );
  }

  const c = principal.collaborator;
  const isPending = c.status === "pending";
  const isRevoked = c.status === "revoked";

  return (
    <div className="space-y-8">
      <SectionHeader
        title={`Welcome, ${principal.name}`}
        description="Your profile, the scopes you hold, and how your contributions are signed."
        action={
          c.status === "active" ? (
            <Button asChild>
              <Link href="/contribute/sandbox">
                <FlaskConical className="size-4" aria-hidden />
                Open a sandbox
              </Link>
            </Button>
          ) : undefined
        }
      />

      {isPending && (
        <div className="flex items-start gap-3 rounded-lg border border-warning/40 bg-warning-subtle/50 p-4 text-sm">
          <Info className="mt-0.5 size-4 shrink-0 text-warning" aria-hidden />
          <div>
            <p className="font-medium text-foreground">
              Your application is under review.
            </p>
            <p className="mt-0.5 text-muted-foreground">
              An operator must approve your application and grant scopes before
              you can lease a sandbox or submit contributions.
            </p>
          </div>
        </div>
      )}

      {isRevoked && (
        <div
          role="alert"
          className="flex items-start gap-3 rounded-lg border border-danger/40 bg-danger-subtle/50 p-4 text-sm"
        >
          <Info className="mt-0.5 size-4 shrink-0 text-danger" aria-hidden />
          <div>
            <p className="font-medium text-foreground">
              Your collaborator access has been revoked.
            </p>
            <p className="mt-0.5 text-muted-foreground">
              Contact an operator if you believe this is in error.
            </p>
          </div>
        </div>
      )}

      <div className="grid gap-6 lg:grid-cols-2">
        {/* Profile */}
        <Card>
          <CardHeader>
            <div className="flex items-center gap-2">
              <UserCircle className="size-4 text-muted-foreground" aria-hidden />
              <CardTitle>Profile</CardTitle>
            </div>
            <CardDescription>
              Identity comes from your sign-in; authorization comes from your
              collaborator record.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3 text-sm">
            <Field label="Name" value={principal.name} />
            <Field label="Email" value={principal.email} mono />
            <Field label="Principal" value={c.principal} mono />
            <div className="flex items-center justify-between gap-4">
              <span className="text-muted-foreground">Role</span>
              <Badge variant={isOperator ? "info" : "secondary"}>
                {isOperator ? "Operator" : "Collaborator"}
              </Badge>
            </div>
            <div className="flex items-center justify-between gap-4">
              <span className="text-muted-foreground">Status</span>
              <StatusPill kind="collaborator" value={c.status} />
            </div>
          </CardContent>
        </Card>

        {/* Scopes */}
        <Card>
          <CardHeader>
            <div className="flex items-center gap-2">
              <ShieldCheck className="size-4 text-muted-foreground" aria-hidden />
              <CardTitle>Granted scopes</CardTitle>
            </div>
            <CardDescription>
              Scopes are granted by an operator and are only effective while your
              record is active.
            </CardDescription>
          </CardHeader>
          <CardContent>
            {principal.scopes.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                No effective scopes yet.{" "}
                {isPending
                  ? "Scopes are granted when an operator approves your application."
                  : "An operator has not granted you any scopes."}
              </p>
            ) : (
              <ul className="space-y-3">
                {principal.scopes.map((scope) => {
                  const copy = SCOPE_COPY[scope];
                  return (
                    <li key={scope} className="flex items-start gap-3">
                      <ShieldCheck
                        className="mt-0.5 size-4 shrink-0 text-success"
                        aria-hidden
                      />
                      <div>
                        <p className="text-sm font-medium text-foreground">
                          {copy.label}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          {copy.what}
                        </p>
                      </div>
                    </li>
                  );
                })}
              </ul>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Key mode */}
      <Card>
        <CardHeader>
          <div className="flex items-center gap-2">
            <KeyRound className="size-4 text-muted-foreground" aria-hidden />
            <CardTitle>Signing key mode</CardTitle>
          </div>
          <CardDescription>
            How your contributions are cryptographically signed. This determines
            the signing path in the Submit flow.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-5">
          <KeyModeToggle
            value={keyMode}
            onChange={setKeyMode}
            publicKey={c.public_key}
          />
          {keyMode === "self_held" && !c.public_key && (
            <p
              className={cn(
                "rounded-md border border-warning/40 bg-warning-subtle/50 px-3 py-2 text-xs text-muted-foreground",
              )}
            >
              No public key is on file for your collaborator record. Register a
              public key with an operator before submitting self-held
              signatures, or your capsule will fail the provenance gate.
            </p>
          )}
          {c.status === "active" && (
            <Button asChild variant="outline" size="sm">
              <Link href="/contribute/submit">
                <Send className="size-4" aria-hidden />
                Submit a contribution
              </Link>
            </Button>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function Field({
  label,
  value,
  mono,
}: {
  label: string;
  value: string;
  mono?: boolean;
}) {
  return (
    <div className="flex items-center justify-between gap-4">
      <span className="text-muted-foreground">{label}</span>
      <span
        className={cn(
          "text-right text-foreground",
          mono && "font-mono text-xs",
        )}
      >
        {value}
      </span>
    </div>
  );
}
