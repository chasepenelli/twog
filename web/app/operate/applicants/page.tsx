"use client";

/**
 * Pending-applicant queue. The operator reviews each application and either
 * approves it — granting an explicit set of scopes — or denies it. Approve is a
 * deliberate, scoped grant (not a blanket yes); operator-only scopes are never
 * offered for a collaborator.
 *
 * OPERATE owns app/operate/*.
 */

import * as React from "react";
import { UserCheck, KeyRound, Inbox } from "lucide-react";

import { api } from "@/lib/api";
import type { Collaborator, Scope } from "@/lib/types/domain";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useToast } from "@/lib/hooks/use-toast";
import { useAsync } from "../_lib/use-async";
import { PageHeader } from "../_components/page-header";
import { AsyncSection } from "../_components/async-section";
import { ScopePicker } from "../_components/scope-picker";

const DEFAULT_SCOPES: Scope[] = [
  "lease_workspace",
  "submit_compute",
  "submit_capsule",
];

export default function ApplicantsPage() {
  const state = useAsync<Collaborator[]>((signal) =>
    api.collaborators.list(signal),
  );
  const { toast } = useToast();

  const [approving, setApproving] = React.useState<Collaborator | null>(null);
  const [grant, setGrant] = React.useState<Scope[]>(DEFAULT_SCOPES);
  const [busyId, setBusyId] = React.useState<string | null>(null);

  function openApprove(c: Collaborator) {
    setApproving(c);
    setGrant(DEFAULT_SCOPES);
  }

  async function confirmApprove() {
    if (!approving) return;
    setBusyId(approving.collaborator_id);
    try {
      const updated = await api.collaborators.approve({
        collaborator_id: approving.collaborator_id,
        scopes: grant,
      });
      toast({
        title: "Applicant approved",
        description: `${updated.name} is now active with ${grant.length} scope${grant.length === 1 ? "" : "s"}.`,
      });
      setApproving(null);
      state.reload();
    } catch (err) {
      toast({
        variant: "danger",
        title: "Approval failed",
        description: (err as Error).message,
      });
    } finally {
      setBusyId(null);
    }
  }

  async function deny(c: Collaborator) {
    setBusyId(c.collaborator_id);
    try {
      await api.collaborators.deny(c.collaborator_id);
      toast({ title: "Applicant denied", description: `${c.name} was denied.` });
      state.reload();
    } catch (err) {
      toast({
        variant: "danger",
        title: "Deny failed",
        description: (err as Error).message,
      });
    } finally {
      setBusyId(null);
    }
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Applicant queue"
        description="Pending collaborator applications awaiting your decision. Approving grants an explicit, scoped set of permissions."
      />

      <AsyncSection
        state={state}
        isEmpty={(rows) => rows.filter((c) => c.status === "pending").length === 0}
        empty={{
          icon: Inbox,
          title: "No pending applicants",
          description: "New applications will appear here for review.",
        }}
        render={(rows) => {
          const pending = rows.filter((c) => c.status === "pending");
          return (
            <ul className="space-y-3">
              {pending.map((c) => {
                const busy = busyId === c.collaborator_id;
                return (
                  <li key={c.collaborator_id}>
                    <Card>
                      <CardContent className="flex flex-col gap-4 p-4 sm:flex-row sm:items-center sm:justify-between">
                        <div className="min-w-0 space-y-1">
                          <p className="truncate font-medium">{c.name}</p>
                          <p className="truncate font-mono text-xs text-muted-foreground">
                            {c.principal}
                          </p>
                          <p className="flex items-center gap-1.5 text-xs text-muted-foreground">
                            {c.public_key ? (
                              <>
                                <KeyRound className="size-3" aria-hidden />
                                Self-held key:{" "}
                                <span className="font-mono">
                                  {c.public_key.slice(0, 12)}…
                                </span>
                              </>
                            ) : (
                              "Custodial signing (no self-held key)"
                            )}
                          </p>
                        </div>
                        <div className="flex shrink-0 items-center gap-2">
                          <Button
                            variant="outline"
                            size="sm"
                            disabled={busy}
                            onClick={() => deny(c)}
                          >
                            Deny
                          </Button>
                          <Button
                            size="sm"
                            disabled={busy}
                            onClick={() => openApprove(c)}
                          >
                            <UserCheck className="size-4" />
                            Approve…
                          </Button>
                        </div>
                      </CardContent>
                    </Card>
                  </li>
                );
              })}
            </ul>
          );
        }}
      />

      <Dialog
        open={approving !== null}
        onOpenChange={(open) => !open && setApproving(null)}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Approve {approving?.name}</DialogTitle>
            <DialogDescription>
              Grant the scopes this collaborator may hold. They activate
              immediately on approval.
            </DialogDescription>
          </DialogHeader>
          <ScopePicker value={grant} onChange={setGrant} />
          <DialogFooter>
            <Button variant="outline" onClick={() => setApproving(null)}>
              Cancel
            </Button>
            <Button
              onClick={confirmApprove}
              disabled={
                grant.length === 0 || busyId === approving?.collaborator_id
              }
            >
              Approve &amp; grant {grant.length} scope
              {grant.length === 1 ? "" : "s"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
