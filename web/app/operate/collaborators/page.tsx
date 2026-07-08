"use client";

/**
 * Collaborators roster. Lists every collaborator (operator + active + revoked,
 * excluding the pending queue which lives on /operate/applicants) with their
 * role, status, held scopes, and signing mode. Active collaborators can be
 * revoked; revocation withdraws all scopes.
 *
 * OPERATE owns app/operate/*.
 */

import * as React from "react";
import { Users, KeyRound, Building2 } from "lucide-react";

import { api } from "@/lib/api";
import type { Collaborator } from "@/lib/types/domain";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { StatusPill } from "@/components/ui/status-pill";
import { useToast } from "@/lib/hooks/use-toast";
import { humanize } from "../_lib/format";
import { useAsync } from "../_lib/use-async";
import { PageHeader } from "../_components/page-header";
import { AsyncSection } from "../_components/async-section";

export default function CollaboratorsPage() {
  const state = useAsync<Collaborator[]>((signal) =>
    api.collaborators.list(signal),
  );
  const { toast } = useToast();
  const [revoking, setRevoking] = React.useState<Collaborator | null>(null);
  const [busy, setBusy] = React.useState(false);

  async function confirmRevoke() {
    if (!revoking) return;
    setBusy(true);
    try {
      await api.collaborators.revoke(revoking.collaborator_id);
      toast({
        title: "Access revoked",
        description: `${revoking.name} can no longer act on the platform.`,
      });
      setRevoking(null);
      state.reload();
    } catch (err) {
      toast({
        variant: "danger",
        title: "Revoke failed",
        description: (err as Error).message,
      });
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Collaborators"
        description="The active roster and revoked history. Pending applications are handled in the applicant queue."
      />

      <AsyncSection
        state={state}
        isEmpty={(rows) =>
          rows.filter((c) => c.status !== "pending").length === 0
        }
        empty={{
          icon: Users,
          title: "No collaborators yet",
          description: "Approved applicants will appear here.",
        }}
        render={(rows) => {
          const roster = rows
            .filter((c) => c.status !== "pending")
            .sort((a, b) => {
              const rank = (c: Collaborator) =>
                c.role === "operator" ? 0 : c.status === "active" ? 1 : 2;
              return rank(a) - rank(b);
            });
          return (
            <div className="rounded-lg border border-border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Collaborator</TableHead>
                    <TableHead>Role</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Scopes</TableHead>
                    <TableHead className="text-right">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {roster.map((c) => (
                    <TableRow key={c.collaborator_id}>
                      <TableCell>
                        <div className="space-y-0.5">
                          <div className="flex items-center gap-1.5 font-medium">
                            {c.name}
                          </div>
                          <div className="flex items-center gap-1.5 font-mono text-xs text-muted-foreground">
                            {c.public_key ? (
                              <KeyRound className="size-3" aria-hidden />
                            ) : (
                              <Building2 className="size-3" aria-hidden />
                            )}
                            {c.principal}
                          </div>
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge
                          variant={c.role === "operator" ? "info" : "outline"}
                        >
                          {humanize(c.role)}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <StatusPill kind="collaborator" value={c.status} />
                      </TableCell>
                      <TableCell>
                        {c.scopes.length === 0 ? (
                          <span className="text-xs text-muted-foreground">
                            —
                          </span>
                        ) : (
                          <div className="flex flex-wrap gap-1">
                            {c.scopes.map((s) => (
                              <Badge key={s} variant="secondary">
                                {humanize(s)}
                              </Badge>
                            ))}
                          </div>
                        )}
                      </TableCell>
                      <TableCell className="text-right">
                        {c.status === "active" && c.role !== "operator" ? (
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setRevoking(c)}
                          >
                            Revoke
                          </Button>
                        ) : (
                          <span className="text-xs text-muted-foreground">
                            —
                          </span>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          );
        }}
      />

      <Dialog
        open={revoking !== null}
        onOpenChange={(open) => !open && setRevoking(null)}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Revoke {revoking?.name}?</DialogTitle>
            <DialogDescription>
              This withdraws all scopes and ends their access immediately.
              In-flight contributions already at the gate are unaffected.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setRevoking(null)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={confirmRevoke}
              disabled={busy}
            >
              Revoke access
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
