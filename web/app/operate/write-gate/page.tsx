"use client";

/**
 * The WRITE GATE — the terminal human gate.
 *
 * This is the only place evidence enters the record or a candidate is promoted,
 * and only an operator acting deliberately can do it. Nothing reaches here
 * automatically and nothing leaves it without an explicit click. Each submitted
 * capsule is shown with its signal and its automated confound + provenance
 * verdicts; the operator may ACCEPT it into the evidence record (does not
 * promote the candidate) or PROMOTE the candidate behind it (the irreversible-
 * feeling, heaviest action — guarded by a confirmation).
 *
 * OPERATE owns app/operate/*.
 */

import * as React from "react";
import { ShieldCheck, AlertTriangle, Inbox, FileSignature } from "lucide-react";

import { api } from "@/lib/api";
import type { ProofCapsule } from "@/lib/types/domain";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
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
import { ScopeNotice } from "../_components/scope-notice";

export default function WriteGatePage() {
  const state = useAsync<ProofCapsule[]>((signal) =>
    api.capsules.listPending(signal),
  );
  const { toast } = useToast();
  const [promoting, setPromoting] = React.useState<ProofCapsule | null>(null);
  const [busyId, setBusyId] = React.useState<string | null>(null);

  async function accept(cap: ProofCapsule) {
    setBusyId(cap.capsule_id);
    try {
      await api.capsules.accept(cap.capsule_id);
      toast({
        variant: "success",
        title: "Capsule accepted",
        description: `${cap.capsule_id} entered the evidence record. The candidate was NOT promoted.`,
      });
      state.reload();
    } catch (err) {
      toast({
        variant: "danger",
        title: "Accept failed",
        description: (err as Error).message,
      });
    } finally {
      setBusyId(null);
    }
  }

  async function confirmPromote() {
    if (!promoting) return;
    setBusyId(promoting.capsule_id);
    try {
      await api.capsules.promote(promoting.capsule_id);
      toast({
        variant: "success",
        title: "Candidate promoted",
        description: `You promoted the candidate behind ${promoting.capsule_id}. This was a human decision.`,
      });
      setPromoting(null);
      state.reload();
    } catch (err) {
      toast({
        variant: "danger",
        title: "Promote failed",
        description: (err as Error).message,
      });
    } finally {
      setBusyId(null);
    }
  }

  return (
    <div className="space-y-6">
      <PageHeader
        title="Write gate"
        description="The terminal human gate. Evidence only enters the record, and candidates only advance, when you decide it here. Nothing auto-promotes."
      />

      <ScopeNotice scope="accept_capsule">
        You do not hold the <span className="font-mono">accept_capsule</span> /{" "}
        <span className="font-mono">promote_candidate</span> scopes. These
        actions are operator-only; the server will reject them.
      </ScopeNotice>

      <AsyncSection
        state={state}
        isEmpty={(rows) => rows.length === 0}
        empty={{
          icon: Inbox,
          title: "Nothing waiting at the gate",
          description:
            "Submitted capsules that have cleared the automated checks will queue here for your decision.",
        }}
        render={(rows) => (
          <ul className="space-y-4">
            {rows.map((cap) => {
              const provenanceFailed = cap.provenance_verdict === "fail";
              const confoundFailed = cap.confound_verdict === "fail";
              const blocked = provenanceFailed || confoundFailed;
              const busy = busyId === cap.capsule_id;
              return (
                <li key={cap.capsule_id}>
                  <Card>
                    <CardHeader className="flex-row items-start justify-between gap-3 space-y-0">
                      <div className="space-y-1">
                        <CardTitle className="font-mono text-sm">
                          {cap.capsule_id}
                        </CardTitle>
                        <p className="text-xs text-muted-foreground">
                          Candidate:{" "}
                          <span className="font-mono">{cap.candidate_id}</span> ·{" "}
                          {humanize(cap.validation_type)}
                        </p>
                      </div>
                      <StatusPill kind="signal" value={cap.signal} />
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm">
                        <GateReadout
                          label="Confound"
                          verdict={cap.confound_verdict}
                        />
                        <GateReadout
                          label="Provenance"
                          verdict={cap.provenance_verdict}
                        />
                        <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                          <FileSignature className="size-3.5" aria-hidden />
                          {cap.signature
                            ? `Self-held key (${cap.signature.slice(0, 16)}…)`
                            : "Custodial signature"}
                        </span>
                      </div>

                      {blocked && (
                        <div
                          role="note"
                          className="flex items-start gap-2 rounded-md border border-danger/40 bg-danger-subtle/50 px-3 py-2 text-sm text-danger"
                        >
                          <AlertTriangle
                            className="mt-0.5 size-4 shrink-0"
                            aria-hidden
                          />
                          <span>
                            An automated gate{" "}
                            <span className="font-medium">
                              {confoundFailed ? "confound" : "provenance"}
                            </span>{" "}
                            failed. Accepting this evidence over a failed gate is
                            possible but deliberate — confirm the verdict before
                            you do.
                          </span>
                        </div>
                      )}

                      <div className="flex flex-wrap items-center justify-end gap-2">
                        <Button
                          variant="accept"
                          size="sm"
                          disabled={busy}
                          onClick={() => accept(cap)}
                        >
                          <ShieldCheck className="size-4" />
                          Accept evidence
                        </Button>
                        <Button
                          variant="default"
                          size="sm"
                          disabled={busy}
                          onClick={() => setPromoting(cap)}
                        >
                          Promote candidate…
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                </li>
              );
            })}
          </ul>
        )}
      />

      <Dialog
        open={promoting !== null}
        onOpenChange={(open) => !open && setPromoting(null)}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Promote this candidate?</DialogTitle>
            <DialogDescription>
              You are about to promote the candidate behind{" "}
              <span className="font-mono">{promoting?.capsule_id}</span>{" "}
              (candidate{" "}
              <span className="font-mono">{promoting?.candidate_id}</span>).
              This is the terminal human gate — the platform never does this on
              its own. Promote only because you, the operator, judge the
              evidence sufficient.
            </DialogDescription>
          </DialogHeader>
          {promoting && (
            <div className="flex flex-wrap items-center gap-2 rounded-md border border-border bg-muted/40 p-3 text-sm">
              <StatusPill kind="signal" value={promoting.signal} />
              {promoting.confound_verdict && (
                <GateReadout
                  label="Confound"
                  verdict={promoting.confound_verdict}
                />
              )}
              {promoting.provenance_verdict && (
                <GateReadout
                  label="Provenance"
                  verdict={promoting.provenance_verdict}
                />
              )}
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setPromoting(null)}>
              Cancel
            </Button>
            <Button
              onClick={confirmPromote}
              disabled={busyId === promoting?.capsule_id}
            >
              I am promoting this candidate
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

function GateReadout({
  label,
  verdict,
}: {
  label: string;
  verdict?: ProofCapsule["confound_verdict"];
}) {
  return (
    <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
      {label}
      {verdict ? (
        <StatusPill kind="gate" value={verdict} />
      ) : (
        <Badge variant="neutral">n/a</Badge>
      )}
    </span>
  );
}
