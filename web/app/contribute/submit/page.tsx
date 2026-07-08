"use client";

/**
 * Submit a contribution from a leased sandbox.
 *
 * Two in-app modes:
 *  - evidence_capsule: a signed unit of evidence (supports/refutes/neutral) for
 *    an existing candidate. Lands in the confound + provenance gates.
 *  - candidate_proposal: a new hypothesis entered into the falsification queue.
 *    Lands in operator triage.
 *
 * Both surface the signing path: custodial (platform signs) vs self-held (you
 * attach a signature). The default comes from your dashboard key-mode preference.
 * On success we link to the tracker so the collaborator can follow the artifact
 * through the gates to the operator's terminal decision.
 *
 * CONTRIBUTE specialist owns app/contribute/*.
 */

import * as React from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { CheckCircle2, Route } from "lucide-react";

import { api } from "@/lib/api";
import { useSession } from "@/lib/auth/use-session";
import type { Candidate, ContributionMode } from "@/lib/types/domain";
import type { Contribution } from "@/lib/api";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { useToast } from "@/lib/hooks/use-toast";
import { SectionHeader } from "@/components/contribute/section";
import { useKeyMode } from "@/components/contribute/key-mode";
import { EvidenceCapsuleForm } from "@/components/contribute/evidence-capsule-form";
import { CandidateProposalForm } from "@/components/contribute/candidate-proposal-form";

type SubmitMode = Extract<
  ContributionMode,
  "evidence_capsule" | "candidate_proposal"
>;

export default function SubmitPage() {
  // useSearchParams must sit under a Suspense boundary (Next.js App Router).
  return (
    <React.Suspense fallback={<SubmitFallback />}>
      <SubmitView />
    </React.Suspense>
  );
}

function SubmitFallback() {
  return (
    <div className="space-y-8">
      <SectionHeader
        title="Submit a contribution"
        description="Produce an artifact from your leased sandbox. It is admitted only after clearing its gate — a human operator holds the terminal accept/promote decision."
      />
      <div className="h-9 w-64 animate-pulse rounded-lg bg-muted" />
    </div>
  );
}

function SubmitView() {
  const params = useSearchParams();
  const initialMode: SubmitMode =
    params.get("mode") === "candidate_proposal"
      ? "candidate_proposal"
      : "evidence_capsule";

  const { principal } = useSession();
  const publicKey = principal?.collaborator?.public_key;
  const [tab, setTab] = React.useState<SubmitMode>(initialMode);
  const [keyMode, setKeyMode] = useKeyMode();
  const [submitted, setSubmitted] = React.useState<{
    contribution: Contribution;
    mode: SubmitMode;
  } | null>(null);
  const { toast } = useToast();

  function onSubmitted(contribution: Contribution, mode: SubmitMode) {
    setSubmitted({ contribution, mode });
    toast({
      title:
        mode === "evidence_capsule"
          ? "Evidence capsule submitted"
          : "Candidate proposal submitted",
      description:
        mode === "evidence_capsule"
          ? "It now enters the confound + provenance gates. Nothing is auto-promoted."
          : "It now enters operator triage. Nothing is auto-promoted.",
    });
  }

  if (submitted) {
    return (
      <SubmittedView
        contribution={submitted.contribution}
        mode={submitted.mode}
        onReset={() => setSubmitted(null)}
      />
    );
  }

  return (
    <div className="space-y-8">
      <SectionHeader
        title="Submit a contribution"
        description="Produce an artifact from your leased sandbox. It is admitted only after clearing its gate — a human operator holds the terminal accept/promote decision."
      />

      <Tabs value={tab} onValueChange={(v) => setTab(v as SubmitMode)}>
        <TabsList>
          <TabsTrigger value="evidence_capsule">Evidence capsule</TabsTrigger>
          <TabsTrigger value="candidate_proposal">
            Candidate proposal
          </TabsTrigger>
        </TabsList>

        <TabsContent value="evidence_capsule">
          <EvidenceCapsuleForm
            keyMode={keyMode}
            onKeyModeChange={setKeyMode}
            publicKey={publicKey}
            onSubmitted={(c) => onSubmitted(c, "evidence_capsule")}
          />
        </TabsContent>

        <TabsContent value="candidate_proposal">
          <CandidateProposalForm
            keyMode={keyMode}
            onKeyModeChange={setKeyMode}
            publicKey={publicKey}
            onSubmitted={(c) => onSubmitted(c, "candidate_proposal")}
          />
        </TabsContent>
      </Tabs>
    </div>
  );
}

function SubmittedView({
  contribution,
  mode,
  onReset,
}: {
  contribution: Contribution;
  mode: SubmitMode;
  onReset: () => void;
}) {
  const isCapsule = mode === "evidence_capsule";
  const id =
    "capsule_id" in contribution
      ? contribution.capsule_id
      : (contribution as Candidate).candidate_id;

  return (
    <div className="space-y-8">
      <SectionHeader
        title="Contribution submitted"
        description="Your artifact is in the queue. It will not be promoted automatically."
      />
      <Card className="max-w-xl">
        <CardHeader>
          <div className="flex items-center gap-2">
            <CheckCircle2 className="size-5 text-success" aria-hidden />
            <CardTitle>
              {isCapsule
                ? "Evidence capsule submitted"
                : "Candidate proposal submitted"}
            </CardTitle>
          </div>
          <CardDescription>
            {isCapsule
              ? "Your capsule now moves through the confound and provenance gates. Once both clear, it reaches the operator's terminal accept gate."
              : "Your proposal now enters operator triage. If accepted, it joins the falsification queue."}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3 text-sm">
          <div className="flex items-center justify-between gap-4">
            <span className="text-muted-foreground">
              {isCapsule ? "Capsule id" : "Candidate id"}
            </span>
            <span className="font-mono text-xs text-foreground">{id}</span>
          </div>
          <div className="flex flex-wrap gap-2 pt-2">
            {isCapsule && (
              <Button asChild>
                <Link href="/contribute/tracker">
                  <Route className="size-4" aria-hidden />
                  Track in the gate pipeline
                </Link>
              </Button>
            )}
            <Button variant="outline" onClick={onReset}>
              Submit another
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
