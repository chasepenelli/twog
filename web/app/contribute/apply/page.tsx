"use client";

/**
 * Apply for collaborator access.
 *
 * Submitting lands the applicant as status: "pending" — an operator reviews each
 * application and grants scopes on approval. After submit we show a dedicated
 * "pending review" state rather than redirecting, so the applicant understands
 * the human-gated nature of the platform.
 *
 * An applicant may optionally register a public key, declaring intent to
 * self-hold their signing key (vs. custodial signing).
 *
 * CONTRIBUTE specialist owns app/contribute/*.
 */

import * as React from "react";
import Link from "next/link";
import { CheckCircle2, KeyRound, Loader2, ShieldQuestion } from "lucide-react";

import { api, ApiError } from "@/lib/api";
import type { Collaborator } from "@/lib/types/domain";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { StatusPill } from "@/components/ui/status-pill";
import { SectionHeader } from "@/components/contribute/section";

type ViewState =
  | { phase: "form" }
  | { phase: "submitting" }
  | { phase: "submitted"; collaborator: Collaborator }
  | { phase: "error"; message: string };

export default function ApplyPage() {
  const [state, setState] = React.useState<ViewState>({ phase: "form" });
  const [name, setName] = React.useState("");
  const [principal, setPrincipal] = React.useState("");
  const [publicKey, setPublicKey] = React.useState("");

  const canSubmit =
    name.trim().length > 1 &&
    principal.trim().length > 2 &&
    state.phase !== "submitting";

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!canSubmit) return;
    setState({ phase: "submitting" });
    try {
      const collaborator = await api.collaborators.apply({
        name: name.trim(),
        principal: principal.trim(),
        public_key: publicKey.trim() || undefined,
      });
      setState({ phase: "submitted", collaborator });
    } catch (err) {
      const message =
        err instanceof ApiError
          ? err.message
          : "Could not submit your application. Please try again.";
      setState({ phase: "error", message });
    }
  }

  if (state.phase === "submitted") {
    return (
      <div className="space-y-8">
        <SectionHeader
          title="Application submitted"
          description="Your application is now in the operator's review queue."
        />
        <Card className="max-w-xl">
          <CardHeader>
            <div className="flex items-center gap-2">
              <CheckCircle2 className="size-5 text-success" aria-hidden />
              <CardTitle>Pending operator review</CardTitle>
            </div>
            <CardDescription>
              Access on this platform is human-gated. An operator must approve
              your application and grant scopes before you can lease a sandbox or
              submit contributions. Nothing is granted automatically.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3 text-sm">
            <Row label="Name" value={state.collaborator.name} />
            <Row label="Principal" value={state.collaborator.principal} mono />
            <div className="flex items-center justify-between gap-4">
              <span className="text-muted-foreground">Status</span>
              <StatusPill kind="collaborator" value={state.collaborator.status} />
            </div>
            {state.collaborator.public_key && (
              <Row
                label="Public key"
                value={state.collaborator.public_key}
                mono
              />
            )}
          </CardContent>
          <CardFooter className="gap-2">
            <Button asChild variant="outline">
              <Link href="/contribute">Go to dashboard</Link>
            </Button>
          </CardFooter>
        </Card>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      <SectionHeader
        title="Apply for collaborator access"
        description="Collaborators contribute evidence to a comparative-oncology falsification platform — we try to disprove hypotheses, and a human operator holds the terminal accept/promote gate. Applications are reviewed by an operator."
      />

      <Card className="max-w-xl">
        <form onSubmit={onSubmit} noValidate>
          <CardHeader>
            <CardTitle>Your application</CardTitle>
            <CardDescription>
              Tell us who you are. An operator grants scopes on approval.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-5">
            {state.phase === "error" && (
              <div
                role="alert"
                className="rounded-md border border-danger/40 bg-danger-subtle/50 px-3 py-2 text-sm text-danger"
              >
                {state.message}
              </div>
            )}

            <div className="space-y-2">
              <Label htmlFor="apply-name">Name / organization</Label>
              <Input
                id="apply-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Kestrel Comparative Oncology Lab"
                autoComplete="organization"
                required
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="apply-principal">Principal</Label>
              <Input
                id="apply-principal"
                value={principal}
                onChange={(e) => setPrincipal(e.target.value)}
                placeholder="did:key:z6Mk… or auth0|your-id"
                className="font-mono text-xs"
                required
              />
              <p className="text-xs text-muted-foreground">
                Your stable identity — an auth subject or DID. This becomes your
                collaborator principal.
              </p>
            </div>

            <div className="space-y-2">
              <Label htmlFor="apply-key" className="flex items-center gap-1.5">
                <KeyRound className="size-3.5" aria-hidden />
                Public key{" "}
                <span className="font-normal text-muted-foreground">
                  (optional)
                </span>
              </Label>
              <Textarea
                id="apply-key"
                value={publicKey}
                onChange={(e) => setPublicKey(e.target.value)}
                placeholder="z6Mk…  — leave blank to use custodial signing"
                className="font-mono text-xs"
                rows={2}
              />
              <p className="flex items-start gap-1.5 text-xs text-muted-foreground">
                <ShieldQuestion className="mt-0.5 size-3.5 shrink-0" aria-hidden />
                Provide a public key only if you intend to self-hold your signing
                key. Otherwise the platform signs custodially on your behalf.
              </p>
            </div>
          </CardContent>
          <CardFooter className="justify-end">
            <Button type="submit" disabled={!canSubmit}>
              {state.phase === "submitting" && (
                <Loader2 className="size-4 animate-spin" aria-hidden />
              )}
              Submit application
            </Button>
          </CardFooter>
        </form>
      </Card>
    </div>
  );
}

function Row({
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
        className={
          mono
            ? "max-w-[60%] truncate text-right font-mono text-xs text-foreground"
            : "text-right text-foreground"
        }
      >
        {value}
      </span>
    </div>
  );
}
