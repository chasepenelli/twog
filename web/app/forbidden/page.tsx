/**
 * /forbidden — shown when a signed-in user lacks the scope a surface requires.
 *
 * AUTH specialist authored this page (it backs the requireScope() redirect
 * target); its own header invites POLISH to restyle, which is done here with the
 * design system so it matches the rest of the app (light + dark).
 */
import Link from "next/link";
import { ShieldAlert } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

export default function ForbiddenPage({
  searchParams,
}: {
  searchParams: { scope?: string };
}) {
  const scope = searchParams?.scope;
  const isOperatorScope =
    scope === "accept_capsule" || scope === "promote_candidate";

  return (
    <div className="mx-auto flex min-h-[60vh] w-full max-w-lg items-center px-4 py-16 sm:px-6">
      <Card className="w-full">
        <CardHeader>
          <div className="mb-2 grid size-10 place-items-center rounded-md bg-warning-subtle text-warning">
            <ShieldAlert className="size-5" aria-hidden />
          </div>
          <CardTitle>Not authorized</CardTitle>
          <CardDescription>
            You are signed in, but your collaborator record does not hold the{" "}
            {scope ? (
              <span className="font-mono text-foreground">{scope}</span>
            ) : (
              "required"
            )}{" "}
            scope for this surface.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4 text-sm text-muted-foreground">
          <p>
            {isOperatorScope
              ? "This is an operator-only action — the terminal human gate. Nothing auto-promotes."
              : "If you believe this is a mistake, ask an operator to review your scopes."}
          </p>
          <Button asChild>
            <Link href="/contribute">Back to your dashboard</Link>
          </Button>
        </CardContent>
      </Card>
    </div>
  );
}
