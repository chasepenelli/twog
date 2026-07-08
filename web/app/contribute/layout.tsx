"use client";

/**
 * Contribute surface shell — local section navigation for collaborators.
 *
 * This is the collaborator-facing surface: apply, see your profile + scopes,
 * open a sandbox, submit a contribution, and track it through the admission
 * gates to the operator's terminal decision. The global app-shell + providers
 * are wired by POLISH in app/layout.tsx; this layout only owns the in-surface
 * navigation.
 *
 * CONTRIBUTE specialist owns app/contribute/*.
 */

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  FlaskConical,
  Send,
  Route,
  UserPlus,
  type LucideIcon,
} from "lucide-react";

import { cn } from "@/lib/utils";

interface NavItem {
  href: string;
  label: string;
  icon: LucideIcon;
}

const NAV: NavItem[] = [
  { href: "/contribute", label: "Dashboard", icon: LayoutDashboard },
  { href: "/contribute/sandbox", label: "Sandbox", icon: FlaskConical },
  { href: "/contribute/submit", label: "Submit", icon: Send },
  { href: "/contribute/tracker", label: "Tracker", icon: Route },
  { href: "/contribute/apply", label: "Apply", icon: UserPlus },
];

export default function ContributeLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = usePathname();

  return (
    <div className="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6 lg:px-8">
      <nav
        aria-label="Contribute sections"
        className="mb-8 flex flex-wrap gap-1 rounded-lg border border-border bg-card p-1"
      >
        {NAV.map((item) => {
          const active =
            item.href === "/contribute"
              ? pathname === "/contribute"
              : pathname.startsWith(item.href);
          const Icon = item.icon;
          return (
            <Link
              key={item.href}
              href={item.href}
              aria-current={active ? "page" : undefined}
              className={cn(
                "inline-flex items-center gap-2 rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                active
                  ? "bg-background text-foreground shadow-sm"
                  : "text-muted-foreground hover:text-foreground",
              )}
            >
              <Icon className="size-4" aria-hidden />
              {item.label}
            </Link>
          );
        })}
      </nav>
      {children}
    </div>
  );
}
