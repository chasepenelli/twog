"use client";

/**
 * OperateNav — local sub-navigation for the operator console.
 *
 * The global app-shell/nav is POLISH's responsibility; this is the operate
 * surface's own section nav so the six operator workflows are reachable while
 * the surfaces are reviewed in isolation. Marks the active route.
 *
 * OPERATE owns app/operate/*.
 */

import Link from "next/link";
import { usePathname } from "next/navigation";

import { cn } from "@/lib/utils";

const LINKS: { href: string; label: string; exact?: boolean }[] = [
  { href: "/operate", label: "Overview", exact: true },
  { href: "/operate/applicants", label: "Applicants" },
  { href: "/operate/collaborators", label: "Collaborators" },
  { href: "/operate/candidates", label: "Candidates" },
  { href: "/operate/campaigns", label: "Campaigns" },
  { href: "/operate/write-gate", label: "Write gate" },
  { href: "/operate/targets", label: "Targets" },
];

export function OperateNav() {
  const pathname = usePathname();
  return (
    <nav
      aria-label="Operator console"
      className="flex flex-wrap items-center gap-1 border-b border-border pb-3"
    >
      {LINKS.map((link) => {
        const active = link.exact
          ? pathname === link.href
          : pathname === link.href || pathname.startsWith(`${link.href}/`);
        return (
          <Link
            key={link.href}
            href={link.href}
            aria-current={active ? "page" : undefined}
            className={cn(
              "rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
              active
                ? "bg-secondary text-secondary-foreground"
                : "text-muted-foreground hover:bg-secondary/60 hover:text-foreground",
            )}
          >
            {link.label}
          </Link>
        );
      })}
    </nav>
  );
}
