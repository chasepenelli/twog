"use client";

/**
 * AppShell — the global chrome: brand, role-aware primary nav, theme toggle, and
 * a user menu with sign-out. Mounted by POLISH in app/layout.tsx, INSIDE the
 * SessionProvider, so it can read the resolved twog principal (role + scopes).
 *
 * Nav is role-aware:
 *  - Everyone signed in sees Contribute.
 *  - Operators (role === "operator") additionally see Operate.
 * Surfaces enforce their own server-side scope guards; the nav is UX only.
 *
 * The marketing/landing route ("/") renders without the shell — see layout.
 *
 * POLISH owns components/app-shell/*.
 */

import * as React from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  Beaker,
  Gavel,
  LogOut,
  Menu,
  UserCircle,
  X,
} from "lucide-react";

import { useSession } from "@/lib/auth/use-session";
import { signOutAction } from "@/lib/auth/actions";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { StatusPill } from "@/components/ui/status-pill";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { ThemeToggle } from "@/components/app-shell/theme-toggle";

interface NavLink {
  href: string;
  label: string;
  icon: typeof Beaker;
}

function isActive(pathname: string, href: string): boolean {
  return pathname === href || pathname.startsWith(`${href}/`);
}

export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const { principal, isAuthenticated, isOperator } = useSession();
  const [mobileOpen, setMobileOpen] = React.useState(false);

  // Close the mobile menu on navigation.
  React.useEffect(() => {
    setMobileOpen(false);
  }, [pathname]);

  // The marketing/landing screen owns its full-bleed layout: render bare chrome.
  if (pathname === "/") {
    return <>{children}</>;
  }

  const links: NavLink[] = [
    { href: "/contribute", label: "Contribute", icon: Beaker },
  ];
  if (isOperator) {
    links.push({ href: "/operate", label: "Operate", icon: Gavel });
  }

  const initials =
    principal?.name
      ?.split(/\s+/)
      .map((p) => p[0])
      .filter(Boolean)
      .slice(0, 2)
      .join("")
      .toUpperCase() || "?";

  return (
    <div className="flex min-h-screen flex-col bg-background">
      <header className="sticky top-0 z-40 border-b border-border bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/80">
        <div className="mx-auto flex h-14 w-full max-w-6xl items-center gap-4 px-4 sm:px-6 lg:px-8">
          {/* Brand */}
          <Link
            href={isAuthenticated ? "/contribute" : "/"}
            className="flex items-center gap-2 rounded-md font-semibold tracking-tight focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            <span className="grid size-7 place-items-center rounded-md bg-primary text-primary-foreground">
              <Beaker className="size-4" aria-hidden />
            </span>
            <span>twog</span>
            <span className="hidden text-xs font-normal text-muted-foreground sm:inline">
              falsification platform
            </span>
          </Link>

          {/* Desktop primary nav */}
          <nav
            aria-label="Primary"
            className="ml-2 hidden items-center gap-1 md:flex"
          >
            {links.map((link) => {
              const active = isActive(pathname, link.href);
              const Icon = link.icon;
              return (
                <Link
                  key={link.href}
                  href={link.href}
                  aria-current={active ? "page" : undefined}
                  className={cn(
                    "inline-flex items-center gap-2 rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                    active
                      ? "bg-secondary text-secondary-foreground"
                      : "text-muted-foreground hover:bg-secondary/60 hover:text-foreground",
                  )}
                >
                  <Icon className="size-4" aria-hidden />
                  {link.label}
                </Link>
              );
            })}
          </nav>

          <div className="ml-auto flex items-center gap-1">
            <ThemeToggle />

            {isAuthenticated ? (
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="gap-2"
                    aria-label="Account menu"
                  >
                    <span className="grid size-6 place-items-center rounded-full bg-secondary text-xs font-semibold text-secondary-foreground">
                      {initials}
                    </span>
                    <span className="hidden max-w-[12ch] truncate sm:inline">
                      {principal?.name}
                    </span>
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" className="w-64">
                  <DropdownMenuLabel className="normal-case">
                    <div className="flex flex-col gap-1">
                      <span className="text-sm font-medium text-foreground">
                        {principal?.name}
                      </span>
                      <span className="truncate text-xs font-normal text-muted-foreground">
                        {principal?.email}
                      </span>
                      <div className="mt-1 flex flex-wrap items-center gap-2">
                        <Badge variant={isOperator ? "info" : "secondary"}>
                          {isOperator ? "Operator" : "Collaborator"}
                        </Badge>
                        {principal?.collaborator ? (
                          <StatusPill
                            kind="collaborator"
                            value={principal.collaborator.status}
                          />
                        ) : (
                          <Badge variant="neutral">Not yet applied</Badge>
                        )}
                      </div>
                    </div>
                  </DropdownMenuLabel>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem asChild>
                    <Link href="/contribute">
                      <UserCircle aria-hidden />
                      Your dashboard
                    </Link>
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                  {/* Sign-out is a Server Action; the form's submit drives it.
                      We don't close the menu on click so the submit isn't
                      interrupted by an unmount. */}
                  <DropdownMenuItem
                    onSelect={(e) => e.preventDefault()}
                    className="p-0"
                  >
                    <form action={signOutAction} className="w-full">
                      <button
                        type="submit"
                        className="flex w-full items-center gap-2 px-2 py-1.5"
                      >
                        <LogOut aria-hidden />
                        Sign out
                      </button>
                    </form>
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            ) : (
              <Button asChild size="sm">
                <Link href="/login">Sign in</Link>
              </Button>
            )}

            {/* Mobile menu toggle */}
            {links.length > 0 && (
              <Button
                variant="ghost"
                size="icon"
                className="md:hidden"
                aria-label="Toggle navigation menu"
                aria-expanded={mobileOpen}
                onClick={() => setMobileOpen((v) => !v)}
              >
                {mobileOpen ? (
                  <X aria-hidden />
                ) : (
                  <Menu aria-hidden />
                )}
              </Button>
            )}
          </div>
        </div>

        {/* Mobile primary nav */}
        {mobileOpen && (
          <nav
            aria-label="Primary mobile"
            className="border-t border-border bg-background px-4 py-2 md:hidden"
          >
            <ul className="flex flex-col gap-1">
              {links.map((link) => {
                const active = isActive(pathname, link.href);
                const Icon = link.icon;
                return (
                  <li key={link.href}>
                    <Link
                      href={link.href}
                      aria-current={active ? "page" : undefined}
                      className={cn(
                        "flex items-center gap-2 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                        active
                          ? "bg-secondary text-secondary-foreground"
                          : "text-muted-foreground hover:bg-secondary/60 hover:text-foreground",
                      )}
                    >
                      <Icon className="size-4" aria-hidden />
                      {link.label}
                    </Link>
                  </li>
                );
              })}
            </ul>
          </nav>
        )}
      </header>

      <main className="flex-1">{children}</main>

      <footer className="border-t border-border">
        <div className="mx-auto flex w-full max-w-6xl flex-col gap-1 px-4 py-6 text-xs text-muted-foreground sm:flex-row sm:items-center sm:justify-between sm:px-6 lg:px-8">
          <p>
            twog — comparative-oncology falsification platform. We try to
            disprove hypotheses.
          </p>
          <p className="font-medium text-foreground/70">
            Nothing auto-promotes. A human operator holds the terminal gate.
          </p>
        </div>
      </footer>
    </div>
  );
}
