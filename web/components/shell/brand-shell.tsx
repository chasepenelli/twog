"use client";

/**
 * BrandShell — the app frame, continuous with twog.bio. Sticky mono header (twog wordmark + section
 * nav) + the twog.bio footer line. Public sections (STATE / EVIDENCE / RUNS) always show; CONTRIBUTE
 * leads to the gated path (sign-in handled there). Operator powers live INLINE on surfaces, not here.
 */

import Link from "next/link";
import { usePathname } from "next/navigation";

import { useSession } from "@/lib/auth/use-session";

const SECTIONS = [
  { href: "/", label: "STATE" },
  { href: "/evidence", label: "EVIDENCE" },
  { href: "/runs", label: "RUNS" },
];

function isActive(pathname: string, href: string): boolean {
  return href === "/" ? pathname === "/" : pathname.startsWith(href);
}

export function BrandShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const { principal } = useSession();

  return (
    <>
      <nav
        style={{
          position: "sticky",
          top: 0,
          zIndex: 40,
          background: "rgba(255,255,255,.85)",
          backdropFilter: "blur(12px)",
          borderBottom: "1px solid var(--line)",
        }}
      >
        <div
          className="wrap"
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            minHeight: 62,
            gap: 16,
            flexWrap: "wrap",
            paddingTop: 10,
            paddingBottom: 10,
          }}
        >
          <div style={{ display: "flex", alignItems: "baseline", gap: 10 }}>
            {/* wordmark = brand anchor → the marketing homepage (twog.bio) */}
            <a href="https://twog.bio" style={{ fontWeight: 700, letterSpacing: "-0.03em", fontSize: 19 }}>twog</a>
            {/* label = "you are here" → the app home (research state) */}
            <Link href="/" className="mono" style={{ fontSize: 11.5, letterSpacing: "0.14em", color: "var(--muted)" }}>
              RESEARCH STATE
            </Link>
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: 26, flexWrap: "wrap" }}>
            {SECTIONS.map((s) => (
              <Link
                key={s.href}
                href={s.href}
                className="mono"
                style={{
                  fontSize: 13,
                  letterSpacing: "0.08em",
                  color: isActive(pathname, s.href) ? "var(--ink)" : "var(--muted)",
                  fontWeight: isActive(pathname, s.href) ? 600 : 400,
                }}
              >
                {s.label}
              </Link>
            ))}
            <Link
              href="/contribute"
              className="mono"
              style={{
                fontSize: 13,
                letterSpacing: "0.08em",
                color: pathname.startsWith("/contribute") ? "var(--ink)" : "var(--accent)",
                fontWeight: 600,
              }}
            >
              CONTRIBUTE →
            </Link>
            {principal ? (
              <span className="mono" style={{ fontSize: 12, color: "var(--muted)" }}>
                {principal.name}
                {principal.role === "operator" ? " · op" : ""}
              </span>
            ) : null}
          </div>
        </div>
      </nav>

      <main style={{ minHeight: "70vh" }}>{children}</main>

      <footer style={{ borderTop: "1px solid var(--line)", marginTop: "12vh" }}>
        <div
          className="wrap mono"
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 12,
            justifyContent: "space-between",
            padding: "28px 0",
            fontSize: 12,
            color: "var(--muted)",
          }}
        >
          <span>twog · canine hemangiosarcoma × human angiosarcoma · an autonomous research engine</span>
          <span>every result is a signed, re-checkable proof capsule</span>
        </div>
      </footer>
    </>
  );
}
