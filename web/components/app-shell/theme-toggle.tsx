"use client";

/**
 * ThemeToggle — light/dark switch with no extra dependency.
 *
 * We avoid pulling in `next-themes` (not in package.json) and instead toggle the
 * `dark` class on <html> directly, persisting the choice to localStorage. The
 * matching no-flash inline script lives in app/layout.tsx so the correct theme
 * is applied before first paint. Tailwind is configured with darkMode:"class".
 *
 * POLISH owns components/app-shell/*.
 */

import * as React from "react";
import { Moon, Sun } from "lucide-react";

import { Button } from "@/components/ui/button";

type Theme = "light" | "dark";

const STORAGE_KEY = "twog-theme";

function applyTheme(theme: Theme) {
  const root = document.documentElement;
  root.classList.toggle("dark", theme === "dark");
  root.style.colorScheme = theme;
}

export function ThemeToggle() {
  const [theme, setTheme] = React.useState<Theme | null>(null);

  // Read the theme that the no-flash script already applied to <html>.
  React.useEffect(() => {
    const isDark = document.documentElement.classList.contains("dark");
    setTheme(isDark ? "dark" : "light");
  }, []);

  function toggle() {
    const next: Theme = theme === "dark" ? "light" : "dark";
    setTheme(next);
    applyTheme(next);
    try {
      localStorage.setItem(STORAGE_KEY, next);
    } catch {
      /* storage may be unavailable (private mode) — theme still applies */
    }
  }

  const isDark = theme === "dark";
  return (
    <Button
      type="button"
      variant="ghost"
      size="icon"
      onClick={toggle}
      aria-label={isDark ? "Switch to light theme" : "Switch to dark theme"}
      title={isDark ? "Switch to light theme" : "Switch to dark theme"}
    >
      {/* Render both icons; show the one for the *current* theme. Before hydration
          (theme === null) we keep it visually stable by defaulting to the sun. */}
      <Sun className={isDark ? "hidden" : "block"} aria-hidden />
      <Moon className={isDark ? "block" : "hidden"} aria-hidden />
    </Button>
  );
}
