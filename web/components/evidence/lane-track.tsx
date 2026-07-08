import type { LaneStep } from "@/lib/evidence/verdict";

/* The public hero visual: a light, plain-language tracker of how far an idea has been tested. No charts,
   no gauges — just ✓ held / ● checking now / ○ not yet / ✕ ruled out, with the lanes AFTER a ruling
   greyed (the chain is broken; they're moot). `compact` is for list cards; `full` adds a sentence per
   lane for the candidate page. Server component, no interactivity. */

const ICON: Record<string, { mark: string; color: string }> = {
  held: { mark: "✓", color: "var(--green)" },
  controlled: { mark: "✓", color: "var(--green)" },
  now: { mark: "●", color: "var(--accent)" },
  "not-yet": { mark: "○", color: "var(--muted)" },
  "ruled-out": { mark: "✕", color: "var(--red)" },
};
const PHRASE: Record<string, string> = {
  held: "it held",
  controlled: "controlled",
  now: "checking now",
  "not-yet": "not yet",
  "ruled-out": "ruled out",
};

export function LaneTrack({ steps, variant = "compact" }: { steps: LaneStep[]; variant?: "compact" | "full" }) {
  if (!steps.length) return null;

  if (variant === "compact") {
    return (
      <div style={{ display: "flex", gap: 16, alignItems: "center", flexWrap: "wrap" }}>
        {steps.map((s) => {
          const ic = ICON[s.state] ?? ICON["not-yet"];
          return (
            <span key={s.lane} style={{ display: "inline-flex", alignItems: "center", gap: 6, opacity: s.broken ? 0.4 : 1 }}>
              <span style={{ color: s.broken ? "var(--muted)" : ic.color, fontSize: 13 }}>{ic.mark}</span>
              <span className="mono" style={{ fontSize: 12, color: s.broken ? "var(--muted)" : "var(--ink)" }}>{s.short}</span>
            </span>
          );
        })}
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
      {steps.map((s) => {
        const ic = ICON[s.state] ?? ICON["not-yet"];
        return (
          <div key={s.lane} style={{ display: "flex", gap: 13, alignItems: "baseline", opacity: s.broken ? 0.45 : 1 }}>
            <span style={{ color: s.broken ? "var(--muted)" : ic.color, fontSize: 15, width: 16, flexShrink: 0, textAlign: "center" }}>{ic.mark}</span>
            <div>
              <div style={{ fontSize: 15.5 }}>
                <strong>{s.short}</strong> — {PHRASE[s.state] ?? "not yet"}
              </div>
              {s.question ? (
                <div className="muted" style={{ fontSize: 13.5, marginTop: 2, maxWidth: "54ch" }}>{s.question}</div>
              ) : null}
            </div>
          </div>
        );
      })}
    </div>
  );
}
