/**
 * Small formatting helpers shared across the OPERATE surface.
 *
 * OPERATE owns app/operate/*.
 */

/** Render a USD amount with cents (campaign costs are tiny — keep the cents). */
export function usd(amount: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(amount);
}

/** Humanize a snake_case backend token, e.g. "provenance_gate_failed". */
export function humanize(token: string): string {
  return token
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase())
    .trim();
}

/** Format an ISO timestamp for compact display; falls back to the raw string. */
export function formatDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}
