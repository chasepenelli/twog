import type { CapsuleProvenance } from "@/lib/types/domain";

/* Provenance-forward, re-derivable surface (pep-fold ethos): show the fold → hash → sign → attest → pin
   pipeline and the verifiable anchors — recomputable content hash, Ed25519 signature + signer public key
   + verdict, lineage chain, audit checks — so a reader can confirm "this came from them, unaltered"
   instead of being asked to take it on faith. Server component, no interactivity. */

function check(ok: boolean | null | undefined): { cls: string; mark: string } {
  if (ok === true) return { cls: "b-green", mark: "✓" };
  if (ok === false) return { cls: "b-red", mark: "✕" };
  return { cls: "b-muted", mark: "—" };
}

function Row({ label, value, mono = true }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div style={{ display: "flex", gap: 12, alignItems: "baseline", flexWrap: "wrap", padding: "5px 0" }}>
      <span className="mono muted" style={{ fontSize: 11.5, fontWeight: 700, letterSpacing: "0.06em", minWidth: 132 }}>{label}</span>
      <span className={mono ? "mono" : undefined} style={{ fontSize: 12.5, wordBreak: "break-all", maxWidth: "100%" }}>{value}</span>
    </div>
  );
}

export function ProvenancePanel({ prov, producedBy }: { prov: CapsuleProvenance | null; producedBy?: string }) {
  if (!prov) {
    return (
      <p className="muted" style={{ fontSize: 13.5, maxWidth: "60ch" }}>
        Produced in-engine (custodial){producedBy ? ` by ${producedBy}` : ""} — provenance record not available.
      </p>
    );
  }

  const sig = prov.signed ? check(prov.signature_valid) : check(null);
  const chain = check(prov.lineage.chain_ok);

  return (
    <div>
      {/* the pipeline, made first-class */}
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap", alignItems: "center", marginBottom: 16 }}>
        {prov.pipeline.map((step, i) => (
          <span key={i} style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
            <span className="badge b-muted" style={{ fontSize: 11 }}>{step}</span>
            {i < prov.pipeline.length - 1 ? <span className="muted" style={{ fontSize: 12 }}>→</span> : null}
          </span>
        ))}
      </div>

      {/* the verdicts — what anyone can independently re-derive */}
      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
          <span className="badge b-green" style={{ minWidth: 150, justifyContent: "center" }}>content hash · present</span>
          <span className="muted" style={{ fontSize: 14 }}>Recompute it from the capsule&rsquo;s content to confirm it is unaltered.</span>
        </div>
        <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
          <span className={`badge ${sig.cls}`} style={{ minWidth: 150, justifyContent: "center" }}>
            {sig.mark} signature {prov.signed ? (prov.signature_valid ? "verified" : "INVALID") : "· custodial"}
          </span>
          <span className="muted" style={{ fontSize: 14 }}>
            {prov.signed
              ? (prov.signature_valid
                  ? <>Ed25519 signature checks out against <span className="mono">{prov.signer ?? "the signer"}</span>&rsquo;s public key — proof it came from them, unaltered.</>
                  : "The signature does NOT verify — this capsule cannot be trusted as-is.")
              : "Produced in-engine (custodial) — not yet countersigned by an external lab. Custodial is normal, not a defect."}
          </span>
        </div>
        <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
          <span className={`badge ${chain.cls}`} style={{ minWidth: 150, justifyContent: "center" }}>{chain.mark} lineage intact</span>
          <span className="muted" style={{ fontSize: 14 }}>
            Version {prov.lineage.index ?? 0} of {prov.lineage.versions ?? 1} — the hash-linked edit chain has no breaks.
          </span>
        </div>
        {prov.audit ? (
          <div style={{ display: "flex", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
            <span className={`badge ${prov.audit.mismatches.length ? "b-red" : "b-green"}`} style={{ minWidth: 150, justifyContent: "center" }}>
              {prov.audit.mismatches.length ? "✕" : "✓"} audit · {prov.audit.status ?? (prov.audit.mismatches.length ? "failed" : "passed")}
            </span>
            <span className="mono muted" style={{ fontSize: 12.5 }}>
              {prov.audit.checks_passed.join(" · ") || "—"}
              {prov.audit.mismatches.length ? ` · mismatches: ${prov.audit.mismatches.join(", ")}` : ""}
            </span>
          </div>
        ) : null}
      </div>

      {/* the raw anchors, so it's actually re-derivable, not just asserted */}
      <div className="tile" style={{ marginTop: 16, padding: 14, background: "var(--soft)" }}>
        <Row label="CONTENT HASH" value={prov.content_hash ?? "—"} />
        {prov.signature ? <Row label="SIGNATURE" value={prov.signature} /> : null}
        {prov.signer_public_key ? <Row label="SIGNER PUBKEY" value={prov.signer_public_key} /> : null}
        {prov.signer ? <Row label="SIGNER" value={prov.signer} /> : null}
        <Row label="HOW TO VERIFY" value={prov.hashing} mono={false} />
      </div>

      <p className="muted" style={{ fontSize: 13, marginTop: 14, maxWidth: "62ch" }}>
        Clearing provenance is necessary, not sufficient — a human operator still holds the terminal
        accept/promote decision. Nothing auto-promotes.
      </p>
    </div>
  );
}
