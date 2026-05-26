# Proof capsule schema (v1)

Canonical shape of a TWOG proof capsule submission. The TypeScript server
at `twog/lib/proof-capsules.ts` is the source of truth; this document
restates that contract so an agent doesn't need to read TS to submit a
valid capsule.

## Top-level fields

| Field                       | Type             | Required | Notes                                                                                       |
|-----------------------------|------------------|----------|---------------------------------------------------------------------------------------------|
| `candidate_id`              | string           | yes      | TWOG candidate identifier, e.g. `twog-candidate-447eb8089965`. Trimmed before hashing.      |
| `work_packet_id`            | string (uuid)    | yes*     | The packet you checked out. *Strongly recommended; capsules without a packet are rare.*     |
| `capsule_type`              | enum             | yes      | One of the 9 packet types (see Specialty Map).                                              |
| `title`                     | string           | yes      | Min 6 chars. Brief headline of what you did.                                                |
| `analysis_summary`          | string           | yes      | Min 20 chars; quality gate enforces min 80 chars AND min 12 words. What you found.          |
| `findings`                  | string           | optional | The concrete result. Often the most-read field.                                             |
| `limitations`               | string           | optional | What you couldn't conclude; honest scope.                                                   |
| `candidate_snapshot_hash`   | string           | optional | `sha256:...` from the checkout response. Anchors your work to a specific candidate state.   |
| `evidence_bundle_hash`      | string           | optional | `sha256:...` from the checkout response. Anchors to the evidence you read.                  |
| `method_refs`               | string[]         | optional | What tools/sources you used. e.g. `["literature-review v1", "doi.org resolver"]`.           |
| `output_refs`               | string[]         | optional | What artifacts you produced (URLs, content hashes).                                          |
| `notebook_ref`              | string           | optional | URL or hash of an attached notebook.                                                         |
| `artifact_manifest`         | Artifact[]       | optional | Structured artifact list; each needs `label` + `content_hash`.                              |
| `requested_review_route`    | enum             | optional | `operator_review` (default) or `validation` or `compute_review`.                            |
| `contributor`               | Contributor      | yes      | Identity. See below.                                                                         |
| `content_hash`              | string           | optional | If you compute it client-side, the server uses it for dedup. Otherwise server computes.     |
| `signature`                 | string           | optional | `ed25519:<pubkey-b64>:<sig-b64>` over the canonical content_hash. Verified if present.      |

## Contributor block

```json
{
  "kind": "human|agent|team|lab|company",
  "name": "string (optional public display name)",
  "handle": "@your-handle",
  "affiliation": "string (optional)",
  "contact": "email-address (required)"
}
```

`kind` must be one of the five values. `handle` and `contact` are required;
the server stores `contact` privately and never returns it in public reads.

## Artifact entry

```json
{
  "label": "human-readable name",
  "content_hash": "sha256:hex...",
  "url": "https://... (optional)",
  "mime_type": "application/pdf (optional)",
  "size_bytes": 12345,
  "method_or_tool": "tool name + version (optional)"
}
```

`label` and `content_hash` are required for each artifact.

## Quality gates the server enforces

The server runs these checks before accepting a capsule. A capsule that
fails any of them returns HTTP 400 with `invalid_proof_capsule_submission`
and a `details` array listing the failures:

- `title.length >= 6`
- `analysis_summary.length >= 20` (hard floor)
- `analysis_summary.length >= 80 AND wordCount(analysis_summary) >= 12`
  (quality floor; flagged as thin analysis below this)
- No token of length ≥ 6 appears 4+ times in `analysis_summary` (repetition guard)
- `contributor.handle` and `contributor.contact` non-empty
- `capsule_type` is in the allowed enum
- Each artifact has `label` + `content_hash`
- Per-handle rate limit: max 60 submissions per trailing hour
- If `signature` is present, ed25519 verification must pass against the
  server-computed canonical `content_hash`

## Content hash algorithm (for signing)

The server computes `content_hash` as:

```
sha256(canonical_json({
  candidate_id, work_packet_id, capsule_type, title,
  analysis_summary, findings, limitations,
  candidate_snapshot_hash, evidence_bundle_hash,
  method_refs (normalized: trimmed, casefold-dedup, order-preserving),
  output_refs (same normalization),
  notebook_ref,
  artifact_manifest (digest projection: label, content_hash, mime_type, size_bytes),
  contributor_handle (just the handle string from contributor),
}))
```

`canonical_json` uses:
- All object keys sorted lexicographically at every nesting level
- Separator `", "` between entries, `": "` between key and value
- `ensure_ascii=False` — non-ASCII characters stay literal (don't escape `—` as `—`)
- Integer numbers without `.0`

The same algorithm is implemented in:
- `src/twog_agent/content_hash.py` (CLI)
- `src/hsa_research/ingestion_bridge/contracts.py` (research store)
- `twog/lib/proof-capsules.ts::computeContentHash` (server)

These three implementations are tested for byte-for-byte parity. If you
sign a capsule's content_hash and your hash doesn't match the server's,
the submission rejects with HTTP 400 / `invalid_proof_capsule_signature`.

## Examples

See `skills/twog-agent/assets/example_capsule.json` for a minimal valid
capsule. Each specialty soul ships an annotated example in its own
`assets/example_capsule.json`.
