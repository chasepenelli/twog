# TWOG Proof Network Delta Plan

## Add Techtree-Style Proof Capsules To The Nookplot-Style Participation Layer

This document updates the current `TWOG_Nookplot_Aligned_Proof_Network_Vision.md` plan.

The current plan is right about the public participation layer: identity, activity, contribution receipts, proof points, contributor profiles, and agent-compatible check-in. The missing piece is the **work object**. If TWOG only builds contribution forms and reward events, the network may feel exciting but scientifically thin.

The revised direction:

> Nookplot gives TWOG the public network. Techtree gives TWOG the proof capsule. TWOG keeps the scientific source of truth.

The next plan should combine them:

```text
candidate record
  -> open work packet
  -> proof capsule checkout
  -> human or agent performs work
  -> notebook + outputs + review file + artifacts
  -> TWOG review gate
  -> accepted capsule earns proof points
  -> accepted capsule becomes part of candidate decision history
```

The public product should feel sexy and alive, but the rewards should be backed by serious, inspectable work.

---

## 1. Core Change To The Product Concept

### Current Plan

The current plan centers on:

- candidate workbench,
- contribution intake,
- public activity stream,
- contributor profile,
- reward console,
- agent-compatible endpoints.

This is the right Nookplot-style surface.

### Revised Plan

Add one new primary object:

## `ProofCapsule`

A proof capsule is a portable, inspectable work object tied to a candidate, claim, open question, or validation need.

It contains:

- task manifest,
- candidate payload snapshot,
- evidence bundle reference,
- method references,
- contributor identity,
- notebook or structured analysis,
- output files,
- artifact manifest,
- review file,
- hashes,
- status,
- reward eligibility.

The network should not reward generic submissions. It should reward reviewed proof capsules.

---

## 2. Updated Positioning

### Old Framing

> Pick up a proof packet.

### Stronger Framing

> Pick up a live scientific work packet. Run the work. Submit the proof capsule. Earn credit if it holds up.

This gives TWOG a cleaner promise:

- not comments,
- not social posting,
- not vague community feedback,
- not "AI said so."

Instead:

> Work has inputs, methods, outputs, review, and receipts.

---

## 3. New Object Model

### WorkPacket

A work packet is what TWOG publishes as an open task.

```text
work_packet_id
candidate_id
packet_type
title
question
why_it_matters
target_claim_ids
required_inputs
suggested_methods
expected_outputs
acceptance_criteria
reward_hint
difficulty
status
created_at
```

Example packet types:

```text
citation_repair
claim_critique
evidence_addition
omics_note
docking_replication
md_review
validation_proposal
demotion_case
methods_review
```

### ProofCapsule

A proof capsule is what a person or agent submits after doing the work.

```text
proof_capsule_id
work_packet_id
candidate_id
contributor_id
capsule_type
task_manifest
candidate_snapshot_hash
evidence_bundle_hash
method_refs
notebook_ref_optional
analysis_summary
output_refs
artifact_manifest
review_file
limitations
content_hash
signature_optional
status
submitted_at
updated_at
```

### ProofCapsuleReview

The review file determines whether a capsule can affect the record or earn signal.

```text
review_id
proof_capsule_id
reviewer_type: operator | llm_evaluator | system | external_expert
reviewer_id
verdict: accepted | needs_changes | rejected | archived | routed
confidence
scientific_usefulness
provenance_strength
reproducibility
actionability
novelty
clarity
rationale
required_changes
linked_agent_run_id_optional
created_at
```

### RewardEvent

Reward events should be emitted only from reviewed outcomes.

```text
reward_event_id
proof_capsule_id
work_packet_id
contributor_id
candidate_id
reward_type
proof_points_delta
dimension_scores
rationale
created_at
```

---

## 4. Revised User Flow

### Human Contributor

```text
1. Opens candidate page.
2. Sees open work packets.
3. Chooses "repair citation C4" or "review omics evidence gap."
4. Downloads proof capsule starter.
5. Adds evidence, notes, links, and artifacts.
6. Submits capsule.
7. Gets receipt and status page.
8. TWOG reviews capsule.
9. Accepted capsule earns proof points and appears in the candidate decision history.
```

### Agent Contributor

```text
1. Calls twog.work_packets.list.
2. Selects packet.
3. Calls twog.work_packet.checkout.
4. Receives candidate payload, evidence bundle, methods, and output schema.
5. Runs bounded task.
6. Produces notebook or structured analysis.
7. Produces review.json and artifact manifest.
8. Calls twog.proof_capsule.submit.
9. Polls twog.proof_capsule.status.
10. If accepted, agent profile gains proof points and reliability signal.
```

---

## 5. What Changes In The Public Site

### `/network`

The network page should become a mission board.

Add:

- active work packets,
- proof capsules submitted,
- recently accepted capsules,
- top contributors,
- agent contributors,
- proof points issued,
- live review stream.

The headline should move away from "contribute to TWOG" and toward:

> Pick up a live research packet.

### Candidate Pages

Add a **Work Packets** section.

Each packet card should show:

- title,
- packet type,
- target claim or section,
- difficulty,
- expected output,
- reward hint,
- checkout button.

Add an **Accepted Proof Capsules** section.

Each accepted capsule should show:

- contributor,
- capsule type,
- what changed,
- review verdict,
- proof points,
- linked decision event,
- artifact/notebook refs.

### Contributor Pages

Contributor pages should become proof portfolios.

Show:

- identity,
- accepted capsules,
- proof points,
- contribution types,
- candidate records improved,
- strongest accepted work,
- review notes,
- agent reliability if applicable.

---

## 6. What Changes In The Backend Plan

The current Nookplot-aligned plan proposed `ContributionPacket`. Keep that, but demote it conceptually.

New hierarchy:

```text
ContributionPacket = generic intake shape
ProofCapsule = serious research contribution with inputs, methods, outputs, artifacts, and review
RewardEvent = reviewed outcome from a proof capsule
```

Add:

- `WorkPacketRecord`
- `ProofCapsuleRecord`
- `ProofCapsuleReviewRecord`
- `ProofCapsuleArtifactRecord`
- `ContributorProfileRecord`
- `PublicNetworkActivityEvent`

Use Neon/Postgres as source of truth. SQLite stays local/dev.

---

## 7. What Changes In The Agent/MCP Plan

The previous MCP plan exposed candidate and contribution tools.

Add proof-capsule tools:

```text
twog.work_packets.list
twog.work_packets.get
twog.work_packets.checkout
twog.proof_capsule.template
twog.proof_capsule.submit
twog.proof_capsule.status
twog.proof_capsule.review_summary
twog.contributors.get
```

Keep these tools safe:

- no direct candidate mutation,
- no direct reward issuance,
- no direct compute dispatch,
- no direct validation dispatch.

The agent can submit work. TWOG decides what holds up.

---

## 8. Notebook Strategy

Techtree's biggest useful signal is notebook-backed evidence.

TWOG should not require notebooks for every contribution, but it should support them as first-class artifacts.

### Notebook-Recommended Capsule Types

- omics note,
- docking replication,
- MD review,
- source analysis,
- methods review,
- validation proposal,
- benchmark or comparison.

### Lightweight Capsule Types

- citation repair,
- claim critique,
- evidence addition,
- demotion case.

These can be structured JSON plus sources, without a notebook.

### Recommended V1

Support:

- markdown notes,
- JSON review file,
- artifact manifest,
- optional notebook URL or uploaded file.

Defer:

- hosted notebook execution,
- in-browser notebooks,
- public replay infrastructure,
- paid compute execution.

---

## 9. Reward System Update

The reward system should reward proof capsules, not raw submissions.

### Old Reward Trigger

Contribution accepted.

### New Reward Trigger

Proof capsule reviewed and accepted, routed, or used to improve a candidate record.

### Reward Categories

```text
citation_repair_credit
claim_critique_credit
evidence_addition_credit
replication_credit
omics_credit
compute_review_credit
validation_planning_credit
protective_demotion_credit
methods_review_credit
```

### Scoring Inputs

```text
review_verdict
scientific_usefulness
provenance_strength
reproducibility
actionability
difficulty
clarity
downstream_impact
```

### Important Rule

Negative findings can earn high rewards.

If someone proves that a claim is weak, a citation does not support the statement, a candidate should be demoted, or a compute result is not reproducible, that is valuable scientific work.

---

## 10. Revised Build Plan

### Slice 1: Work Packet Model

Add durable work packet records.

Start with manually curated packets for the existing public candidate:

- repair/verify key citation claims,
- inspect evidence bundle,
- suggest validation readouts,
- review computational artifact readiness,
- identify demotion risks.

### Slice 2: Proof Capsule Schema

Add proof capsule records and a starter template.

Support:

- JSON submission,
- markdown analysis,
- optional notebook/artifact refs,
- content hash,
- status receipt.

### Slice 3: Candidate Workbench UI

Add Work Packets and Accepted Proof Capsules sections to candidate pages.

This is the first visible product change that will feel different.

### Slice 4: Public Network Page

Build `/network` as the sexy participation layer:

- live work packets,
- recently submitted capsules,
- accepted capsules,
- proof points issued,
- top contributors,
- agent plug-in docs.

### Slice 5: Review Gate

Route proof capsules through existing review infrastructure:

- system validation,
- optional LLM evaluator,
- operator decision,
- status updates,
- reward event emission.

### Slice 6: Reward Profiles

Add contributor pages and proof point summaries.

### Slice 7: Agent Checkout

Expose safe JSON endpoints first.

MCP comes after the HTTP path works.

### Slice 8: Notebook Artifact Support

Add notebook/artifact manifest support.

Do not build hosted execution yet.

### Slice 9: Optional External Network Bridge

After the native TWOG loop works, evaluate Nookplot identity/reputation bridge, signed packets, and public reputation mirroring.

---

## 11. Updated Acceptance Criteria

The next implementation should not be considered successful just because a page exists.

It is successful when:

- a candidate exposes at least three open work packets,
- a human can check out a packet,
- an agent can read the packet schema,
- a contributor can submit a proof capsule,
- TWOG returns a receipt,
- the capsule has a review status,
- accepted capsules can appear on candidate pages,
- reward events are emitted from reviewed capsules,
- contributor profiles show proof points and accepted work,
- no public action mutates candidate state without review.

---

## 12. Why This Is Better

Nookplot-style participation gives TWOG energy.

Techtree-style proof capsules give TWOG seriousness.

Together they create the product shape TWOG actually needs:

> A sexy public research network where every reward is backed by inspectable scientific work.

This is a much stronger direction than a generic contribution form. It lets TWOG invite the community in while protecting the scientific record. It gives agents a real protocol surface. It gives helpers attribution. It gives rewards legitimacy.

Most importantly, it turns the candidate page from a static artifact into the front door of a living research workflow.

