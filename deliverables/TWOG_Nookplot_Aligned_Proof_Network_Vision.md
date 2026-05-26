# TWOG Proof Network

## A Nookplot-Aligned Vision For Public Scientific Participation

TWOG should not simply publish candidate pages. It should publish scientific work objects that people and agents can inspect, check out, improve, and check back in.

The first version of the idea was too much like a polished explanation layer. That is useful, but it misses the thing that makes Nookplot feel alive: identity, participation, receipts, visible activity, and agent-native interaction. The TWOG version should be a public research participation layer built around proof records, not a social network.

The central product idea:

> Public candidate records become interactive research surfaces. Humans and agents can take a bounded work packet, contribute evidence or critique, receive a receipt, track review status, and earn attribution when the work improves the record.

TWOG remains the scientific source of truth. Public participants add work into a review queue; they do not mutate candidates directly.

The tone should be much more ambitious than "submit feedback." This should feel like a sexy, high-signal participation layer for scientific work: part public lab notebook, part agent coordination surface, part proof-of-work reputation system.

---

## What Went Wrong Before

The previous implementation translated the idea into content:

- homepage copy,
- public proof language,
- a PDF plan,
- a contact path,
- static candidate records.

That made TWOG easier to explain, but it did not make TWOG feel like a living coordination system.

The Nookplot-inspired version needs to translate the idea into interaction:

- who is helping,
- what they checked out,
- what they submitted,
- what record it targets,
- what status it is in,
- what changed because of it,
- and how an external agent can do the same flow without private context.

The difference is simple:

**Broadcast site:** "Here is what TWOG is doing."

**Proof Network:** "Here is the work. Pick up a piece, improve it, and get a receipt."

---

## Product North Star

TWOG Proof Network is a public proof-and-contribution layer for AI-assisted scientific records.

It lets:

- readers inspect a candidate record,
- researchers repair or challenge claims,
- agents retrieve machine-readable work packets,
- contributors submit structured improvements,
- TWOG review every incoming packet through explicit gates,
- accepted work become part of the public decision history,
- helpers receive visible attribution.

The page is not the end of the workflow. The page is the entrance to the workflow.

The emotional hook:

> Do real work on a live scientific record. Get credit when that work makes the record stronger.

---

## Nookplot-Inspired, TWOG-Owned

Nookplot points toward a world where agents have identity, reputation, signed activity, communication, MCP access, and economic coordination. TWOG should borrow the interaction pattern, not the full on-chain/economic stack on day one.

### Borrow Now

- Agent-readable public records.
- Contributor identity and attribution.
- Signed or hashable contribution packets.
- Public activity stream.
- Status receipts.
- Agent-compatible APIs.
- Future MCP bridge.
- Reputation based on accepted work.

### Defer

- Token mechanics.
- Investment framing.
- Public-triggered compute.
- Autonomous public mutation.
- On-chain reputation as a dependency.
- Bounties or payments before the review loop proves useful.

### TWOG Rule

> Public contribution is additive. Scientific state changes only through reviewed decision events.

That one rule keeps the system credible.

---

## The Core Loop

1. **Check Out**
   A human or agent opens a candidate record and downloads the public work packet: payload, evidence bundle, methods, artifact manifest, open questions, and contribution template.

2. **Work On It**
   The contributor performs a bounded task: citation repair, claim critique, source search, replication, omics note, compute review, validation proposal, or demotion case.

3. **Check In**
   The contributor submits a structured packet with identity, target claim, summary, evidence refs, method notes, artifact links, content hash, and requested action.

4. **Receive A Receipt**
   TWOG returns a stable contribution ID, status, submitted timestamp, content hash, and review expectations.

5. **Get Reviewed**
   The packet enters intake. Agents can critique and route it. Operators decide whether to accept, reject, request changes, archive, or route to validation planning.

6. **Become Part Of The Record**
   Accepted work appears on the candidate page as a decision event, evidence update, artifact reference, or review note with contributor attribution.

7. **Earn Signal**
   Accepted work produces reputation, badges, contribution score, and visible proof receipts tied to the scientific record it improved.

---

## Public Surfaces

### 1. Proof Network Home

A new public page, likely `/network`, that explains the participation layer.

It should show:

- active candidate records,
- open work packets,
- recent contribution activity,
- accepted contributions,
- how humans can help,
- how agents can plug in.

This page should feel alive. It should not read like a whitepaper.

It should feel closer to a mission board:

- open work packets,
- active candidate records,
- live contribution stream,
- top accepted repairs,
- contributor profiles,
- proof receipts,
- reward status,
- agent-compatible checkout.

The call to action is not "contact us." It is:

> Pick up a proof packet.

### 2. Candidate Workbench

Candidate pages should become workbenches, not just case studies.

Add visible actions:

- Check out payload.
- Download evidence bundle.
- View open questions.
- Submit a contribution.
- Track a contribution.
- See accepted contributions.
- See review status and decision history.

The candidate page should answer:

> What is this record? What needs help? What can I do next?

### 3. Contribution Intake

A structured public form and JSON endpoint for check-ins.

The form should support:

- evidence addition,
- citation repair,
- claim critique,
- replication result,
- compute artifact,
- omics note,
- validation proposal,
- safety or translation note,
- candidate demotion case.

Every submission becomes a pending intake record.

### 4. Activity Stream

The public network should show movement:

- contribution submitted,
- routed to review,
- accepted,
- needs changes,
- rejected,
- candidate updated,
- validation packet created,
- artifact attached.

This is the "alive" layer.

### 5. Contributor Profile

Start simple:

- display name,
- kind: human, agent, team, lab, company,
- optional GitHub,
- optional ORCID,
- optional website,
- optional agent ID,
- optional wallet or DID later,
- accepted contribution count,
- contribution history.

This is not clout chasing. It is attribution infrastructure.

### 6. Reward Console

The network should make contribution quality visible.

Show:

- proof points earned,
- accepted contributions,
- contribution types,
- strongest accepted repair,
- records improved,
- reviewer notes,
- badges earned,
- agent/human/team identity,
- current reputation tier.

This is where TWOG becomes participation-native instead of just public.

---

## Reward System

The reward system should start as non-financial reputation and proof receipts. Financial rewards, bounties, compute credits, or patronage can come later.

The first reward primitive:

> Useful work earns visible credit only after review.

Submissions do not earn reputation by existing. They earn signal when they improve a record, repair a claim, add provenance, reproduce a result, surface a real blocker, or prevent weak science from moving forward.

### Reward Units

Use several reward surfaces instead of one vague score.

```text
proof_points
accepted_contribution_count
record_improvement_count
replication_credit
citation_repair_credit
validation_signal_credit
protective_critique_credit
agent_reliability_score
```

### Reward Dimensions

Every reviewed contribution can be scored across dimensions:

```text
evidence_quality
provenance_strength
scientific_usefulness
novelty
reproducibility
actionability
difficulty
clarity
```

These scores should feed the public profile carefully. The goal is not a noisy leaderboard. The goal is a visible trail of useful work.

### Reward Tiers

Early human-readable tiers:

```text
Observer
Scout
Citation Repairer
Record Builder
Replication Contributor
Validation Contributor
Trusted Reviewer
Proof Partner
```

Agents can have parallel tiers:

```text
New Agent
Reliable Retriever
Citation Repair Agent
Replication Agent
Validation Planner
Trusted Proof Agent
```

### Reward Events

Every accepted or routed contribution should emit a reward event.

```text
reward_event_id
contribution_id
contributor_id
candidate_id
reward_type
dimension_scores
proof_points_delta
reviewer_type
rationale
created_at
```

This aligns with TWOG's existing internal reward-event thinking, but turns it outward into a community participation primitive.

### What Gets Rewarded

Reward:

- accepted citation repairs,
- high-quality negative findings,
- replicated compute artifacts,
- well-sourced claim critiques,
- validation proposals that survive review,
- evidence additions that improve confidence,
- demotion cases that prevent bad candidates from advancing,
- agent submissions that are consistently accepted or useful.

Do not reward:

- volume without acceptance,
- unsupported hype,
- low-effort comments,
- duplicate source dumps,
- unreviewed claims,
- attempts to push compute without approval.

### Future Rewards

Later, once the loop works:

- sponsored work packets,
- compute-credit sponsorship,
- expert-review bounties,
- lab-validation patronage,
- signed attestations,
- external reputation mirrors,
- Nookplot-style identity/reputation bridges.

But the v1 should be clean:

> No speculation. No investment language. No candidate ownership. Rewards are receipts for useful work.

---

## Agent Plug-In Layer

The goal is that an outside agent can participate without asking Chase for private context.

Minimum public endpoints:

```text
GET  /api/public-candidates
GET  /api/public-candidates/:candidateId
GET  /api/public-candidates/:candidateId/evidence-bundle
GET  /api/public-candidates/:candidateId/contribution-template
POST /api/public-candidates/:candidateId/contributions
GET  /api/contributions/:contributionId/status
```

Future MCP tools:

```text
twog.candidates.list
twog.candidates.get
twog.candidate.checkout
twog.evidence_bundle.get
twog.methods.list
twog.contribution.template
twog.contribution.submit
twog.contribution.status
```

The MCP surface should be safe by design:

- read public records,
- fetch public evidence bundles,
- submit contribution packets,
- check status.

It should not:

- mutate candidates,
- approve evidence,
- trigger GPU compute,
- dispatch validation jobs,
- write directly to source records.

---

## Suggested Data Model

### ContributorProfile

```text
contributor_id
kind: human | agent | team | lab | company
display_name
description
contact_email_optional
github_optional
orcid_optional
website_optional
agent_id_optional
did_optional
wallet_address_optional
verification_status
created_at
updated_at
```

### ContributionPacket

```text
contribution_id
candidate_id
contributor_id
contribution_type
target_section
target_claim_id_optional
summary
requested_action
evidence_refs
source_urls
artifact_refs
method_refs
limitations
conflicts_or_disclosures
content_hash
signature_optional
status
submitted_at
updated_at
```

### ContributionReviewEvent

```text
event_id
contribution_id
event_type
reviewer_type: operator | llm_evaluator | system | external_expert
reviewer_id
rationale
status_before
status_after
linked_agent_run_id_optional
linked_decision_event_id_optional
created_at
```

### PublicActivityEvent

```text
activity_id
entity_type
entity_id
actor_type
actor_id
activity_type
public_summary
status
created_at
```

---

## Visual And Interaction Direction

The current site can remain mission-driven, but the network layer should feel more structural and alive.

Design language:

- white/off-white background,
- black mono or IBM Plex typography,
- small structural icons,
- compact status badges,
- packet cards,
- activity rails,
- contribution receipts,
- object-like candidate records,
- subtle motion around check-out/check-in loops,
- no cute floating effects,
- no generic SaaS dashboards.

Key visual objects:

- candidate record card,
- work packet,
- contribution receipt,
- review gate,
- activity stream,
- contributor identity chip,
- accepted-work badge.

The interaction should make the system feel usable:

- "Check out this record"
- "Pick a work packet"
- "Submit a repair"
- "Track your contribution"
- "See what changed"

Make it feel desirable:

- work packets should feel like claimable missions,
- accepted contributions should feel like receipts,
- contributor profiles should feel like proof portfolios,
- rewards should feel earned, not gamified fluff,
- the activity stream should make the network feel alive,
- agent-compatible endpoints should feel like a real protocol surface.

The design should have swagger, but the science should stay disciplined.

---

## Boundaries

TWOG should be open without becoming uncontrolled.

### Evidence Boundary

Public contributors can submit evidence, critique, and artifacts. They cannot silently change evidence records.

### Review Boundary

LLMs can route, summarize, and critique. Operator approval is the write gate.

### Compute Boundary

Public contribution cannot trigger GPU compute. Compute proposals enter review.

### Candidate Boundary

Candidate status changes require decision events with rationale.

### Public Boundary

Public pages show accepted work, pending contribution status, and review history. They do not expose private notes, protected data, secrets, or copyrighted full text.

---

## Reputation Model

Do not lead with tokens or investment framing.

Let reputation emerge from real accepted contributions:

- accepted citation repairs,
- useful claim critiques,
- replicated compute artifacts,
- validation proposals routed forward,
- evidence additions that survive review,
- demotion cases that prevent bad records from advancing.

Later, reputation can support:

- trusted contributor badges,
- agent capability profiles,
- external reviewer pages,
- signed attestations,
- partner lab profiles,
- compute patronage.

But the first version should simply answer:

> Who helped, what did they submit, and what happened to it?

---

## Build Plan

### Slice 1: Public Network Page

Create `/network`.

Show:

- what the Proof Network is,
- active candidates,
- open work packets,
- recent activity,
- how humans help,
- how agents plug in.

Also show the reward premise:

- accepted work earns proof points,
- contribution receipts are public,
- trusted contributors become visible,
- agents can build reputation through useful outputs.

### Slice 2: Candidate Workbench Actions

Add to candidate pages:

- check out payload,
- evidence bundle,
- contribution template,
- submit contribution,
- contribution status,
- accepted contribution history.

### Slice 3: Contribution Packet V1

Finalize schema and public language.

Add statuses:

```text
pending
in_review
needs_changes
accepted
rejected
archived
routed_to_validation
routed_to_compute_review
```

### Slice 4: Public Intake And Receipts

Enable public contribution POST behind limits.

Return:

- contribution ID,
- status,
- timestamp,
- content hash,
- candidate ID,
- next review expectation.

### Slice 5: Review And Triage

Use the existing TWOG agent/review/orchestration layer to route contributions.

Do not auto-mutate candidate records.

### Slice 6: Activity Stream

Public activity events make the network feel alive and accountable.

### Slice 7: MCP/Agent Docs

Add "For Agents" documentation with examples.

### Slice 8: Optional Nookplot Bridge

Only after the TWOG-native loop works:

- accept DID/wallet signatures,
- mirror accepted contributions to an external coordination graph,
- experiment with Nookplot identity or reputation hooks.

### Slice 9: Reward System V1

Add proof points and reward events for accepted/routed contributions.

Start with:

- accepted contribution count,
- proof points,
- contribution type credits,
- contributor profile page,
- public reward receipt.

Avoid token or payment mechanics in this slice.

---

## The Community Story

The story is simple and powerful:

> TWOG is not asking people to watch an AI system from the outside. It is building a research engine where the public record can be inspected, challenged, repaired, and improved.

People can help without needing to become insiders. Agents can help without needing private context. Every useful contribution gets a receipt. Accepted work becomes part of the record.

That is the Nookplot-like energy, translated into TWOG's mission:

**a living, inspectable research network for humans and agents working on hard scientific problems together.**

---

## Source Notes

This vision is inspired by Nookplot's public framing around agent coordination infrastructure, including identity, reputation, signed communication, MCP access, and external agent actions. TWOG should borrow the participatory pattern while keeping scientific provenance, review gates, and candidate records as the source of truth.
