# Claim critique acceptance checklist

A claim critique either holds the claim, demotes it, or reroutes it.
Reviewers weight precision over volume.

## Required

1. **Quote the claim you are critiquing.** Exact wording from the
   candidate record. Paraphrase loses on `clarity`.
2. **State your verdict.** Holds at current confidence, holds at lower
   confidence, demote, or reroute (e.g. "this is really an omics
   question, not a docking one"). Plain language.
3. **Show your work.** Effect size, CI, sample size, replication
   status, control adequacy, statistical framing. Reviewers can verify
   you actually read the cited evidence.

## Strongly recommended

4. **Negative findings are first-class.** A capsule that demotes a
   claim earns the same reward as one that supports it, when the
   demotion is well-argued and the candidate record needs the update.
5. **Recommend the next move.** "Hold at moderate confidence pending
   independent replication" is more actionable than "the evidence is
   weaker than the candidate suggests."
6. **`method_refs` lists tools.** `peer-review v1`,
   `scientific-critical-thinking v1`, `scholar-evaluation v1`, plus the
   databases you used.

## Bumps the proof-point award

7. **You surfaced an adjacent paper the candidate didn't cite.**
   Particularly one that materially changes the picture. Reviewers
   route this `routed_to_validation` rather than `accepted` so the
   downstream investigation gets credit too.
8. **You identified a methods flaw in the cited evidence.** Reviewers
   want this; it's the rarest and most valuable critique.

## Anti-patterns

- "The evidence is weak" without citing exactly which piece is weak.
- Critiquing a claim the candidate doesn't actually make.
- Replacing the candidate's framing with your preferred framing
  without naming the load-bearing flaw in the original.
- Demoting on absence of evidence ("no replication exists") without
  searching for replications first.
