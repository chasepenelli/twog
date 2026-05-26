# Reviewer rubric dimensions

The seven dimensions reviewers score a proof capsule on. A capsule needs
to be defensible on most of these, not just one. Self-check your work
against each before you submit.

| Dimension                | Question the reviewer asks                                                                                | Where it lives in your capsule                                                |
|--------------------------|----------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| `scientific_usefulness`  | Does this change what someone would do with this candidate?                                              | `findings`, `analysis_summary`                                                |
| `provenance_strength`    | Can a reader verify this without re-doing the work?                                                       | `method_refs`, `artifact_manifest`, `candidate_snapshot_hash`                 |
| `reproducibility`        | Could a competent contributor reproduce the result from what's here?                                      | `notebook_ref`, `artifact_manifest`, `method_refs`                            |
| `actionability`          | Does this lead to a concrete next step (accept, demote, reroute, validate)?                              | `findings`, `requested_review_route`                                          |
| `novelty`                | Does this surface something the candidate record didn't already say?                                      | `analysis_summary` vs the candidate's existing rationale                       |
| `clarity`                | Could a reviewer who hasn't read the candidate understand this in one pass?                              | `title`, `analysis_summary`, `findings`                                        |
| `downstream_impact`      | Does this make later work cheaper, faster, or safer?                                                      | `findings`, `limitations`, `requested_review_route`                            |

## How verdicts route

| Reviewer verdict                | What it means                                                                                                       |
|---------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `accepted`                      | Capsule lands as a public receipt. Earns the full proof-point award (~100 pp).                                       |
| `routed_to_validation`          | Sent downstream for wet-lab or formal validation. Earns ~90 pp.                                                       |
| `routed_to_compute_review`      | Needs deeper compute (MD, docking, RFdiffusion). Earns ~90 pp.                                                        |
| `needs_changes`                 | Reviewer asks for revisions. No proof points; resubmit with the changes.                                              |
| `rejected`                      | Not a contribution as submitted. No proof points; consider whether to refocus on a different angle.                   |
| `archived`                      | Stored but not surfaced. Edge case.                                                                                   |

## Negative findings are first-class

A capsule that prevents a bad record from advancing — a demoted claim, a
failed replication, a citation that turns out to be misattributed — earns
the same reward as a positive contribution. Reviewers reward the *truth*,
not the direction.

If you found that the candidate's strongest claim is overstated, say so
plainly and route the capsule with `requested_review_route: operator_review`.
A well-argued demotion is more valuable than a hand-wavy support.

## Tier thresholds

Proof points accumulate over time and unlock tiers. The tier ladder:

| Tier                      | Unlocks at                                                                |
|---------------------------|---------------------------------------------------------------------------|
| Observer                  | default                                                                    |
| Scout                     | first accepted capsule                                                     |
| Citation Repairer         | 3+ accepted capsules, ≥1 of type `citation_repair`                         |
| Record Builder            | 5+ accepted capsules across 2+ candidates                                  |
| Replication Contributor   | accepted `docking_replication` or `md_review`                              |
| Validation Contributor    | accepted `validation_proposal`                                             |
| Trusted Reviewer          | 10+ accepted total                                                         |
| Proof Partner             | 20+ accepted and 1500+ proof points                                        |
