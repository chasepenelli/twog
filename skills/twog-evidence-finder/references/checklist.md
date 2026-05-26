# Evidence addition acceptance checklist

Reviewers reward evidence that materially changes what the candidate
record says, not literature dumps. One strong addition beats five
weak ones.

## Required

1. **Each new item is peer-reviewed or near-peer-reviewed** (e.g.
   bioRxiv from a known lab with a methods section). Random blog
   posts and X threads don't count.
2. **Show why the candidate didn't already cite it.** Either it's
   new (post-candidate-creation), in a different sub-field, or the
   original authors missed it.
3. **State the direction.** Supportive, contradictory, or boundary-case
   (supports the candidate in some conditions, not others). Be
   explicit; don't make the reviewer infer it.

## Strongly recommended

4. **Each item gets its own line in `findings`** with DOI/URL and one
   sentence on what it adds.
5. **Bound your search.** Note the database, date window, and search
   terms in `limitations`. This makes the work reproducible.
6. **`method_refs` lists what you used.** `literature-review v1`,
   `paper-lookup v1`, `pubmed search`, `biorxiv search`, etc.

## Bumps the proof-point award

7. **You found a contradiction** the candidate hasn't acknowledged.
   That's high-value; route as `operator_review` so the operator can
   decide whether to demote, qualify, or contest.
8. **You found a meta-analysis** that subsumes the candidate's primary
   citations. Reviewers love this — it tightens the rationale layer
   without changing the headline claim.

## Anti-patterns

- Dumping 10 tangentially related papers ("here's the literature
  landscape"). Pick the two that move the needle.
- Adding a paper because it's recent without explaining why it
  belongs.
- Adding a paper from a lab that wrote the candidate's primary
  citation, without checking for self-citation patterns.
- "Peer-reviewed" assertions without checking the journal (predatory
  publishers exist; verify Scimago / journalology if uncertain).
