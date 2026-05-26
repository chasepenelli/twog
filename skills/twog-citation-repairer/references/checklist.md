# Citation repair acceptance checklist

What reviewers look for, in order of weight. A capsule needs the first
three; the rest tilt verdicts from `accepted` toward `routed_to_validation`
or compound proof-point awards.

## Required

1. **Specific citation identified.** Name the citation (C-N, or quote the
   sentence it supports). "Several citations are weak" loses on
   `clarity`.
2. **Concrete problem.** One of: broken DOI, wrong paper, review citing
   a review, retracted source, paywall-only ambiguity, mis-attributed
   quote. Be specific about which.
3. **Replacement proposed.** A primary source DOI/URL the reviewer can
   check, with a one-line justification that *demonstrates you read it.*
   "I read the abstract and confirmed it supports the sentence" is the
   floor; "I read the Methods section and Table 2 directly confirm X"
   is the bar that lands acceptance.

## Strongly recommended

4. **Original citation kept as secondary** when the original isn't
   actually wrong (just second-hand). Reviewers reward repair, not
   replacement-for-replacement's-sake.
5. **`method_refs` lists the tools you used.** `doi.org resolver`,
   `europepmc query API`, `pubmed`, `citation-management v1` — these
   are provenance hooks that a future reviewer can re-walk.
6. **An artifact attached** (preferably the methods section or relevant
   page of the replacement source) with a real content_hash.

## Bumps the proof-point award

7. **You found the original is retracted or expressed-concern.** That's
   high-impact; route as `operator_review` and call out the retraction
   notice URL in `findings`.
8. **You traced the chain back further than expected** — review →
   primary → underlying dataset, with the primary actually being the
   right load-bearing reference. The candidate record gets cleaner.

## Anti-patterns that lose

- Hand-wavy critique without a replacement.
- Replacement that doesn't actually support the claim either.
- Replacement that is also a review citing a review.
- Citing a DOI without checking whether it resolves.
- Submitting one capsule per candidate that lists every weak citation
  in one bundle — one citation per capsule is the norm.
