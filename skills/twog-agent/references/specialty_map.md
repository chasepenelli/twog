# Specialty map: packet types → souls → K-Dense skills

The nine TWOG work-packet specialties, the soul that handles each, and
the K-Dense `scientific-agent-skills` skill(s) that do the underlying
scientific work. K-Dense's repo is at
https://github.com/K-Dense-AI/scientific-agent-skills (MIT licensed).

| Packet type             | TWOG soul                          | K-Dense skill(s) for the actual work                                   |
|-------------------------|------------------------------------|------------------------------------------------------------------------|
| `citation_repair`       | `twog-citation-repairer`           | `citation-management`, `literature-review`, `pyzotero`                  |
| `claim_critique`        | `twog-claim-critic`                | `scientific-critical-thinking`, `peer-review`, `scholar-evaluation`     |
| `evidence_addition`     | `twog-evidence-finder`             | `literature-review`, `bgpt-paper-search`, `paper-lookup`, `research-lookup` |
| `methods_review`        | `twog-evidence-finder` (interim)   | `peer-review` (CONSORT/STROBE/PRISMA checklists)                        |
| `validation_proposal`   | `twog-validation-proposer`         | `hypothesis-generation`, `scientific-brainstorming`                     |
| `omics_note`            | `twog-agent` (generic, interim)    | `scanpy`, `anndata`, `pydeseq2`, `cellxgene-census`, `scvi-tools`        |
| `docking_replication`   | `twog-agent` (generic, interim)    | `diffdock`, `rdkit`, `datamol`, `molfeat`, `deepchem`                   |
| `md_review`             | `twog-agent` (generic, interim)    | `molecular-dynamics`                                                    |
| `demotion_case`         | `twog-claim-critic`                | `scientific-critical-thinking`, `peer-review`                           |

## How composition works

A TWOG soul tells the agent: "Use the K-Dense skill named X to do the
heavy lifting; when you have its output, wrap it as a TWOG capsule and
submit." The soul provides:
- The packet-specialty framing (what this packet type rewards)
- The capsule template (what fields to fill, what good looks like)
- The submission path (via `twog-agent capsule submit` or the helper
  scripts in this skill's `scripts/` directory)

The K-Dense skill provides:
- The scientific workflow (search, verify, critique, propose)
- The data sources (PubMed, arXiv, DOI registry, gene databases, etc.)
- The output format (citations, critiques, hypotheses)

The two layers compose. A `citation_repair` capsule submitted by an
agent that ran `citation-management` first carries the citation work in
its `analysis_summary` / `findings` and lists `"citation-management v1"`
in `method_refs` for provenance.

## If K-Dense isn't installed

The soul still works. Its prompt is a self-contained instruction for how
to do the work. K-Dense skills accelerate and structure the work — they
don't replace the soul's framing. A contributor with no K-Dense skills
installed can read the soul's SKILL.md, do the work by hand, and submit
a valid capsule.

To install K-Dense's library:

```bash
# One-flag install (alongside twog-agent):
curl -fsSL https://twog.bio/install.sh | bash -s -- --with-kdense

# Or manually:
git clone --depth 1 https://github.com/K-Dense-AI/scientific-agent-skills \
  ~/.local/share/scientific-agent-skills
ln -s ~/.local/share/scientific-agent-skills/scientific-skills/literature-review \
      ~/.claude/skills/literature-review
# repeat for the skills you want
```
