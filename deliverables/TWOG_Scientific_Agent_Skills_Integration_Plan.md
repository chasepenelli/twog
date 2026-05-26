# TWOG ↔ K-Dense Scientific Agent Skills Integration Plan

*Where TWOG sits in the agent-skill ecosystem, what we borrow, what we don't,
and how the Proof Network earns the K-Dense user base as its contributor
population.*

---

## TL;DR

K-Dense's `scientific-agent-skills` is the largest scientific Agent Skills
library on the open web: **138 skills, MIT licensed, 25k stars, identical
SKILL.md format to ours, and very active.** They solved "how does an agent
do real scientific work" — literature review, peer review, MD, docking,
RDKit chem, scanpy single-cell, etc. We solved "how does that work become
a signed, verifiable contribution with reputation attached."

These are **complementary layers, not competitors.** A contributor running
`literature-review` (K-Dense) produces the actual review; a contributor
running `twog-evidence-finder` (TWOG) wraps that output into a signed,
content-hashed proof capsule and submits it. The skills compose — they do
not overlap.

**Recommended posture: sit on top, reference, do not fork.** Update each
of our 5 souls to point at the appropriate K-Dense skill(s) for the
heavy lifting. Optionally bundle a `twog-agent install --with-kdense`
that clones their repo and symlinks selected skills into
`~/.claude/skills/`. Lift their composability pattern and per-skill
`assets/references/scripts/` layout for our own souls. Zero coupling,
immediate value, no maintenance burden on someone else's tools.

**The strategic prize: K-Dense BYOK.** Their desktop AI co-scientist
(github.com/K-Dense-AI/k-dense-byok) runs the 138 skills locally with
the user's API keys. Those users *are* the contributor base the Proof
Network needs. The medium-term play is a "submit to TWOG" bridge that
turns any BYOK research session into a Proof Network contribution.

---

## 1. What K-Dense Is

| Dimension | K-Dense `scientific-agent-skills` |
|---|---|
| Owner | K-Dense Inc. (k-dense.ai) |
| Repo | github.com/K-Dense-AI/scientific-agent-skills |
| License | MIT |
| Stars | 25,868 (and rising) |
| Last commit | Today (2026-05-25) |
| Total skills | 138 |
| Format | Anthropic Agent Skills standard (agentskills.io) — same YAML frontmatter we use |
| Domains | Bioinformatics, cheminformatics, drug discovery, proteomics, clinical research, healthcare AI, medical imaging, ML/AI, materials science, physics/astronomy, engineering, geospatial, lab automation, scientific communication, multi-omics, protein engineering, research methodology |
| Companion product | **K-Dense BYOK** — open-source desktop AI co-scientist, bring-your-own-API-keys, runs all 138 skills locally, optional Modal cloud scale |
| Per-skill bundle | `SKILL.md` + `assets/` + `references/` + `scripts/` |
| Cross-skill composition | Skills explicitly invoke other skills (e.g. `literature-review` calls `parallel-web` + `scientific-schematics` + `gget`) |

Their model is "ready-to-use Agent Skills" — not just prompts, but skills
that bundle real executable scripts, asset templates, and reference
material. The standard they aligned to (`agentskills.io`) is the open
Agent Skills standard Anthropic published; it's what our `twog-agent`
already speaks.

This is the most strategically interesting open-source release in the
scientific-agent space because:

1. **The format matches ours byte-for-byte.** Their skills drop into
   `~/.claude/skills/` and work in Claude Code today. Same YAML
   frontmatter, same Read/Write/Edit/Bash `allowed-tools` field, same
   "## When to Use This Skill" section structure.
2. **The MIT license is permissive.** We can use, modify, redistribute,
   and bundle without negotiation. Their authorship attribution is in
   the YAML metadata; preserving it costs us nothing.
3. **The composability pattern is more mature than ours.** Their
   `literature-review` skill *explicitly* calls `parallel-web`,
   `scientific-schematics`, `gget`, and `bioservices`. Our 5 souls are
   currently flat — they don't reference each other or external skills.
4. **They've productized the contributor side via K-Dense BYOK.** Their
   users run these skills *to do real research*. Ours run our souls
   *to submit to the Proof Network.* These two user populations are the
   same person at different points in a workflow.

---

## 2. The Mapping That Matters

For each TWOG work-packet specialty, the K-Dense skill(s) that do the
underlying scientific work:

| TWOG packet type | K-Dense skill(s) that do the work | TWOG soul that submits the capsule |
|---|---|---|
| `citation_repair` | `citation-management`, `literature-review`, `pyzotero` | `twog-citation-repairer` |
| `claim_critique` | `scientific-critical-thinking`, `peer-review`, `scholar-evaluation` | `twog-claim-critic` |
| `evidence_addition` | `literature-review`, `bgpt-paper-search`, `paper-lookup`, `research-lookup` | `twog-evidence-finder` |
| `methods_review` | `peer-review` (CONSORT/STROBE/PRISMA checklists) | `twog-evidence-finder` (proposed split to a `twog-methods-reviewer` soul) |
| `validation_proposal` | `hypothesis-generation`, `scientific-brainstorming` | `twog-validation-proposer` |
| `omics_note` | `scanpy`, `anndata`, `pydeseq2`, `cellxgene-census`, `scvi-tools`, `scvelo` | *(future)* `twog-omics-annotator` |
| `docking_replication` | `diffdock`, `rdkit`, `datamol`, `molfeat`, `deepchem` | *(future)* `twog-docking-replicator` |
| `md_review` | `molecular-dynamics` | *(future)* `twog-md-reviewer` |

The first five rows are *immediately actionable* — they map onto specialties
that already have open work packets in staging and onto souls we already
ship. The bottom three rows are the ones [task #54] flagged as having no
matching packets; rather than seeding fake packets, we can wait until real
contributors check out real packets and run real K-Dense tooling.

**This table dissolves the "demo personas with no matching packet" gap.**
Today five of our eight demo personas sit overdue because their specialties
(`methods_review`, `md_review`, `docking_replication`, `omics_note`,
`validation_proposal`) have no open work packets seeded. With the K-Dense
integration documented, real contributors with the relevant K-Dense skill
installed can fill these specialties organically — no synthetic seeding
required.

---

## 3. Strategic Frame: Compose, Don't Fork

The LiteFold comparison doc said it about LiteFold; it applies cleanly to
K-Dense:

> TWOG's defensibility isn't in the algorithms (Boltz-2, AF2, DiffDock,
> BoltzGen are all available); it's in the HSA-specific domain knowledge,
> the autonomous orchestration, the data layer, and the community.

K-Dense's 138 skills are in the same category as Boltz-2: well-defined,
broad, open-source, and *not* in TWOG's moat. Forking them into our tree
would inherit a maintenance burden on tools that aren't unique to us, and
would force us to defend territory we don't need to own.

**What TWOG owns and should not compromise:**

1. **The Proof Network's data layer** (Neon/Postgres + Supabase HSADAYS).
   Open, queryable, ours. Same logic the LiteFold doc applied to
   structure prediction: "the moment you depend on someone else's
   database for your research data, you've handed them strategic
   control."
2. **The signed proof capsule** with deterministic content_hash and
   ed25519 signature. This is the verifiable contribution receipt; it's
   the reason `@adversarial-rate-test` can't game the reputation system,
   and it's the load-bearing primitive that makes leaderboards
   meaningful.
3. **The autonomous orchestration** (multi-agent Claude Code, demo
   persona tick, reward event emission). Our orchestrator runs without
   a human; their `Rosalind` (and similar interactive co-scientists)
   need one. Different paradigm, different value.
4. **HSA-specific domain knowledge** when applicable. K-Dense skills are
   general; our packets are concrete (specific candidates, specific
   target claims, specific evidence bundles). The combination — general
   tool + specific question + signed receipt — is what makes a TWOG
   proof capsule different from a K-Dense literature review.
5. **The reputation tier ladder** (Observer → Scout → Citation Repairer
   → … → Proof Partner). Reputation isn't a K-Dense concept. It's ours.

**What K-Dense provides that we should explicitly leverage:**

1. **138 ready-made scientific tools** with curated documentation,
   examples, and best practices. Our contributors don't need to invent
   how to do a literature review — they install K-Dense's, run it,
   and submit the output.
2. **Cross-skill composition** as a working pattern, not a theoretical
   one. Their skills call each other through the standard skill system,
   not through a custom orchestrator. We should adopt the same pattern.
3. **`assets/references/scripts/` per skill.** Real executable scripts,
   not just prompts. Our souls are currently prompt-only; theirs ship
   `python scripts/generate_schematic.py "..."` in the SKILL.md body.
4. **A user base that already runs scientific agents.** 25k stars and
   a desktop product means tens of thousands of people are already
   doing the work K-Dense skills support. That's our contributor
   pipeline if we wire it correctly.

---

## 4. What We Lift From K-Dense

In order of effort-to-value ratio:

### 4.1 Reference K-Dense skills from our 5 soul `SKILL.md` files (LOW effort, HIGH value)

Each TWOG soul gets a new section: **"## Tools that do the underlying
work."** It lists the K-Dense skill(s) that the contributor should
install and use, with a one-line install snippet:

```bash
# Install the K-Dense literature-review skill (free, MIT, no account):
mkdir -p ~/.claude/skills
ln -sf "$HOME/path/to/scientific-agent-skills/scientific-skills/literature-review" \
        ~/.claude/skills/literature-review
```

The soul then references that skill by name in its prompt:
"For the actual literature search, invoke the `literature-review` skill,
which uses parallel-web + Semantic Scholar + arXiv + bioRxiv. When it
returns, take its citation list and wrap it as a `citation_repair`
proof capsule via `twog-agent capsule submit`."

This is one-paragraph + one-bash-block per soul. ~30 minutes of writing.

### 4.2 Adopt their cross-skill composition pattern (LOW effort, MEDIUM value)

Our souls should explicitly reference each other and external skills,
the way K-Dense does. Example: our `twog-citation-repairer` soul today
contains the full citation-repair logic inline. The K-Dense pattern
would be: "use `citation-management` to do the actual repair; use this
soul only for the wrapping + submission step."

This makes our souls *thin*, which is good. Souls that change rarely;
underlying tools that change frequently. Same separation as Linux's
"do one thing well."

### 4.3 Add `assets/references/scripts/` directories to our souls (MEDIUM effort, MEDIUM value)

Today our skill directories contain `SKILL.md` only. K-Dense skills
ship:
- `assets/` — templates, example outputs, reference data
- `references/` — papers, schemas, checklists the agent should read
  before acting
- `scripts/` — real Python or shell scripts the agent invokes

For TWOG souls, the highest-value additions would be:
- `scripts/wrap_as_capsule.py` — takes the output of a K-Dense skill
  (literature review markdown, peer-review checklist JSON, etc.) and
  emits a valid `capsule.json` ready for `twog-agent capsule submit`
- `references/capsule_schema_v1.md` — the canonical proof capsule
  shape, so an agent doesn't have to read TypeScript to know what to
  submit
- `references/rubric_dimensions.md` — the seven rubric dimensions
  (scientific_usefulness, provenance_strength, reproducibility,
  actionability, novelty, clarity, downstream_impact) so the agent
  pre-checks its own work before submitting

### 4.4 Bundle K-Dense skill discovery in `twog-agent install` (MEDIUM effort, HIGH value)

Add an `--with-kdense` flag:

```bash
curl -fsSL https://twog.bio/install.sh | bash -s -- --with-kdense
```

What it does:
1. Clones `K-Dense-AI/scientific-agent-skills` into `~/.local/share/`
   (or detects an existing checkout)
2. Symlinks a curated subset into `~/.claude/skills/`:
   `literature-review`, `peer-review`, `citation-management`,
   `scientific-critical-thinking`, `hypothesis-generation`,
   `scientific-brainstorming`, `paper-lookup`, `research-lookup`,
   plus the deep specialty skills (`scanpy`, `pydeseq2`, `diffdock`,
   `molecular-dynamics`) gated behind an optional `--with-bio`
3. Prints a confirmation: "Installed 5 TWOG souls + 8 K-Dense
   scientific skills. Claude Code will discover them on next start."

This makes the K-Dense integration a one-flag, opt-in choice. Default
install stays slim (TWOG souls only); power contributors get the full
toolkit with one extra flag.

### 4.5 Soul split: introduce `twog-methods-reviewer`, `twog-omics-annotator`, `twog-docking-replicator`, `twog-md-reviewer` (MEDIUM-HIGH effort, MEDIUM value)

Right now four of our nine packet specialties don't have a dedicated
soul; they share the generic `twog-agent` skill. Splitting them out
mirrors the K-Dense granularity and gives contributors a clearer
specialty path. Each new soul is a 200–300 line `SKILL.md` referencing
the appropriate K-Dense skill.

Hold this until Phase A lands; don't ship souls without a tested
underlying workflow.

---

## 5. What We Do NOT Lift

1. **Don't fork the K-Dense skills wholesale.** The LiteFold doc's logic
   applies: "TWOG's defensibility isn't in the algorithms." Forking
   `literature-review` into `skills/twog-literature-review/` means
   committing to upstream-tracking 138 skills, fixing their bugs, and
   re-explaining their improvements. We don't have the resources, and
   we don't want the diff anchored to our repo.
2. **Don't take a hard runtime dependency on K-Dense being installed.**
   Our souls should *prefer* K-Dense skills when present and *gracefully
   degrade* when not. The contributor without K-Dense installed should
   still be able to write a capsule by hand and submit; the soul's
   "## Tools" section becomes documentation, not a hard prerequisite.
3. **Don't adopt their attribution model.** They list `K-Dense Inc.` as
   `skill-author` in YAML. Ours should keep `TWOG` (or, more
   accurately, leave the author field for human contributors who write
   custom souls). Mixing attribution would be confusing.
4. **Don't proxy K-Dense skills through the TWOG site.** Their skills
   run on the contributor's machine; ours do too. The Proof Network
   server never executes contributor code. Keep that boundary clean —
   the server's job is intake, hash verification, review routing, and
   reputation accounting, not arbitrary execution.
5. **Don't fragment the install path.** One install command, one set
   of credentials, one Claude Desktop restart. If K-Dense integration
   becomes a separate ceremony, we've lost the install-friendliness we
   just built.

---

## 6. Phased Rollout

### Phase A (this sprint) — Reference K-Dense in soul SKILL.md files

Lowest-risk, highest-leverage starting point. Doesn't change runtime
behavior; doesn't add a dependency; doesn't require K-Dense's
cooperation.

**Deliverables:**

1. Update `skills/twog-citation-repairer/SKILL.md` with a "Tools that
   do the work" section pointing at `citation-management` + `pyzotero`.
2. Same for `twog-claim-critic` → `peer-review` +
   `scientific-critical-thinking`.
3. Same for `twog-evidence-finder` → `literature-review` +
   `bgpt-paper-search` + `paper-lookup`.
4. Same for `twog-validation-proposer` → `hypothesis-generation` +
   `scientific-brainstorming`.
5. Same for the generic `twog-agent` skill — a top-level overview of
   the K-Dense ecosystem with the full mapping table from §2.
6. Add a "K-Dense integration" section to
   `docs/AGENT_PROOF_NETWORK_GUIDE.md` linking to their repo.
7. Add a `--with-kdense` flag prototype to `install.sh` (clones +
   symlinks; no-ops on re-run).
8. Update `docs/INSTALL_AGENT_SKILL.md` to mention the optional
   `--with-kdense` flag in the troubleshooting section.

**Acceptance:**
- Each soul SKILL.md ends with a clear "next step if you have K-Dense
  installed" instruction.
- A contributor with K-Dense skills already in `~/.claude/skills/` can
  follow the soul prompt end-to-end without any TWOG-specific tools
  beyond `twog-agent capsule submit`.
- The `--with-kdense` flag installs 8 curated K-Dense skills on a fresh
  machine in under 60 seconds.

### Phase B (next sprint) — Composability + assets layout

Lift K-Dense's cross-skill composition + per-skill directory pattern.

**Deliverables:**

1. Each TWOG soul gains `assets/`, `references/`, `scripts/`
   subdirectories.
2. `scripts/wrap_as_capsule.py` ships under each soul, taking the
   K-Dense skill's output and emitting a valid `capsule.json`.
3. `references/capsule_schema_v1.md` ships in `skills/twog-agent/` so
   it can be referenced by all souls (single source of truth).
4. Souls explicitly invoke each other when relevant (e.g.
   `twog-citation-repairer` references `twog-agent` for the
   submission step).
5. Optionally split the four packet specialties without dedicated souls
   into new souls (`twog-methods-reviewer`, etc.).

**Acceptance:**
- A contributor running `twog-evidence-finder` end-to-end never has to
  hand-edit JSON; the soul's `scripts/wrap_as_capsule.py` does it.
- Soul SKILL.md files contain logic that's hard to express elsewhere
  (rubric framing, capsule field requirements, HSA-specific framing
  when applicable) and *not* logic that's better off in K-Dense.

### Phase C (medium term) — K-Dense BYOK bridge

This is the partnership move. Not a code-only change.

**Deliverables:**

1. Reach out to K-Dense Inc. (Anindyadeep Sannigrahi, Cory Kornowicz,
   Aditi Sinha, et al.) with a proposal: "Your BYOK users do the
   science; TWOG closes the loop with provenance + reputation. Want to
   ship a 'Submit to TWOG' button?"
2. Build a `k-dense-byok-bridge` module that exposes a callable from
   inside BYOK: takes the current research session's output (markdown,
   JSON, citations), wraps as a TWOG proof capsule, signs with the
   user's local twog-agent identity, submits to the Proof Network.
3. Co-author a blog post on `blog.k-dense.ai` (or wherever K-Dense
   publishes) explaining the integration to their user base.
4. The reverse linkage: TWOG's `/connect` page mentions K-Dense BYOK
   as the recommended desktop environment for serious research-grade
   contributions.

**Acceptance:**
- A K-Dense BYOK user submits a literature review, methods critique,
  or hypothesis as a TWOG proof capsule with one click.
- The TWOG leaderboard reflects K-Dense BYOK contributors with a
  recognizable kind/affiliation (kind=`agent`, affiliation=`K-Dense BYOK`
  or similar).
- TWOG's network feed shows steady contributions from real K-Dense
  users running their actual research workflows.

**Risks:**
- K-Dense may not want the partnership, or may want different terms.
  Don't block Phase A or Phase B on this.
- Their roadmap may shift; cross-team coordination has overhead.

---

## 7. What Changes in the TWOG Souls (Concrete File Edits)

For Phase A, the file-by-file delta:

### `skills/twog-agent/SKILL.md`
Add a top-level section:

```markdown
## Composing with K-Dense scientific-agent-skills

This soul is the entry point for TWOG Proof Network submissions. For
the actual scientific work, use K-Dense's open-source agent skills
(github.com/K-Dense-AI/scientific-agent-skills, MIT). Their library
covers 138 scientific workflows. Below is the mapping for each TWOG
work-packet specialty:

[full mapping table from §2 of this doc]

Install K-Dense skills with one of:
  - `twog-agent install --with-kdense` (recommended)
  - Manual: clone scientific-agent-skills and symlink selected
    skills into ~/.claude/skills/
```

### `skills/twog-citation-repairer/SKILL.md`
After "## When to Use This Skill", add:

```markdown
## Tools that do the underlying work

For the actual citation repair, use these K-Dense skills (free, MIT):
  - `citation-management` — DOI resolution, citation verification,
    formatting in APA/Nature/Vancouver styles
  - `pyzotero` — Zotero library integration if the contributor uses
    Zotero
  - `literature-review` — broader literature scan when the cited
    source needs to be re-verified against current papers

After the citation work is complete, this soul wraps the output as a
`citation_repair` proof capsule and submits via twog-agent. The K-Dense
skills produce a citation list; this soul produces a signed,
content-hashed receipt.
```

### `skills/twog-claim-critic/SKILL.md`
Same pattern, referencing `peer-review` + `scientific-critical-thinking`
+ `scholar-evaluation`.

### `skills/twog-evidence-finder/SKILL.md`
Same pattern, referencing `literature-review` + `bgpt-paper-search` +
`paper-lookup` + `research-lookup`.

### `skills/twog-validation-proposer/SKILL.md`
Same pattern, referencing `hypothesis-generation` +
`scientific-brainstorming`.

### `twog/public/install.sh`
Add `--with-kdense` flag. Pseudocode:

```bash
if [ "${WITH_KDENSE:-0}" = "1" ]; then
  step "Installing K-Dense scientific-agent-skills"
  if [ ! -d "$HOME/.local/share/scientific-agent-skills" ]; then
    git clone --depth 1 \
      https://github.com/K-Dense-AI/scientific-agent-skills \
      "$HOME/.local/share/scientific-agent-skills"
  fi
  for sk in literature-review peer-review citation-management \
             scientific-critical-thinking hypothesis-generation \
             scientific-brainstorming paper-lookup research-lookup; do
    ln -sf "$HOME/.local/share/scientific-agent-skills/scientific-skills/$sk" \
            "$HOME/.claude/skills/$sk"
  done
fi
```

### `docs/AGENT_PROOF_NETWORK_GUIDE.md`
Add a new section "## Using K-Dense scientific-agent-skills" linking
to the K-Dense repo and explaining the composability pattern.

### `docs/INSTALL_AGENT_SKILL.md`
Update the install command examples to mention `--with-kdense`. Add a
troubleshooting entry: "If your K-Dense skills aren't showing up,
confirm `ls ~/.claude/skills/` lists them and restart Claude Desktop."

---

## 8. Risks and Open Questions

### Risks

1. **K-Dense pivots or stops maintaining the repo.** Mitigation: we
   never depend on the repo at runtime. We reference it in
   documentation and optionally bundle a snapshot via `--with-kdense`.
   If K-Dense disappears tomorrow, our souls still work as standalone
   prompts.

2. **Their licensing changes from MIT.** Unlikely but possible.
   Mitigation: we pin the commit hash in `--with-kdense` so users get
   a known-good snapshot. We can fork on the MIT commit and maintain
   our own bundle if the future is hostile.

3. **A K-Dense skill silently breaks a TWOG workflow.** Mitigation:
   our soul's "Tools that do the work" section is a *recommendation*,
   not a binding. The capsule schema is ours; the proof capsule
   submission path doesn't depend on K-Dense executing correctly.

4. **Contributor confusion: "is this a K-Dense thing or a TWOG
   thing?"** Mitigation: clear attribution in each SKILL.md. "K-Dense
   does the science; TWOG records the contribution." Repeat it in the
   `/connect` page copy.

### Open Questions

1. **Should we attempt the BYOK bridge in Phase C, or skip straight to
   it as Phase A?** The doc recommends Phase A first because it
   unblocks immediate value with no external dependency. But the BYOK
   bridge is the bigger prize. If K-Dense is responsive and the
   relationship is easy to start, Phase C could move forward in
   parallel with Phase A.

2. **Should the `--with-kdense` flag be on by default?** Argument for:
   most TWOG contributors will want the scientific tooling anyway;
   not having it means a half-installed experience. Argument against:
   it adds ~50MB to the install (cloning their repo) and ties our
   install to their availability. Default off until we see the install
   funnel data.

3. **Do we split the four un-souled specialties now or wait?** §4.5
   above says wait. But four overdue demo personas every tick is a
   nag in the orchestrator output. A middle path: ship Phase A, watch
   what real contributors do, then decide whether the specialty splits
   are worth the maintenance.

4. **Attribution norm in proof capsules submitted via K-Dense skills.**
   Should the capsule's `method_refs` field carry the K-Dense skill
   name (`["literature-review v1"]`)? Argument for: provenance — a
   reviewer can see what tooling produced the finding. Argument
   against: clutter; the contributor's `contributor.name` and
   `contributor.handle` are enough. Lean toward including K-Dense
   skill names in `method_refs`; it costs nothing and adds verifiable
   provenance.

5. **Co-marketing posture.** When K-Dense (rightfully) writes about
   their skills, do we want to be name-checked? When TWOG writes
   about contributions, do we link to K-Dense's BYOK? Yes to both,
   but the right cadence is a Phase C question.

---

## 9. Success Metrics

After Phase A lands and one week passes:
- ≥ 1 proof capsule submitted whose `method_refs` includes a K-Dense
  skill name
- ≥ 3 unique contributors have run `twog-agent install --with-kdense`
- Zero ship-blocker bugs surfaced in the K-Dense integration path

After Phase B lands and four weeks pass:
- ≥ 5 proof capsules submitted via the soul `wrap_as_capsule.py`
  scripts
- ≥ 2 of the four currently un-souled specialties have at least one
  accepted capsule
- A new contributor can go from "install" to "first capsule submitted"
  in under 30 minutes

After Phase C lands (if it lands):
- ≥ 1 K-Dense BYOK user has submitted a capsule via the integration
- K-Dense and TWOG have at least one piece of co-marketing live
  (blog post, README cross-link, social mention)

---

## 10. Decision Points

The following are decisions only Chase can make:

1. **Phase A approval.** Greenlight ~1 day of editing souls + adding
   `--with-kdense` flag.
2. **Phase B approval.** Greenlight ~1 sprint of restructuring souls
   into `assets/references/scripts/` layout.
3. **Phase C approval and outreach.** Send the K-Dense team an email
   proposing the partnership. Risk-free; their response shapes the
   timeline.
4. **`--with-kdense` default.** Off (recommended) or on.
5. **Specialty soul splits.** Now or wait for usage data.

Recommendation: greenlight Phase A immediately, scope Phase B for the
sprint after, send the K-Dense outreach email in parallel with Phase A
work, default `--with-kdense` off, hold specialty splits until usage
data justifies them.

---

## 11. Appendix: Why This Doc Exists Now

The Proof Network MVP is solid: 667 tests passing, 8 personas
orchestrated, end-to-end install/connect path working, cross-language
hash drift fixed. The question that surfaced after that win was: *who
actually contributes to this?*

The first answer was "demo personas on a cron." That works for proving
the network breathes, but it's synthetic. The second answer was
"K-Dense BYOK users." That's real. They already do the science; we
already built the layer that records the science as a verifiable,
reputation-bearing contribution. The integration is the bridge.

This doc captures that bridge so the team can decide on it consciously
rather than drift into it. The LiteFold comparison doc helped clarify
what TWOG should *not* try to be (a hosted SaaS platform); this doc
clarifies what TWOG *should* be (the contribution + reputation layer
sitting on top of the best-in-class open scientific tooling).

The cherry-pick list from the LiteFold doc remains valid for the HSA
pipeline (composable MD, natural-language config, metric dashboard).
This doc is about a different surface: the Proof Network's relationship
with the open agent-skill ecosystem.

---

*Doc generated 2026-05-25. Live document — update as the K-Dense
relationship and the Proof Network's contributor base evolve.*
