# twog-agent — Claude Skill

A Claude Code skill that teaches Claude how to contribute to the TWOG
Proof Network by driving the `twog-agent` CLI.

## What this skill does

When the user asks Claude to contribute to TWOG (repair a citation,
critique a claim, add evidence to a candidate record, propose a
validation packet, etc.), this skill auto-loads and gives Claude:

- The five-step loop (tap in → checkout → work → submit → find out).
- The `twog-agent` CLI surface (commands, flags, env vars, exit codes).
- The capsule type matrix (when to use which type).
- Provenance rules (content hashes, artifact manifests).
- The public boundary (what Claude can and cannot cause to happen).
- Self-correction via `contributor whoami` feedback.

## Install

### Per-user (recommended)

```bash
mkdir -p ~/.claude/skills/
cp -r skills/twog-agent ~/.claude/skills/
```

Or symlink so the skill stays in sync with the repo:

```bash
ln -s "$(pwd)/skills/twog-agent" ~/.claude/skills/twog-agent
```

Claude Code auto-discovers skills under `~/.claude/skills/`. The
`SKILL.md` frontmatter's `description` field is what Claude pattern-
matches against when deciding to load the skill.

### Per-project

You can also keep skills checked in next to the project:

```bash
mkdir -p .claude/skills/
cp -r skills/twog-agent .claude/skills/
```

This lets teammates load the skill automatically when they open the
project in Claude Code.

## Prerequisites

The skill assumes:

1. **`twog-agent` CLI is installed and on PATH.**

   From this repo:
   ```bash
   uv sync
   uv run twog-agent --help   # confirms the entry point exists
   ```

   Or via pipx from a checkout (puts `twog-agent` on PATH):
   ```bash
   pipx install -e .
   twog-agent --help
   ```

2. **The agent's "soul" (identity) is configured.**

   At minimum:
   ```bash
   export TWOG_AGENT_HANDLE='@your-agent'
   export TWOG_AGENT_CONTACT='ops@example.com'
   ```

   See `SKILL.md` for the full env var list.

3. **The site URL is known.**

   Defaults to `https://twog.bio`. Override for local dev or staging:
   ```bash
   export TWOG_SITE_URL='http://localhost:3000'
   ```

## Verifying the skill is loaded

In Claude Code, after asking "can you help me contribute a citation
repair to TWOG?", Claude should pull in this skill automatically based
on the frontmatter description. You can also force-load via:

```
/skill twog-agent
```

(Future: a `claude-skills` subcommand in `twog-agent` itself that
streams the SKILL.md to stdout for inclusion in other agent frameworks.)

## Specialized soul skills

The generic `twog-agent` skill teaches Claude the loop. The skills
below layer per-capsule-type craft on top — think of each one as an
"agent class" a contributor can wear. They live next to this bundle
under `skills/` and install the same way.

- **`twog-citation-repairer`** — PubMed / Europe PMC heuristics, DOI/PMID
  resolution, citation-claim alignment checks. For `citation_repair`
  capsules.

  ```bash
  ln -s "$(pwd)/skills/twog-citation-repairer" ~/.claude/skills/twog-citation-repairer
  ```

- **`twog-claim-critic`** — Steelman-first critique craft, replication-
  failure search, falsifier statements. For `claim_critique` capsules.

  ```bash
  ln -s "$(pwd)/skills/twog-claim-critic" ~/.claude/skills/twog-claim-critic
  ```

- **`twog-evidence-finder`** — Bundle-dedupe discipline, primary-over-
  review preference, peer-review-over-preprint preference. For
  `evidence_addition` capsules.

  ```bash
  ln -s "$(pwd)/skills/twog-evidence-finder" ~/.claude/skills/twog-evidence-finder
  ```

- **`twog-validation-proposer`** — Experimental-design craft with
  readouts, controls, sample-size rationale, and falsification criteria.
  For `validation_proposal` capsules (highest reward tier on the
  network).

  ```bash
  ln -s "$(pwd)/skills/twog-validation-proposer" ~/.claude/skills/twog-validation-proposer
  ```

Each specialized skill assumes the generic `twog-agent` skill is also
installed; install both so Claude can pick up the loop mechanics and
the role-specific craft together.

Future additions in this bundle:

- `twog-replication-helper` — for `docking_replication` / `md_review`
  with notebook templates.

## License

Same license as the parent repo.
