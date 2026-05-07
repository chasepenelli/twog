---
name: hunter-system-review
description: Use when reviewing or improving the TWOG research hunter loop. Launch bounded critique agents to inspect recent hunt results, evidence gaps, source routing, synthesis handoffs, and operational controls, then convert useful recommendations into concrete backlog items or follow-up research tasks.
---

# Hunter System Review

Use this skill when the user asks to critique, harden, tune, or improve the TWOG hunter loop.

The goal is not to generate loose advice. The goal is to inspect the hunter system from multiple useful angles, identify specific failure modes, and produce changes that can be implemented, tested, or routed into durable follow-up work.

## Inputs To Gather

Gather only the minimum recent context needed:

- Recent `research-hunt-queue-report` output.
- Recent `research-followup-loop`, `research-hunt-tasks`, `validation-gap-ingest`, and synthesis/evaluation run IDs when relevant.
- Representative lead IDs, brief IDs, evaluation IDs, and source query IDs.
- Any observed failure: weak evidence, duplicated citations, stalled broad tasks, missing source coverage, poor synthesis readiness, or expensive/low-value agent calls.
- Relevant code surfaces: `service.py`, `research_brief_agent.py`, `validation_planning.py`, `validation_tool_catalog.py`, `cli.py`, `dagster_assets.py`, and tests.

Do not expose secrets or paste API keys into reviewer prompts.

## Critique Cell

When the user has authorized parallel agents, launch 3-5 independent reviewers. Keep each task bounded and avoid duplicate scopes.

Recommended reviewer roles:

- Retrieval strategist: source coverage, query strategy, API routing, missed journals/databases, citation repair.
- Evidence skeptic: evidence quality, causal overreach, negative evidence, duplicate support, provenance risk.
- Agent ops engineer: queue state, failure recovery, costs, model routing, rate limits, Dagster/manual controls.
- Scientific validation planner: whether hunter outputs are ready for committee, validation planning, or more evidence.
- Operator UX reviewer: what should be visible in Command Center and which manual decisions need clearer controls.

Each reviewer should return:

```json
{
  "role": "reviewer role",
  "top_findings": [
    {
      "title": "short finding",
      "severity": "high|medium|low",
      "evidence": ["run id, lead id, file, or output signal"],
      "recommended_change": "specific change",
      "expected_impact": "what improves",
      "validation": "how to test it"
    }
  ],
  "do_not_change": ["guardrails or working behavior to preserve"],
  "open_questions": ["only questions that block implementation"]
}
```

## Aggregation Rules

After reviewers finish:

1. Deduplicate overlapping findings.
2. Separate code changes from research follow-up tasks.
3. Reject recommendations that bypass citation-first evidence, mutate durable data without explicit write paths, or promote weak hypotheses around the quality gate.
4. Rank accepted items by impact, risk, and implementation size.
5. Convert accepted work into one of:
   - code backlog item,
   - source query or research lead,
   - validation-planning requirement,
   - Command Center visibility/control change,
   - documentation/SOP update.

## Required Output

Return a compact operator report:

- Current hunter health: one paragraph.
- Accepted improvements: prioritized list with concrete next action.
- Rejected or deferred ideas: short reason.
- Immediate command/code recommendation.

If making code changes, keep them scoped and add or update tests. If routing research work, use existing repository/service/CLI paths rather than ad hoc notes.

