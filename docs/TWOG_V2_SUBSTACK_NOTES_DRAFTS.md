# TWOG v2 Substack Notes Drafts

Short-form drafts adapted from the non-technical TWOG v2 overview.

These are intended for Substack Notes, not full articles. Each note should be able to stand alone. The voice is plain, curious, and public-facing: enough depth to make the work feel real, without asking the reader to understand the technical architecture.

Suggested use:

- Post one idea at a time.
- Keep each note focused on a single principle, problem, or design choice.
- Avoid clinical claims or treatment advice.
- Use "research infrastructure," "evidence system," "research operating system," and "comparative oncology" as recurring anchors.
- When discussing AI, keep the emphasis on guardrails, provenance, and human review.

---

## Short Note Bank

### 1. The Core Idea

TWOG v2 is being built around one simple belief:

If a research system finds something promising, it should also remember where it came from, why it mattered, who reviewed it, and what happened next.

The point is not just more information. It is better research memory.

### 2. Why This Matters

Canine hemangiosarcoma research is scattered across papers, trials, molecular databases, compound records, sequencing datasets, and clinical registries.

No small team can manually keep all of that in view forever.

TWOG v2 is our attempt to build infrastructure that can watch widely, remember carefully, and help humans decide what deserves attention.

### 3. Not A Chatbot

TWOG v2 is not a chatbot.

It uses AI, but the goal is not to ask a model for answers and hope they are right.

The goal is to collect evidence, preserve sources, run structured analysis, record outputs, and route important decisions through human review.

AI is part of the workflow. It is not the authority.

### 4. Research Memory

One of the most important things TWOG v2 can give a small research program is memory.

Not just "we saw this paper once."

More like: when it was found, what source it came from, what it was connected to, what an agent said about it, what a human decided, and whether it led to validation work.

### 5. The Operating System Frame

I think of TWOG v2 less like a database and more like a research operating system.

It does not just store papers.

It helps coordinate evidence intake, source tracking, AI-assisted synthesis, human review, validation planning, and follow-up.

That coordination layer is where a lot of the value lives.

### 6. Why Traceability Comes First

In research, a promising idea without traceability is fragile.

Where did it come from? Was it a paper, a trial registry, a database, a generated summary, or a public conversation? Was it reviewed? Did anyone decide what should happen next?

TWOG v2 is being designed so those questions stay answerable.

### 7. Discovery Is Not Proof

A key design principle for TWOG v2:

Discovery is not proof.

Finding an interesting signal is only the beginning. The system still needs source context, review, comparison, and validation planning before the idea should influence research priorities.

That separation matters, especially when AI is involved.

### 8. Why Source Lanes Matter

TWOG v2 uses "source lanes" because not all evidence means the same thing.

A journal abstract, a clinical trial record, a compound database entry, a sequencing dataset, and a social post should not be treated as equal evidence.

Each has a role. Each needs context.

That is the point of keeping lanes separate.

### 9. The Evidence Library

The dream is an evidence library that does more than collect papers.

It should know where each record came from, what type of source it is, whether full text was available, which ideas are related, and whether anyone has turned it into a validation lead.

That is a different kind of library.

### 10. Why Dagster

Dagster is the operations board behind TWOG v2.

It helps answer practical questions: what ran, what failed, what changed, and what evidence was current when a brief or validation plan was created.

For research automation, that kind of operational memory is not extra. It is part of trust.

### 11. Small Teams Need Leverage

TWOG v2 is partly about giving a small team the coordination power of a much larger research operation.

Not by replacing judgment.

By helping with the work around judgment: watching sources, preserving provenance, finding related evidence, drafting briefs, organizing queues, and keeping decisions visible.

### 12. Why Durable Ledgers

A ledger sounds dry until you need one.

When an AI agent drafts a lead, TWOG v2 should remember the run. What evidence did it use? What did it produce? Was it reviewed? Did it become a validation item?

Without that record, the output may look useful but be hard to trust later.

### 13. The Human Gate

The architecture keeps coming back to one idea:

Every risky step should have a human gate.

The system can gather, summarize, compare, and prepare. But consequential decisions still need people.

That is not a weakness. It is how the system stays usable for serious research.

### 14. Meaning-Based Search

Keyword search is helpful, but biology does not always use the same words twice.

TWOG v2 uses embeddings so related ideas can be found even when the language changes.

That means a pathway, protein target, compound class, or trial phrase can connect to evidence that might otherwise stay buried.

### 15. Embeddings Are Not Proof

Embeddings help TWOG v2 find related research.

They do not prove that a lead is true. They do not validate a treatment. They do not replace scientific review.

They are a better search memory, not a truth machine.

That distinction is essential.

### 16. Why Comparative Oncology

The comparative oncology frame matters because useful clues may cross species boundaries.

A finding in canine hemangiosarcoma may connect to human angiosarcoma biology, and vice versa.

TWOG v2 is being built to help surface those connections while still keeping source context and validation separate.

### 17. The Command Center

The command center is the visible surface of TWOG v2.

It should help operators see what is running, what needs review, what failed, what was approved, and what is waiting in the validation queue.

Research automation should not be invisible. Visibility is part of governance.

### 18. A Better Next Question

The point of TWOG v2 is not only to ask, "What papers exist?"

It is to ask better next questions:

What changed? What connects? What has been reviewed? What looks promising? What is blocked? What should be validated next?

That is where infrastructure starts to become strategy.

### 19. What AI Is Good For Here

AI can help summarize papers, compare evidence, draft research briefs, identify missing context, and structure validation ideas.

But those jobs need boundaries.

TWOG v2 tries to give AI narrow tasks, source-backed inputs, structured outputs, and review steps.

Useful AI is managed AI.

### 20. Deterministic Floors

TWOG v2 uses a concept I like: deterministic floors.

That means the stable rules sit underneath the AI. Required fields, source labels, queue states, blockers, approvals, and schemas do not depend on a model's mood.

The AI can help above the floor. The floor stays solid.

### 21. Why OpenRouter

OpenRouter gives TWOG v2 a way to use external AI models for selected tasks without making one model the center of the system.

That flexibility only works because the architecture still requires contracts, records, checks, and human gates.

Model choice should be flexible. Research governance should not be.

### 22. Validation Queue

A promising research idea needs somewhere to go.

That is why TWOG v2 has a validation queue.

Instead of leaving leads in notes or chat threads, the system can capture what is proposed, why it matters, what evidence supports it, what is missing, and what decision was made.

### 23. From Idea To Follow-Up

The useful path is:

Evidence appears.
The system preserves it.
Related evidence is found.
A brief is drafted.
A human reviews it.
A validation item is created.
Follow-up is approved, blocked, deferred, or rejected.

That path is the heart of TWOG v2.

### 24. Why Rejections Matter

Rejected ideas are still valuable.

If a lead is reviewed and rejected, TWOG v2 should remember why. That prevents the team from repeatedly rediscovering the same weak idea.

Good research memory includes the paths not taken.

### 25. Autopilot, Carefully

Autopilot in TWOG v2 does not mean "let the system make scientific decisions."

It means approved, low-risk work can be prepared or dispatched with clear records, blockers, and review boundaries.

The goal is disciplined automation, not unsupervised action.

### 26. Dry Runs Matter

A dry run is a simple idea with a lot of value:

Show what the system would do before it does it.

For research workflows, that matters. It lets humans review the planned action, catch missing context, and approve only what makes sense.

### 27. Full Text Is Powerful

Full-text papers can reveal details that abstracts miss.

But TWOG v2 has to handle full text carefully: licensing, source location, parser quality, and whether a sentence came from a title, abstract, method, result, or body section all matter.

Richer evidence needs stronger provenance.

### 28. Why Full-Text Hardening

Full-text hardening is about avoiding false confidence.

If the system stores richer paper text, it must also preserve where that text came from and what kind of text it is.

Otherwise, a generated summary can start to feel more certain than the underlying evidence deserves.

### 29. Social Signals

X/Twitter monitoring belongs in TWOG v2 only as an early signal lane.

A post might point toward a new paper, trial, dataset, or conversation.

But it is not scientific evidence by itself. Anything important should route back to primary sources and human review.

### 30. Why The X Lane Is Constrained

The X/Twitter lane is intentionally limited.

It can help discover what people are discussing, but it should not decide what is true.

That is why it is manual-review oriented and separated from higher-trust evidence lanes.

### 31. Local And Hosted

TWOG v2 is designed to work both locally and in a hosted environment.

That matters because researchers need a practical workbench, while the larger program needs durable shared storage.

Same concepts. Different settings. Less friction.

### 32. The Library Catalog Analogy

The repository abstraction is like using the same library catalog whether the books are on a local shelf or in a larger institutional archive.

The storage location can change.

The research concepts should stay consistent.

### 33. Why Postgres

Postgres matters because TWOG v2 is not only a personal notebook.

As the research program grows, it needs durable hosted storage, stronger querying, shared access patterns, and production-style operation.

Local storage is for flexibility. Postgres is for continuity.

### 34. MCP In Plain English

MCP is a standard doorway into the system.

Instead of letting every AI assistant invent its own way to request evidence or submit ideas, TWOG v2 can define contracts.

What is allowed? What gets returned? What gets recorded? What needs approval?

That is governance at the interface.

### 35. Why Contracts Matter

AI tools are much safer when their jobs are explicit.

An MCP contract can define the request, response, permissions, and records for a tool.

That means assistants can help without getting vague access to everything.

Boundaries are what make collaboration possible.

### 36. Future GPU Lane

Some future tasks may need heavier compute: large embedding refreshes, molecular analysis, structural work, or larger dataset processing.

TWOG v2 anticipates a future GPU lane for that kind of work.

The core system should coordinate heavy compute, not become the heavy compute environment.

### 37. RunPod Frame

RunPod or similar GPU services could become useful later.

But in TWOG v2, heavy compute should still produce artifacts that are tracked, reviewed, and fed back into the evidence library.

Compute output is not automatically a conclusion.

It is another source of evidence to evaluate.

### 38. The Real Asset

The real asset is not just the pile of collected research.

It is the connected memory:

sources, evidence, summaries, decisions, validation plans, results, and the history of why the team moved one direction instead of another.

That is what TWOG v2 is trying to preserve.

### 39. What Success Looks Like

Success looks like being able to ask:

What changed this week?
Which leads are ready for review?
What evidence supports this idea?
What did we reject and why?
What needs validation next?

Those are operational questions, not just search questions.

### 40. Better Continuity

Research programs lose a lot when context lives in people's heads.

TWOG v2 is built to reduce that loss.

If someone new joins, they should be able to see what was found, what was considered, what was approved, what was rejected, and what still needs work.

### 41. Why This Is Not Just Data Collection

Collecting data is the first step.

The harder part is turning evidence into reviewed, traceable, prioritized follow-up.

That is why TWOG v2 includes ledgers, queues, agents, contracts, and a command center.

The system is built around movement, not storage alone.

### 42. The Guardrail Philosophy

For TWOG v2, guardrails are not there to make the system timid.

They are there to make the system usable.

When evidence is traceable and automation is reviewable, people can move faster because they are not guessing what happened behind the scenes.

### 43. Small Pieces, Long Memory

Every paper, dataset, trial record, brief, validation plan, and decision is a small piece.

TWOG v2 is about making those pieces accumulate into long memory.

That is how a small team can build momentum instead of repeatedly starting from zero.

### 44. A Research Engine

The goal is a research engine that can search widely, remember carefully, act conservatively, and learn over time.

That phrase keeps me oriented.

Wide discovery.
Careful memory.
Conservative action.
Continuous learning.

### 45. What Humans Keep

Humans keep the judgment.

The system helps with the surrounding work: monitoring sources, preserving provenance, retrieving related evidence, drafting summaries, organizing validation ideas, and keeping decisions visible.

That division of labor feels right for serious AI-assisted research.

### 46. Why The Architecture Is Boring On Purpose

Some parts of TWOG v2 are intentionally boring: ledgers, queues, schemas, approvals, source labels, storage abstractions.

That is a feature.

In a research system, the boring parts are often what make the exciting parts trustworthy.

### 47. The Patient-Impact Frame

The long-term hope is better research direction for diseases that need more attention.

But the near-term work is infrastructure: finding evidence, preserving context, surfacing leads, and making validation easier to coordinate.

That is how careful systems can support urgent problems.

### 48. One Sentence Version

TWOG v2 is infrastructure for comparative oncology research: it watches many evidence sources, remembers provenance, uses AI carefully, keeps humans in the loop, and turns promising ideas into reviewable validation work.

---

## Tiny Notes

Use these when you want something closer to a single thought.

### Tiny 1

AI in research is much more useful when it has a job description, a source trail, and a human review step.

### Tiny 2

The most important feature in a research system may be memory: what we found, why it mattered, and what we decided.

### Tiny 3

Discovery is not validation. TWOG v2 is being built to keep that line visible.

### Tiny 4

The goal is not to replace researchers. The goal is to give a small team better research continuity.

### Tiny 5

Every promising claim should stay attached to its source.

### Tiny 6

Good automation should make the work more visible, not less visible.

### Tiny 7

Embeddings help find related ideas. They do not decide what is true.

### Tiny 8

Social signals can point toward evidence, but they are not evidence by themselves.

### Tiny 9

A validation queue turns "interesting" into "what should we review next?"

### Tiny 10

The boring parts of infrastructure are often what make AI-assisted research trustworthy.

### Tiny 11

TWOG v2 is being designed as a research operating system, not a one-off analysis tool.

### Tiny 12

If a system cannot explain what it used and what it did, it should not be trusted with important research decisions.

---

## Question-Driven Notes

### Question 1

What would it look like if a small research team had institutional memory?

Not just a folder of papers, but a system that remembers sources, evidence, agent summaries, human decisions, rejected ideas, validation plans, and follow-up results.

That is the shape TWOG v2 is moving toward.

### Question 2

What if AI in research was treated less like an oracle and more like a structured assistant?

Give it narrow tasks. Attach sources. Validate the output shape. Record the run. Require human review.

That is the direction TWOG v2 is taking.

### Question 3

What is the difference between finding a lead and trusting a lead?

For TWOG v2, the answer is provenance, comparison, review, and validation planning.

Discovery is only the first step.

### Question 4

What should happen when a promising idea appears?

It should not vanish into a chat thread.

It should become a traceable item: source-backed, reviewed, prioritized, and either advanced, blocked, deferred, or rejected.

### Question 5

How do you make AI useful without making it too powerful?

Put stable rules underneath it.
Give it specific tasks.
Record what it does.
Keep approval gates for risky actions.

That is the basic TWOG v2 philosophy.

---

## Slightly More Personal Notes

### Personal 1

One thing I keep coming back to with TWOG v2:

The system does not need to be magical.

It needs to be reliable enough that a small team can ask better questions, preserve more context, and move promising leads into review faster.

That alone would be meaningful.

### Personal 2

I am less interested in AI that sounds confident and more interested in AI that leaves a trail.

What did it read?
What did it compare?
What did it produce?
What is uncertain?
What needs review?

That is the kind of AI support TWOG v2 is being built around.

### Personal 3

The work keeps pointing back to infrastructure.

Not because infrastructure is glamorous, but because serious research needs memory, provenance, queues, decisions, and review paths.

Without those, even good ideas can get lost.

### Personal 4

TWOG v2 is partly an answer to a practical question:

How can a small, mission-driven team keep up with a research landscape that is too large to track manually?

The answer is not "let AI decide."

The answer is better systems around human judgment.

### Personal 5

I like the phrase "careful acceleration" for TWOG v2.

The aim is to move faster without getting sloppier:

more sources watched, more context preserved, more leads surfaced, and more explicit review before action.

---

## Calls To Action

Use sparingly. These are designed to be gentle, not salesy.

### CTA 1

If you work in comparative oncology, veterinary oncology, rare cancer research, or research operations, I would love to hear what you think a system like this should never lose track of.

### CTA 2

I am especially interested in the handoff between discovery and validation.

What information makes a research lead feel ready for serious review?

### CTA 3

For anyone building AI-assisted research workflows: what guardrail has mattered most in practice?

Source tracking? Human approval? Structured outputs? Audit logs? Something else?

### CTA 4

If you have seen promising research ideas get lost between literature review and follow-up, I would love to understand where that handoff broke down.

### CTA 5

The question I keep asking is:

How do we help small teams build research memory that compounds over time?

Would love thoughts from people who have lived this problem.
