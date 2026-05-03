# Building a Research Memory for Canine Hemangiosarcoma

## Why the TWOG / HSA project starts with organized evidence, not AI hype

Most people imagine biomedical AI as a model answering questions.

That is the visible part. It is not the foundation.

Before an AI system can help with a serious research problem, it needs something reliable to read from. It needs organized evidence. It needs source history. It needs labels, links, dates, review status, and context. It needs to know whether a record came from a peer-reviewed paper, a drug database, a protein database, an open-access metadata source, or a social post that simply pointed to something worth checking.

That is the layer we are building now for the TWOG / HSA research system.

I have started calling it the research memory.

The phrase is useful because it keeps the project grounded. We are not trying to make an AI system invent answers from thin air. We are building the memory layer that lets future AI tools search, compare, summarize, and reason from organized biomedical evidence.

For a disease area like canine hemangiosarcoma, that matters.

The useful information is not sitting neatly in one place. It is spread across veterinary oncology papers, human angiosarcoma research, sarcoma biology, drug databases, protein records, open-access archives, adverse event data, and sometimes links surfaced by researchers or advocates online. A person can spend hours chasing references across platforms and still miss something important.

The research memory is designed to reduce that friction.

It does not replace scientists, veterinarians, oncologists, or careful human review. It gives them a better starting point.

## What the pipeline does

At a high level, the pipeline continuously watches relevant biomedical sources, pulls in records, organizes them, checks them, and prepares them for search and review.

A paper might come in from PubMed or Europe PMC. The system stores the title, abstract, authors, DOI, publication date, source, and other identifiers. If the paper has legally available full text, the system may attempt to fetch that too. The text is then split into smaller passages so each piece can be searched and linked back to the original paper.

A compound might come in from PubChem or ChEMBL. A protein might come from UniProt or the Protein Data Bank. A safety signal might come from OpenFDA animal adverse event data. A discovery link might come from a social source, but that link is treated as a lead, not as evidence by itself.

The important part is that everything enters the system with context.

Where did it come from? What type of thing is it? What topic does it relate to? Is it about canine hemangiosarcoma, human angiosarcoma, a related sarcoma, a molecular pathway, a protein, a drug candidate, or something else? Is it ready for use, or does it need review?

That structure turns scattered information into something more useful than a pile of files.

It becomes a living research memory.

## Why ingestion quality matters

The quality of the research memory depends on the quality of ingestion.

If the system pulls in duplicate records and treats them as separate findings, it can accidentally overemphasize one paper. If it mixes canine and human evidence without clear labels, it can create confusion. If it stores full text without checking access rules, the system becomes risky to operate. If it fails silently when a source breaks, the research memory can become stale without anyone noticing.

Biomedical AI is only as useful as the material it is grounded in.

A model can sound confident while missing key papers. It can summarize a weak abstract as if it were strong evidence. It can blur the difference between a peer-reviewed article, a database entry, and a social post. It can also answer from outdated information if the underlying corpus is not maintained.

That is why we are building the pipeline as an operating system for evidence, not as a one-time scrape.

The goal is not simply to collect more data. The goal is to collect the right data, preserve where it came from, and make it usable later.

## What sources it watches

The system watches several kinds of sources because the research problem spans several kinds of knowledge.

For scientific literature, it uses sources such as PubMed, Europe PMC, PMC Open Access, OpenAlex, Crossref, and Unpaywall. These help the system find papers, abstracts, metadata, citations, open-access status, and possible full-text paths.

For molecular and drug context, it uses sources such as PubChem, ChEMBL, UniProt, and the RCSB Protein Data Bank. These are important because cancer research is not only about papers. It is also about proteins, pathways, structures, compounds, targets, and mechanisms.

For safety and real-world signal awareness, the broader system includes OpenFDA animal adverse event data.

We are also exploring social and web discovery. That does not mean treating posts as scientific proof. It means recognizing that useful links often surface in public conversation before they are easy to find elsewhere. A post might point to a paper, preprint, trial page, conference abstract, or researcher thread. The pipeline can flag that link for proper ingestion through stronger sources.

This distinction matters.

Peer-reviewed literature, curated databases, open-access metadata, and social discovery links should not be treated as equal evidence. The research memory keeps those differences visible.

## How the system keeps data organized

The pipeline separates information into different layers.

Raw source records preserve what came directly from each source. Research objects represent normalized items the system cares about, such as a paper, compound, protein, dataset, or adverse event record. Text chunks represent smaller passages from abstracts or full text. Tags and extracted context help the system understand what a record is about.

Every piece should remain connected to its source.

That is the difference between a useful research memory and a loose document dump. If a future assistant gives an answer, we want it to point back to the evidence. If an expert wants to review a claim, they should be able to trace it to the original paper or database record. If a source produced low-quality data, we need to know that before it shapes downstream analysis.

The pipeline also tracks operational health.

Which sources ran successfully? Which failed? Which records are missing search embeddings? Which full-text pulls need review? Which sources are ready for automatic schedules, and which should stay manual or triage-only?

Those questions are not glamorous, but they are essential.

If the research memory is going to grow every day, it needs maintenance.

## Where agents and LLM review fit in

The agent layer is being added carefully.

Some jobs should be deterministic. Fetch this source. Store this record. Count missing embeddings. Check whether a source returned data. These tasks should behave the same way every time.

But other jobs benefit from review.

An AI agent can look at a failed full-text pull and suggest whether the problem looks like a licensing boundary, a broken URL, a parser issue, or a source outage. Another agent can review links found from social discovery and decide whether they look worth ingesting through stronger sources. Another can compare a generated summary against the original passage and flag weak or unsupported wording.

The key design choice is that agents begin as recommend-only.

They can review, classify, and propose next actions. They do not silently change schedules, make treatment recommendations, or turn weak evidence into conclusions. Their work is logged in an agent run ledger so we can see what they reviewed, what they recommended, and whether their recommendation was useful.

That gives us a practical path: use AI where judgment helps, keep deterministic checks where repeatability matters, and preserve an audit trail for both.

## Why full text is hard

Reading a paper is not as simple as downloading a PDF.

Some papers only give us a title and abstract. Some have full text available, but only from certain open-access sources. Some are stored as clean scientific XML, which is easier for software to read. Others are PDFs, which can be messy.

PDFs often contain columns, tables, captions, references, page headers, footnotes, and formatting artifacts. Software can accidentally mix these together. A parser might grab the reference list instead of the main result. It might duplicate text. It might break a table into nonsense. It might lose the connection between a passage and the section it came from.

The system has to ask a few basic questions before storing full text:

- Are we allowed to access this article?
- Are we allowed to store and reuse the text?
- Is the text clean enough to search?
- Did we capture the actual scientific content?
- Can every passage link back to the original source?

This is why full text gets its own lane.

Speed matters, but accuracy matters more. A bad full-text process can pollute the research memory with broken passages, duplicate material, or misleading fragments. Once that bad text enters the memory, future search and AI review can become less reliable.

So the full-text pipeline is being hardened step by step. Small tests first. Health checks next. Agent review for failures. Then schedules only for sources that prove stable.

The goal is not to collect the most documents as fast as possible. The goal is to build a body of research that can be searched, reviewed, and trusted later.

## Why orchestration matters

The system is being orchestrated with Dagster so the research memory can run like an actual pipeline, not a collection of manual scripts.

That means ingestion jobs can run on schedules. Embedding jobs can run after new text arrives. Health reports can check whether sources are current. Failed jobs can be inspected. Manual jobs can be launched when a source needs testing. The system can show which parts are healthy, which are stale, and which need review.

This matters because research memory is not static.

New papers appear. APIs change. Sources fail. Open-access status changes. Better parsers get added. New agents get tested. If the system is going to support real work over time, it needs visibility and control.

Orchestration gives the project a control panel.

It also creates discipline. A new source should not simply be added because it sounds useful. It should pass a small smoke test, produce organized records, survive health checks, get indexed, and show up clearly in the control panel. That standard keeps the system from becoming messy as it grows.

## What the next phase enables

Once the research memory is stable, the next layer becomes much more powerful.

A future assistant can answer questions from the actual corpus instead of relying only on general model knowledge. It can search relevant passages, compare sources, and show where an answer came from. It can help surface papers about shared biology between canine hemangiosarcoma and human angiosarcoma. It can flag proteins, pathways, compounds, and research clusters that deserve closer human review.

It can also support better hypothesis work.

A hypothesis is only useful if it is grounded. The research memory gives the system a way to connect ideas back to papers, databases, and reviewed context. Later, GPU-heavy modeling or validation workflows can sit on top of this foundation, but the candidate selection should begin with organized evidence.

That is why this phase matters.

We are not just building a data pipeline. We are building the memory layer for a research system.

For canine hemangiosarcoma, the useful clues may be scattered across veterinary studies, human sarcoma papers, drug records, protein databases, open-access archives, and expert conversations. The job of the pipeline is to gather those clues carefully, preserve their context, and make them available for responsible search and review.

AI can help.

But first, it needs something worth remembering.

## Alternate Titles

1. Before Biomedical AI Can Think, It Needs a Research Memory
2. Building the Research Memory for Canine Hemangiosarcoma
3. Why the TWOG / HSA Project Starts With Better Data Ingestion
