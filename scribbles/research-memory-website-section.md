# Website Section Draft: The Research Memory Pipeline

## Section Headline

The Research Memory Pipeline

## Short Subtitle

TWOG is building a living research memory for canine hemangiosarcoma and related human angiosarcoma research.

## Homepage Copy

Before AI can help answer serious biomedical questions, it needs organized evidence to work from.

The TWOG / HSA pipeline gathers research from scientific literature, biomedical databases, open-access sources, and discovery channels. Each record is stored with its source, date, identifiers, topic tags, review status, and links back to the original material.

This system does not treat every piece of information the same. A peer-reviewed paper, a drug record, a protein database entry, an open-access metadata record, and a social discovery link all have different levels of evidence and different rules for use. The research memory keeps those differences visible.

The result is a structured foundation for search, AI-assisted review, hypothesis generation, and future validation work.

## Simple Pipeline Visual

Sources -> Ingestion -> Research Memory -> Agent Review -> Search + Hypothesis Tools

## Six-Step Explainer

### 1. Watch

The system monitors relevant sources across biomedical literature, open-access archives, molecular databases, drug databases, protein resources, safety datasets, and selected discovery channels.

Examples include PubMed, Europe PMC, PMC Open Access, OpenAlex, Crossref, Unpaywall, PubChem, ChEMBL, UniProt, RCSB PDB, OpenFDA, and curated web or social links.

### 2. Ingest

New records are pulled into the pipeline with their source details, identifiers, titles, abstracts, dates, links, and available access information.

The goal is not to grab everything. The goal is to collect relevant material with enough context to make it useful later.

### 3. Organize

Records are grouped and tagged by disease, species, research area, source type, molecule, protein, drug, pathway, and review status.

When text is available, it is split into searchable passages while preserving links back to the original source.

### 4. Review

Deterministic checks and AI-assisted agents help flag missing data, broken full-text pulls, source failures, weak signals, and records that need human review.

Agents begin as recommend-only. They help surface what needs attention without silently changing the system or making medical claims.

### 5. Orchestrate

Dagster runs the pipeline, tracks job history, schedules recurring work, and shows which sources are healthy, stale, blocked, or ready for automation.

This turns ingestion from a manual process into a visible operating system for research data.

### 6. Enable

Once organized, the research memory becomes ready for retrieval-augmented search, expert review, hypothesis generation, and future validation workflows.

## Short Website Version

TWOG is not just building an AI chatbot. We are building the organized research memory an AI system needs in order to be useful.

The pipeline watches scientific and biomedical sources, ingests relevant records, preserves source context, organizes text into searchable passages, checks source health, and uses agent review to flag what needs attention. This creates a durable foundation for responsible AI-assisted research in canine hemangiosarcoma and related human angiosarcoma biology.

## Recommended Design Notes

- Use a simple horizontal pipeline visual: Sources -> Ingestion -> Research Memory -> Agent Review -> Search + Hypothesis Tools.
- Keep the section plain and evidence-focused. Avoid futuristic AI imagery.
- Show source categories rather than overwhelming visitors with every database name.
- Make "research memory" the anchor phrase.
- Use "agent review" carefully. Position agents as reviewers and triage helpers, not autonomous medical decision makers.
