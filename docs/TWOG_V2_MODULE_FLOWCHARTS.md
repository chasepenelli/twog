# TWOG V2 Module Flowcharts

This companion document maps the Ingestion Bridge v2 code by lane. It is based
on the current source modules under `src/hsa_research/ingestion_bridge` and the
Dagster entrypoint under `src/hsa_dagster`.

## 1. Cross-Lane Spine

The shared contract is:

```text
source query -> harvester/scraper -> raw source record -> research object
-> document chunks -> resolved entities -> claims -> curated claims
-> embeddings/retrieval -> agents -> validation plans/queues -> command center
```

```mermaid
flowchart TD
    Registry["source_registry.py<br/>ResearchSource registry"]
    Queries["query_policy.py + dagster_assets.py<br/>starter SourceQuery rows"]
    Harvesters["harvesters_v2.py<br/>API harvesters"]
    Scraper["scraper_bridge.py<br/>approved scrape artifacts"]
    Pipeline["local_ingest.py<br/>LocalIngestionPipeline"]
    Store["local_store.py / postgres_store.py<br/>ResearchRepository adapters"]
    Chunks["chunker.py<br/>DocumentChunk rows"]
    Entities["entity_resolution.py<br/>entities, aliases, mentions"]
    Claims["claim_extractor.py + claim_curator.py<br/>draft and curated claims"]
    Embeddings["embeddings.py<br/>TextEmbedding index"]
    Service["service.py<br/>HSAResearchService use cases"]
    Agents["research_brief_agent.py<br/>research and validation agents"]
    Dagster["dagster_assets.py<br/>assets, jobs, schedules"]
    MCP["mcp_server.py<br/>typed MCP tools/resources"]
    Web["command_center_web.py + static assets<br/>operator UI"]

    Registry --> Queries --> Harvesters --> Pipeline --> Store
    Scraper --> Store
    Store --> Chunks --> Entities --> Claims --> Embeddings --> Service
    Service --> Agents
    Service --> MCP
    Service --> Web
    Dagster --> Pipeline
    Dagster --> Service
    Dagster --> Store
```

## 2. Ingestion Harvesters

Main modules:

| Module | Role |
| --- | --- |
| `source_registry.py` | Defines registered sources, source class, license policy, phase, capabilities, and enabled state. |
| `query_policy.py` | Builds required comparative oncology source queries and expands scholarly queries to include canine HSA plus human angiosarcoma/vascular sarcoma analogs. |
| `source_sets.py` | Groups source keys into operational lanes such as literature corpus, full text, structured sources, and all API smoke sources. |
| `source_query_params.py` | Strips internal validation-gap metadata before passing params to external APIs. |
| `harvesters_v2.py` | Implements the v2 source contract and source-specific API normalizers. |
| `local_ingest.py` | Runs active `SourceQuery` rows through the harvester, persists raw/object rows, and replaces chunks. |

Registered v2 harvesters include `openalex`, `pubmed`, `europe_pmc`,
`crossref`, `pmc_oa`, `unpaywall`, `clinicaltrials_gov`, `avma_vctr`,
`icdc`, `geo`, `sra`, `pubchem`, `chembl`, `uniprot`, `rcsb_pdb`, and
`openfda_animal_events`.

```mermaid
flowchart TD
    Init["LocalIngestionPipeline.initialize()"]
    Sources["get_initial_sources()"]
    StarterQueries["build_source_queries()<br/>scholarly, clinical, canine data, omics,<br/>chemistry, targets/structures, safety"]
    ActiveQueries["repository.list_source_queries(source_key)"]
    SafeParams["with_source_safe_query_params()<br/>drop internal validation-gap keys"]
    GetHarvester["get_harvester(source_key)"]
    Comparative["ScholarlyHarvesterV2.prepare_query()<br/>comparative policy expansion"]
    Fetch["source-specific fetch()"]
    Normalize["normalize API payload<br/>RawSourceRecord + ResearchObject"]
    Filter["filter_relevant()<br/>matched policy concepts where required"]
    UpsertRaw["repository.upsert_raw_record()"]
    UpsertObject["repository.upsert_research_object()"]
    Sections["harvester.chunk_text_sections()"]
    Chunker["chunk_text()<br/>paragraph-aware chunks"]
    ReplaceChunks["repository.replace_document_chunks()"]
    Result["IngestionResult"]

    Init --> Sources
    Init --> StarterQueries
    StarterQueries --> ActiveQueries
    ActiveQueries --> SafeParams --> GetHarvester --> Comparative --> Fetch --> Normalize --> Filter
    Filter --> UpsertRaw --> UpsertObject --> Sections --> Chunker --> ReplaceChunks --> Result
```

Notes:

- Harvesters collect and normalize; they do not reason.
- `ResearchObject.dedupe_key` and source identifiers are created in source
  normalizers.
- When a stored object already has full text, `LocalIngestionPipeline` avoids
  replacing it with a metadata-only refresh.

## 3. Scraper And Full-Text Lanes

Main modules:

| Module | Role |
| --- | --- |
| `harvesters_v2.py` | `EuropePMCHarvesterV2` and `PMCOAHarvesterV2` fetch legal full text, parse JATS body sections, and label chunks as `title_abstract` or `full_text:*`. |
| `scraper_bridge.py` | Controlled non-API bridge. Requires source profile review, URL allowlists, approval, immutable artifacts, parse review, and explicit ingest promotion. |
| `scrape_parsers.py` | Deterministic HTML parsers and manifest discovery for generic linked articles and AVMA VCTR pages. |
| `full_text_triage.py` | Classifies full-text failures into bounded actions such as retry, reduce batch size, parser fix, or license review. |
| `full_text_ops.py` | Recommend-only ops agent that combines source health, partition reports, and recent agent runs into schedule recommendations. |
| `structured_orchestration.py` | Runs full-text source pipelines and computes persisted/current-run full-text QA. |

```mermaid
flowchart TD
    EuropePMC["EuropePMCHarvesterV2<br/>open_access and fetch_full_text"]
    PMCOA["PMCOAHarvesterV2<br/>license-aware PMC OAI-PMH"]
    JATS["JATS parsing<br/>_jats_full_text_sections()"]
    TitleAbstract["title_abstract chunk"]
    BodyChunks["full_text:* body chunks"]
    QA["full_text_source_qa()<br/>persisted and current-run checks"]
    Triage["FullTextTriageAgent<br/>no_action / retry_later / reduce_batch_size<br/>needs_parser_fix / needs_license_review"]
    Ops["FullTextOpsAgent<br/>mark clean, run smoke, keep schedule stopped,<br/>ready to enable"]
    DagsterFT["Dagster full-text jobs<br/>ingest, refresh, smoke, source/date"]

    EuropePMC --> JATS
    PMCOA --> JATS
    JATS --> TitleAbstract
    JATS --> BodyChunks
    TitleAbstract --> QA
    BodyChunks --> QA
    QA --> Triage --> Ops --> DagsterFT
```

```mermaid
flowchart TD
    Profile["SCRAPE_SOURCE_PROFILES<br/>source profile + URL patterns"]
    ReviewProfile["review_profile()<br/>robots/TOS and fetch approval"]
    Fetch["fetch()<br/>approved URLs only"]
    Artifact["ArtifactHandle<br/>scrape_snapshot"]
    Manifest["build_manifest() / fetch_manifest()<br/>candidate URL discovery"]
    Parse["parse()<br/>parse_scrape_html()"]
    Review["ScrapeReviewRecord<br/>needs_review -> accepted/rejected"]
    Ingest["ingest()<br/>requires approved_by and accepted reviews"]
    RawObj["RawSourceRecord + ResearchObject<br/>provenance=scrape_bridge"]
    Chunk["scrape_metadata chunks"]

    Profile --> ReviewProfile --> Fetch --> Artifact
    Artifact --> Manifest --> Fetch
    Artifact --> Parse --> Review --> Ingest --> RawObj --> Chunk
```

Notes:

- Full-text body chunks are kept distinct from `title_abstract` chunks.
- Source/date partitions use publication-date query params for full-text
  sources and allow clean empty partitions.
- The scraper lane is not a general crawler. It is profile-gated, review-gated,
  and promotion-gated.

## 4. Storage And Repository

Main modules:

| Module | Role |
| --- | --- |
| `contracts.py` | Pydantic contracts shared by MCP, service, Dagster, agents, and storage adapters. |
| `repository.py` | `ResearchRepository` protocol plus in-memory implementation and keyword/cosine helpers. |
| `local_store.py` | Default SQLite repository with source registry, raw records, objects, chunks, entities, claims, embeddings, agent runs, queues, artifacts, leads, and reports. |
| `postgres_store.py` | Postgres adapter preserving the repository contract for hosted runtime. |
| `storage.py` | Repository factory using `HSA_STORAGE_BACKEND`, `HSA_DATABASE_URL`, and local SQLite defaults. |
| `dagster_resources.py` | Dagster `ResearchRepositoryResource` for sqlite/postgres-backed jobs. |

```mermaid
flowchart TD
    Contracts["contracts.py<br/>typed records and requests"]
    Protocol["repository.py<br/>ResearchRepository protocol"]
    Factory["storage.py<br/>build_research_repository()"]
    Env{"HSA_STORAGE_BACKEND"}
    Memory["InMemoryResearchRepository<br/>tests/smoke"]
    SQLite["SQLiteResearchRepository<br/>local default"]
    Postgres["PostgresResearchRepository<br/>hosted"]
    Tables["durable tables<br/>sources, queries, runs, raw, objects,<br/>chunks, embeddings, entities, claims,<br/>agents, queues, artifacts, leads"]
    Services["service.py / Dagster / MCP / command center"]

    Contracts --> Protocol --> Factory --> Env
    Env -->|"memory"| Memory
    Env -->|"sqlite"| SQLite
    Env -->|"postgres"| Postgres
    SQLite --> Tables
    Postgres --> Tables
    Memory --> Services
    Tables --> Services
```

Notes:

- The repository stores typed JSON payloads plus query-friendly columns.
- `replace_document_chunks()` removes old chunks plus derived entity mentions
  and embeddings for the object being refreshed.
- Retrieval reads never expose raw embedding vectors through service/MCP tools.

## 5. Chunking, Entities, Claims, Curation, Embeddings

Main modules:

| Module | Role |
| --- | --- |
| `chunker.py` | Deterministic paragraph-aware chunking with stable content hashes. |
| `entity_resolution.py` | Local dictionary resolver plus optional PubTator annotations; persists canonical entities, aliases, and chunk mentions. |
| `claim_extractor.py` | Conservative local rule extractor over chunks and structured source metadata. |
| `claim_curator.py` | Deterministic curator that scores, dedupes, promotes, rejects, or marks claims for review. |
| `embeddings.py` | Local hash and OpenRouter embedding providers, embedding text builder, indexer, and orphan maintenance. |
| `structured_orchestration.py` | Source pipeline wrapper that chains ingestion, entity resolution, extraction, curation, and QA. |

```mermaid
flowchart TD
    Text["harvester/scraper text sections"]
    Chunker["chunk_text()<br/>max_chars, overlap, section_label"]
    ChunkRows["document_chunks"]
    Resolver["resolve_entities_for_repository()"]
    LocalVocab["local_vocabulary()<br/>compounds, targets, biomarkers,<br/>pathways, disease analogs"]
    PubTator["optional PubTator BioC JSON"]
    Entities["resolved_entities + entity_aliases + entity_mentions"]
    Extractor["LocalRuleClaimExtractor"]
    DraftClaims["draft ClaimSearchResult rows<br/>source object + chunk provenance"]
    Curator["ClaimCuratorAgent<br/>score, dedupe, promote/review/reject"]
    Curated["curated/promoted claims"]
    EmbedText["build_chunk_embedding_text()<br/>object context + chunk + canonical entities"]
    Provider["Local hash or OpenRouter provider"]
    Embeddings["text_embeddings"]
    Maintenance["maintain_embedding_index()<br/>prune orphan rows + coverage"]

    Text --> Chunker --> ChunkRows
    ChunkRows --> Resolver
    LocalVocab --> Resolver
    PubTator --> Resolver
    Resolver --> Entities
    ChunkRows --> Extractor
    Entities --> Extractor
    Extractor --> DraftClaims --> Curator --> Curated
    ChunkRows --> EmbedText
    Entities --> EmbedText
    EmbedText --> Provider --> Embeddings --> Maintenance
```

Notes:

- Entity IDs, alias IDs, mention IDs, and extractor claim IDs are deterministic.
- Claims remain low-confidence drafts until curation updates metadata and
  confidence.
- Active embedding model selection prefers `HSA_EMBEDDING_MODEL`, then
  OpenRouter large embeddings when `OPENROUTER_API_KEY` exists, then
  `local-hash-v1`.

## 6. Research Brief Agents And Evaluator

Main modules:

| Module | Role |
| --- | --- |
| `research_brief_agent.py` | Builds retrieval evidence bundles, runs perspective agents, and synthesizes final citation-first briefs. |
| `research_brief_evaluation.py` | Scores persisted briefs for citation coverage, perspective balance, contradiction handling, novelty, actionability, and transparency. |
| `agent_runner.py` | Persists agent run records before and after execution. |
| `research_brief_errors.py` | Splits hard errors from evidence limitations. |
| `service.py` | Orchestrates brief runs, persistence, evaluations, queue items, quality reports, and follow-up leads. |

```mermaid
flowchart TD
    Request["ResearchBriefRequest"]
    Evidence["ResearchBriefAgent.build_evidence()"]
    Retrieval["search chunks<br/>embedding + keyword fallback"]
    Claims["search_claims()"]
    Leads["active_research_leads_for_brief()<br/>watchlist context only"]
    Bundle["ResearchBriefEvidenceBundle"]
    Citations["ResearchBriefCitation bundle"]
    P1["evidence_scout perspective"]
    P2["translational_hypothesis perspective"]
    P3["skeptic_validation perspective"]
    Runner["AgentRunner<br/>persist running/completed/failed"]
    Reports["perspective reports"]
    Synthesis["research_synthesis_editor_agent<br/>synthesize final brief"]
    BriefRecord["repository.upsert_research_brief()"]
    Eval["evaluate_research_brief_synthesis()"]
    EvalRecord["repository.upsert_research_brief_evaluation()"]
    Quality["build_research_brief_quality_report()<br/>ready / failed / follow-up / needs evaluation"]

    Request --> Evidence
    Evidence --> Retrieval --> Citations
    Evidence --> Claims --> Bundle
    Evidence --> Leads --> Bundle
    Citations --> Bundle
    Bundle --> Runner
    Runner --> P1 --> Reports
    Runner --> P2 --> Reports
    Runner --> P3 --> Reports
    Reports --> Synthesis --> BriefRecord --> Eval --> EvalRecord --> Quality
```

Notes:

- Perspective agents may run deterministically, produce external playground
  prompts, or call OpenRouter depending on review mode.
- Findings must use supplied citation IDs; research leads can inform open
  questions but cannot support findings.
- The evaluator gates downstream validation planning through
  `passes_quality_bar` and `readiness`.

## 7. Validation Planning, Queue, Agents, Autopilot

Main modules:

| Module | Role |
| --- | --- |
| `validation_planning.py` | Builds recommend-only validation plans from ready evaluated briefs. |
| `validation_agents.py` | Reviews approved validation queue items and returns promote/hold/demote decisions. |
| `service.py` | Queues validation requests, approves, dispatches, blocks missing context, and runs conservative autopilot. |
| `evidence_gap_resolver.py` | Converts validation-agent evidence gaps into research leads and optional brief queue items. |
| `validation_gap_source_pack.py` and `validation_gap_ingest.py` | Build and ingest targeted source-query packs for validation gaps. |

```mermaid
flowchart TD
    Brief["completed ResearchBriefRecord"]
    Evaluation["ResearchBriefEvaluationRecord<br/>ready_for_hypothesis_review + passes_quality_bar"]
    Planner["plan_validation_from_research_brief()"]
    Blocked["blocked/draft plan<br/>needs_better_synthesis"]
    ReadyPlan["ValidationPlanRecord<br/>ready_for_review + ready_for_expert_review"]
    QueuePreview["queue_validation_requests_from_plan()<br/>dry_run default"]
    QueueItems["ValidationRequestQueueItem<br/>needs_approval"]
    Approval["approve_validation_request_queue_item()<br/>operator approval"]
    DispatchGate["dispatch_validation_request_queue_item()<br/>approval and assay-context gates"]
    Blockers["blocked<br/>missing target/candidate/species/disease/model/safety context"]
    Agent["run_validation_queue_item_agent()<br/>validation_agents.py"]
    Completed["queue item completed<br/>validation_agent_result in metadata"]
    Gaps["resolve_evidence_gaps()<br/>research leads / brief queue"]
    Autopilot["run_validation_autopilot()<br/>manual grace, budgets, allowlists"]

    Brief --> Evaluation --> Planner
    Planner -->|"not ready or errors"| Blocked
    Planner -->|"ready"| ReadyPlan --> QueuePreview --> QueueItems
    QueueItems --> Approval --> DispatchGate
    DispatchGate -->|"missing context"| Blockers
    DispatchGate -->|"approved + clear"| Agent --> Completed --> Gaps
    QueueItems --> Autopilot --> Approval
```

Notes:

- Creating a validation plan does not dispatch work.
- Queueing a validation request does not dispatch work.
- Dispatch requires explicit approval and execution context. `expert_review`
  is the only validation type exempt from dispatch blockers.
- Autopilot is intentionally narrow: it selects only `needs_approval` items,
  respects manual activity grace periods and budgets, skips risky execution
  types, and is stopped by default in Dagster.

## 8. Dagster Assets, Jobs, Schedules

Main modules:

| Module | Role |
| --- | --- |
| `dagster_assets.py` | Defines placeholder foundation assets, live report assets, asset checks, jobs, schedules, and `dg.Definitions`. |
| `dagster_resources.py` | Configurable repository resource for Dagster jobs. |
| `src/hsa_dagster/definitions.py` | Dagster+ entrypoint returning `ingestion_bridge_defs`. |

```mermaid
flowchart TD
    DagDefs["hsa_dagster.definitions.defs()"]
    BridgeDefs["ingestion_bridge.dagster_assets.defs"]
    Resource["ResearchRepositoryResource<br/>sqlite/postgres"]
    Foundation["foundation assets<br/>source_registry -> source_queries -> raw -> objects -> chunks -> entities -> claims"]
    SourceRefresh["structured/literature/full-text/source-followup assets"]
    Reports["source health, command center,<br/>brief/evaluation/validation reports"]
    Agents["agent ops assets<br/>briefs, therapy committee, full-text ops,<br/>validation autopilot"]
    Checks["asset checks<br/>minimum outputs, source health,<br/>embedding coverage"]
    Jobs["define_asset_job() and full_text_source_date_ops_job"]
    Schedules["ScheduleDefinition<br/>America/Denver"]

    DagDefs --> BridgeDefs
    BridgeDefs --> Resource
    Resource --> Foundation
    Resource --> SourceRefresh
    Resource --> Reports
    Resource --> Agents
    Foundation --> Checks
    SourceRefresh --> Checks
    Reports --> Checks
    BridgeDefs --> Jobs --> Schedules
```

Live scheduled jobs in code:

| Schedule | Job | Cron | Default |
| --- | --- | --- | --- |
| `literature_corpus_daily_schedule` | `literature_corpus_harvest_job` | `0 1 * * *` | running |
| `literature_full_text_source_date_daily_schedule` | `literature_full_text_source_date_job` | `30 2 * * *` | running |
| `literature_full_text_weekly_schedule` | `literature_full_text_refresh_job` | `0 2 * * 0` | stopped |
| `structured_source_pipeline_weekly_schedule` | `structured_source_pipeline_job` | `0 2 * * 1` | running |
| `all_api_smoke_weekly_schedule` | `all_api_smoke_job` | `0 3 * * 2` | running |
| `source_followup_queue_daily_schedule` | `source_followup_queue_job` | `5 3 * * *` | running |
| `pubmed_source_followup_ingest_daily_schedule` | `pubmed_source_followup_ingest_job` | `20 3 * * *` | running |
| `crossref_source_followup_ingest_daily_schedule` | `crossref_source_followup_ingest_job` | `35 3 * * *` | running |
| `pmc_oa_source_followup_ingest_daily_schedule` | `pmc_oa_source_followup_ingest_job` | `50 3 * * *` | running |
| `clinicaltrials_gov_source_followup_ingest_daily_schedule` | `clinicaltrials_gov_source_followup_ingest_job` | `5 4 * * *` | running |
| `unpaywall_source_followup_ingest_daily_schedule` | `unpaywall_source_followup_ingest_job` | `20 4 * * *` | running |
| `research_leads_daily_schedule` | `research_leads_job` | `35 4 * * *` | running |
| `embedding_index_daily_schedule` | `embedding_index_job` | `0 5 * * *` | running |
| `embedding_maintenance_daily_schedule` | `embedding_maintenance_job` | `45 5 * * *` | running |
| `source_health_daily_schedule` | `source_health_report_job` | `15 6 * * *` | running |
| `validation_autopilot_hourly_schedule` | `validation_autopilot_job` | `0 * * * *` | stopped |

## 9. MCP Tools And Resources

Main modules:

| Module | Role |
| --- | --- |
| `mcp_server.py` | FastMCP server exposing service methods as typed tools and resources. |
| `service.py` | Shared implementation used by MCP, Dagster, CLI, and web command center. |
| `contracts.py` | Request/response validation at the tool boundary. |

```mermaid
flowchart TD
    Client["MCP client<br/>Claude, inspector, future tools"]
    FastMCP["FastMCP server<br/>streamable HTTP"]
    ToolFns["*_tool helper functions<br/>UUID parsing + contract construction"]
    Service["get_service()<br/>HSAResearchService singleton"]
    Repository["ResearchRepository"]
    Resources["MCP resources<br/>claim, chunk, object, run,<br/>agent, brief, validation, artifact"]

    Client --> FastMCP --> ToolFns --> Service --> Repository
    FastMCP --> Resources --> Service
```

Tool groups:

| Group | Representative tools |
| --- | --- |
| Retrieval/read | `search_research_chunks`, `get_chunk_context`, `get_research_object`, `run_retrieval_smoke`, `search_claims` |
| Briefs/ideas | `run_research_brief`, `build_research_brief_playground_pack`, `run_therapy_committee`, brief list/get/evaluate tools |
| Validation | `plan_validation`, `queue_validation_requests`, validation queue list/get/approve/dispatch, `run_validation_autopilot` |
| Evidence gaps/leads | `resolve_evidence_gaps`, `build_validation_gap_source_pack`, `ingest_validation_gap_source_queries`, research lead tools, research followup resolver tools |
| Full text/social/followups | `triage_full_text_issue`, `run_full_text_ops`, X topic and linked-article review tools, source followup queue/ingest tools |
| Operations | `command_center`, `get_agent_run`, `list_agent_runs`, model profile tools |
| Legacy/async validation | `get_candidate`, `propose_hypothesis`, `commit_hypothesis`, `run_boltz`, `request_validation`, run/artifact reads |

Resources include `claim://{claim_id}`, `chunk://{chunk_id}`,
`research-object://{research_object_id}`, `run://{run_id}`,
`agent-run://{agent_run_id}`, `research-lead://{lead_id}`,
`research-brief://{brief_id}`,
`research-brief-evaluation://{evaluation_id}`,
`validation-plan://{plan_id}`,
`validation-request-queue://{queue_item_id}`,
`research-brief-queue://{queue_item_id}`, and
`artifact://{artifact_id}`.

## 10. Command Center Web

Main modules:

| Module | Role |
| --- | --- |
| `command_center_web.py` | Stdlib HTTP server over `HSAResearchService`; serves static assets and JSON APIs. |
| `command_center_static/index.html` | Operator layout for operations, briefs, and ideas pages. |
| `command_center_static/app.js` | Fetches API payloads, renders tables/cards, approves/dispatches validation requests, runs autopilot, and updates research lead status. |
| `command_center_static/styles.css` | Lightweight dashboard styling. |
| `service.py` | Builds command center reports, quality reports, lead updates, validation actions, and autopilot runs. |

```mermaid
flowchart TD
    Browser["Operator browser"]
    Static["index.html + app.js + styles.css"]
    Server["command_center_web.py<br/>HTTPServer"]
    Runtime["/api/runtime"]
    Command["/api/command-center"]
    Actions["/api/action-items"]
    Validation["/api/validation-requests"]
    Autopilot["/api/validation-autopilot"]
    Leads["/api/research-leads"]
    Briefs["/api/research-briefs"]
    Ideas["/api/ideas"]
    Env["environment readiness<br/>OPENROUTER_API_KEY + model env"]
    Service["HSAResearchService"]
    Repo["ResearchRepository"]

    Browser --> Static --> Server
    Server --> Runtime
    Server --> Command
    Server --> Actions
    Server --> Validation
    Server --> Autopilot
    Server --> Leads
    Server --> Briefs
    Server --> Ideas
    Runtime --> Env
    Command --> Service
    Actions --> Service
    Validation --> Service
    Autopilot --> Service
    Leads --> Service
    Briefs --> Service
    Ideas --> Service
    Service --> Repo
```

POST actions:

| Route shape | Action |
| --- | --- |
| `/api/validation-requests/{queue_item_id}/approve` | Approve one validation queue item. |
| `/api/validation-requests/{queue_item_id}/dispatch` | Dispatch one approved item, with OpenRouter readiness checked for live model profiles. |
| `/api/validation-autopilot/run` | Dry-run or apply one autopilot pass. |
| `/api/research-leads/{lead_id}/status` | Update a research lead lifecycle status. |

Notes:

- The command center is local-first and binds to `127.0.0.1:8787` by default.
- Runtime readiness intentionally exposes whether validation dispatch is
  configured without leaking secrets.
- The web UI is a thin operator surface over the same service contract used by
  MCP and Dagster.
