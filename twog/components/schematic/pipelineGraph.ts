import type { Node, Edge } from 'reactflow';

/* ── Types ── */

export type AgentTagType = 'api' | 'claude' | 'nim' | 'local' | '';

export interface AgentNodeData {
  label: string;
  schedule: string;
  tag: string;
  tagType: AgentTagType;
  tip: string;
  isPulsing?: boolean;
  pulseIndex?: number;
}

export interface TierNodeData {
  label: string;
  index?: string;
}

export interface ReportNodeData {
  label: string;
  tip: string;
  schedule: string;
  isPulsing?: boolean;
  pulseIndex?: number;
}

/* ── Layout ──
 * Vertical stack, tier-by-tier. Within a tier, nodes sit in a row of
 * up to 6 columns. The "schedule" and "CLAUDE" naming is retained in
 * the data but no longer rendered.
 */

const COL_0 = 0;
const COL_1 = 320;
const COL_2 = 640;
const COL_3 = 960;
const COL_4 = 1280;
const COL_5 = 1600;

/* Tier band: header at Y_T, nodes start 70 below. Bands stack 240 apart. */
const Y_SD_HDR      = 0;
const Y_SD          = 70;

const Y_T1_HDR      = 240;
const Y_T1          = 310;

const Y_TA_HDR      = 480;
const Y_TA          = 550;

const Y_T2_HDR      = 720;
const Y_T2          = 790;

const Y_T3_HDR      = 960;
const Y_T3          = 1030;

const Y_T4_HDR      = 1200;
const Y_T4          = 1270;

const Y_SWARM_HDR   = 1440;
const Y_SWARM_R1    = 1510;
const Y_SWARM_R2    = 1660;
const Y_CONSENSUS   = 1830;

const Y_T5_HDR      = 2020;
const Y_T5          = 2090;

const Y_T6_HDR      = 2260;
const Y_T6          = 2330;

const Y_T7_HDR      = 2500;
const Y_T7          = 2570;

const Y_REP_HDR     = 2740;
const Y_REP         = 2810;

const Y_COMM_HDR    = 2980;
const Y_COMM        = 3050;

/* ── Nodes ── */

export const NODES: Node[] = [
  /* ── Source Discovery (top) ── */
  {
    id: 'sd-tier',
    type: 'tier',
    position: { x: COL_0, y: Y_SD_HDR },
    data: {
      label: 'Source Discovery',
      index: 'SD',
    } satisfies TierNodeData,
  },
  {
    id: 'sd-agent',
    type: 'agent',
    position: { x: COL_0, y: Y_SD },
    data: {
      label: 'Source Discovery',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'AI discovers new data sources: veterinary journals, supplement databases, clinical registries, herbal medicine DBs. Evaluates relevance, API availability, scraping feasibility.',
    } satisfies AgentNodeData,
  },

  /* ── Tier 1 · Sources ── */
  {
    id: 't1-tier',
    type: 'tier',
    position: { x: COL_0, y: Y_T1_HDR },
    data: {
      label: 'Tier 1 · Sources',
      index: 'T1',
    } satisfies TierNodeData,
  },
  {
    id: 't1-pubmed',
    type: 'agent',
    position: { x: COL_0, y: Y_T1 },
    data: {
      label: 'PubMed',
      schedule: '',
      tag: 'API',
      tagType: 'api',
      tip: 'Searches PubMed with 26 rotating HSA-specific queries including immunotherapy + checkpoint terms. Deduplicates by PMID. Writes to: papers',
    } satisfies AgentNodeData,
  },
  {
    id: 't1-s2',
    type: 'agent',
    position: { x: COL_1, y: Y_T1 },
    data: {
      label: 'Semantic Scholar',
      schedule: '',
      tag: 'API',
      tagType: 'api',
      tip: 'Fetches 100 results/query from Semantic Scholar. Captures preprints and citation networks. Writes to: papers',
    } satisfies AgentNodeData,
  },
  {
    id: 't1-biorxiv',
    type: 'agent',
    position: { x: COL_2, y: Y_T1 },
    data: {
      label: 'BioRxiv',
      schedule: '',
      tag: 'API',
      tagType: 'api',
      tip: 'Scans BioRxiv for HSA preprints within a 90-day lookback window. Writes to: papers',
    } satisfies AgentNodeData,
  },
  {
    id: 't1-europepmc',
    type: 'agent',
    position: { x: COL_3, y: Y_T1 },
    data: {
      label: 'Europe PMC',
      schedule: '',
      tag: 'API',
      tagType: 'api',
      tip: 'Searches Europe PMC for EU + veterinary literature. Full-text search. Writes to: papers',
    } satisfies AgentNodeData,
  },

  /* ── Track A · Nutrition (its own tier band) ── */
  {
    id: 'ta-tier',
    type: 'tier',
    position: { x: COL_0, y: Y_TA_HDR },
    data: {
      label: 'Track A · Nutrition',
      index: 'TA',
    } satisfies TierNodeData,
  },
  {
    id: 'ta-literature',
    type: 'agent',
    position: { x: COL_0, y: Y_TA },
    data: {
      label: 'Nutrition Literature',
      schedule: '',
      tag: 'API',
      tagType: 'api',
      tip: 'Searches PubMed with 25 nutrition-focused queries: supplements, holistic medicine, diet, functional foods for HSA. Writes to: papers (source=nutrition_pubmed)',
    } satisfies AgentNodeData,
  },
  {
    id: 'ta-analysis',
    type: 'agent',
    position: { x: COL_1, y: Y_TA },
    data: {
      label: 'Nutrition Analysis',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'AI extracts actionable treatment data from nutrition papers: compound type, canine safety, dosing, evidence level, mechanisms. Writes to: actionable_treatments',
    } satisfies AgentNodeData,
  },
  {
    id: 'ta-scoring',
    type: 'agent',
    position: { x: COL_2, y: Y_TA },
    data: {
      label: 'Nutrition Scoring',
      schedule: '',
      tag: 'LOCAL',
      tagType: 'local',
      tip: 'Scores treatments by evidence strength (40%), canine safety (35%), and HSA pathway relevance (25%). Generates Top 10 actionable treatments list.',
    } satisfies AgentNodeData,
  },
  {
    id: 'ta-top10',
    type: 'report',
    position: { x: COL_3, y: Y_TA },
    data: {
      label: 'Top 10 Actionable',
      schedule: '',
      tip: 'Ranked list of actionable supplements, herbs, foods, and dietary protocols for canine HSA — prioritized by evidence + safety + pathway relevance.',
    } satisfies ReportNodeData,
  },

  /* ── Tier 2 · Corpus + RAG ── */
  {
    id: 't2-tier',
    type: 'tier',
    position: { x: COL_0, y: Y_T2_HDR },
    data: {
      label: 'Corpus · RAG',
      index: 'T2',
    } satisfies TierNodeData,
  },
  {
    id: 't2-corpus',
    type: 'agent',
    position: { x: COL_0, y: Y_T2 },
    data: {
      label: 'Corpus Index',
      schedule: '',
      tag: 'LOCAL',
      tagType: 'local',
      tip: 'Indexed corpus of all papers with topic maps (topic_map_v2.json) and field guide synthesis (field_guide_v2.json).',
    } satisfies AgentNodeData,
  },
  {
    id: 't2-rag',
    type: 'agent',
    position: { x: COL_1, y: Y_T2 },
    data: {
      label: 'RAG Retrieval',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'RAG lookup over the corpus. Primary consumer: Hypothesis agent. Secondary: Swarm personas for prior-art checks.',
    } satisfies AgentNodeData,
  },

  /* ── Tier 3 · Analyze ── */
  {
    id: 't3-tier',
    type: 'tier',
    position: { x: COL_0, y: Y_T3_HDR },
    data: {
      label: 'Tier 3 · Analyze',
      index: 'T3',
    } satisfies TierNodeData,
  },
  {
    id: 't3-analysis',
    type: 'agent',
    position: { x: COL_0, y: Y_T3 },
    data: {
      label: 'Analysis',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'AI extracts compounds, targets, mechanisms, and evidence type from unanalyzed papers. Batch size: 10. Scores relevance 1-10.',
    } satisfies AgentNodeData,
  },
  {
    id: 't3-pubchem',
    type: 'agent',
    position: { x: COL_1, y: Y_T3 },
    data: {
      label: 'PubChem',
      schedule: '',
      tag: 'API',
      tagType: 'api',
      tip: 'Resolves CID, canonical SMILES, molecular weight, and safety data. Writes to: compounds',
    } satisfies AgentNodeData,
  },
  {
    id: 't3-chembl',
    type: 'agent',
    position: { x: COL_2, y: Y_T3 },
    data: {
      label: 'ChEMBL',
      schedule: '',
      tag: 'API',
      tagType: 'api',
      tip: 'Fetches IC50/EC50/Ki bioactivity, mechanism of action, max clinical phase. Writes to: compound_activities',
    } satisfies AgentNodeData,
  },
  {
    id: 't3-opentargets',
    type: 'agent',
    position: { x: COL_3, y: Y_T3 },
    data: {
      label: 'OpenTargets',
      schedule: '',
      tag: 'API',
      tagType: 'api',
      tip: 'Queries target-disease associations. Returns scores and known drugs. Writes to: target_disease_assocs',
    } satisfies AgentNodeData,
  },

  /* ── Tier 4 · Score ── */
  {
    id: 't4-tier',
    type: 'tier',
    position: { x: COL_0, y: Y_T4_HDR },
    data: {
      label: 'Tier 4 · Score',
      index: 'T4',
    } satisfies TierNodeData,
  },
  {
    id: 't4-scoring',
    type: 'agent',
    position: { x: COL_0, y: Y_T4 },
    data: {
      label: 'Scoring',
      schedule: '',
      tag: 'LOCAL',
      tagType: 'local',
      tip: 'Aggregates evidence across 20+ HSA signaling pathways. Composite score 0-100. Flags discoveries on score jumps >10pts.',
    } satisfies AgentNodeData,
  },
  {
    id: 't4-synthesis',
    type: 'agent',
    position: { x: COL_1, y: Y_T4 },
    data: {
      label: 'Synthesis',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'AI generates narrative summaries of current pipeline state and top compound rankings. Writes to: digests',
    } satisfies AgentNodeData,
  },

  /* ── Swarm · 8 personas + Consensus ── */
  {
    id: 'sw-tier',
    type: 'tier',
    position: { x: COL_0, y: Y_SWARM_HDR },
    data: {
      label: 'Swarm · parallel vote',
      index: 'SW',
    } satisfies TierNodeData,
  },
  {
    id: 'sw-medchem',
    type: 'agent',
    position: { x: COL_0, y: Y_SWARM_R1 },
    data: {
      label: 'Medicinal Chemist',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'Drug-likeness, synthesis feasibility, SAR validation (18y small-molecule pharma).',
    } satisfies AgentNodeData,
  },
  {
    id: 'sw-onco',
    type: 'agent',
    position: { x: COL_1, y: Y_SWARM_R1 },
    data: {
      label: 'Clinical Oncologist',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'Sarcoma specialist. Doxorubicin-vs-novel, QoL, canine dosing (22y).',
    } satisfies AgentNodeData,
  },
  {
    id: 'sw-regulatory',
    type: 'agent',
    position: { x: COL_2, y: Y_SWARM_R1 },
    data: {
      label: 'Regulatory',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'FDA-CVM pathway. Approval <5yr, repurposing faster than de novo (15y).',
    } satisfies AgentNodeData,
  },
  {
    id: 'sw-canine',
    type: 'agent',
    position: { x: COL_3, y: Y_SWARM_R1 },
    data: {
      label: 'Canine Vet Oncologist',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'Comparative oncology. CDKN2A, PIK3CA, TP53, breed risk, cross-species PK (20y).',
    } satisfies AgentNodeData,
  },
  {
    id: 'sw-vp',
    type: 'agent',
    position: { x: COL_0, y: Y_SWARM_R2 },
    data: {
      label: 'Biotech VP Discovery',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'Differentiation vs. SOC, market economics, 3-5yr moat (25y mid-cap biotech).',
    } satisfies AgentNodeData,
  },
  {
    id: 'sw-patent',
    type: 'agent',
    position: { x: COL_1, y: Y_SWARM_R2 },
    data: {
      label: 'Patent Attorney',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'IP position, CoM + method-of-use claims, FTO, blocking prior art (14y).',
    } satisfies AgentNodeData,
  },
  {
    id: 'sw-biochem',
    type: 'agent',
    position: { x: COL_2, y: Y_SWARM_R2 },
    data: {
      label: 'Biochem Contrarian',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'Kinase structural biology. Residence time, off-target promiscuity (12y).',
    } satisfies AgentNodeData,
  },
  {
    id: 'sw-patient',
    type: 'agent',
    position: { x: COL_3, y: Y_SWARM_R2 },
    data: {
      label: 'Patient Advocate',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'Treatment burden, vet visit frequency, QoL vs. survival gain (8y).',
    } satisfies AgentNodeData,
  },
  {
    id: 'sw-consensus',
    type: 'report',
    position: { x: COL_1, y: Y_CONSENSUS },
    data: {
      label: 'Consensus',
      schedule: '',
      tip: 'Binary verdicts aggregated by simple majority × confidence weighting. Output: pipeline_actions, prior_art_alerts. Feeds Design tier priority.',
    } satisfies ReportNodeData,
  },

  /* ── Tier 5 · Design ── */
  {
    id: 't5-tier',
    type: 'tier',
    position: { x: COL_0, y: Y_T5_HDR },
    data: {
      label: 'Tier 5 · Design',
      index: 'T5',
    } satisfies TierNodeData,
  },
  {
    id: 't5-hypothesis',
    type: 'agent',
    position: { x: COL_0, y: Y_T5 },
    data: {
      label: 'Hypothesis',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'AI reasons over full dataset to generate novel treatment hypotheses. Identifies cross-source patterns.',
    } satisfies AgentNodeData,
  },
  {
    id: 't5-repurposing',
    type: 'agent',
    position: { x: COL_1, y: Y_T5 },
    data: {
      label: 'Repurposing',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'AI analyzes FDA-approved drugs for potential HSA activity based on shared targets and mechanisms.',
    } satisfies AgentNodeData,
  },
  {
    id: 't5-admet',
    type: 'agent',
    position: { x: COL_2, y: Y_T5 },
    data: {
      label: 'ADMET',
      schedule: '',
      tag: 'LOCAL',
      tagType: 'local',
      tip: 'Predicts 41 ADMET properties: absorption, distribution, metabolism, excretion, toxicity. Filters unsafe candidates.',
    } satisfies AgentNodeData,
  },
  {
    id: 't5-molgen',
    type: 'agent',
    position: { x: COL_3, y: Y_T5 },
    data: {
      label: 'MolGen',
      schedule: '',
      tag: 'NVIDIA',
      tagType: 'nim',
      tip: 'NVIDIA MolMIM generates molecular variants from lead SMILES. RDKit filters for Lipinski + synthetic accessibility.',
    } satisfies AgentNodeData,
  },
  {
    id: 't5-combo',
    type: 'agent',
    position: { x: COL_4, y: Y_T5 },
    data: {
      label: 'Combination',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'Pairs top monotherapy candidates into rational combination strategies. Scores synergy by shared vs. orthogonal pathways.',
    } satisfies AgentNodeData,
  },

  /* ── Tier 6 · Structure + Dock + Peptide (6 in a row) ── */
  {
    id: 't6-tier',
    type: 'tier',
    position: { x: COL_0, y: Y_T6_HDR },
    data: {
      label: 'Tier 6 · Structure + Dock · Peptide Design',
      index: 'T6',
    } satisfies TierNodeData,
  },
  {
    id: 't6-structure',
    type: 'agent',
    position: { x: COL_0, y: Y_T6 },
    data: {
      label: 'Structure',
      schedule: '',
      tag: 'API',
      tagType: 'api',
      tip: 'Fetches and cleans 3D protein structures. Removes waters, selects chain, detects binding sites. Writes to: protein_targets',
    } satisfies AgentNodeData,
  },
  {
    id: 't6-docking',
    type: 'agent',
    position: { x: COL_1, y: Y_T6 },
    data: {
      label: 'Docking',
      schedule: '',
      tag: 'NVIDIA',
      tagType: 'nim',
      tip: 'NVIDIA DiffDock V2 virtual docking. SMILES to SDF via RDKit, 10 poses per compound/target pair. Writes to: docking_results',
    } satisfies AgentNodeData,
  },
  {
    id: 't6b-esm2',
    type: 'agent',
    position: { x: COL_2, y: Y_T6 },
    data: {
      label: 'ESM-2 Embeddings',
      schedule: '',
      tag: 'NVIDIA',
      tagType: 'nim',
      tip: 'ESM-2 protein language model embeddings. Encodes target pocket + binder context for downstream scaffold generation.',
    } satisfies AgentNodeData,
  },
  {
    id: 't6b-rfdiff',
    type: 'agent',
    position: { x: COL_3, y: Y_T6 },
    data: {
      label: 'RFdiffusion',
      schedule: '',
      tag: 'NVIDIA',
      tagType: 'nim',
      tip: 'Generates novel binder backbone scaffolds.',
    } satisfies AgentNodeData,
  },
  {
    id: 't6b-mpnn',
    type: 'agent',
    position: { x: COL_4, y: Y_T6 },
    data: {
      label: 'ProteinMPNN',
      schedule: '',
      tag: 'NVIDIA',
      tagType: 'nim',
      tip: 'Inverse folding: sequences for given structures.',
    } satisfies AgentNodeData,
  },
  {
    id: 't6b-peptide-loop',
    type: 'agent',
    position: { x: COL_5, y: Y_T6 },
    data: {
      label: 'Peptide Design Loop',
      schedule: '',
      tag: 'NVIDIA',
      tagType: 'nim',
      tip: 'Iterative: RFdiffusion → MPNN → structure check → filter. Outputs novel peptide binders.',
    } satisfies AgentNodeData,
  },

  /* ── Tier 7 · Validate ── */
  {
    id: 't7-tier',
    type: 'tier',
    position: { x: COL_0, y: Y_T7_HDR },
    data: {
      label: 'Tier 7 · Validate',
      index: 'T7',
    } satisfies TierNodeData,
  },
  {
    id: 't7-designloop',
    type: 'agent',
    position: { x: COL_0, y: Y_T7 },
    data: {
      label: 'Design Loop',
      schedule: '',
      tag: 'NVIDIA',
      tagType: 'nim',
      tip: 'Iterative design: Generate → ADMET → Dock → Score → feed winners back. Up to 5 rounds per target.',
    } satisfies AgentNodeData,
  },
  {
    id: 't7-md',
    type: 'agent',
    position: { x: COL_1, y: Y_T7 },
    data: {
      label: 'MD Validation',
      schedule: '',
      tag: 'LOCAL',
      tagType: 'local',
      tip: 'OpenMM v3 explicit solvent MD. AMBER14 + SMIRNOFF. Calibrated stable < 0.163nm. Runs on RunPod 3x RTX 4090 or local Mac.',
    } satisfies AgentNodeData,
  },

  /* ── Reports ── */
  {
    id: 'tr-tier',
    type: 'tier',
    position: { x: COL_0, y: Y_REP_HDR },
    data: {
      label: 'Reports',
      index: 'R',
    } satisfies TierNodeData,
  },
  {
    id: 'tr-report',
    type: 'report',
    position: { x: COL_0, y: Y_REP },
    data: {
      label: 'Report',
      schedule: '',
      tip: 'Executive + ELI5 digest summaries. Aggregates papers, discoveries, compound rankings.',
    } satisfies ReportNodeData,
  },
  {
    id: 'tr-director',
    type: 'report',
    position: { x: COL_1, y: Y_REP },
    data: {
      label: 'Director',
      schedule: '',
      tip: 'Renders molecule reveal + pipeline videos via Remotion.',
    } satisfies ReportNodeData,
  },
  {
    id: 'tr-content',
    type: 'report',
    position: { x: COL_2, y: Y_REP },
    data: {
      label: 'Content',
      schedule: '',
      tip: 'Generates social media assets for Twitter, LinkedIn, Substack, and email.',
    } satisfies ReportNodeData,
  },
  {
    id: 'tr-thesis',
    type: 'report',
    position: { x: COL_3, y: Y_REP },
    data: {
      label: 'Thesis',
      schedule: '',
      tip: 'Comprehensive research synthesis. Deltas, lead compound analysis.',
    } satisfies ReportNodeData,
  },
  {
    id: 'tr-status',
    type: 'report',
    position: { x: COL_4, y: Y_REP },
    data: {
      label: 'Status',
      schedule: '',
      tip: 'Health monitoring. Checks agent uptime, pipeline throughput.',
    } satisfies ReportNodeData,
  },

  /* ── Committee (bottom tier, feeds back up to Design) ── */
  {
    id: 'tc-tier',
    type: 'tier',
    position: { x: COL_0, y: Y_COMM_HDR },
    data: {
      label: 'Committee',
      index: 'C',
    } satisfies TierNodeData,
  },
  {
    id: 'tc-medchem',
    type: 'agent',
    position: { x: COL_0, y: Y_COMM },
    data: {
      label: 'Medicinal Chemist',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'Reviews scaffold diversity and SAR. Recommends seed compounds, adjusts diversity penalties.',
    } satisfies AgentNodeData,
  },
  {
    id: 'tc-biologist',
    type: 'agent',
    position: { x: COL_1, y: Y_COMM },
    data: {
      label: 'Target Biologist',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'Weighs RNA-seq expression evidence, reorders target priorities based on fold-change data.',
    } satisfies AgentNodeData,
  },
  {
    id: 'tc-safety',
    type: 'agent',
    position: { x: COL_2, y: Y_COMM },
    data: {
      label: 'Safety Pharmacologist',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'Flags toxicity risks, sets selectivity constraints (e.g. JAK1 vs JAK2/3). Reviews supplement safety.',
    } satisfies AgentNodeData,
  },
  {
    id: 'tc-compbio',
    type: 'agent',
    position: { x: COL_3, y: Y_COMM },
    data: {
      label: 'Computational Bio',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'Validates docking confidence, structure quality, TM-scores and pLDDT thresholds.',
    } satisfies AgentNodeData,
  },
  {
    id: 'tc-vet',
    type: 'agent',
    position: { x: COL_4, y: Y_COMM },
    data: {
      label: 'Integrative Vet Advisor',
      schedule: '',
      tag: 'AI',
      tagType: 'claude',
      tip: 'Reviews Track A treatments: supplement evidence, nutrition protocols, drug-supplement interactions, canine dosing guidance. Recommends combinations.',
    } satisfies AgentNodeData,
  },
  {
    id: 'tc-directive',
    type: 'report',
    position: { x: COL_5, y: Y_COMM },
    data: {
      label: 'Directive',
      schedule: '',
      tip: 'directive.json — target_order, rounds_per_target, diversity_penalty, seeds, selectivity, score_cap. Input to Design tier.',
    } satisfies ReportNodeData,
  },
];

/* ── Edges ── */

const mainEdge = {
  type: 'flow' as const,
  style: { stroke: 'var(--gray-400)', strokeWidth: 1.5 },
};

const fadedEdge = {
  type: 'flow' as const,
  style: { stroke: 'var(--gray-300)', strokeWidth: 1 },
};

const dashedEdge = {
  type: 'flow' as const,
  style: {
    stroke: 'var(--gray-300)',
    strokeWidth: 1,
    strokeDasharray: '6 4',
  },
  data: { dashed: true },
};

const reportEdge = {
  type: 'flow' as const,
  style: { stroke: 'var(--gray-400)', strokeWidth: 1.5 },
};

export const EDGES: Edge[] = [
  /* Source Discovery → Tier 1 */
  { id: 'sd-pubmed', source: 'sd-agent', target: 't1-pubmed', ...dashedEdge },

  /* Tier 1 Sources → Corpus */
  { id: 'pubmed-corpus', source: 't1-pubmed', target: 't2-corpus', ...mainEdge },
  { id: 's2-corpus', source: 't1-s2', target: 't2-corpus', ...mainEdge },
  { id: 'biorxiv-corpus', source: 't1-biorxiv', target: 't2-corpus', ...mainEdge },
  { id: 'europepmc-corpus', source: 't1-europepmc', target: 't2-corpus', ...mainEdge },

  /* Corpus → RAG */
  { id: 'corpus-rag', source: 't2-corpus', target: 't2-rag', ...mainEdge },

  /* RAG fans out */
  { id: 'rag-analysis', source: 't2-rag', target: 't3-analysis', ...mainEdge },
  { id: 'rag-hypothesis', source: 't2-rag', target: 't5-hypothesis', ...fadedEdge },

  /* Analysis → enrichment + Scoring */
  { id: 'analysis-pubchem', source: 't3-analysis', target: 't3-pubchem', ...mainEdge },
  { id: 'analysis-chembl', source: 't3-analysis', target: 't3-chembl', ...mainEdge },
  { id: 'analysis-opentargets', source: 't3-analysis', target: 't3-opentargets', ...mainEdge },
  { id: 'analysis-scoring', source: 't3-analysis', target: 't4-scoring', ...mainEdge },

  /* Enrichment → Scoring */
  { id: 'pubchem-scoring', source: 't3-pubchem', target: 't4-scoring', ...fadedEdge },
  { id: 'chembl-scoring', source: 't3-chembl', target: 't4-scoring', ...fadedEdge },
  { id: 'opentargets-scoring', source: 't3-opentargets', target: 't4-scoring', ...fadedEdge },

  /* Scoring → Synthesis */
  { id: 'scoring-synthesis', source: 't4-scoring', target: 't4-synthesis', ...mainEdge },

  /* Scoring → Swarm personas */
  { id: 'scoring-sw-medchem', source: 't4-scoring', target: 'sw-medchem', ...fadedEdge },
  { id: 'scoring-sw-onco', source: 't4-scoring', target: 'sw-onco', ...fadedEdge },
  { id: 'scoring-sw-regulatory', source: 't4-scoring', target: 'sw-regulatory', ...fadedEdge },
  { id: 'scoring-sw-canine', source: 't4-scoring', target: 'sw-canine', ...fadedEdge },
  { id: 'scoring-sw-vp', source: 't4-scoring', target: 'sw-vp', ...fadedEdge },
  { id: 'scoring-sw-patent', source: 't4-scoring', target: 'sw-patent', ...fadedEdge },
  { id: 'scoring-sw-biochem', source: 't4-scoring', target: 'sw-biochem', ...fadedEdge },
  { id: 'scoring-sw-patient', source: 't4-scoring', target: 'sw-patient', ...fadedEdge },

  /* Swarm → Consensus */
  { id: 'sw-medchem-consensus', source: 'sw-medchem', target: 'sw-consensus', ...mainEdge },
  { id: 'sw-onco-consensus', source: 'sw-onco', target: 'sw-consensus', ...mainEdge },
  { id: 'sw-regulatory-consensus', source: 'sw-regulatory', target: 'sw-consensus', ...mainEdge },
  { id: 'sw-canine-consensus', source: 'sw-canine', target: 'sw-consensus', ...mainEdge },
  { id: 'sw-vp-consensus', source: 'sw-vp', target: 'sw-consensus', ...mainEdge },
  { id: 'sw-patent-consensus', source: 'sw-patent', target: 'sw-consensus', ...mainEdge },
  { id: 'sw-biochem-consensus', source: 'sw-biochem', target: 'sw-consensus', ...mainEdge },
  { id: 'sw-patient-consensus', source: 'sw-patient', target: 'sw-consensus', ...mainEdge },

  /* Consensus → Hypothesis */
  { id: 'consensus-hypothesis', source: 'sw-consensus', target: 't5-hypothesis', ...reportEdge },

  /* Design fan */
  { id: 'scoring-repurposing', source: 't4-scoring', target: 't5-repurposing', ...fadedEdge },
  { id: 'scoring-admet', source: 't4-scoring', target: 't5-admet', ...fadedEdge },
  { id: 'scoring-molgen', source: 't4-scoring', target: 't5-molgen', ...fadedEdge },
  { id: 'hypothesis-molgen', source: 't5-hypothesis', target: 't5-molgen', ...mainEdge },
  { id: 'molgen-combo', source: 't5-molgen', target: 't5-combo', ...fadedEdge },

  /* Design → Structure + Dock */
  { id: 'molgen-structure', source: 't5-molgen', target: 't6-structure', ...mainEdge },
  { id: 'molgen-docking', source: 't5-molgen', target: 't6-docking', ...mainEdge },
  { id: 'structure-docking', source: 't6-structure', target: 't6-docking', ...fadedEdge },

  /* Structure → ESM-2 (peptide entry) */
  { id: 'structure-esm2', source: 't6-structure', target: 't6b-esm2', ...fadedEdge },

  /* Peptide lane */
  { id: 'esm2-rfdiff', source: 't6b-esm2', target: 't6b-rfdiff', ...mainEdge },
  { id: 'rfdiff-mpnn', source: 't6b-rfdiff', target: 't6b-mpnn', ...mainEdge },
  { id: 'mpnn-peptide-loop', source: 't6b-mpnn', target: 't6b-peptide-loop', ...mainEdge },
  { id: 'peptide-loop-md', source: 't6b-peptide-loop', target: 't7-md', ...fadedEdge },

  /* Docking → Design Loop */
  { id: 'docking-designloop', source: 't6-docking', target: 't7-designloop', ...mainEdge },

  /* Design Loop → MolGen (feedback, dashed) */
  {
    id: 'designloop-feedback',
    source: 't7-designloop',
    target: 't5-molgen',
    ...dashedEdge,
    label: '×5 rounds',
    labelStyle: {
      fontFamily: 'var(--font-jetbrains-mono), monospace',
      fontSize: '0.72rem',
      letterSpacing: '0.16em',
      textTransform: 'uppercase',
      fill: 'var(--gray-500)',
    },
    labelBgStyle: { fill: 'var(--background)' },
    labelBgPadding: [6, 6] as [number, number],
    labelBgBorderRadius: 2,
  },

  /* Design Loop → MD */
  { id: 'designloop-md', source: 't7-designloop', target: 't7-md', ...mainEdge },

  /* MD → Reports */
  { id: 'md-report', source: 't7-md', target: 'tr-report', ...reportEdge },
  { id: 'md-director', source: 't7-md', target: 'tr-director', ...reportEdge },
  { id: 'md-content', source: 't7-md', target: 'tr-content', ...reportEdge },
  { id: 'md-thesis', source: 't7-md', target: 'tr-thesis', ...reportEdge },
  { id: 'md-status', source: 't7-md', target: 'tr-status', ...reportEdge },

  /* Synthesis → Reports (faded) */
  { id: 'synthesis-report', source: 't4-synthesis', target: 'tr-report', ...fadedEdge },
  { id: 'synthesis-thesis', source: 't4-synthesis', target: 'tr-thesis', ...fadedEdge },

  /* Track A chain */
  { id: 'ta-lit-analysis', source: 'ta-literature', target: 'ta-analysis', ...mainEdge },
  { id: 'ta-analysis-scoring', source: 'ta-analysis', target: 'ta-scoring', ...mainEdge },
  { id: 'ta-scoring-top10', source: 'ta-scoring', target: 'ta-top10', ...mainEdge },
  { id: 'ta-top10-vet', source: 'ta-top10', target: 'tc-vet', ...dashedEdge },

  /* Committee → Directive */
  { id: 'tc-medchem-directive', source: 'tc-medchem', target: 'tc-directive', ...mainEdge },
  { id: 'tc-biologist-directive', source: 'tc-biologist', target: 'tc-directive', ...mainEdge },
  { id: 'tc-safety-directive', source: 'tc-safety', target: 'tc-directive', ...mainEdge },
  { id: 'tc-compbio-directive', source: 'tc-compbio', target: 'tc-directive', ...mainEdge },
  { id: 'tc-vet-directive', source: 'tc-vet', target: 'tc-directive', ...mainEdge },

  /* Directive → MolGen (dashed feedback) */
  { id: 'directive-molgen', source: 'tc-directive', target: 't5-molgen', ...dashedEdge },

  /* Consensus → Directive (dashed feedback) */
  { id: 'consensus-directive', source: 'sw-consensus', target: 'tc-directive', ...dashedEdge },
];

