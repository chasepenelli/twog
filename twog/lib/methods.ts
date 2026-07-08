export interface MethodSection {
  heading: string;
  body: string;
}

export interface MethodFlowStep {
  label: string;
  detail: string;
}

export interface MethodEndpoint {
  label: string;
  path: string;
  detail: string;
  href?: string;
}

export interface MethodRecord {
  methodId: string;
  title: string;
  summary: string;
  version: string;
  category: string;
  status: string;
  appliesTo: string;
  claimsLevel: string;
  heroStatement: string;
  operatorLine?: string;
  flow: MethodFlowStep[];
  auditFields: Array<[string, string]>;
  sections: MethodSection[];
  endpoints?: MethodEndpoint[];
  interpretationRules: string[];
  boundary: string;
}

export const methods: MethodRecord[] = [
  {
    methodId: 'autonomous-discovery-loop-v1',
    title: 'Autonomous Discovery Loop v2.2',
    version: 'v2.2',
    category: 'discovery orchestration',
    status: 'active',
    appliesTo: 'candidate cruxes',
    claimsLevel: 'falsification attempt',
    summary:
      'How TWOG drives its own scientific crux: it proposes the next falsifying test, pre-registers a hashed kill-criterion before compute, resolves its own inputs, dispatches real GPU work, and audits the result through confound and provenance gates before deciding what to test next.',
    heroStatement:
      'The loop is falsification-first. For a leading hypothesis it proposes the test most likely to kill it, commits the kill-criterion before any compute runs, and never auto-promotes what survives. A supporting result is evidence the idea is harder to kill, not proof that it is true.',
    operatorLine: 'Falsification first. The human write-gate is strictly terminal.',
    flow: [
      {
        label: 'Propose the kill test',
        detail:
          'Given a candidate, the loop selects the next experiment most likely to falsify the leading hypothesis rather than the one most likely to confirm it.',
      },
      {
        label: 'Pre-register the criterion',
        detail:
          'A kill-criterion is hashed and committed before any compute runs, so the threshold for failure cannot be moved after the result is known. This is the anti p-hacking gate.',
      },
      {
        label: 'Resolve inputs',
        detail:
          'From a candidate’s named target and therapy, the loop fetches its own inputs: PubChem SMILES for the compound and an RCSB structure for the target, with no hand-curation.',
      },
      {
        label: 'Dispatch compute',
        detail:
          'It runs real GPU compute on Modal and ledgers the artifact. In CI it falls back to an in-process mock provider so the loop is reproducible without a GPU.',
      },
      {
        label: 'Gate and terminate',
        detail:
          'A result must clear mandatory confound and provenance pre-gates before it can be accepted; a supports result cannot be accepted until its known confounds survive controls. The loop then decides what to test next, and the human write-gate remains terminal.',
      },
    ],
    auditFields: [
      ['What is being tested', 'The leading hypothesis for a candidate and the falsifying experiment chosen to attack it.'],
      ['Pre-registered criterion', 'The hashed kill-criterion committed before compute, with the hash and commit time.'],
      ['Resolved inputs', 'The named target and therapy, the fetched RCSB structure, the PubChem SMILES, and their source provenance.'],
      ['Compute run', 'Provider (Modal GPU or CI mock), job ID, docking and pose outputs, cost, and ledger reference.'],
      ['Gate outcome', 'Confound-control results, provenance checks, the resulting verdict, and the explicit non-promotion record.'],
    ],
    sections: [
      {
        heading: 'Why falsification-first',
        body:
          'A confirmation-seeking engine drifts toward agreeable results. The loop instead proposes the test most likely to kill the leading hypothesis, so the work that survives is the work that resisted a genuine attempt to break it.',
      },
      {
        heading: 'Why the criterion is pre-registered',
        body:
          'The kill-criterion is hashed and committed before any compute runs. Pre-registration removes the freedom to redefine success after seeing the numbers, which is the main route to p-hacking in an automated pipeline.',
      },
      {
        heading: 'How inputs are resolved',
        body:
          'The loop starts from a candidate’s named target and therapy and fetches its own inputs — the RCSB structure for the target and the PubChem SMILES for the compound — instead of relying on a curated file someone prepared by hand.',
      },
      {
        heading: 'How results are gated',
        body:
          'Confound and provenance pre-gates are mandatory. A supports result cannot be accepted until its known confounds survive controls, and verdicts are never recorded as true. Nothing the loop concludes is auto-promoted; the human write-gate is the only path to a record change.',
      },
      {
        heading: 'What it does not claim',
        body:
          'Everything here is in-silico and hypothesis-generating. A surviving hypothesis is not a validated treatment. The bar the loop optimizes for is integrity and reproducibility, not clinical proof.',
      },
    ],
    interpretationRules: [
      'A surviving hypothesis is harder to kill, not proven true.',
      'A pre-registered kill-criterion cannot be revised after the result is known.',
      'A supports verdict is invalid until its known confounds survive controls.',
      'No loop result is auto-promoted; the human write-gate is terminal.',
      'In-silico survival is hypothesis-generating, not clinical validation.',
    ],
    boundary:
      'Autonomous Discovery Loop v2.2 does not certify efficacy, safety, or clinical readiness. It runs reproducible, pre-registered falsification attempts in silico and leaves every promotion decision to a human operator.',
  },
  {
    methodId: 'candidate-record-v1',
    title: 'Candidate Record v1',
    version: 'v1',
    category: 'public proof layer',
    status: 'active',
    appliesTo: 'candidate pages',
    claimsLevel: 'research artifact',
    summary:
      'How TWOG converts internal research artifacts into stable public candidate pages with rationale, evidence, risks, decision history, and reproducibility metadata.',
    heroStatement:
      'A candidate page is a frozen public argument. It should show what TWOG currently believes, what supports it, what is still weak, and what would change the record.',
    operatorLine: 'LLMs argue and synthesize. Operator approval is the write gate.',
    flow: [
      {
        label: 'Internal evidence',
        detail:
          'The source material starts as TWOG research artifacts: briefs, therapy ideas, validation packets, citation refs, decision events, and reproducibility metadata.',
      },
      {
        label: 'Public snapshot',
        detail:
          'The exporter creates a bounded public payload with the hypothesis, status, rationale, evidence refs, risk notes, known blockers, and linked records.',
      },
      {
        label: 'Citation expansion',
        detail:
          'Internal labels such as C1, C20, or C22 are expanded into a reference dossier with title, DOI, PMID, PMCID, source, evidence kind, supported claim, and provenance counts.',
      },
      {
        label: 'Decision trail',
        detail:
          'Status changes and snapshot events are preserved as dated decision entries. A candidate should never silently move from proposed to investigating to advanced.',
      },
      {
        label: 'Hash and publish',
        detail:
          'The public snapshot receives a content hash. The page can be cited, compared, challenged, and regenerated without pretending the record is more final than it is.',
      },
    ],
    auditFields: [
      ['What is being proposed', 'The candidate ID, status, target family, therapy family, and current priority score.'],
      ['What evidence supports it', 'Short citation labels expanded into readable source compartments and supported claims.'],
      ['What is still weak', 'Known limitations, missing data, citation gaps, and reasons the record has not advanced.'],
      ['Why the status changed', 'Timestamped rationale for snapshot generation, evidence updates, status changes, and reviewer actions.'],
      ['How this version was made', 'Pipeline version, source brief/evaluation IDs, committee run IDs, content hash, and method reference.'],
    ],
    sections: [
      {
        heading: 'What the record is',
        body:
          'A candidate record is a public snapshot of a research idea at a specific moment. It preserves the hypothesis, current status, source evidence, known risks, decision history, and reproducibility metadata without treating the idea as clinically settled.',
      },
      {
        heading: 'How evidence labels work',
        body:
          'Short labels such as C1 or C20 are local citation identifiers from a source brief. The public page must expand them into article titles, identifiers, links, evidence type, source provenance, duplicate handling, and the exact claim each source supports.',
      },
      {
        heading: 'What hashes mean',
        body:
          'The content hash is computed from the public snapshot payload. It lets a reader cite or compare a specific version of the candidate page when later evidence changes the record.',
      },
      {
        heading: 'How decision logs work',
        body:
          'Decision entries record when a candidate is proposed, updated, advanced, parked, or regenerated. Each meaningful transition should carry a short rationale and a link back to the related agent, evidence packet, validation decision, or compute run when available.',
      },
      {
        heading: 'What the page does not claim',
        body:
          'A candidate page is not a diagnosis, prescription, treatment recommendation, or proof of efficacy. It is a research artifact that makes the reasoning chain easier to inspect, challenge, reproduce, and improve.',
      },
    ],
    endpoints: [
      {
        label: 'Checkout / one candidate',
        path: '/api/public-candidates/{candidate_id}',
        detail:
          'Returns one public candidate record: metadata, latest snapshot, rationale, literature, decision events, and reproducibility fields.',
      },
      {
        label: 'Browse / all candidates',
        path: '/api/public-candidates',
        href: '/api/public-candidates',
        detail:
          'Returns the exported public candidate dataset so readers can find available records and compare snapshot metadata.',
      },
      {
        label: 'Example checkout',
        path: '/api/public-candidates/rly2608-pik3ca',
        href: '/api/public-candidates/rly2608-pik3ca',
        detail:
          'A live candidate payload using the stable candidate ID. Display IDs resolve on the page; the API uses durable IDs.',
      },
      {
        label: 'Evidence bundle',
        path: '/api/public-candidates/rly2608-pik3ca/evidence-bundle',
        href: '/api/public-candidates/rly2608-pik3ca/evidence-bundle',
        detail:
          'Returns the actionable checkout packet: source-document dossier, chunk manifest, artifact manifest, compute/MD reproducibility contract, and check-in endpoints.',
      },
      {
        label: 'Contribution template',
        path: '/api/public-candidates/rly2608-pik3ca/contribution-template',
        href: '/api/public-candidates/rly2608-pik3ca/contribution-template',
        detail:
          'Returns a fillable contribution packet for evidence, critique, replication notes, artifacts, or validation proposals tied to this snapshot.',
      },
    ],
    interpretationRules: [
      'A record is an inspectable research artifact, not a clinical recommendation.',
      'Evidence labels are local to the source brief; the reference dossier is the public decoding layer.',
      'A candidate can be promising and still not validation-ready.',
      'Human analog evidence is treated as context unless canine-specific evidence closes the bridge.',
      'A content hash identifies the public snapshot, not permanent scientific truth.',
    ],
    boundary:
      'Candidate Record v1 does not certify efficacy, safety, dosing, clinical readiness, regulatory fitness, or veterinary use. It only makes the research reasoning chain easier to inspect.',
  },
  {
    methodId: 'evidence-bundle-v1',
    title: 'Evidence Bundle v1',
    version: 'v1',
    category: 'checkout packet',
    status: 'active',
    appliesTo: 'public payloads',
    claimsLevel: 'source dossier',
    summary:
      'What TWOG includes when someone checks out a candidate for outside review: source records, chunk provenance, artifact manifests, compute settings, and contribution routes.',
    heroStatement:
      'The evidence bundle is the action layer behind a candidate page. It gives a reviewer enough structure to inspect, replicate, challenge, or extend a record without guessing where the claim came from.',
    operatorLine: 'The readable page tells the story. The evidence bundle carries the working materials.',
    flow: [
      {
        label: 'Select snapshot',
        detail:
          'The bundle is pinned to one public candidate snapshot and content hash so outside work targets the same record TWOG published.',
      },
      {
        label: 'Gather references',
        detail:
          'Expanded literature entries, source URLs, identifiers, duplicate groups, research object IDs, and chunk IDs are packaged together.',
      },
      {
        label: 'Attach artifacts',
        detail:
          'Compute outputs, MD settings, plots, payload hashes, and generated files are represented as manifests instead of being hidden in private notes.',
      },
      {
        label: 'Expose gaps',
        detail:
          'Missing citations, unresolved refs, unsupported claims, and absent raw artifacts remain visible as work items instead of being smoothed over.',
      },
      {
        label: 'Enable check-in',
        detail:
          'The same packet points to the contribution template so reviewers can return structured evidence, critique, replication notes, artifacts, or validation proposals.',
      },
    ],
    auditFields: [
      ['Snapshot identity', 'Candidate ID, display ID, content hash, pipeline version, and method reference.'],
      ['Source dossier', 'Citation records, identifiers, links, duplicate handling, and supported claims.'],
      ['Chunk provenance', 'Research object IDs and chunk IDs that connect public claims to stored source material.'],
      ['Artifact manifest', 'Plots, payloads, compute outputs, method versions, container images, and reproducibility notes when available.'],
      ['Open gaps', 'Negative coverage, missing identifiers, unresolved citations, and blockers that need follow-up work.'],
    ],
    sections: [
      {
        heading: 'Why bundles exist',
        body:
          'A candidate page is readable, but a bundle is actionable. It puts the public snapshot, evidence dossier, artifact manifest, and contribution routes into one packet for outside review.',
      },
      {
        heading: 'What source access means',
        body:
          'The public packet exposes source metadata and provenance fields. It does not automatically publish every licensed full-text document; rights-restricted material stays behind the evidence boundary while the identifiers and claims remain auditable.',
      },
      {
        heading: 'How chunk provenance works',
        body:
          'Chunk IDs are pointers into TWOG processed research records. They let the internal system re-open the exact supporting material while the public page shows the readable claim and source label.',
      },
      {
        heading: 'How compute settings travel',
        body:
          'If MD, docking, omics, or another compute lane informs the candidate, the bundle should include the method reference, input contract, settings, artifact IDs, and run ledger identifiers needed for review.',
      },
    ],
    endpoints: [
      {
        label: 'Evidence bundle',
        path: '/api/public-candidates/rly2608-pik3ca/evidence-bundle',
        href: '/api/public-candidates/rly2608-pik3ca/evidence-bundle',
        detail: 'Live example of the actionable checkout packet attached to a public candidate.',
      },
      {
        label: 'Candidate payload',
        path: '/api/public-candidates/rly2608-pik3ca',
        href: '/api/public-candidates/rly2608-pik3ca',
        detail: 'The public record payload the evidence bundle extends.',
      },
    ],
    interpretationRules: [
      'A bundle is for audit and follow-up work, not medical decision-making.',
      'Source metadata can be public even when source text has access restrictions.',
      'Unresolved references are useful defects, not decoration.',
      'Artifacts should be hashable or otherwise traceable back to their generation run.',
    ],
    boundary:
      'Evidence Bundle v1 does not grant rights to restricted source documents or certify that a compute artifact is scientifically sufficient. It packages what TWOG can expose for reproducible review.',
  },
  {
    methodId: 'contribution-intake-v1',
    title: 'Contribution Intake v1',
    version: 'v1',
    category: 'public check-in',
    status: 'paused',
    appliesTo: 'outside submissions',
    claimsLevel: 'queued contribution',
    summary:
      'How outside evidence, critiques, replication notes, artifacts, and validation proposals are checked back into TWOG without directly mutating public candidate records.',
    heroStatement:
      'Contribution intake is deliberately gated. A public reader can submit better evidence, but the submission becomes a review packet first, not an automatic change to the record.',
    operatorLine: 'Public check-in creates review work. It does not rewrite candidate state.',
    flow: [
      {
        label: 'Template',
        detail:
          'The contributor starts from a structured template tied to a candidate ID, snapshot hash, contribution type, and requested review route.',
      },
      {
        label: 'Submission',
        detail:
          'The packet records contributor contact, evidence URL or artifact reference, claim text, relation to the candidate, limitations, and requested action.',
      },
      {
        label: 'Intake queue',
        detail:
          'Neon-backed storage receives the packet as queued intake. The public record remains unchanged.',
      },
      {
        label: 'Operator decision',
        detail:
          'An operator can request more information, reject, archive, or route the packet into citation repair, evidence review, validation planning, or compute review.',
      },
      {
        label: 'Record update',
        detail:
          'Only after review clears does TWOG write a new decision entry, snapshot version, or candidate update.',
      },
    ],
    auditFields: [
      ['Contributor context', 'Name, contact, organization, and declared relation to the candidate record.'],
      ['Contribution type', 'Evidence, critique, replication, artifact, validation proposal, or compute result.'],
      ['Route request', 'The lane the contributor believes should handle the packet.'],
      ['Snapshot link', 'Candidate ID and content hash so review is tied to a specific public version.'],
      ['Review outcome', 'Operator decision, rationale, and any downstream queue item created.'],
    ],
    sections: [
      {
        heading: 'Why intake is gated',
        body:
          'TWOG wants outside work to be useful without letting public submissions mutate research records directly. Intake preserves openness while keeping record changes accountable.',
      },
      {
        heading: 'What can be submitted',
        body:
          'The first accepted packet types are evidence, critique, replication note, artifact, validation proposal, and compute result. Free-form discussion belongs elsewhere.',
      },
      {
        heading: 'What operators decide',
        body:
          'Operators decide whether the packet is complete, source-traceable, non-duplicative, and worth routing into an internal review lane.',
      },
      {
        heading: 'Why the form is paused',
        body:
          'The public check-in form can remain paused while TWOG tightens review rails. Payloads and templates still make the intended exchange model visible.',
      },
    ],
    endpoints: [
      {
        label: 'Contribution template',
        path: '/api/public-candidates/rly2608-pik3ca/contribution-template',
        href: '/api/public-candidates/rly2608-pik3ca/contribution-template',
        detail: 'The structured check-in packet shape for one candidate snapshot.',
      },
      {
        label: 'Check-in endpoint',
        path: '/api/public-candidates/rly2608-pik3ca/contributions',
        href: '/api/public-candidates/rly2608-pik3ca/contributions',
        detail: 'The endpoint shape for submissions when public intake is enabled.',
      },
    ],
    interpretationRules: [
      'A submitted packet is not an accepted correction.',
      'A contributor can challenge a claim without becoming part of the candidate decision log.',
      'Public intake should preserve provenance before it adds speed.',
      'No contribution triggers GPU compute or validation dispatch directly.',
    ],
    boundary:
      'Contribution Intake v1 does not create an open wiki. It creates an accountable queue for work that may later become evidence review, validation planning, or record updates.',
  },
  {
    methodId: 'md-smoke-v1',
    title: 'MD Smoke v1',
    version: 'v1',
    category: 'compute gate',
    status: 'operator-gated',
    appliesTo: 'Modal MD smoke jobs',
    claimsLevel: 'workflow proof',
    summary:
      'The first molecular-dynamics compute contract for TWOG: expert-reviewed inputs, small smoke settings, structured worker diagnostics, and ledgered artifacts.',
    heroStatement:
      'MD Smoke v1 proves that TWOG can submit, track, and inspect a molecular-dynamics style job. It does not prove a molecule works.',
    operatorLine: 'GPU work is approval-first, ledgered, and treated as workflow evidence until stronger science is attached.',
    flow: [
      {
        label: 'Input packet',
        detail:
          'The job requires protein PDB text, compound SMILES, target and compound names, simulation steps, temperature, and provenance for protein and ligand preparation.',
      },
      {
        label: 'Expert gate',
        detail:
          'A review packet checks whether the inputs and assumptions are suitable for a smoke run before live compute is allowed.',
      },
      {
        label: 'Worker execution',
        detail:
          'The Modal GPU worker validates inputs, prepares protein and ligand artifacts, runs the enabled smoke stages, and returns structured stage diagnostics.',
      },
      {
        label: 'Ledger',
        detail:
          'Provider job IDs, worker outputs, stage errors, artifacts, cost fields, and status transitions are persisted in the TWOG compute ledger.',
      },
      {
        label: 'Record attachment',
        detail:
          'A candidate can link to compute evidence only as an inspectable artifact, with clear limits on what the smoke result does and does not show.',
      },
    ],
    auditFields: [
      ['Inputs', 'Protein source, ligand source, SMILES, PDB validation, simulation settings, and preparation method.'],
      ['Approval', 'Expert approval metadata tied to the exact packet version.'],
      ['Worker stages', 'Input validation, protein prep, ligand 3D, ligand PDBQT, docking, MD smoke, warnings, and errors.'],
      ['Artifacts', 'Sanitized PDB, ligand files, output payloads, plots, and hashes when available.'],
      ['Cost and status', 'Provider job ID, status, timestamps, runner profile, and any cost estimates or actuals.'],
    ],
    sections: [
      {
        heading: 'What smoke means',
        body:
          'A smoke run is a systems test. It checks that the compute path, inputs, worker image, diagnostics, and artifact handling behave correctly.',
      },
      {
        heading: 'What it does not mean',
        body:
          'A smoke run is not a binding claim, efficacy claim, dosing claim, or safety claim. It is not a substitute for docking validation, long MD, free energy analysis, or wet-lab work.',
      },
      {
        heading: 'Why ligand prep matters',
        body:
          'The worker preserves ligand chemistry from SMILES through RDKit-based 3D generation before conversion into downstream formats, instead of relying on a lossy PDB-only ligand intermediate.',
      },
      {
        heading: 'Why structured failures matter',
        body:
          'A failed worker run is useful when the stage, command, return code, stdout tail, stderr tail, and payload are persisted. Opaque failures are treated as worker defects.',
      },
    ],
    interpretationRules: [
      'Successful smoke means the compute lane worked, not that the candidate is validated.',
      'Failed smoke can still improve the worker if it returns structured diagnostics.',
      'No public contribution should launch MD directly.',
      'Every compute result needs a method reference and ledger ID before it belongs on a candidate page.',
    ],
    boundary:
      'MD Smoke v1 is a compute-contract method. Scientific interpretation requires additional validated protocols, controls, replication, and expert review.',
  },
  {
    methodId: 'citation-dedupe-v1',
    title: 'Citation Dedupe v1',
    version: 'v1',
    category: 'evidence hygiene',
    status: 'active',
    appliesTo: 'briefs and candidate references',
    claimsLevel: 'provenance repair',
    summary:
      'How TWOG collapses duplicate citation refs, preserves merged provenance, and prevents repeated chunks from masquerading as independent evidence.',
    heroStatement:
      'Citation Dedupe v1 keeps the evidence count honest. Multiple chunks can support a claim, but they should not look like multiple independent papers when they came from the same source.',
    operatorLine: 'Evidence volume is not evidence diversity.',
    flow: [
      {
        label: 'Collect refs',
        detail:
          'Source briefs and candidate snapshots may contain citation labels, source URLs, identifiers, research object IDs, titles, and chunk IDs.',
      },
      {
        label: 'Build keys',
        detail:
          'The dedupe layer compares DOI, PMID, PMCID, NCT ID, research object ID, normalized title, and chunk provenance.',
      },
      {
        label: 'Choose primary',
        detail:
          'Duplicate groups retain one primary public citation while preserving merged citation IDs and duplicate matches.',
      },
      {
        label: 'Preserve support',
        detail:
          'The system keeps the supported claim, evidence kind, source sections, and chunk IDs so source density is not lost.',
      },
      {
        label: 'Flag gaps',
        detail:
          'Missing identifiers, unresolved refs, stale validation refs, and weak provenance become visible repair tasks.',
      },
    ],
    auditFields: [
      ['Identifier keys', 'DOI, PMID, PMCID, NCT ID, research object ID, normalized title, and source URL.'],
      ['Duplicate group', 'Primary citation ID, duplicate citation IDs, duplicate count, and matched keys.'],
      ['Merged provenance', 'Research object IDs, chunk IDs, source sections, and source brief IDs.'],
      ['Supported claim', 'The exact claim the citation is being used to support or challenge.'],
      ['Repair status', 'Whether the public reference is resolved, partially resolved, or still unresolved.'],
    ],
    sections: [
      {
        heading: 'Why dedupe is needed',
        body:
          'A single full-text article can produce many chunks and many local citation labels. Without dedupe, one paper can appear to be an entire evidence base.',
      },
      {
        heading: 'What gets merged',
        body:
          'Duplicate citation labels are merged by durable identifiers first, then research object IDs, then normalized titles when identifiers are missing.',
      },
      {
        heading: 'What does not get erased',
        body:
          'Chunk-level provenance, duplicate labels, source sections, and supported claims remain visible so reviewers can see both evidence diversity and evidence density.',
      },
      {
        heading: 'How unresolved refs should read',
        body:
          'Unresolved citation labels should be treated as public defects. They can remain visible temporarily, but they should trigger citation repair before a record is promoted.',
      },
    ],
    interpretationRules: [
      'A high citation count can collapse into one source after dedupe.',
      'Duplicate chunks can strengthen source coverage but not independent support.',
      'Missing identifiers should reduce public confidence until repaired.',
      'Dedupe should preserve provenance, not hide it.',
    ],
    boundary:
      'Citation Dedupe v1 improves evidence hygiene. It does not evaluate whether the source itself is high quality or sufficient for validation.',
  },
  {
    methodId: 'omics-readout-v1',
    title: 'Omics Readout v1',
    version: 'v1',
    category: 'processed omics',
    status: 'active internal',
    appliesTo: 'processed expression matrices',
    claimsLevel: 'descriptive signal',
    summary:
      'The processed-first omics method for scoring target expression and gene-set signals from public matrices before any raw FASTQ/BAM reprocessing.',
    heroStatement:
      'Omics Readout v1 starts with processed public matrices. It computes reproducible target and pathway scores first, then lets specialist agents interpret what the numbers can and cannot support.',
    operatorLine: 'Deterministic math first. LLM interpretation second.',
    flow: [
      {
        label: 'Discover matrix',
        detail:
          'The resolver looks for GEO or SRA-derived processed matrix files and skips raw CEL, FASTQ, or BAM files with explicit reason codes.',
      },
      {
        label: 'Cache artifact',
        detail:
          'Accepted files are downloaded into artifact storage so the readout can be rerun against the same input.',
      },
      {
        label: 'Parse table',
        detail:
          'The parser converts supported matrix formats into sample-by-gene tables with dataset labels where available.',
      },
      {
        label: 'Score panels',
        detail:
          'TWOG computes VIM target expression plus mesenchymal/ECM, angiogenesis/endothelial, and coagulation/vascular injury gene-set scores.',
      },
      {
        label: 'Review result',
        detail:
          'The omics validation agent interprets the computed readout, separating descriptive evidence from tumor/control differential claims.',
      },
    ],
    auditFields: [
      ['Dataset source', 'Accession, source URL, artifact hash, and matrix type.'],
      ['Parsing status', 'Supported format, skipped raw file reason, missing labels, and unsupported columns.'],
      ['Normalization', 'Count-like CPM/log handling or dataset-level z-scoring for processed expression.'],
      ['Scores', 'Target expression and gene-set score distributions by sample or cohort.'],
      ['Interpretation limits', 'Whether labels support tumor/control comparison or only descriptive evidence.'],
    ],
    sections: [
      {
        heading: 'Why processed-first',
        body:
          'Processed matrices are small enough for local or Dagster CPU execution and let TWOG test whether a signal is worth deeper raw-data reprocessing later.',
      },
      {
        heading: 'What gets scored',
        body:
          'The first built-in panels cover vimentin target expression, mesenchymal/ECM state, angiogenesis/endothelial signal, and coagulation/vascular injury signal.',
      },
      {
        heading: 'How claims are bounded',
        body:
          'If sample labels support tumor/control comparison, the method can report cohort differences. If labels are missing, the output stays descriptive.',
      },
      {
        heading: 'When heavy GPU compute enters',
        body:
          'The Modal GPU lane is reserved for later raw-data work: FASTQ/BAM reprocessing, large single-cell jobs, spatial analysis, or containerized workflows that exceed local CPU scope.',
      },
    ],
    interpretationRules: [
      'Processed-matrix readouts are reproducible summaries, not final biological proof.',
      'Missing labels prevent differential claims.',
      'Unsupported raw files are recorded as negative coverage, not hidden failures.',
      'Agent review should explain how a signal changes candidate confidence.',
    ],
    boundary:
      'Omics Readout v1 does not reprocess raw sequencing data or prove mechanism. It produces bounded, reproducible expression evidence for specialist review.',
  },
];

export function getMethod(methodId: string): MethodRecord | undefined {
  return methods.find((method) => method.methodId === methodId);
}
