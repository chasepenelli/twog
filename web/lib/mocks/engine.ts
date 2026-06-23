import type { ActivityFeed, EngineState } from "@/lib/types/domain";

/** Live "research state" fixture — mirrors the real twog.bio dashboard numbers. */
export const MOCK_ENGINE_STATE: EngineState = {
  online: true,
  context: "canine HSA × human AS",
  phase: "phase 0 locked",
  tracks: "free track live · paid track running",
  headline: {
    hypothesesFalsified: 3,
    validatedResults: 4,
    computeLanes: 4,
    testsPassing: 574,
    coverage: "76.5%",
    bestRedockRmsd: "1.80 Å",
  },
  loop: [
    { key: "hypothesize", title: "Hypothesize", blurb: "With the test that would kill it." },
    { key: "compute", title: "Compute", blurb: "Pluggable GPU lanes — dock, co-fold, MD, omics." },
    { key: "falsify", title: "Falsify", blurb: "Cheap pre-registered crux, run first.", live: true },
    { key: "capsule", title: "Capsule", blurb: "Portable evidence: signal, confidence, limits." },
    { key: "compound", title: "Compound", blurb: "Publish datasets & models; the next run gets cheaper." },
  ],
  lanes: [
    { lane: "gnina docking", sublabel: "CNN docking · gnina v1.3.1", compute: "A100", status: "verified", lastResult: "alpelisib→PI3Kα redock · RMSD 1.80 Å" },
    { lane: "Boltz-2 cofolding", sublabel: "structure + affinity", compute: "A100", status: "verified", lastResult: "alpelisib–PI3Kα · iptm 0.99 · ~11 nM" },
    { lane: "OpenMM MD", sublabel: "checkpoint / resume", compute: "T4·GPU", status: "verified", lastResult: "cross-invocation resume on durable Volume" },
    { lane: "Omics TME review", sublabel: "deconvolution · purity-adjusted", compute: "CPU", status: "verified", lastResult: "PIK3CA crux · purity confound caught" },
  ],
};

/** Live activity-feed fixture — a real-shaped reverse-chron stream. `idle:false` here for a lively mock;
 *  the real endpoint reports honest idle ($0) when no compute job is running. */
export const MOCK_ACTIVITY_FEED: ActivityFeed = {
  running_jobs: 1,
  idle: false,
  idle_reason: null,
  last_event_at: "2026-06-18T22:05:00+00:00",
  events: [
    { type: "capsule", occurred_at: "2026-06-18T22:05:00+00:00", status: "submitted",
      title: "capsule · supports · docking", signal: "supports", candidate_id: "cand_pik3ca_immunosuppr", capsule_id: "cap_pik3ca_supports" },
    { type: "compute", occurred_at: "2026-06-18T22:03:00+00:00", status: "running",
      title: "docking lane · modal/gpu_a100", candidate_id: "cand_pik3ca_immunosuppr", lane: "docking" },
    { type: "agent", occurred_at: "2026-06-18T22:01:00+00:00", status: "completed",
      title: "active_falsification_planner · completed" },
    { type: "capsule", occurred_at: "2026-06-18T21:40:00+00:00", status: "submitted",
      title: "capsule · refutes · omics", signal: "refutes", candidate_id: "cand_mtor_alpelisib_synergy", capsule_id: "cap_mtor_refutes" },
    { type: "campaign", occurred_at: "2026-06-18T20:00:00+00:00", status: "completed",
      title: "Falsification campaign — 4 candidates" },
  ],
};
