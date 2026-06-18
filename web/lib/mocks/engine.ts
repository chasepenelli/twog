import type { EngineState } from "@/lib/types/domain";

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
