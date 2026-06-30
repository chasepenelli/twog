/**
 * Shared evidence design language — the plain-English vocabulary the public site speaks.
 *
 * Two honesty-critical maps live here:
 *  - LANE_QUESTION / LANE_SHORT are keyed on the rubric's lane KEY ("docking", "cofolding", "md",
 *    "omics") — NOT the home page's display names ("gnina docking", "OpenMM MD"). Mixing them silently
 *    blanks the tracker, so everything derives from the lane key.
 *  - the verdict + lane-state derivations stay faithful to the engine: a single refuted lane rules the
 *    whole idea out (and visually breaks the chain after it); a supporting-but-unaudited lane is
 *    "still standing", never "proven".
 */

import type { MoonshotRubric, RubricLaneTest } from "@/lib/types/domain";

// ---- verdict -------------------------------------------------------------------------------------

export type Verdict = "still-standing" | "ruled-out" | "needs-more";

export const VERDICT_META: Record<Verdict, { label: string; tone: string; gloss: string }> = {
  "still-standing": {
    label: "still standing",
    tone: "b-green",
    gloss: "It has survived every test we've run so far. Nothing here is proven — that's the point.",
  },
  "ruled-out": {
    label: "ruled out",
    tone: "b-red",
    gloss: "A pre-registered test disproved it. Being wrong, on purpose, is how this works.",
  },
  "needs-more": {
    label: "needs more testing",
    tone: "b-amber",
    gloss: "Tested as far as its inputs allowed; no verdict yet — more lanes still to run.",
  },
};

/** The plain verdict for a candidate, derived from its rubric. A single refuted lane rules it out. */
export function verdictOf(rubric: MoonshotRubric | null | undefined): Verdict {
  if (!rubric) return "needs-more";
  const tp = rubric.test_plan ?? [];
  if (tp.some((t) => t.standing === "refuted") || rubric.net_signal === "refutes") return "ruled-out";
  if (tp.some((t) => t.standing === "supports_unaudited" || t.standing === "controlled") || rubric.net_signal === "supports")
    return "still-standing";
  return "needs-more";
}

// ---- lane track ----------------------------------------------------------------------------------

/** lane KEY -> the plain-English question that lane answers (keyed on "docking"/"cofolding"/"md"/"omics"). */
export const LANE_QUESTION: Record<string, string> = {
  docking: "Does the drug physically grip its target?",
  cofolding: "Does a second, independent method agree it binds?",
  md: "Does that grip hold once the atoms start moving?",
  omics: "Does the same signal show up in real tumors — dog and human?",
};
export const LANE_SHORT: Record<string, string> = {
  docking: "Docking",
  cofolding: "Cofolding",
  md: "Stability",
  omics: "Cross-species",
};

export type LaneState = "held" | "now" | "not-yet" | "ruled-out" | "controlled";
export interface LaneStep {
  lane: string;
  short: string;
  question: string;
  state: LaneState;
  /** true for lanes that come AFTER a ruled-out lane — the chain is already broken, so they're moot. */
  broken: boolean;
}

const STATE_OF: Record<string, LaneState> = {
  supports_unaudited: "held",
  controlled: "controlled",
  queued: "now",
  untested: "not-yet",
  refuted: "ruled-out",
};
// when a lane appears more than once (e.g. the omics cross-species test + its confound audit), keep the
// most-advanced standing so the tracker shows the lane's real progress.
const RANK: Record<string, number> = { untested: 0, queued: 1, controlled: 2, supports_unaudited: 3, refuted: 4 };

/** Project a rubric's test plan into one ordered step per lane, in the canonical logical order, with the
 *  chain broken after any ruled-out lane (so "one failure kills the idea" is visible). */
export function deriveLaneTrack(rubric: MoonshotRubric | null | undefined): LaneStep[] {
  if (!rubric) return [];
  const byLane = new Map<string, RubricLaneTest>();
  for (const t of rubric.test_plan ?? []) {
    const prev = byLane.get(t.lane);
    if (!prev || (RANK[t.standing] ?? 0) > (RANK[prev.standing] ?? 0)) byLane.set(t.lane, t);
  }
  const order = ["docking", "cofolding", "md", "omics"];
  const lanes = [
    ...order.filter((l) => byLane.has(l)),
    ...[...byLane.keys()].filter((l) => !order.includes(l)),
  ];
  let broken = false;
  return lanes.map((lane) => {
    const t = byLane.get(lane)!;
    const state = STATE_OF[t.standing] ?? "not-yet";
    const step: LaneStep = {
      lane,
      short: LANE_SHORT[lane] ?? lane,
      question: LANE_QUESTION[lane] ?? "",
      state,
      broken,
    };
    if (state === "ruled-out") broken = true; // every lane AFTER a kill is moot
    return step;
  });
}

/** "Falsify: does honokiol engage PIK3CA in canine HSA × human angiosarcoma?" -> a clean public question. */
export function publicQuestion(title: string | undefined, fallback = ""): string {
  const t = (title ?? "").trim();
  if (!t) return fallback;
  return t.replace(/^falsify:\s*/i, (m) => "").replace(/^./, (c) => c.toUpperCase());
}
