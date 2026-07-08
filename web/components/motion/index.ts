/* Motion toolkit barrel — one import surface (mirrors lib/api/index.ts).
   P1: Reveal + MotionProvider. P2: StaggerGroup/Parallax/PinnedSection/TextReveal/MagneticButton/
   useScrollProgress. P3: HeroField (added when the WebGL hero lands). */

export { MotionProvider, useReducedMotion } from "./motion-provider";
export { Reveal } from "./reveal";
export { StaggerGroup } from "./stagger-group";
export { Parallax } from "./parallax";
export { PinnedSection } from "./pinned-section";
export { TextReveal } from "./text-reveal";
export { MagneticButton } from "./magnetic-button";
export { useScrollProgress } from "./use-scroll-progress";
export { HeroFieldClient } from "./hero-field.client";
export { stateForStatus, type FieldPoint } from "./field-shared";
