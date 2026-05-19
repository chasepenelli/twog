import { Composition } from "remotion";
import { PipelineJourney, PipelineJourneyProps } from "./PipelineJourney";
import { MoleculeReveal, MoleculeRevealProps } from "./MoleculeReveal";

const defaultPipelineProps: PipelineJourneyProps = {};

const defaultMoleculeProps: MoleculeRevealProps = {
  compoundName: "TWOG-001",
  targetGene: "cKDR",
  compositeScore: 0.8542,
  qedScore: 0.72,
  moleculeSvg: "",
};

export const PipelineVideo: React.FC = () => {
  return (
    <>
      <Composition
        id="PipelineJourney"
        component={PipelineJourney}
        durationInFrames={450}
        fps={30}
        width={1920}
        height={1080}
        defaultProps={defaultPipelineProps}
      />
      <Composition
        id="MoleculeReveal"
        component={MoleculeReveal}
        durationInFrames={150}
        fps={30}
        width={1280}
        height={720}
        defaultProps={defaultMoleculeProps}
      />
    </>
  );
};
