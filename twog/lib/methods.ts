export interface MethodRecord {
  methodId: string;
  title: string;
  summary: string;
  version: string;
  sections: Array<{
    heading: string;
    body: string;
  }>;
}

export const methods: MethodRecord[] = [
  {
    methodId: 'candidate-record-v1',
    title: 'Candidate Record v1',
    version: 'v1',
    summary:
      'The public candidate record converts internal TWOG research artifacts into a stable, inspectable page for human review, source audit, and decision history.',
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
  },
];

export function getMethod(methodId: string): MethodRecord | undefined {
  return methods.find((method) => method.methodId === methodId);
}
