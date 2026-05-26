'use client';

import { useEffect, useState } from 'react';
import {
  WorkPacketCard,
  WorkPacketCardData,
} from '@/components/proof-network/WorkPacketCard';
import {
  AcceptedCapsuleCard,
  AcceptedCapsuleCardData,
} from '@/components/proof-network/AcceptedCapsuleCard';

interface CandidateWorkbenchProps {
  candidateId: string;
  displayId: string;
}

interface WorkPacketResponse {
  work_packets?: WorkPacketCardData[];
  work_packet_count?: number;
  error?: string;
  message?: string;
}

interface ProofCapsuleResponse {
  proof_capsules?: AcceptedCapsuleCardData[];
  proof_capsule_count?: number;
  error?: string;
  message?: string;
}

export function CandidateWorkbench({ candidateId, displayId }: CandidateWorkbenchProps) {
  const [workPackets, setWorkPackets] = useState<WorkPacketCardData[] | null>(null);
  const [workPacketsError, setWorkPacketsError] = useState<string | null>(null);
  const [acceptedCapsules, setAcceptedCapsules] = useState<AcceptedCapsuleCardData[] | null>(null);
  const [capsulesError, setCapsulesError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    const workPacketsUrl = `/api/public-candidates/${encodeURIComponent(candidateId)}/work-packets`;
    const capsulesUrl = `/api/public-candidates/${encodeURIComponent(candidateId)}/proof-capsules`;

    Promise.all([
      fetch(workPacketsUrl, { cache: 'no-store' })
        .then(async (response) => {
          const body = (await response.json()) as WorkPacketResponse;
          if (!response.ok) {
            if (!cancelled) setWorkPacketsError(body.message ?? body.error ?? 'unable to load work packets');
            return [];
          }
          return body.work_packets ?? [];
        })
        .catch((err) => {
          if (!cancelled) setWorkPacketsError(String(err));
          return [];
        }),
      fetch(capsulesUrl, { cache: 'no-store' })
        .then(async (response) => {
          const body = (await response.json()) as ProofCapsuleResponse;
          if (!response.ok) {
            if (!cancelled) setCapsulesError(body.message ?? body.error ?? 'unable to load proof capsules');
            return [];
          }
          return body.proof_capsules ?? [];
        })
        .catch((err) => {
          if (!cancelled) setCapsulesError(String(err));
          return [];
        }),
    ]).then(([packets, capsules]) => {
      if (cancelled) return;
      setWorkPackets(packets);
      setAcceptedCapsules(capsules);
      setLoading(false);
    });

    return () => {
      cancelled = true;
    };
  }, [candidateId]);

  return (
    <section className="candidate-workbench" aria-label={`Workbench for ${displayId}`}>
      <div className="candidate-workbench-heading">
        <p className="section-kicker">Workbench</p>
        <h2>Pick up a live research packet</h2>
        <p>
          Each packet below is a bounded task on this record. Check one out, do the work,
          and submit a proof capsule. Accepted capsules earn proof points and appear in
          the decision history.
        </p>
      </div>

      <div className="work-packet-block">
        <div className="workbench-block-heading">
          <p className="section-kicker">Open packets</p>
          <span className="workbench-count">
            {workPackets ? `${workPackets.length} open` : loading ? 'loading…' : '—'}
          </span>
        </div>
        {workPacketsError ? (
          <p className="workbench-empty">{workPacketsError}</p>
        ) : workPackets && workPackets.length === 0 ? (
          <p className="workbench-empty">
            No open work packets yet. The hosted pipeline will seed packets here once the
            record is wired into the work-packets table.
          </p>
        ) : (
          <div className="work-packet-grid">
            {(workPackets ?? []).map((packet) => (
              <WorkPacketCard packet={packet} key={packet.work_packet_id} />
            ))}
          </div>
        )}
      </div>

      <div className="accepted-capsule-block">
        <div className="workbench-block-heading">
          <p className="section-kicker">Accepted proof capsules</p>
          <span className="workbench-count">
            {acceptedCapsules ? `${acceptedCapsules.length} accepted` : loading ? 'loading…' : '—'}
          </span>
        </div>
        {capsulesError ? (
          <p className="workbench-empty">{capsulesError}</p>
        ) : acceptedCapsules && acceptedCapsules.length === 0 ? (
          <p className="workbench-empty">
            No accepted capsules yet. Once contributors submit work and operators accept
            it, attribution will appear here alongside the artifact manifest.
          </p>
        ) : (
          <div className="accepted-capsule-grid">
            {(acceptedCapsules ?? []).map((capsule) => (
              <AcceptedCapsuleCard capsule={capsule} key={capsule.proof_capsule_id} />
            ))}
          </div>
        )}
      </div>
    </section>
  );
}
