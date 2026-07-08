"use client";

import { useEffect, useState } from "react";

import type { LoopStep } from "@/lib/types/domain";

/** The loop, turning: the lit node travels step-to-step every few seconds — watch it think a lap. */
export function LiveLoop({ steps }: { steps: LoopStep[] }) {
  const [active, setActive] = useState(0);
  useEffect(() => {
    if (steps.length < 2) return;
    const id = setInterval(() => setActive((a) => (a + 1) % steps.length), 2600);
    return () => clearInterval(id);
  }, [steps.length]);

  return (
    <div className="flow">
      {steps.map((s, i) => (
        <div key={s.key} className={`fstep${i === active ? " lit" : ""}`} style={{ transition: "background .55s ease" }}>
          <div className="s">0{i + 1}{i === active ? " · live" : ""}</div>
          <h4>{s.title}</h4>
          <p>{s.blurb}</p>
        </div>
      ))}
    </div>
  );
}
