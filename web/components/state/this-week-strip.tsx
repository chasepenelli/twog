import { CountUp } from "./count-up";

/* The signature moment: the engine's latest sweep, on first paint. The kill count and the survivor count
   count up; the negative controls it planted to test ITSELF (ethanol, aspirin) strike through in red.
   A machine executing its own dead-ends, before you scroll. All real numbers from the latest campaign. */

export function ThisWeekStrip({
  ruledOut,
  stillStanding,
  struck,
}: {
  ruledOut: number;
  stillStanding: number;
  struck: string[];
}) {
  return (
    <div className="mono" style={{ background: "#070707", border: "1px solid #1c1c1c", borderRadius: 16, padding: "18px 20px", marginBottom: 14, color: "#e9eaec" }}>
      <div style={{ fontSize: 11, letterSpacing: "0.14em", color: "#8a8f98", marginBottom: 13 }}>ITS LATEST SWEEP</div>
      <div style={{ display: "flex", gap: 30, flexWrap: "wrap", alignItems: "baseline" }}>
        <div>
          <div style={{ fontSize: 34, fontWeight: 700, color: "#ff6f63", lineHeight: 1 }}><CountUp to={ruledOut} /></div>
          <div style={{ fontSize: 12, color: "#8a8f98", marginTop: 6 }}>ruled out</div>
        </div>
        <div style={{ color: "#3a3a3a", fontSize: 26, alignSelf: "center" }}>·</div>
        <div>
          <div style={{ fontSize: 34, fontWeight: 700, color: "#3ec27a", lineHeight: 1 }}><CountUp to={stillStanding} /></div>
          <div style={{ fontSize: 12, color: "#8a8f98", marginTop: 6 }}>still standing · on two methods</div>
        </div>
      </div>
      {struck.length ? (
        <div style={{ marginTop: 16, fontSize: 12.5, color: "#8a8f98", lineHeight: 1.5 }}>
          It even tests — and kills — the dead-ends it plants to check itself:{" "}
          {struck.map((n, i) => (
            <span key={n}>
              <span className="strike">{n}</span>
              {i < struck.length - 1 ? ", " : "."}
            </span>
          ))}
        </div>
      ) : null}
    </div>
  );
}
