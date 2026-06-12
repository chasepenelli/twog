"""Failure Corpus (increment 6) — the queryable record of negative knowledge.

Pure and deterministic, derived from the persistent proof-capsule ledger (no new storage): a capsule
carrying a pre-registration whose kill-criterion was met is a REFUTED TEST; a capsule that controlled
a confound is a CAUGHT CONFOUND. This is the accumulating "what have we ruled out, and why" asset —
the planner reads it to deprioritize already-settled approaches (a novelty penalty), and operators can
query it to see the dead-ends without re-deriving them.
"""

from __future__ import annotations

from typing import Any


def _payload(capsule: Any) -> dict[str, Any]:
    payload = getattr(capsule, "payload", {})
    return payload if isinstance(payload, dict) else {}


def _kill_met(prereg: dict[str, Any], signal: Any) -> bool:
    kill_criterion = prereg.get("kill_criterion") or {}
    return signal is not None and signal == kill_criterion.get("observed_signal_kills")


def derive(capsules: list[Any]) -> list[dict[str, Any]]:
    """Return failure-corpus entry dicts derived from a candidate's capsules."""
    entries: list[dict[str, Any]] = []
    for capsule in capsules:
        payload = _payload(capsule)
        capsule_id = getattr(capsule, "capsule_id", None)
        signal = payload.get("signal")

        prereg = payload.get("falsification_preregistration")
        if isinstance(prereg, dict) and _kill_met(prereg, signal):
            lane = prereg.get("lane")
            entries.append(
                {
                    "kind": "refuted_test",
                    "lane": lane,
                    "validation_type": prereg.get("validation_type"),
                    "observed_signal": signal,
                    "preregistration_hash": prereg.get("preregistration_hash"),
                    "related_capsule_id": capsule_id,
                    "summary": (
                        f"The {lane} test met its pre-registered kill-criterion "
                        f"(observed '{signal}') — this approach is settled."
                    ),
                }
            )

        confound = payload.get("controls_confound")
        if confound:
            entries.append(
                {
                    "kind": "caught_confound",
                    "confound": confound,
                    "lane": payload.get("validation_type"),
                    "validation_type": payload.get("validation_type"),
                    "observed_signal": signal,
                    "related_capsule_id": capsule_id,
                    "summary": f"A control for the {confound} confound was run (observed '{signal}').",
                }
            )
    return entries


def ruled_out_lanes(capsules: list[Any]) -> set[str]:
    """Lanes whose test already met its kill-criterion — approaches the planner should not revisit."""
    return {e["lane"] for e in derive(capsules) if e["kind"] == "refuted_test" and e.get("lane")}
