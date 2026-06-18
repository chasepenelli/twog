"""Autonomous candidate generation — propose NEW dockable candidates, gated on real resolvability.

Hermetic: a fake SMILES resolver + a monkeypatched verified target library (no network, no GPU). The
regression guard is that a generated candidate's docking lane actually resolves — i.e. it is real work
for the loop, never another no_runnable_proposal.
"""

from __future__ import annotations

import json

import pytest

from tests._helpers import *  # noqa: F401,F403
from tests._helpers import HSAResearchService, SQLiteResearchRepository

from hsa_research.ingestion_bridge import candidate_generator, lane_inputs, target_library
from hsa_research.ingestion_bridge.contracts import PublicCandidateLibraryRequest, PublicCandidateRecord

_VERIFIED_LIB = {
    "version": "target-library-v1",
    "entries": {
        "PIK3CA": {
            "verified": True, "pdb_id": "4JPS", "redock_rmsd": 0.745,
            "receptor_pdb": "ATOM      1  CA  ALA A   1       0.000   0.000   0.000\nEND\n",
            "box": {"center_x": 0.0, "center_y": 0.0, "center_z": 0.0, "size_x": 20.0, "size_y": 20.0, "size_z": 20.0},
        }
    },
}


class _FakeResolver:
    """Stands in for NetworkInputResolvers — returns SMILES only for the therapies we choose to know."""

    def __init__(self, known: dict[str, str] | None = None) -> None:
        self._known = {k.lower(): v for k, v in (known or {}).items()}

    def compound_smiles(self, name: str) -> str | None:
        return self._known.get((name or "").lower())

    def target_structure(self, target: str):
        return None

    def protein_sequence(self, target: str):
        return None


@pytest.fixture
def verified_library(monkeypatch):
    monkeypatch.setattr(target_library, "load_target_library", lambda *a, **k: _VERIFIED_LIB)
    return _VERIFIED_LIB


def _service(tmp_path, *, known=None):
    repo = SQLiteResearchRepository(tmp_path / "gen.sqlite3", seed=False)
    service = HSAResearchService(repo)
    service.input_resolvers = _FakeResolver({"copanlisib": "C1=CC=CC=C1"} if known is None else known)
    return service


def _seed_file(tmp_path, rows):
    p = tmp_path / "seed.json"
    p.write_text(json.dumps({"version": "candidate-generation-seed-v1", "candidates": rows}))
    return str(p)


def test_id_scheme_stable():
    assert candidate_generator.candidate_id_for("Copanlisib", "PIK3CA") == "copanlisib-pik3ca"
    assert candidate_generator.candidate_id_for("copanlisib", "PIK3CA") == "copanlisib-pik3ca"  # idempotent
    assert candidate_generator.candidate_id_for("Sunitinib", "KDR") == "sunitinib-vegfr2"


def test_generate_seeds_runnable_candidate(tmp_path, verified_library):
    service = _service(tmp_path)
    seed = _seed_file(tmp_path, [{"compound": "copanlisib", "target": "PIK3CA", "evidence_refs": ["curate:x"]}])
    manifest = service.generate_candidate_ideas(source="curated_seed", seed_path=seed)

    assert manifest.output_refs["rollup"]["seeded"] == 1
    cand = service.get_public_candidate("copanlisib-pik3ca")
    assert cand is not None and cand.validation_ready is True
    # the regression guard: the generated candidate's docking lane actually resolves
    res = lane_inputs.resolve(cand, "docking", resolvers=service.input_resolvers)
    assert res.resolved is True


def test_unresolvable_therapy_is_rejected_not_seeded(tmp_path, verified_library):
    service = _service(tmp_path, known={})  # resolver knows no SMILES
    seed = _seed_file(tmp_path, [{"compound": "copanlisib", "target": "PIK3CA"}])
    manifest = service.generate_candidate_ideas(source="curated_seed", seed_path=seed)

    assert manifest.output_refs["rollup"]["seeded"] == 0
    assert manifest.output_refs["rollup"]["rejected"] == 1
    assert service.get_public_candidate("copanlisib-pik3ca") is None


def test_unverified_target_is_rejected(tmp_path, verified_library):
    service = _service(tmp_path, known={"everolimus": "CCO"})
    seed = _seed_file(tmp_path, [{"compound": "everolimus", "target": "MTOR"}])  # MTOR not in verified lib
    manifest = service.generate_candidate_ideas(source="curated_seed", seed_path=seed)

    assert manifest.output_refs["rollup"]["seeded"] == 0
    assert manifest.output_refs["rollup"]["rejected"] == 1


def test_dedup_against_existing(tmp_path, verified_library):
    service = _service(tmp_path)
    service.seed_validation_ready_candidate(
        "copanlisib-pik3ca", title="pre-existing", evidence_refs=["curate:x"],
        targets=["PIK3CA"], candidate_therapies=["copanlisib"],
    )
    seed = _seed_file(tmp_path, [{"compound": "copanlisib", "target": "PIK3CA"}])
    manifest = service.generate_candidate_ideas(source="curated_seed", seed_path=seed)

    assert manifest.output_refs["rollup"]["seeded"] == 0
    assert manifest.output_refs["rollup"]["skipped_exists"] == 1


def test_max_new_caps_seeding(tmp_path, verified_library):
    service = _service(tmp_path, known={"copanlisib": "C", "taselisib": "CC", "pictilisib": "CCC"})
    seed = _seed_file(tmp_path, [
        {"compound": "copanlisib", "target": "PIK3CA"},
        {"compound": "taselisib", "target": "PIK3CA"},
        {"compound": "pictilisib", "target": "PIK3CA"},
    ])
    manifest = service.generate_candidate_ideas(source="curated_seed", seed_path=seed, max_new=2)
    assert manifest.output_refs["rollup"]["seeded"] == 2


def test_dry_run_persists_nothing(tmp_path, verified_library):
    service = _service(tmp_path)
    seed = _seed_file(tmp_path, [{"compound": "copanlisib", "target": "PIK3CA"}])
    manifest = service.generate_candidate_ideas(source="curated_seed", seed_path=seed, dry_run=True)
    assert manifest.output_refs["rollup"]["seeded"] == 1  # would-seed
    assert service.get_public_candidate("copanlisib-pik3ca") is None  # but nothing persisted


def test_idempotent_double_run(tmp_path, verified_library):
    service = _service(tmp_path)
    seed = _seed_file(tmp_path, [{"compound": "copanlisib", "target": "PIK3CA"}])
    service.generate_candidate_ideas(source="curated_seed", seed_path=seed)
    second = service.generate_candidate_ideas(source="curated_seed", seed_path=seed)
    assert second.output_refs["rollup"]["seeded"] == 0
    assert second.output_refs["rollup"]["skipped_exists"] == 1
    cands = service.list_public_candidates(PublicCandidateLibraryRequest(limit=500)).candidates
    assert sum(1 for c in cands if c.candidate_id == "copanlisib-pik3ca") == 1
