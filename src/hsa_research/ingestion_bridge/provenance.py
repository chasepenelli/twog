"""Cryptographic provenance for proof capsules — tamper-evident lineage + unforgeable authorship.

Three standard primitives, no blockchain required for the first two:
- **content hashing** (each capsule already carries `content_hash`) makes a capsule tamper-evident;
- **parent_content_hash** links versions into a Merkle DAG → an immutable EDIT HISTORY
  ("proof of change") — exactly git's / Certificate Transparency's model;
- **Ed25519 signatures** over the content_hash make authorship/review unforgeable (anyone holding a
  principal's public key can verify it).

A periodic **Merkle root** over the capsule log (`merkle_root`) is what you would *anchor* to a
public ledger (e.g. OpenTimestamps → Bitcoin) for decentralized, third-party verifiability — the one
job a blockchain genuinely earns. That step is opt-in and not built here.

Honest limit: this proves **integrity** (unchanged, and when it existed), NOT **validity** (that the
science is correct). Integrity complements falsification-first rigor; it cannot replace it.
"""

from __future__ import annotations

import hashlib

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey


# --- authorship: Ed25519 keypairs + signatures over a capsule's content_hash --------------------
def generate_keypair() -> tuple[str, str]:
    """Return (private_key_hex, public_key_hex). The principal keeps the private key; the registry
    stores only the public key (CollaboratorRecord.public_key)."""
    sk = Ed25519PrivateKey.generate()
    return sk.private_bytes_raw().hex(), sk.public_key().public_bytes_raw().hex()


def sign(content_hash: str, private_key_hex: str) -> str:
    """Sign a capsule's content_hash with a principal's private key. Returns a hex signature."""
    sk = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(private_key_hex))
    return sk.sign(content_hash.encode("utf-8")).hex()


def verify(content_hash: str, signature_hex: str, public_key_hex: str) -> bool:
    """True iff `signature_hex` is a valid Ed25519 signature of `content_hash` by `public_key_hex`.
    Anyone can run this — that's the point."""
    try:
        pk = Ed25519PublicKey.from_public_bytes(bytes.fromhex(public_key_hex))
        pk.verify(bytes.fromhex(signature_hex), content_hash.encode("utf-8"))
        return True
    except (InvalidSignature, ValueError):
        return False


# --- lineage: a hash-linked Merkle DAG of capsule versions ("proof of change") ------------------
def verify_lineage(chain: list[tuple[str, str | None, int]]) -> tuple[bool, list[str]]:
    """Verify an edit chain. `chain` is a list of (content_hash, parent_content_hash, lineage_index).
    Genesis (index 0) must have no parent; every later version's parent_content_hash must equal the
    previous version's content_hash. Tampering with any version's content changes its hash and breaks
    the child's link → detected. Returns (ok, problems)."""
    problems: list[str] = []
    ordered = sorted(chain, key=lambda c: c[2])
    prev_hash: str | None = None
    for i, (content_hash, parent, idx) in enumerate(ordered):
        if idx != i:
            problems.append(f"lineage_index gap at position {i}: got {idx}")
        if i == 0:
            if parent is not None:
                problems.append("genesis version must have no parent_content_hash")
        elif parent != prev_hash:
            problems.append(f"broken link at index {idx}: parent {parent!r} != prior content_hash {prev_hash!r}")
        prev_hash = content_hash
    return (not problems), problems


def next_lineage(parent_content_hash: str, parent_index: int) -> tuple[str, int]:
    """Helper for the next version in a chain: (parent_content_hash, parent_index + 1)."""
    return parent_content_hash, parent_index + 1


# --- anchoring: a Merkle root over many capsules (what you'd timestamp to a public ledger) -------
def _leaf(h: str) -> bytes:
    return hashlib.sha256(b"\x00" + h.encode("utf-8")).digest()


def _node(a: bytes, b: bytes) -> bytes:
    return hashlib.sha256(b"\x01" + a + b).digest()


def merkle_root(content_hashes: list[str]) -> str | None:
    """Deterministic Merkle root over a set of capsule content_hashes. Anchor THIS (not the data) to
    a public ledger to prove the whole evidence log existed and was not rewritten. None if empty."""
    if not content_hashes:
        return None
    layer = [_leaf(h) for h in content_hashes]
    while len(layer) > 1:
        nxt = []
        for i in range(0, len(layer), 2):
            a = layer[i]
            b = layer[i + 1] if i + 1 < len(layer) else layer[i]  # duplicate last if odd
            nxt.append(_node(a, b))
        layer = nxt
    return layer[0].hex()
