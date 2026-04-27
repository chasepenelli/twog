"""Local deterministic embeddings for the ingestion bridge retrieval foundation."""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
import json
import math
import re
from typing import Protocol
from uuid import UUID

from .contracts import DocumentChunk, EntityMention, ResearchObject, TextEmbedding

LOCAL_HASH_EMBEDDING_MODEL = "local-hash-v1"
LOCAL_HASH_EMBEDDING_DIMENSIONS = 384
_TOKEN_PATTERN = re.compile(r"[a-z0-9]+(?:[._/-][a-z0-9]+)*")


class LocalDeterministicEmbeddingProvider:
    """Dependency-free feature-hashing embedder for local indexing and tests."""

    def __init__(
        self,
        *,
        embedding_model: str = LOCAL_HASH_EMBEDDING_MODEL,
        dimensions: int = LOCAL_HASH_EMBEDDING_DIMENSIONS,
    ) -> None:
        if dimensions < 1:
            raise ValueError("dimensions must be at least 1")
        self.embedding_model = embedding_model
        self.dimensions = dimensions

    @property
    def embedding_dimensions(self) -> int:
        return self.dimensions

    def embed(self, text: str) -> list[float]:
        return self.embed_text(text)

    def embed_text(self, text: str) -> list[float]:
        """Return a stable L2-normalized token-hash vector."""

        vector = [0.0] * self.dimensions
        for token in _tokenize(text):
            digest = sha256(f"{self.embedding_model}:{token}".encode("utf-8")).digest()
            index = int.from_bytes(digest[:8], "big") % self.dimensions
            sign = 1.0 if digest[8] & 1 else -1.0
            vector[index] += sign

        norm = math.sqrt(sum(value * value for value in vector))
        if norm == 0.0:
            return vector
        return [value / norm for value in vector]

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Return stable vectors for multiple texts."""

        return [self.embed_text(text) for text in texts]


@dataclass(frozen=True)
class EmbeddingIndexResult:
    embedding_model: str
    source_key: str | None = None
    chunks_seen: int = 0
    embeddings_created: int = 0
    embeddings_updated: int = 0
    embeddings_skipped: int = 0
    errors: tuple[str, ...] = field(default_factory=tuple)


class _EmbeddingRepository(Protocol):
    def list_document_chunks(
        self,
        object_id: UUID | None = None,
        source_key: str | None = None,
        object_type: str | None = None,
        limit: int | None = None,
    ) -> list[DocumentChunk]:
        ...

    def get_research_object(self, object_id: UUID) -> ResearchObject | None:
        ...

    def list_entity_mentions(
        self,
        *,
        source_key: str | None = None,
        object_id: UUID | None = None,
        chunk_id: UUID | None = None,
        entity_type: str | None = None,
        limit: int | None = None,
    ) -> list[EntityMention]:
        ...

    def list_text_embeddings(
        self,
        *,
        embedding_model: str | None = None,
        source_key: str | None = None,
        research_object_id: UUID | None = None,
        chunk_id: UUID | None = None,
        object_type: str | None = None,
        limit: int | None = None,
    ) -> list[TextEmbedding]:
        ...

    def get_text_embedding(self, embedding_id: UUID) -> TextEmbedding | None:
        ...

    def upsert_text_embedding(self, embedding: TextEmbedding) -> TextEmbedding:
        ...


def build_chunk_embedding_text(
    chunk: DocumentChunk,
    research_object: ResearchObject | None,
    entity_mentions: list[EntityMention] | None = None,
) -> str:
    """Build the deterministic text payload embedded for a document chunk."""

    lines: list[str] = []
    if research_object:
        _append_line(lines, "object_type", str(research_object.object_type))
        _append_line(lines, "source_key", research_object.source_key)
        _append_line(lines, "title", research_object.title)
        _append_line(lines, "publication_year", research_object.publication_year)
        _append_line(lines, "canonical_url", research_object.canonical_url)
        if research_object.identifiers:
            identifiers = "; ".join(
                f"{key}={value}"
                for key, value in sorted(research_object.identifiers.items())
                if value
            )
            _append_line(lines, "identifiers", identifiers)
        _append_line(lines, "object_abstract", research_object.abstract)

    _append_line(lines, "chunk_index", chunk.chunk_index)
    _append_line(lines, "section", chunk.section_label)
    _append_line(lines, "chunk_text", chunk.text_content)

    canonical_entities = _canonical_entity_context(entity_mentions or [])
    if canonical_entities:
        _append_line(lines, "canonical_entities", "; ".join(canonical_entities))

    return "\n".join(lines)


def index_embeddings_for_repository(
    repository: _EmbeddingRepository,
    source_key: str | None = None,
    limit: int | None = None,
    embedding_model: str = LOCAL_HASH_EMBEDDING_MODEL,
    force: bool = False,
) -> EmbeddingIndexResult:
    """Index local deterministic embeddings for repository document chunks."""

    provider = LocalDeterministicEmbeddingProvider(embedding_model=embedding_model)
    chunks = repository.list_document_chunks(source_key=source_key, limit=limit)
    created = 0
    updated = 0
    skipped = 0
    errors: list[str] = []

    for chunk in chunks:
        research_object = repository.get_research_object(chunk.research_object_id)
        entity_mentions = repository.list_entity_mentions(chunk_id=chunk.id)
        embedding_text = build_chunk_embedding_text(chunk, research_object, entity_mentions)
        embedding_content_hash = _stable_json_hash(
            {
                "embedding_model": embedding_model,
                "text": embedding_text,
            }
        )
        existing = _get_existing_embedding(repository, chunk.id, embedding_model)

        if existing and existing.content_hash == embedding_content_hash and not force:
            skipped += 1
            continue

        vector = provider.embed_text(embedding_text)
        saved = repository.upsert_text_embedding(
            TextEmbedding(
                chunk_id=chunk.id,
                research_object_id=chunk.research_object_id,
                chunk_index=chunk.chunk_index,
                source_key=research_object.source_key if research_object else None,
                object_type=research_object.object_type if research_object else None,
                content_hash=embedding_content_hash,
                embedding_model=embedding_model,
                embedding_dimensions=provider.dimensions,
                embedding=vector,
                text_preview=_text_preview(embedding_text),
                metadata={
                    "provider": "local_deterministic_hash",
                    "provider_version": "1",
                    "chunk_content_hash": chunk.content_hash,
                    "embedding_text_hash": embedding_content_hash,
                    "canonical_entity_count": len(_canonical_entity_context(entity_mentions)),
                },
            )
        )
        if repository.get_text_embedding(saved.embedding_id) is None:
            errors.append(f"embedding not readable after upsert: {saved.embedding_id}")

        if existing:
            updated += 1
        else:
            created += 1

    return EmbeddingIndexResult(
        embedding_model=embedding_model,
        source_key=source_key,
        chunks_seen=len(chunks),
        embeddings_created=created,
        embeddings_updated=updated,
        embeddings_skipped=skipped,
        errors=tuple(errors),
    )


def _append_line(lines: list[str], label: str, value: object | None) -> None:
    if value is None:
        return
    text = _normalize_whitespace(str(value))
    if text:
        lines.append(f"{label}: {text}")


def _canonical_entity_context(entity_mentions: list[EntityMention]) -> list[str]:
    entities: dict[tuple[str, str, str], str] = {}
    for mention in entity_mentions:
        external_ids = ", ".join(
            f"{key}={value}" for key, value in sorted(mention.external_ids.items()) if value
        )
        label = f"{mention.entity_type}: {mention.canonical_name}"
        if mention.normalized_key:
            label += f" [{mention.normalized_key}]"
        if external_ids:
            label += f" ({external_ids})"
        entities[(mention.entity_type, mention.canonical_name.lower(), mention.normalized_key)] = label
    return [entities[key] for key in sorted(entities)]


def _get_existing_embedding(
    repository: _EmbeddingRepository,
    chunk_id: UUID,
    embedding_model: str,
) -> TextEmbedding | None:
    matches = repository.list_text_embeddings(
        chunk_id=chunk_id,
        embedding_model=embedding_model,
        limit=1,
    )
    return matches[0] if matches else None


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _stable_json_hash(value: dict[str, object]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return sha256(encoded).hexdigest()


def _text_preview(text: str) -> str:
    return _normalize_whitespace(text)[:500]


def _tokenize(text: str) -> list[str]:
    return _TOKEN_PATTERN.findall(text.lower())
