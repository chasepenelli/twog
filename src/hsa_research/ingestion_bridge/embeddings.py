"""Local deterministic embeddings for the ingestion bridge retrieval foundation."""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
import json
import math
import os
import re
import time
from typing import Any, Protocol
from urllib import error as urllib_error
from urllib import request as urllib_request
from uuid import UUID

from .contracts import DocumentChunk, EmbeddingCoverageSummary, EntityMention, ResearchObject, TextEmbedding

LOCAL_HASH_EMBEDDING_MODEL = "local-hash-v1"
LOCAL_HASH_EMBEDDING_DIMENSIONS = 384
OPENROUTER_EMBEDDING_MODEL_SMALL = "openrouter:openai/text-embedding-3-small"
OPENROUTER_EMBEDDING_MODEL_LARGE = "openrouter:openai/text-embedding-3-large"
OPENROUTER_EMBEDDING_MODELS = {
    OPENROUTER_EMBEDDING_MODEL_SMALL,
    OPENROUTER_EMBEDDING_MODEL_LARGE,
}
OPENROUTER_MODEL_PREFIX = "openrouter:"
OPENROUTER_EMBEDDINGS_URL = "https://openrouter.ai/api/v1/embeddings"
_TOKEN_PATTERN = re.compile(r"[a-z0-9]+(?:[._/-][a-z0-9]+)*")


class EmbeddingProvider(Protocol):
    embedding_model: str

    @property
    def embedding_dimensions(self) -> int:
        ...

    @property
    def provider_name(self) -> str:
        ...

    @property
    def provider_model(self) -> str:
        ...

    def embed_text(self, text: str) -> list[float]:
        ...

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        ...


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

    @property
    def provider_name(self) -> str:
        return "local_deterministic_hash"

    @property
    def provider_model(self) -> str:
        return self.embedding_model

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


class OpenRouterEmbeddingProvider:
    """OpenRouter-backed embedding provider for production retrieval."""

    def __init__(
        self,
        *,
        embedding_model: str = OPENROUTER_EMBEDDING_MODEL_SMALL,
        api_key: str | None = None,
        base_url: str = OPENROUTER_EMBEDDINGS_URL,
        dimensions: int | None = None,
        timeout_seconds: float = 60.0,
        max_retries: int = 3,
    ) -> None:
        self.embedding_model = _normalize_openrouter_embedding_model(embedding_model)
        self.api_model = self.embedding_model.removeprefix(OPENROUTER_MODEL_PREFIX)
        self.api_key = api_key or os.environ.get("OPENROUTER_API_KEY")
        self.base_url = base_url
        self.dimensions = dimensions
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        if not self.api_key:
            raise RuntimeError("OPENROUTER_API_KEY is required for OpenRouter embeddings")

    @property
    def embedding_dimensions(self) -> int:
        if self.dimensions is None:
            raise RuntimeError("embedding dimensions are unknown until the first OpenRouter response")
        return self.dimensions

    @property
    def provider_name(self) -> str:
        return "openrouter"

    @property
    def provider_model(self) -> str:
        return self.api_model

    def embed_text(self, text: str) -> list[float]:
        return self.embed_texts([text])[0]

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        payload: dict[str, Any] = {
            "model": self.api_model,
            "input": texts,
            "encoding_format": "float",
        }
        if self.dimensions is not None:
            payload["dimensions"] = self.dimensions
        response = self._post_json(payload)
        data = response.get("data")
        if not isinstance(data, list):
            raise RuntimeError("OpenRouter embeddings response did not include data")
        ordered = sorted(data, key=lambda item: int(item.get("index", 0)))
        vectors: list[list[float]] = []
        for item in ordered:
            raw_vector = item.get("embedding") if isinstance(item, dict) else None
            if not isinstance(raw_vector, list) or not raw_vector:
                raise RuntimeError("OpenRouter embeddings response contained an empty vector")
            vector = [float(value) for value in raw_vector]
            if self.dimensions is None:
                self.dimensions = len(vector)
            if len(vector) != self.dimensions:
                raise RuntimeError(
                    f"OpenRouter embedding dimension mismatch: expected {self.dimensions}, got {len(vector)}"
                )
            vectors.append(vector)
        if len(vectors) != len(texts):
            raise RuntimeError(f"OpenRouter returned {len(vectors)} embeddings for {len(texts)} inputs")
        return vectors

    def _post_json(self, payload: dict[str, Any]) -> dict[str, Any]:
        encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://twog.bio",
            "X-Title": "TWOG HSA Research Engine",
        }
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            req = urllib_request.Request(self.base_url, data=encoded, headers=headers, method="POST")
            try:
                with urllib_request.urlopen(req, timeout=self.timeout_seconds) as response:
                    body = response.read().decode("utf-8")
                return json.loads(body)
            except (urllib_error.HTTPError, urllib_error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                time.sleep(min(2.0, 0.25 * attempt))
        raise RuntimeError(f"OpenRouter embedding request failed: {last_error}") from last_error


@dataclass(frozen=True)
class EmbeddingIndexResult:
    embedding_model: str
    source_key: str | None = None
    chunks_seen: int = 0
    embeddings_created: int = 0
    embeddings_updated: int = 0
    embeddings_skipped: int = 0
    errors: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class EmbeddingMaintenanceResult:
    embedding_model: str
    prune_embedding_model: str | None
    source_key: str | None
    object_type: str | None
    orphan_embeddings_seen: int
    orphan_embeddings_deleted: int
    prune_enabled: bool
    embedding_coverage: EmbeddingCoverageSummary
    coverage: dict[str, Any]
    errors: tuple[str, ...] = field(default_factory=tuple)

    @property
    def passes_minimum_bar(self) -> bool:
        if self.errors:
            return False
        if self.embedding_coverage.total_chunks == 0:
            return self.embedding_coverage.embedded_chunks == 0
        return self.embedding_coverage.missing_chunks == 0

    def to_report(self) -> dict[str, Any]:
        return {
            "embedding_model": self.embedding_model,
            "prune_embedding_model": self.prune_embedding_model,
            "source_key": self.source_key,
            "object_type": self.object_type,
            "orphan_embeddings": {
                "seen": self.orphan_embeddings_seen,
                "deleted": self.orphan_embeddings_deleted,
                "prune_enabled": self.prune_enabled,
                "embedding_model": self.prune_embedding_model or "all",
            },
            "errors": list(self.errors),
            "embedding_coverage": self.embedding_coverage.model_dump(mode="json"),
            "coverage": self.coverage,
            "passes_minimum_bar": self.passes_minimum_bar,
            "passed": self.passes_minimum_bar,
        }


def build_embedding_provider(
    embedding_model: str = LOCAL_HASH_EMBEDDING_MODEL,
    *,
    dimensions: int | None = None,
) -> EmbeddingProvider:
    """Build the provider matching the stored embedding model identifier."""

    if is_openrouter_embedding_model(embedding_model):
        return OpenRouterEmbeddingProvider(embedding_model=embedding_model, dimensions=dimensions)
    return LocalDeterministicEmbeddingProvider(
        embedding_model=embedding_model,
        dimensions=dimensions or LOCAL_HASH_EMBEDDING_DIMENSIONS,
    )


def is_openrouter_embedding_model(embedding_model: str) -> bool:
    return embedding_model.startswith(OPENROUTER_MODEL_PREFIX) or embedding_model in {
        model.removeprefix(OPENROUTER_MODEL_PREFIX) for model in OPENROUTER_EMBEDDING_MODELS
    }


def default_embedding_model_for_environment() -> str:
    configured = os.environ.get("HSA_EMBEDDING_MODEL")
    if configured:
        return configured
    if os.environ.get("OPENROUTER_API_KEY"):
        return OPENROUTER_EMBEDDING_MODEL_LARGE
    return LOCAL_HASH_EMBEDDING_MODEL


def select_embedding_model_from_coverage(embedding_models: dict[str, int]) -> str:
    configured = os.environ.get("HSA_EMBEDDING_MODEL")
    if configured and embedding_models.get(configured):
        return configured
    preferred = default_embedding_model_for_environment()
    if embedding_models.get(preferred):
        return preferred
    return max(embedding_models.items(), key=lambda item: item[1])[0] if embedding_models else preferred


def _normalize_openrouter_embedding_model(embedding_model: str) -> str:
    if embedding_model.startswith(OPENROUTER_MODEL_PREFIX):
        return embedding_model
    return f"{OPENROUTER_MODEL_PREFIX}{embedding_model}"


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

    def embedding_coverage(
        self,
        *,
        source_key: str | None = None,
        object_type: str | None = None,
        embedding_model: str | None = None,
    ) -> EmbeddingCoverageSummary:
        ...

    def count_orphan_text_embeddings(
        self,
        *,
        embedding_model: str | None = None,
        source_key: str | None = None,
        object_type: str | None = None,
    ) -> int:
        ...

    def delete_orphan_text_embeddings(
        self,
        *,
        embedding_model: str | None = None,
        source_key: str | None = None,
        object_type: str | None = None,
    ) -> int:
        ...

    def coverage_summary(self) -> dict[str, Any]:
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
    batch_size: int = 32,
) -> EmbeddingIndexResult:
    """Index embeddings for repository document chunks."""

    chunks = repository.list_document_chunks(source_key=source_key, limit=limit)
    created = 0
    updated = 0
    skipped = 0
    errors: list[str] = []
    pending: list[tuple[DocumentChunk, ResearchObject | None, list[EntityMention], str, str, TextEmbedding | None]] = []

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

        pending.append((chunk, research_object, entity_mentions, embedding_text, embedding_content_hash, existing))

    try:
        provider = build_embedding_provider(embedding_model)
    except Exception as exc:
        errors.append(str(exc))
        provider = None

    if provider is not None:
        safe_batch_size = max(1, batch_size)
        for start in range(0, len(pending), safe_batch_size):
            batch = pending[start : start + safe_batch_size]
            try:
                vectors = provider.embed_texts([item[3] for item in batch])
            except Exception as exc:
                errors.append(f"embedding batch {start // safe_batch_size + 1} failed: {exc}")
                continue
            for (chunk, research_object, entity_mentions, embedding_text, embedding_content_hash, existing), vector in zip(
                batch,
                vectors,
                strict=True,
            ):
                saved = repository.upsert_text_embedding(
                    TextEmbedding(
                        chunk_id=chunk.id,
                        research_object_id=chunk.research_object_id,
                        chunk_index=chunk.chunk_index,
                        source_key=research_object.source_key if research_object else None,
                        object_type=research_object.object_type if research_object else None,
                        content_hash=embedding_content_hash,
                        embedding_model=provider.embedding_model,
                        embedding_dimensions=len(vector),
                        embedding=vector,
                        text_preview=_text_preview(embedding_text),
                        metadata={
                            "provider": provider.provider_name,
                            "provider_model": provider.provider_model,
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
        embedding_model=provider.embedding_model if provider is not None else embedding_model,
        source_key=source_key,
        chunks_seen=len(chunks),
        embeddings_created=created,
        embeddings_updated=updated,
        embeddings_skipped=skipped,
        errors=tuple(errors),
    )


def maintain_embedding_index(
    repository: _EmbeddingRepository,
    *,
    embedding_model: str = LOCAL_HASH_EMBEDDING_MODEL,
    prune_embedding_model: str | None = None,
    source_key: str | None = None,
    object_type: str | None = None,
    prune_orphans: bool = True,
) -> EmbeddingMaintenanceResult:
    """Prune unreadable embedding rows and report active-model coverage."""

    errors: list[str] = []
    orphan_embeddings_seen = repository.count_orphan_text_embeddings(
        embedding_model=prune_embedding_model,
        source_key=source_key,
        object_type=object_type,
    )
    orphan_embeddings_deleted = 0
    if prune_orphans:
        orphan_embeddings_deleted = repository.delete_orphan_text_embeddings(
            embedding_model=prune_embedding_model,
            source_key=source_key,
            object_type=object_type,
        )
        remaining_orphans = repository.count_orphan_text_embeddings(
            embedding_model=prune_embedding_model,
            source_key=source_key,
            object_type=object_type,
        )
        if remaining_orphans:
            errors.append(f"{remaining_orphans} orphan text embeddings remain after pruning")

    embedding_coverage = repository.embedding_coverage(
        source_key=source_key,
        object_type=object_type,
        embedding_model=embedding_model,
    )
    return EmbeddingMaintenanceResult(
        embedding_model=embedding_model,
        prune_embedding_model=prune_embedding_model,
        source_key=source_key,
        object_type=object_type,
        orphan_embeddings_seen=orphan_embeddings_seen,
        orphan_embeddings_deleted=orphan_embeddings_deleted,
        prune_enabled=prune_orphans,
        embedding_coverage=embedding_coverage,
        coverage=repository.coverage_summary(),
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
