"""Text chunking utilities for local ingestion."""

from __future__ import annotations

import re
from uuid import UUID

from .contracts import DocumentChunk
from .local_store import stable_json_hash


def chunk_text(
    research_object_id: UUID,
    text: str,
    *,
    section_label: str | None = None,
    start_index: int = 0,
    max_chars: int = 2800,
    overlap_chars: int = 240,
    metadata: dict | None = None,
) -> list[DocumentChunk]:
    """Split text into stable paragraph-aware chunks.

    This intentionally uses simple deterministic chunking. We can swap in a
    tokenizer-aware splitter later without changing the storage contract.
    """

    cleaned = _clean_text(text)
    if not cleaned:
        return []

    chunks: list[DocumentChunk] = []
    start = 0
    index = start_index
    while start < len(cleaned):
        end = min(start + max_chars, len(cleaned))
        if end < len(cleaned):
            boundary = max(cleaned.rfind("\n\n", start, end), cleaned.rfind(". ", start, end))
            if boundary > start + max_chars // 2:
                end = boundary + (2 if cleaned[boundary : boundary + 2] == ". " else 0)

        content = cleaned[start:end].strip()
        if content:
            chunks.append(
                DocumentChunk(
                    research_object_id=research_object_id,
                    chunk_index=index,
                    section_label=section_label,
                    text_content=content,
                    content_hash=stable_json_hash(
                        {
                            "object_id": str(research_object_id),
                            "chunk_index": index,
                            "text": content,
                        }
                    ),
                    token_count=_approx_token_count(content),
                    char_start=start,
                    char_end=end,
                    metadata=metadata or {},
                )
            )
            index += 1

        if end >= len(cleaned):
            break
        start = max(end - overlap_chars, start + 1)

    return chunks


def _clean_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _approx_token_count(text: str) -> int:
    return max(1, round(len(text.split()) * 1.25))
