"""Embedding model comparison helpers for retrieval quality checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .contracts import ResearchChunkSearchRequest
from .embeddings import (
    LOCAL_HASH_EMBEDDING_MODEL,
    OPENROUTER_EMBEDDING_MODEL_LARGE,
    OPENROUTER_EMBEDDING_MODEL_SMALL,
    index_embeddings_for_repository,
)
from .repository import ResearchRepository
from .service import HSAResearchService


DEFAULT_EMBEDDING_BAKEOFF_MODELS = (
    LOCAL_HASH_EMBEDDING_MODEL,
    OPENROUTER_EMBEDDING_MODEL_SMALL,
    OPENROUTER_EMBEDDING_MODEL_LARGE,
)


@dataclass(frozen=True)
class EmbeddingBenchmark:
    name: str
    query: str
    expected_terms: tuple[str, ...]
    preferred_source_keys: tuple[str, ...] = ()
    expected_title_terms: tuple[str, ...] = ()


DEFAULT_EMBEDDING_BENCHMARKS = (
    EmbeddingBenchmark(
        name="human_angiosarcoma_sorafenib_response",
        query="sorafenib KDR angiosarcoma clinical response",
        expected_terms=("sorafenib", "angiosarcoma", "response"),
        preferred_source_keys=("clinicaltrials_gov", "openalex", "pubmed", "europe_pmc"),
        expected_title_terms=("sorafenib", "angiosarcoma"),
    ),
    EmbeddingBenchmark(
        name="canine_sorafenib_safety",
        query="sorafenib dog safety toxicity dose limiting coagulopathy",
        expected_terms=("sorafenib", "dog", "safety", "toxicity"),
        preferred_source_keys=("pmc_oa", "pubmed", "europe_pmc"),
        expected_title_terms=("sorafenib", "dog"),
    ),
    EmbeddingBenchmark(
        name="comparative_vegfr_hsa",
        query="VEGFR KDR canine hemangiosarcoma angiosarcoma therapy",
        expected_terms=("vegfr", "kdr", "hemangiosarcoma", "therapy"),
        preferred_source_keys=("pubmed", "europe_pmc", "openalex", "pmc_oa"),
        expected_title_terms=("hemangiosarcoma", "angiosarcoma"),
    ),
)


def run_embedding_bakeoff(
    repository: ResearchRepository,
    *,
    embedding_models: tuple[str, ...] = DEFAULT_EMBEDDING_BAKEOFF_MODELS,
    benchmarks: tuple[EmbeddingBenchmark, ...] = DEFAULT_EMBEDDING_BENCHMARKS,
    limit: int = 5,
    index_missing: bool = False,
    index_limit: int | None = None,
    source_key: str | None = None,
    force: bool = False,
    batch_size: int = 32,
) -> dict[str, Any]:
    """Compare embedding models against fixed retrieval expectations."""

    service = HSAResearchService(repository)
    model_reports: list[dict[str, Any]] = []

    for model in embedding_models:
        index_report = None
        coverage_before = repository.embedding_coverage(source_key=source_key, embedding_model=model)
        if index_missing and (force or coverage_before.missing_chunks > 0):
            index_report = index_embeddings_for_repository(
                repository,
                source_key=source_key,
                limit=index_limit,
                embedding_model=model,
                force=force,
                batch_size=batch_size,
            )

        coverage = repository.embedding_coverage(source_key=source_key, embedding_model=model)
        benchmark_reports: list[dict[str, Any]] = []
        for benchmark in benchmarks:
            benchmark_reports.append(
                _run_one_benchmark(
                    service,
                    benchmark,
                    embedding_model=model,
                    limit=limit,
                    source_key=source_key,
                )
            )

        completed = [report for report in benchmark_reports if not report["errors"]]
        average_score = (
            round(sum(report["score"] for report in completed) / len(completed), 6)
            if completed
            else 0.0
        )
        model_reports.append(
            {
                "embedding_model": model,
                "average_score": average_score,
                "completed_benchmarks": len(completed),
                "benchmark_count": len(benchmark_reports),
                "coverage": coverage.model_dump(mode="json"),
                "index": None if index_report is None else index_report.__dict__,
                "benchmarks": benchmark_reports,
                "errors": [] if completed else ["no benchmarks completed"],
            }
        )

    ranked = sorted(
        model_reports,
        key=lambda report: (
            -float(report["average_score"]),
            -int(report["coverage"].get("embedded_chunks", 0)),
            report["embedding_model"],
        ),
    )
    best_model = ranked[0]["embedding_model"] if ranked and ranked[0]["average_score"] > 0 else None
    return {
        "best_model": best_model,
        "models": ranked,
        "benchmarks": [benchmark.__dict__ for benchmark in benchmarks],
    }


def _run_one_benchmark(
    service: HSAResearchService,
    benchmark: EmbeddingBenchmark,
    *,
    embedding_model: str,
    limit: int,
    source_key: str | None,
) -> dict[str, Any]:
    try:
        results = service.search_research_chunks(
            ResearchChunkSearchRequest(
                query=benchmark.query,
                source_key=source_key,
                embedding_model=embedding_model,
                limit=limit,
                max_chunk_chars=2000,
                include_keyword_fallback=False,
            )
        )
    except Exception as exc:
        return {
            "name": benchmark.name,
            "query": benchmark.query,
            "score": 0.0,
            "errors": [str(exc)],
            "hits": [],
        }

    hits = []
    for result in results.results:
        research_object = result.research_object
        hits.append(
            {
                "rank": result.rank,
                "score": result.score,
                "match_type": result.match_type,
                "source_key": research_object.source_key if research_object else None,
                "title": research_object.title if research_object else None,
                "chunk_id": str(result.chunk.id),
                "research_object_id": str(result.chunk.research_object_id),
            }
        )

    score = _score_hits(results.results, benchmark)
    return {
        "name": benchmark.name,
        "query": benchmark.query,
        "score": score,
        "search_mode": results.search_mode,
        "embedding_model": results.embedding_model,
        "top_source_key": hits[0]["source_key"] if hits else None,
        "top_title": hits[0]["title"] if hits else None,
        "errors": [],
        "hits": hits,
    }


def _score_hits(results: list[Any], benchmark: EmbeddingBenchmark) -> float:
    if not results:
        return 0.0
    top_text = _result_text(results[0])
    top_k_text = "\n".join(_result_text(result) for result in results[: min(3, len(results))])
    expected_terms = tuple(term.lower() for term in benchmark.expected_terms)
    title_terms = tuple(term.lower() for term in benchmark.expected_title_terms)
    top_term_score = _term_fraction(top_text, expected_terms)
    top_k_term_score = _term_fraction(top_k_text, expected_terms)
    source_score = 0.0
    if benchmark.preferred_source_keys:
        top_source = results[0].research_object.source_key if results[0].research_object else None
        source_score = 1.0 if top_source in benchmark.preferred_source_keys else 0.0
    title_score = _term_fraction((results[0].research_object.title or "").lower(), title_terms) if title_terms else 0.0
    return round(top_k_term_score * 0.55 + top_term_score * 0.25 + source_score * 0.10 + title_score * 0.10, 6)


def _term_fraction(text: str, terms: tuple[str, ...]) -> float:
    if not terms:
        return 1.0
    return sum(1 for term in terms if term in text) / len(terms)


def _result_text(result: Any) -> str:
    research_object = result.research_object
    parts = [
        research_object.title if research_object else "",
        research_object.abstract if research_object else "",
        result.chunk.section_label or "",
        result.chunk.text_content,
    ]
    return "\n".join(part for part in parts if part).lower()
