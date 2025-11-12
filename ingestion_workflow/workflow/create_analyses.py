"""
Create analyses for extracted tables.

This workflow step coordinates the :class:`CreateAnalysesService` so every
``ArticleExtractionBundle`` yields an ``AnalysisCollection`` per table.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import Dict, List, Sequence

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.models import (
    AnalysisCollection,
    ArticleExtractionBundle,
    CreateAnalysesResult,
    ExtractedTable,
)
from ingestion_workflow.services import cache
from ingestion_workflow.services.create_analyses import (
    CreateAnalysesService,
    sanitize_table_id,
)


logger = logging.getLogger(__name__)


def run_create_analyses(
    bundles: Sequence[ArticleExtractionBundle],
    *,
    settings: Settings | None = None,
    extractor_name: str | None = None,
) -> Dict[str, Dict[str, AnalysisCollection]]:
    """
    Run the create-analyses step for a sequence of bundles.

    Returns
    -------
    dict
        Mapping of article hash IDs to table-id-indexed AnalysisCollections.
    """

    if not bundles:
        return {}

    resolved_settings = settings or load_settings()
    service = CreateAnalysesService(
        resolved_settings,
        extractor_name=extractor_name,
    )

    results: Dict[str, Dict[str, AnalysisCollection]] = {}
    cache_candidates: List[CreateAnalysesResult] = []
    for bundle in bundles:
        article_hash = bundle.article_data.hash_id
        logger.info("Creating analyses for article %s", article_hash)
        per_table = _run_bundle_with_cache(
            bundle,
            article_hash,
            service,
            resolved_settings,
            extractor_name,
            cache_candidates,
        )
        results[article_hash] = per_table

    if cache_candidates:
        cache.cache_create_analyses_results(
            resolved_settings,
            extractor_name,
            cache_candidates,
        )

    return results


def _run_bundle_with_cache(
    bundle: ArticleExtractionBundle,
    article_hash: str,
    service: CreateAnalysesService,
    settings: Settings,
    extractor_name: str | None,
    cache_candidates: List[CreateAnalysesResult],
) -> Dict[str, AnalysisCollection]:
    table_results: Dict[str, AnalysisCollection] = {}
    pending_tables: List[ExtractedTable] = []
    pending_info: Dict[str, Dict[str, object]] = {}

    for index, table in enumerate(bundle.article_data.tables):
        sanitized_table_id = sanitize_table_id(table.table_id, index)
        table_key = table.table_id or sanitized_table_id
        cache_key = _compose_cache_key(article_hash, sanitized_table_id)
        cached = cache.get_cached_create_analyses_result(
            settings,
            cache_key,
            extractor_name,
        )
        if cached:
            table_results[table_key] = cached.analysis_collection
            continue
        pending_tables.append(table)
        pending_info[table_key] = {
            "sanitized": sanitized_table_id,
            "cache_key": cache_key,
            "table_number": table.table_number,
            "table_metadata": dict(table.metadata),
        }

    if not pending_tables:
        return table_results

    pruned_content = replace(bundle.article_data, tables=list(pending_tables))
    pruned_bundle = ArticleExtractionBundle(
        article_data=pruned_content,
        article_metadata=bundle.article_metadata,
    )
    new_results = service.run(pruned_bundle)
    for table_key, collection in new_results.items():
        info = pending_info.get(table_key)
        if info is None:
            continue
        table_results[table_key] = collection
        cache_candidates.append(
            CreateAnalysesResult(
                hash_id=info["cache_key"],
                article_hash=article_hash,
                table_id=table_key,
                sanitized_table_id=info["sanitized"],
                analysis_collection=collection,
                metadata={
                    "table_metadata": info["table_metadata"],
                    "table_number": info["table_number"],
                },
            )
        )

    return table_results


def _compose_cache_key(article_hash: str, sanitized_table_id: str) -> str:
    return f"{article_hash}::{sanitized_table_id}"


__all__ = [
    "run_create_analyses",
]
