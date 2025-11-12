"""Orchestrator for the ingestion workflow pipeline."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.models import (
    AnalysisCollection,
    ArticleExtractionBundle,
    ArticleMetadata,
    DownloadResult,
    Identifier,
    Identifiers,
)
from ingestion_workflow.services import cache

logger = logging.getLogger(__name__)

CANONICAL_STAGES: List[str] = [
    "gather",
    "download",
    "extract",
    "create_analyses",
    "upload",
    "sync",
]


@dataclass
class PipelineState:
    identifiers: Identifiers | None = None
    downloads: List[DownloadResult] | None = None
    bundles: List[ArticleExtractionBundle] | None = None
    analyses: Dict[str, Dict[str, AnalysisCollection]] | None = None


def run_pipeline(
    *,
    settings: Settings | None = None,
) -> PipelineState:
    """Execute the configured pipeline stages in order."""

    resolved_settings = settings or load_settings()
    resolved_settings.ensure_directories()
    selected_stages = _normalize_stages(resolved_settings.stages)
    state = PipelineState()
    _seed_identifiers_from_manifest(
        resolved_settings,
        selected_stages,
        state,
    )

    stage_handlers = {
        "gather": _run_gather_stage,
        "download": _run_download_stage,
        "extract": _run_extract_stage,
        "create_analyses": _run_create_analyses_stage,
        "upload": _run_upload_stage,
        "sync": _run_sync_stage,
    }

    for stage in selected_stages:
        handler = stage_handlers[stage]
        logger.info("Starting stage: %s", stage)
        handler(resolved_settings, state)
        logger.info("Completed stage: %s", stage)

    return state


def _run_gather_stage(settings: Settings, state: PipelineState) -> None:
    if settings.dry_run:
        logger.info("Dry-run enabled: gather stage skipped.")
        return
    from ingestion_workflow.workflow.gather import gather_identifiers

    identifiers = gather_identifiers(settings=settings)
    state.identifiers = identifiers


def _run_download_stage(settings: Settings, state: PipelineState) -> None:
    if settings.dry_run:
        logger.info("Dry-run enabled: download stage skipped.")
        return
    _ensure_identifiers(settings, state)
    from ingestion_workflow.workflow.download import run_downloads

    downloads = run_downloads(state.identifiers, settings=settings)
    state.downloads = downloads


def _run_extract_stage(settings: Settings, state: PipelineState) -> None:
    if settings.dry_run:
        logger.info("Dry-run enabled: extract stage skipped.")
        return
    _ensure_downloads(settings, state)
    from ingestion_workflow.workflow.extract import run_extraction

    bundles = run_extraction(state.downloads, settings=settings)
    state.bundles = bundles


def _run_create_analyses_stage(
    settings: Settings, state: PipelineState
) -> None:
    if settings.dry_run:
        logger.info("Dry-run enabled: create_analyses stage skipped.")
        return
    _ensure_bundles(settings, state)
    from ingestion_workflow.workflow.create_analyses import run_create_analyses

    analyses = run_create_analyses(state.bundles, settings=settings)
    state.analyses = analyses


def _run_upload_stage(settings: Settings, state: PipelineState) -> None:
    logger.info("Upload stage not yet implemented; skipping.")


def _run_sync_stage(settings: Settings, state: PipelineState) -> None:
    logger.info("Sync stage not yet implemented; skipping.")


def _normalize_stages(stages: Sequence[str] | None) -> List[str]:
    requested = (
        [stage.lower() for stage in stages if stage]
        if stages
        else list(CANONICAL_STAGES)
    )
    invalid = [stage for stage in requested if stage not in CANONICAL_STAGES]
    if invalid:
        raise ValueError(
            f"Unknown stages requested: {', '.join(sorted(set(invalid)))}"
        )
    requested_set = set(requested) or set(CANONICAL_STAGES)
    return [stage for stage in CANONICAL_STAGES if stage in requested_set]


def _seed_identifiers_from_manifest(
    settings: Settings,
    stages: Sequence[str],
    state: PipelineState,
) -> None:
    if "gather" in stages:
        return
    if settings.manifest_path is None:
        raise ValueError(
            "Gather stage not selected. Please provide manifest_path in "
            "settings or via --manifest."
        )
    state.identifiers = _load_identifiers_from_manifest(settings)


def _ensure_identifiers(settings: Settings, state: PipelineState) -> None:
    if state.identifiers is not None:
        return
    if settings.manifest_path:
        state.identifiers = _load_identifiers_from_manifest(settings)
        return
    raise ValueError(
        "Identifiers are required for this stage. Run the gather stage or "
        "provide a manifest."
    )


def _ensure_downloads(settings: Settings, state: PipelineState) -> None:
    if state.downloads:
        return
    if not settings.use_cached_inputs:
        raise ValueError(
            "Download results are required but missing. "
            "Re-run the download stage or enable cached inputs."
        )
    _ensure_identifiers(settings, state)
    hydrated = _hydrate_downloads_from_cache(settings, state.identifiers)
    if not hydrated:
        raise ValueError(
            "No cached download results were found for the provided "
            "identifiers. Re-run the download stage."
        )
    state.downloads = hydrated


def _ensure_bundles(settings: Settings, state: PipelineState) -> None:
    if state.bundles:
        return
    if not settings.use_cached_inputs:
        raise ValueError(
            "Extraction bundles are required but missing. "
            "Re-run the extract stage or enable cached inputs."
        )
    _ensure_downloads(settings, state)
    hydrated = _hydrate_bundles_from_cache(
        settings,
        state.downloads or [],
    )
    if not hydrated:
        raise ValueError(
            "No cached extraction bundles were found. "
            "Re-run the extract stage."
        )
    state.bundles = hydrated


def _load_identifiers_from_manifest(settings: Settings) -> Identifiers:
    manifest = settings.manifest_path
    if manifest is None:
        raise ValueError("manifest_path must be provided when skipping gather.")
    manifest_path = Path(manifest)
    if not manifest_path.is_absolute():
        manifest_path = settings.data_root / manifest_path
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Manifest file not found: {manifest_path}"
        )
    identifiers = Identifiers.load(manifest_path)
    logger.info(
        "Loaded %d identifiers from manifest %s",
        len(identifiers.identifiers),
        manifest_path,
    )
    return identifiers


def _hydrate_downloads_from_cache(
    settings: Settings,
    identifiers: Identifiers | None,
) -> List[DownloadResult]:
    if identifiers is None or not identifiers.identifiers:
        return []
    hydrated: Dict[str, DownloadResult] = {}
    for source_name in settings.download_sources:
        index = cache.load_download_index(settings, source_name)
        for identifier in identifiers.identifiers:
            entry = index.get_download(identifier.hash_id)
            if entry is None:
                continue
            hydrated[identifier.hash_id] = entry.result
    return list(hydrated.values())


def _hydrate_bundles_from_cache(
    settings: Settings,
    downloads: List[DownloadResult],
) -> List[ArticleExtractionBundle]:
    if not downloads:
        return []

    bundles: Dict[str, ArticleExtractionBundle] = {}
    downloads_by_source: Dict[str, List[DownloadResult]] = {}
    for download in downloads:
        downloads_by_source.setdefault(download.source.value, []).append(
            download
        )

    for source_name, download_list in downloads_by_source.items():
        index = cache.load_extractor_index(settings, source_name)
        for download in download_list:
            entry = index.get_extraction(download.identifier.hash_id)
            if entry is None:
                continue
            content = entry.content
            metadata = _build_placeholder_metadata(download.identifier)
            bundles[download.identifier.hash_id] = ArticleExtractionBundle(
                article_data=content,
                article_metadata=metadata,
            )

    return list(bundles.values())


def _build_placeholder_metadata(identifier: Identifier) -> ArticleMetadata:
    for candidate in (identifier.doi, identifier.pmid, identifier.pmcid):
        if candidate:
            return ArticleMetadata(title=str(candidate))
    label = " / ".join(
        part for part in identifier.hash_id.split("|") if part
    )
    if label:
        return ArticleMetadata(title=label)
    return ArticleMetadata(title=identifier.hash_id or "Unknown Identifier")


__all__ = ["PipelineState", "run_pipeline"]
