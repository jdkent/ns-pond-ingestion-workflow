"""Download workflow stage with shared caching logic."""

from __future__ import annotations

from typing import Callable, Dict, Iterable, List, Sequence

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.extractors import (
    ACEExtractor,
    ElsevierExtractor,
    PubgetExtractor,
)
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.models import (
    DownloadResult,
    DownloadSource,
    Identifier,
    Identifiers,
)
from ingestion_workflow.services import cache


ExtractorFactory = Callable[[Settings], BaseExtractor]


def _elsevier_factory(settings: Settings) -> BaseExtractor:
    return ElsevierExtractor(settings=settings)


def _pubget_factory(settings: Settings) -> BaseExtractor:
    return PubgetExtractor(settings=settings)


def _ace_factory(settings: Settings) -> BaseExtractor:
    return ACEExtractor(settings=settings)


EXTRACTOR_FACTORIES: Dict[DownloadSource, ExtractorFactory] = {
    DownloadSource.ELSEVIER: _elsevier_factory,
    DownloadSource.PUBGET: _pubget_factory,
    DownloadSource.ACE: _ace_factory,
}


def _resolve_extractor(
    source: DownloadSource, settings: Settings
) -> BaseExtractor:
    try:
        factory = EXTRACTOR_FACTORIES[source]
    except KeyError as exc:  # pragma: no cover - defensive guard
        message = f"No extractor registered for source: {source}"
        raise ValueError(message) from exc
    return factory(settings)


def _successful_hashes(results: Sequence[DownloadResult]) -> set[str]:
    return {
        result.identifier.hash_id
        for result in results
        if result.success
    }


def _identifiers_from_hashes(
    pending: Iterable[Identifier],
    success_hashes: set[str],
) -> List[Identifier]:
    return [
        identifier
        for identifier in pending
        if identifier.hash_id not in success_hashes
    ]


def _partition_supported_identifiers(
    extractor: BaseExtractor, identifiers: Iterable[Identifier]
) -> tuple[list[Identifier], list[Identifier]]:
    supported_fields = getattr(extractor, "_SUPPORTED_IDS", None)
    identifiers_list = list(identifiers)

    if not supported_fields:
        return identifiers_list, []

    field_set = {str(field) for field in supported_fields}
    supported: list[Identifier] = []
    unsupported: list[Identifier] = []

    for identifier in identifiers_list:
        if any(getattr(identifier, field, None) for field in field_set):
            supported.append(identifier)
        else:
            unsupported.append(identifier)

    return supported, unsupported


def run_downloads(
    identifiers: Identifiers,
    *,
    settings: Settings | None = None,
) -> List[DownloadResult]:
    """Run download extractors in order and persist successes to cache."""

    settings = settings or load_settings()
    remaining: List[Identifier] = list(identifiers.identifiers)
    collected_results: List[DownloadResult] = []

    for source_name in settings.download_sources:
        if not remaining:
            break

        source = DownloadSource(source_name)
        extractor = _resolve_extractor(source, settings)

        supported, unsupported = _partition_supported_identifiers(
            extractor, remaining
        )
        if not supported:
            remaining = list(unsupported)
            continue

        extractor_identifiers = Identifiers(list(supported))
        cached_results, missing = cache.partition_cached_downloads(
            settings,
            extractor_name=source.value,
            identifiers=extractor_identifiers,
        )
        collected_results.extend(cached_results)

        next_remaining: List[Identifier] = list(unsupported)

        if not missing:
            remaining = next_remaining
            continue

        if settings.cache_only_mode:
            next_remaining.extend(missing)
            remaining = next_remaining
            continue

        download_results = extractor.download(Identifiers(list(missing)))
        collected_results.extend(download_results)

        successes = [result for result in download_results if result.success]
        if successes:
            cache.cache_download_results(
                settings,
                extractor_name=source.value,
                results=successes,
            )

        success_hashes = _successful_hashes(successes)
        failures = _identifiers_from_hashes(missing, success_hashes)
        next_remaining.extend(failures)
        remaining = next_remaining

    return collected_results


__all__ = ["run_downloads"]
