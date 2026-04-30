from pathlib import Path

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.models import (
    DownloadResult,
    DownloadSource,
    DownloadedFile,
    FileType,
    Identifier,
    Identifiers,
)
from ingestion_workflow.services import cache
from ingestion_workflow.workflow.download import (
    EXTRACTOR_FACTORIES,
    run_downloads,
)


class _FakeElsevierExtractor(BaseExtractor):
    DOWNLOAD_BATCH_SIZE: int | None = None

    def __init__(self, *, success_dir: Path, success_targets: set[str]):
        self._success_dir = success_dir
        self._success_targets = success_targets
        self.batch_sizes: list[int] = []

    def download(
        self,
        identifiers: Identifiers,
        progress_hook=None,
    ) -> list[DownloadResult]:
        self.batch_sizes.append(len(identifiers.identifiers))
        results: list[DownloadResult] = []
        for identifier in identifiers.identifiers:
            if identifier.slug in self._success_targets:
                payload_path = self._success_dir / f"{identifier.slug}_content.txt"
                payload_path.parent.mkdir(parents=True, exist_ok=True)
                payload_path.write_text("payload", encoding="utf-8")
                results.append(
                    DownloadResult(
                        identifier=identifier,
                        source=DownloadSource.ELSEVIER,
                        success=True,
                        files=[
                            DownloadedFile(
                                file_path=payload_path,
                                file_type=FileType.TEXT,
                                content_type="text/plain",
                                source=DownloadSource.ELSEVIER,
                            )
                        ],
                    )
                )
            else:
                results.append(
                    DownloadResult(
                        identifier=identifier,
                        source=DownloadSource.ELSEVIER,
                        success=False,
                        files=[],
                        error_message="simulated failure",
                    )
                )
            if progress_hook:
                progress_hook(1)
        return results


def test_run_downloads_mixes_cached_and_new(monkeypatch, tmp_path):
    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        download_sources=[DownloadSource.ELSEVIER.value],
    )

    identifiers = Identifiers(
        [
            Identifier(pmid="100"),
            Identifier(pmid="200"),
            Identifier(pmid="300"),
        ]
    )

    cached_dir = tmp_path / "cached"
    cached_dir.mkdir(parents=True, exist_ok=True)
    cached_path = cached_dir / "cached.txt"
    cached_path.write_text("cached", encoding="utf-8")

    cached_result = DownloadResult(
        identifier=identifiers.identifiers[0],
        source=DownloadSource.ELSEVIER,
        success=True,
        files=[
            DownloadedFile(
                file_path=cached_path,
                file_type=FileType.TEXT,
                content_type="text/plain",
                source=DownloadSource.ELSEVIER,
            )
        ],
    )

    cache.cache_download_results(
        settings,
        extractor_name=DownloadSource.ELSEVIER.value,
        results=[cached_result],
    )

    success_target = identifiers.identifiers[1].slug

    def fake_factory(_settings: Settings) -> BaseExtractor:
        return _FakeElsevierExtractor(
            success_dir=tmp_path / "success",
            success_targets={success_target},
        )

    monkeypatch.setitem(
        EXTRACTOR_FACTORIES,
        DownloadSource.ELSEVIER,
        fake_factory,
    )

    results = run_downloads(identifiers, settings=settings)

    assert len(results) == 3
    assert results[0].identifier is identifiers.identifiers[0]
    assert results[0].success
    assert results[1].identifier is identifiers.identifiers[1]
    assert results[1].success
    assert not results[2].success
    assert results[2].identifier is identifiers.identifiers[2]

    cached_after, missing_after = cache.partition_cached_downloads(
        settings,
        extractor_name=DownloadSource.ELSEVIER.value,
        identifiers=Identifiers([identifiers.identifiers[1], identifiers.identifiers[2]]),
    )

    assert len(cached_after) == 1
    assert cached_after[0].identifier is identifiers.identifiers[1]
    assert missing_after == [identifiers.identifiers[2]]


def test_run_downloads_ignores_cache_when_requested(monkeypatch, tmp_path):
    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        download_sources=[DownloadSource.ELSEVIER.value],
        ignore_cache_stages=["download"],
    )

    identifiers = Identifiers([Identifier(pmid="400")])

    def _fail_partition(*_args, **_kwargs):
        raise AssertionError("Cache partition should not be called when ignoring cache")

    monkeypatch.setattr(
        cache,
        "partition_cached_downloads",
        _fail_partition,
    )

    def fake_factory(_settings: Settings) -> BaseExtractor:
        return _FakeElsevierExtractor(
            success_dir=tmp_path / "success",
            success_targets={identifiers.identifiers[0].slug},
        )

    monkeypatch.setitem(
        EXTRACTOR_FACTORIES,
        DownloadSource.ELSEVIER,
        fake_factory,
    )

    results = run_downloads(identifiers, settings=settings)

    assert len(results) == 1
    assert results[0].success is True
    assert results[0].identifier == identifiers.identifiers[0]


def test_run_downloads_checkpoints_batched_extractors(monkeypatch, tmp_path):
    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        download_sources=[DownloadSource.ELSEVIER.value],
    )

    identifiers = Identifiers(
        [
            Identifier(pmid="500"),
            Identifier(pmid="600"),
            Identifier(pmid="700"),
            Identifier(pmid="800"),
            Identifier(pmid="900"),
        ]
    )

    extractor = _FakeElsevierExtractor(
        success_dir=tmp_path / "success",
        success_targets={identifier.slug for identifier in identifiers.identifiers},
    )
    extractor.DOWNLOAD_BATCH_SIZE = 2

    def fake_factory(_settings: Settings) -> BaseExtractor:
        return extractor

    cache_calls: list[int] = []
    original_cache_download_results = cache.cache_download_results

    def recording_cache_download_results(*args, results, **kwargs):
        cache_calls.append(len(results))
        return original_cache_download_results(*args, results=results, **kwargs)

    monkeypatch.setitem(
        EXTRACTOR_FACTORIES,
        DownloadSource.ELSEVIER,
        fake_factory,
    )
    monkeypatch.setattr(
        cache,
        "cache_download_results",
        recording_cache_download_results,
    )

    results = run_downloads(identifiers, settings=settings)

    assert len(results) == 5
    assert all(result.success for result in results)
    assert extractor.batch_sizes == [2, 2, 1]
    assert cache_calls == [2, 2, 1]
