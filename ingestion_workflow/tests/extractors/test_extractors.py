import json
from pathlib import Path

import pytest

from pubget._typing import ExitCode
from pubget._utils import article_bucket_from_pmcid

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.elsevier_extractor import ElsevierExtractor
from ingestion_workflow.extractors.pubget_extractor import PubgetExtractor
from ingestion_workflow.models import (
    DownloadSource,
    FileType,
    Identifier,
    Identifiers,
)


@pytest.mark.usefixtures("manifest_identifiers")
@pytest.mark.vcr()
def test_elsevier_download_records_articles(tmp_path, manifest_identifiers):
    subset = Identifiers(list(manifest_identifiers.identifiers[:100]))

    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
    )

    if not settings.elsevier_api_key:
        pytest.skip("Elsevier API key not configured for recording")

    extractor = ElsevierExtractor(settings=settings)

    results = extractor.download(subset)

    assert len(results) == len(subset.identifiers)
    successes = [result for result in results if result.success]
    assert successes, "Expected at least one successful Elsevier download"

    for index, result in enumerate(results):
        identifier = subset.identifiers[index]
        assert result.identifier is identifier
        assert result.source is DownloadSource.ELSEVIER

        if result.success:
            assert result.files, "Successful downloads should persist files"

            metadata_file = next(
                file
                for file in result.files
                if file.file_type is FileType.JSON
            )
            metadata = json.loads(
                metadata_file.file_path.read_text(encoding="utf-8")
            )

            expected_lookup_type = "doi" if identifier.doi else "pmid"
            expected_lookup_value = identifier.doi or identifier.pmid

            assert metadata["lookup_type"] == expected_lookup_type
            assert metadata["lookup_value"] == expected_lookup_value
            assert metadata["identifier_hash"] == identifier.hash_id
        else:
            assert not result.files


def test_pubget_download_persists_article_and_tables(tmp_path, monkeypatch):
    identifier = Identifier(pmcid="PMC12345")
    identifiers = Identifiers([identifier])

    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        max_workers=3,
        pubget_cache_root=tmp_path / "pubget_cache",
    )

    extractor = PubgetExtractor(settings=settings)

    data_dir = settings.pubget_cache_root
    assert data_dir is not None
    data_dir.mkdir(parents=True)
    download_dir = data_dir / "pmcidList_demo" / "articlesets"
    download_dir.mkdir(parents=True)
    articles_dir = download_dir.with_name("articles")
    bucket = article_bucket_from_pmcid(12345)
    article_dir = articles_dir / bucket / "pmcid_12345"
    article_dir.mkdir(parents=True)
    article_dir.joinpath("article.xml").write_text(
        "<article />",
        encoding="utf-8",
    )
    tables_dir = article_dir / "tables"
    tables_dir.mkdir(parents=True)
    tables_dir.joinpath("tables.xml").write_text(
        "<tables />",
        encoding="utf-8",
    )

    captured: dict[str, tuple] = {}

    def fake_download(
        pmcids: list[int],
        *,
        data_dir: Path,
        api_key: str | None,
        retmax: int,
    ) -> tuple[Path, ExitCode]:
        captured["download"] = (pmcids, data_dir, api_key, retmax)
        return download_dir, ExitCode.COMPLETED

    def fake_extract(
        articlesets_dir: Path, *, n_jobs: int
    ) -> tuple[Path, ExitCode]:
        captured["extract"] = (articlesets_dir, n_jobs)
        return articles_dir, ExitCode.COMPLETED

    monkeypatch.setattr(
        "ingestion_workflow.extractors.pubget_extractor.download_pmcids",
        fake_download,
    )
    monkeypatch.setattr(
        "ingestion_workflow.extractors.pubget_extractor.extract_articles",
        fake_extract,
    )

    results = extractor.download(identifiers)

    assert captured["download"] == (
        [12345],
        data_dir,
        settings.pubmed_api_key,
        settings.pubmed_batch_size,
    )
    assert captured["extract"] == (download_dir, 3)

    assert len(results) == 1
    result = results[0]
    assert result.identifier is identifier
    assert result.success is True
    assert result.source is DownloadSource.PUBGET
    assert result.error_message is None

    paths = {file.file_path for file in result.files}
    assert article_dir.joinpath("article.xml") in paths
    assert tables_dir.joinpath("tables.xml") in paths


def test_pubget_download_warns_on_incomplete_exit_codes(tmp_path, monkeypatch):
    identifier = Identifier(pmcid="PMC987")
    identifiers = Identifiers([identifier])

    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        max_workers=2,
        pubget_cache_root=tmp_path / "pubget_cache",
    )

    extractor = PubgetExtractor(settings=settings)

    data_dir = settings.pubget_cache_root
    assert data_dir is not None
    data_dir.mkdir(parents=True)
    download_dir = data_dir / "pmcidList_warn" / "articlesets"
    download_dir.mkdir(parents=True)
    articles_dir = download_dir.with_name("articles")
    bucket = article_bucket_from_pmcid(987)
    article_dir = articles_dir / bucket / "pmcid_987"
    article_dir.mkdir(parents=True)
    article_dir.joinpath("article.xml").write_text(
        "<article />",
        encoding="utf-8",
    )
    tables_dir = article_dir / "tables"
    tables_dir.mkdir(parents=True)
    tables_dir.joinpath("tables.xml").write_text(
        "<tables />",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "ingestion_workflow.extractors.pubget_extractor.download_pmcids",
        lambda *_, **__: (download_dir, ExitCode.INCOMPLETE),
    )
    monkeypatch.setattr(
        "ingestion_workflow.extractors.pubget_extractor.extract_articles",
        lambda *_args, **_kwargs: (articles_dir, ExitCode.INCOMPLETE),
    )

    results = extractor.download(identifiers)

    assert len(results) == 1
    result = results[0]
    assert result.success is True
    assert result.error_message is not None
    assert "incomplete" in result.error_message.lower()
