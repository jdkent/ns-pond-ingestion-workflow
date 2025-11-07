import json
from pathlib import Path

import pandas as pd
import pytest

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import ExtractorContext
from elsevier_coordinate_extraction.types import build_article_content

from ingestion_workflow.extractors.elsevier_extractor import ElsevierExtractor
from ingestion_workflow.models import Identifier, DownloadResult
from ingestion_workflow.storage import StorageManager


TEST_DOI = "10.1016/j.dcn.2015.10.001"


@pytest.fixture
def elsevier_extractor(tmp_path: Path) -> ElsevierExtractor:
    settings = Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "pond",
    )
    storage = StorageManager(settings)
    storage.prepare()
    context = ExtractorContext(storage=storage, settings=settings)
    return ElsevierExtractor(context)


@pytest.mark.vcr(record="once")
def test_elsevier_extractor_downloads_article(elsevier_extractor: ElsevierExtractor):
    identifier = Identifier(doi=TEST_DOI)

    results = elsevier_extractor.download([identifier])

    assert results
    result = results[0]

    paths = elsevier_extractor.context.storage.paths_for(identifier)
    processed_dir = paths.processed_for("elsevier")
    source_dir = paths.source_for("elsevier")

    article_path = source_dir / "article.xml"
    coordinates_path = processed_dir / "coordinates.csv"
    metadata_path = processed_dir / "metadata.json"
    analyses_path = processed_dir / "analyses.json"

    assert article_path.exists()
    assert coordinates_path.exists()
    coords_df = pd.read_csv(coordinates_path)
    assert not coords_df.empty
    assert {"table_id", "analysis_name", "x", "y", "z", "space"}.issubset(coords_df.columns)

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["doi"] == TEST_DOI
    assert metadata.get("title")

    analyses = json.loads(analyses_path.read_text(encoding="utf-8"))
    studies = analyses.get("studyset", {}).get("studies", [])
    assert studies and studies[0].get("analyses")

    assert any(artifact.kind == "coordinates_csv" for artifact in result.artifacts)


def test_elsevier_extractor_reuses_persistent_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cache_root = tmp_path / "elsevier_cache"
    settings = Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "pond",
        elsevier_cache_root=cache_root,
    )
    storage = StorageManager(settings)
    storage.prepare()
    context = ExtractorContext(storage=storage, settings=settings)

    primer = ElsevierExtractor(context)
    fake_article = build_article_content(
        doi=TEST_DOI,
        payload=f"<article><doi>{TEST_DOI}</doi></article>".encode("utf-8"),
        content_type="application/xml",
        fmt="xml",
        metadata={"identifier_lookup": {"doi": TEST_DOI, "pmid": "123456"}, "pmid": "123456"},
    )
    primer._article_cache.store(fake_article)

    new_storage = StorageManager(settings)
    new_storage.prepare()
    new_context = ExtractorContext(storage=new_storage, settings=settings)
    extractor = ElsevierExtractor(new_context)

    def _fail_download(_records):
        raise AssertionError("Elsevier download should not be invoked when cache is primed.")

    monkeypatch.setattr(
        "ingestion_workflow.extractors.elsevier_extractor.extract_coordinates",
        lambda articles, **_: {"studyset": {"studies": [{"doi": TEST_DOI, "analyses": []}]}},
    )

    monkeypatch.setattr(extractor, "_download_articles", _fail_download)

    def _fake_process(identifier: Identifier, _article, study):
        return DownloadResult(
            identifier=identifier,
            source="elsevier",
            artifacts=[],
            open_access=False,
            extra_metadata={"cached": True},
        )

    monkeypatch.setattr(extractor, "_process_article", _fake_process)

    identifier = Identifier(doi=TEST_DOI)
    results = extractor.download([identifier])

    assert results
    assert results[0].extra_metadata.get("cached") is True


def test_elsevier_extractor_batches_coordinate_extraction(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cache_root = tmp_path / "elsevier_cache"
    settings = Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        ns_pond_root=tmp_path / "pond",
        elsevier_cache_root=cache_root,
    )
    storage = StorageManager(settings)
    storage.prepare()
    context = ExtractorContext(storage=storage, settings=settings)
    extractor = ElsevierExtractor(context)

    doi_one = "10.1016/j.neuroimage.2011.01.001"
    doi_two = "10.1016/j.neuroimage.2011.01.002"
    articles = {
        doi_one: build_article_content(
            doi=doi_one,
            payload=f"<article><doi>{doi_one}</doi></article>".encode("utf-8"),
            content_type="application/xml",
            fmt="xml",
            metadata={"identifier_lookup": {"doi": doi_one}},
        ),
        doi_two: build_article_content(
            doi=doi_two,
            payload=f"<article><doi>{doi_two}</doi></article>".encode("utf-8"),
            content_type="application/xml",
            fmt="xml",
            metadata={"identifier_lookup": {"doi": doi_two}},
        ),
    }

    def fake_load(record):
        doi = record.get("doi")
        return articles.get(doi)

    monkeypatch.setattr(extractor._article_cache, "load", fake_load)
    monkeypatch.setattr(extractor._article_cache, "store", lambda article: None)

    extract_calls = {"count": 0}

    def fake_extract(batch, **kwargs):
        extract_calls["count"] += 1
        if extractor._supports_extraction_workers:
            assert kwargs.get("extraction_workers") == settings.extraction_workers
        else:
            assert "extraction_workers" not in kwargs
        studies = []
        for article in batch:
            studies.append(
                {
                    "doi": article.doi,
                    "analyses": [
                        {
                            "name": "analysis",
                            "metadata": {"table_id": "table", "table_label": "label"},
                            "points": [{"coordinates": [1, 2, 3], "space": "MNI"}],
                        }
                    ],
                }
            )
        return {"studyset": {"studies": studies}}

    monkeypatch.setattr(
        "ingestion_workflow.extractors.elsevier_extractor.extract_coordinates",
        fake_extract,
    )

    def fake_process(self, identifier, article, study):
        assert study is not None
        return DownloadResult(
            identifier=identifier,
            source="elsevier",
            artifacts=[],
            extra_metadata={"coordinates_found": True, "doi": identifier.doi},
        )

    monkeypatch.setattr(ElsevierExtractor, "_process_article", fake_process)

    identifiers = [Identifier(doi=doi_one), Identifier(doi=doi_two)]
    results = extractor.download(identifiers)

    assert extract_calls["count"] == 1
    assert len(results) == 2
