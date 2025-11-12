from __future__ import annotations

from pathlib import Path

import pytest

from ingestion_workflow.config import Settings
from ingestion_workflow.models import Identifier, Identifiers
from ingestion_workflow.services.id_lookup import IDLookupService
from ingestion_workflow.workflow import gather
from ingestion_workflow.workflow.gather import SearchQuery, gather_identifiers


def _make_settings(
    tmp_path: Path,
    *,
    providers: list[str] | None = None,
) -> Settings:
    settings = Settings(
        data_root=tmp_path / "data",
        cache_root=tmp_path / "cache",
        metadata_providers=providers or [],
    )
    settings.ensure_directories()
    return settings


def test_gather_manifest_only_persists_results(tmp_path: Path) -> None:
    settings = _make_settings(tmp_path)
    manifest = Identifiers(
        [Identifier(pmid="1", doi="10.1/foo", pmcid="PMC1")]
    )

    result = gather_identifiers(
        settings=settings,
        manifest=manifest,
        label="Baseline Run",
    )

    expected_path = settings.data_root / "manifests" / "baseline-run.jsonl"
    assert expected_path.exists()
    saved = Identifiers.load(expected_path)

    assert len(result.identifiers) == 1
    assert len(saved.identifiers) == 1
    assert saved.identifiers[0].pmid == "1"


def test_gather_merges_pubmed_queries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _make_settings(tmp_path)
    manifest = Identifiers([Identifier(pmid="12345")])

    class DummySearchService:
        calls: list[tuple[str, int]] = []

        def __init__(
            self,
            query: str,
            _settings: Settings,
            *,
            start_year: int,
            client=None,
        ) -> None:
            DummySearchService.calls.append((query, start_year))

        def search(self) -> Identifiers:
            return Identifiers(
                [Identifier(pmid="12345"), Identifier(pmid="67890")]
            )

    monkeypatch.setattr(
        gather,
        "PubMedSearchService",
        DummySearchService,
    )

    result = gather_identifiers(
        settings=settings,
        manifest=manifest,
        queries=[SearchQuery("brain imaging", start_year=2000)],
        label="Search Run",
    )

    expected_path = settings.data_root / "manifests" / "search-run.jsonl"
    assert expected_path.exists()
    assert DummySearchService.calls == [("brain imaging", 2000)]
    assert sorted(identifier.pmid for identifier in result.identifiers) == [
        "12345",
        "67890",
    ]


def test_gather_honors_metadata_providers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyLookup(IDLookupService):
        extractor_name = "dummy"
        lookup_order = ("pmid",)
        calls = 0

        def __init__(self, settings: Settings) -> None:
            super().__init__(settings)

        def _lookup_by_type(
            self,
            id_type: str,
            identifiers: Identifiers,
        ) -> None:
            DummyLookup.calls += 1

    monkeypatch.setitem(
        gather.ID_LOOKUP_SERVICE_FACTORIES,
        "dummy",
        DummyLookup,
    )

    settings = _make_settings(tmp_path, providers=["dummy"])
    manifest = Identifiers([Identifier(pmid="42")])

    gather_identifiers(settings=settings, manifest=manifest, label="dummy")

    assert DummyLookup.calls > 0
