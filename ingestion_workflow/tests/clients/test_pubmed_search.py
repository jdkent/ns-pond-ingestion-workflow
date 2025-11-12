import pytest

from ingestion_workflow.clients.pubmed import (
    ESEARCH_MAX_RESULTS,
    PubMedClient,
)
from ingestion_workflow.config import Settings
from ingestion_workflow.models import Identifier, Identifiers


@pytest.fixture
def settings_with_email() -> Settings:
    return Settings(pubmed_email="tests@example.com")


def test_pubmed_client_search_paginates(monkeypatch):
    client = PubMedClient(email="tests@example.com")

    def fake_esearch(
        query: str,
        *,
        retstart: int,
        retmax: int,
        mindate=None,
        maxdate=None,
    ) -> dict:
        count = 2500
        if retstart == 0:
            ids = [str(i) for i in range(0, 1000)]
        elif retstart == 1000:
            ids = [str(i) for i in range(1000, 2000)]
        elif retstart == 2000:
            ids = [str(i) for i in range(2000, 2500)]
        else:
            ids = []
        return {"esearchresult": {"count": str(count), "idlist": ids}}

    monkeypatch.setattr(client, "_esearch", fake_esearch)

    results = client.search("test query")
    assert len(results.identifiers) == 2500
    assert results.lookup("0", key="pmid") is not None
    assert results.lookup("2499", key="pmid") is not None


def test_pubmed_client_search_splits_large_result_set(monkeypatch):
    client = PubMedClient(email="tests@example.com")

    def fake_collect(
        query: str,
        *,
        mindate=None,
        maxdate=None,
        retmax=None,
    ):
        if mindate is None and maxdate is None:
            ids = [str(i) for i in range(ESEARCH_MAX_RESULTS)]
            return ids, ESEARCH_MAX_RESULTS
        year = mindate or 1990
        ids = [f"{year}001", f"{year}002"]
        return ids, len(ids)

    monkeypatch.setattr(client, "_collect_esearch_ids", fake_collect)
    monkeypatch.setattr(client, "_current_year", lambda: 1992)

    results = client.search("test query", start_year=1990)
    expected_count = (1992 - 1990 + 1) * 2
    assert len(results.identifiers) == expected_count
    assert results.lookup("1990001", key="pmid") is not None


def test_pubmed_search_service_delegates(settings_with_email):
    expected = Identifiers([Identifier(pmid="12345")])

    class DummyClient:
        def __init__(self) -> None:
            self.calls = []

        def search(self, query: str, *, start_year: int) -> Identifiers:
            self.calls.append((query, start_year))
            return expected

    from ingestion_workflow.services.search import PubMedSearchService

    dummy_client = DummyClient()
    service = PubMedSearchService(
        "sample query",
        settings_with_email,
        start_year=2000,
        client=dummy_client,
    )

    results = service.search()
    assert results is expected
    assert dummy_client.calls == [("sample query", 2000)]


def test_pubmed_search_service_requires_email(monkeypatch):
    from ingestion_workflow.services.search import PubMedSearchService

    monkeypatch.delenv("PUBMED_EMAIL", raising=False)
    monkeypatch.delenv("EMAIL", raising=False)

    settings = Settings.model_construct(pubmed_email=None)

    with pytest.raises(ValueError):
        PubMedSearchService("query", settings)
