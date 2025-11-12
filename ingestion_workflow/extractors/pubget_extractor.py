"""Download and Extract tables from articles using Pubget."""

from pubget._download import download_pmcids
from ingestion_workflow.extractors.base import BaseExtractor

from ingestion_workflow.models import Identifiers, DownloadResult, ExtractionResult


class PubgetExtractor(BaseExtractor):
    """Extractor that uses Pubget to download and extract tables from articles."""

    def download(self, identifiers: Identifiers) -> list[DownloadResult]:
        """Download articles using Pubget."""
        raise NotImplementedError("PubgetExtractor download method not implemented.")

    def extract(self, download_results: list[DownloadResult]) -> list[ExtractionResult]:
        """Extract tables from downloaded articles using Pubget."""
        raise NotImplementedError("PubgetExtractor extract method not implemented.")
