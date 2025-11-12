"""Download and Extract tables from articles using ACE."""

# scraper has the function `retrieve_articles``
from ace.scrape import Scraper
# also within config, I need to set SAVE_ORIGINAL_HTML = True


from ingestion_workflow.extractors.base import BaseExtractor

from ingestion_workflow.models import Identifiers, DownloadResult, ExtractionResult


class ACEExtractor(BaseExtractor):
    """Extractor that uses ACE to download and extract tables from articles."""

    def download(self, identifiers: Identifiers) -> list[DownloadResult]:
        """Download articles using ACE."""
        raise NotImplementedError("ACEExtractor download method not implemented.")

    def extract(self, download_results: list[DownloadResult]) -> list[ExtractionResult]:
        """Extract tables from downloaded articles using ACE."""
        raise NotImplementedError("ACEExtractor extract method not implemented.")
