from ingestion_workflow.models import (
    DownloadResult,
    ExtractionResult,
    Identifiers,
)


class BaseExtractor:
    """Shared interface for extractor implementations."""

    def download(self, identifiers: Identifiers) -> DownloadResult:
        raise NotImplementedError("Subclasses must implement this method.")

    def extract(self, download_result: DownloadResult) -> ExtractionResult:
        raise NotImplementedError("Subclasses must implement this method.")
