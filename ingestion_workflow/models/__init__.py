"""Convenience re-exports for core workflow data models."""

from .analysis import (
    Analysis,
    AnalysisCollection,
    Condition,
    Contrast,
    Coordinate,
    CoordinateSpace,
    Image,
)
from .download import (
    DownloadIndex,
    DownloadResult,
    DownloadSource,
    DownloadedFile,
    FileType,
)
from .extract import (
    ExtractedContent,
    ExtractedTable,
    ExtractionIndex,
)
from .ids import Identifier, Identifiers
from .metadata import (
    ArticleMetadata,
    Author,
    MetadataCache,
    merge_metadata_from_sources,
)

# Align with earlier interface expectations.
ExtractionResult = ExtractedContent

__all__ = [
    "Analysis",
    "AnalysisCollection",
    "ArticleMetadata",
    "Author",
    "Condition",
    "Contrast",
    "Coordinate",
    "CoordinateSpace",
    "DownloadIndex",
    "DownloadResult",
    "DownloadSource",
    "DownloadedFile",
    "ExtractionIndex",
    "ExtractionResult",
    "ExtractedContent",
    "ExtractedTable",
    "FileType",
    "Identifier",
    "Identifiers",
    "Image",
    "MetadataCache",
    "merge_metadata_from_sources",
]
