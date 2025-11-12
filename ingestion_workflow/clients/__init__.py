"""HTTP clients for external literature services."""

from .coordinate_parsing import CoordinateParsingClient
from .llm import GenericLLMClient
from .openalex import OpenAlexClient
from .pubmed import PubMedClient
from .semantic_scholar import SemanticScholarClient

__all__ = [
    "CoordinateParsingClient",
    "GenericLLMClient",
    "OpenAlexClient",
    "PubMedClient",
    "SemanticScholarClient",
]
