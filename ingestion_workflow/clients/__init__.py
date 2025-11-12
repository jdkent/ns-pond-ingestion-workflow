"""HTTP clients for external literature services."""

from .openalex import OpenAlexClient
from .pubmed import PubMedClient
from .semantic_scholar import SemanticScholarClient

__all__ = [
    "OpenAlexClient",
    "PubMedClient",
    "SemanticScholarClient",
]
