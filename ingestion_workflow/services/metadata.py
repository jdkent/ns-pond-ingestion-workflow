"""
This module will grab metadata for articles
given a list of article ids.
The metadata will be retrieved from the following sources
in this order:
1. Semantic Scholar
2. PubMed
3. fallback to processed metadata from extractors (information from the downloaded article)
"""
