"""
General settings for Semantic Scholar client.
(setting api key, rate limits, etc.)
"""


class SemanticScholarClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        # Initialize other settings like rate limits here
