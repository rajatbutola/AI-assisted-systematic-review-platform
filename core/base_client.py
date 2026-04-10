# core/base_client.py

from abc import ABC, abstractmethod
from typing import List, Optional, Tuple

from models.schemas import Article, ResearchDomain

class BaseLiteratureClient(ABC):
    """
    Abstract base class for all literature source clients.

    Every data source — PubMed, Semantic Scholar, OpenAlex, or any future
    source — must implement exactly these methods.

    Design contract:
      search()          → list of source-native IDs
      fetch()           → list of normalised Article objects
      search_and_fetch() → (articles, error_message) for ML clients
                           or raises an exception for PubMed

    The tuple return convention for search_and_fetch() exists because ML APIs
    can fail partially (e.g. some pages retrieved before a rate limit) and we
    want to save partial results while still showing the user what went wrong.
    PubMed raises exceptions instead because it is an all-or-nothing fetch.

    Concrete subclasses override search_and_fetch() with the appropriate
    return convention for their API. The default implementation here delegates
    to search() + fetch() and raises on error (PubMed behaviour).
    """

    @property
    @abstractmethod
    def domain(self) -> ResearchDomain:
        """The research domain this client serves."""
        ...

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Human-readable name, e.g. 'PubMed', 'Semantic Scholar'."""
        ...

    @abstractmethod
    def search(self, query: str, max_results: int = 20) -> List[str]:
        """
        Execute a search and return source-native paper IDs.

        Raises RuntimeError after all retries are exhausted.
        """
        ...

    @abstractmethod
    def fetch(self, ids: List[str]) -> List[Article]:
        """
        Fetch full metadata for a list of paper IDs.

        Articles that fail to parse are silently skipped (logged as WARNING).
        Raises on network or API errors.
        """
        ...

    def search_and_fetch(self, query: str,
                          max_results: int = 20) -> List[Article]:
        """
        Default implementation: search() then fetch().

        Used by PubMedClient (which raises on error — the caller wraps in try/except).
        ML clients (SemanticScholarClient, OpenAlexClient) override this to return
        (List[Article], Optional[str]) for partial-result and error-surface support.
        """
        ids = self.search(query, max_results)
        if not ids:
            return []
        return self.fetch(ids)
