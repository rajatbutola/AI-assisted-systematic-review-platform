# core/domain_registry.py v1 27th March

# core/domain_registry.py v2 — updated 31st March
# Adds EuropePMCClient to the MEDICAL domain alongside PubMedClient.

import logging
from abc import ABC, abstractmethod
from typing import Dict, List

from models.schemas import ResearchDomain

logger = logging.getLogger(__name__)


class BaseLiteratureClient(ABC):
    @property
    @abstractmethod
    def domain(self) -> ResearchDomain: ...

    @property
    @abstractmethod
    def source_name(self) -> str: ...

    @abstractmethod
    def search(self, query: str, max_results: int = 20) -> List[str]: ...

    @abstractmethod
    def fetch(self, ids: List[str]) -> list: ...

    def search_and_fetch(self, query: str, max_results: int = 20) -> list:
        ids = self.search(query, max_results)
        return self.fetch(ids)


class DomainRegistry:
    def __init__(self):
        self._registry: Dict[ResearchDomain, List[BaseLiteratureClient]] = {}

    def register(self, client: BaseLiteratureClient) -> None:
        domain = client.domain
        if domain not in self._registry:
            self._registry[domain] = []
        self._registry[domain].append(client)
        logger.info("Registered client '%s' for domain '%s'",
                    client.source_name, domain.value)

    def get_clients(self, domain: ResearchDomain) -> List[BaseLiteratureClient]:
        clients = self._registry.get(domain, [])
        if not clients:
            raise ValueError(
                f"No clients registered for domain '{domain.value}'. "
                f"Available: {[d.value for d in self._registry]}"
            )
        return clients

    def available_domains(self) -> List[ResearchDomain]:
        return list(self._registry.keys())

    def domain_display_names(self) -> Dict[str, str]:
        return {
            ResearchDomain.MEDICAL.value: "🏥 Medical Research (PubMed / Europe PMC)",
            ResearchDomain.ML_AI.value:   "🤖 Machine Learning / AI (Semantic Scholar + OpenAlex)",
        }


def build_default_registry() -> DomainRegistry:
    from core.pubmed_client         import PubMedClient
    from core.pmc_client            import PMCClient
    from core.europe_pmc_client     import EuropePMCClient   # ← NEW
    from core.semantic_scholar_client import SemanticScholarClient
    from core.openalex_client         import OpenAlexClient

    registry = DomainRegistry()
    registry.register(PubMedClient())
    registry.register(PMCClient())
    registry.register(EuropePMCClient())     # ← NEW
    registry.register(SemanticScholarClient())
    registry.register(OpenAlexClient())
    return registry

