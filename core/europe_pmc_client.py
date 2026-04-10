# core/europe_pmc_client.py v1 31st March, 2026
#
# core/europe_pmc_client.py
#
# Europe PMC REST API client — follows the same BaseLiteratureClient
# interface as PubMedClient so it slots into the DomainRegistry and
# downstream components without any changes.
#
# API docs: https://europepmc.org/RestfulWebService
# Free, no API key required, returns JSON.
#
# Key identifiers in Europe PMC:
#   - For PubMed-indexed articles: PMID exists (source="MED")
#   - For Europe PMC-only articles: uses accession number (source varies)
#   - We use "EPMC:{id}" as our internal pmid field when no PMID exists,
#     so deduplication can compare against real PMIDs from PubMed.
#
# Deduplication strategy (in app.py):
#   After searching both PubMed and Europe PMC, articles are deduplicated
#   by PMID (exact match). Articles from Europe PMC that have a real PMID
#   (source="MED") will be caught; Europe-PMC-only articles use EPMC:id.

import logging
import time
from typing import List, Optional

import requests

from core.base_client import BaseLiteratureClient
from models.schemas import Article, ArticleSource, ResearchDomain

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
_FETCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/{source}/{id}/fullTextXML"

# Map Europe PMC publication types to readable labels
_PUBTYPE_MAP = {
    "research-article":    "Research Article",
    "review-article":      "Review",
    "systematic-review":   "Systematic Review",
    "meta-analysis":       "Meta-Analysis",
    "randomized-controlled-trial": "RCT",
    "case-reports":        "Case Report",
    "editorial":           "Editorial",
    "letter":              "Letter",
    "clinical-trial":      "Clinical Trial",
    "observational-study": "Observational Study",
    "dataset":             "Dataset",
    "preprint":            "Preprint",
}


class EuropePMCClient(BaseLiteratureClient):
    """
    Literature client for Europe PMC (medical domain).
    Compatible with BaseLiteratureClient interface.
    """

    @property
    def domain(self) -> ResearchDomain:
        return ResearchDomain.MEDICAL

    @property
    def source_name(self) -> str:
        return "Europe PMC"

    def search(self, query: str, max_results: int = 20) -> List[str]:
        """
        Search Europe PMC and return a list of internal IDs.
        Each ID is formatted as "SOURCE:ACCESSION" e.g. "MED:34201565".
        """
        if not query.strip():
            raise ValueError("Search query cannot be empty.")

        ids = []
        cursor = "*"      # Europe PMC uses cursor-based pagination
        page_size = min(max_results, 100)

        while len(ids) < max_results:
            batch_size = min(page_size, max_results - len(ids))
            try:
                params = {
                    "query":       query,
                    "resultType":  "core",       # full metadata
                    "pageSize":    batch_size,
                    "cursorMark":  cursor,
                    "format":      "json",
                    "sort":        "CITED desc",  # most cited first
                }
                resp = requests.get(_BASE_URL, params=params, timeout=20)
                resp.raise_for_status()
                data = resp.json()

                results = data.get("resultList", {}).get("result", [])
                if not results:
                    break

                for r in results:
                    source = r.get("source", "MED")
                    ext_id = r.get("id", "")
                    if ext_id:
                        ids.append(f"{source}:{ext_id}")

                next_cursor = data.get("nextCursorMark")
                if not next_cursor or next_cursor == cursor:
                    break
                cursor = next_cursor
                time.sleep(0.1)  # polite delay

            except Exception as e:
                logger.error("Europe PMC search failed: %s", e)
                break

        return ids[:max_results]

    def fetch(self, ids: List[str]) -> List[Article]:
        """
        Fetch full metadata for a list of "SOURCE:ID" strings.
        Returns Article objects populated with all available metadata.
        """
        if not ids:
            return []

        articles = []
        batch_size = 20

        for i in range(0, len(ids), batch_size):
            batch = ids[i:i + batch_size]
            # Build query from IDs: (EXT_ID:34201565 OR EXT_ID:...)
            id_terms = []
            for raw_id in batch:
                if ":" in raw_id:
                    source, ext_id = raw_id.split(":", 1)
                    id_terms.append(f'EXT_ID:"{ext_id}" AND SRC:{source}')
                else:
                    id_terms.append(f'EXT_ID:"{raw_id}"')

            query = " OR ".join(f"({t})" for t in id_terms)
            try:
                params = {
                    "query":      query,
                    "resultType": "core",
                    "pageSize":   len(batch),
                    "format":     "json",
                }
                resp = requests.get(_BASE_URL, params=params, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                results = data.get("resultList", {}).get("result", [])
                for r in results:
                    art = self._parse_result(r)
                    if art:
                        articles.append(art)
                time.sleep(0.1)
            except Exception as e:
                logger.error("Europe PMC fetch batch failed: %s", e)

        return articles

    def search_and_fetch(self, query: str, max_results: int = 20) -> List[Article]:
        """Combined search + fetch — called by the UI layer."""
        # Europe PMC returns full metadata in the search response itself
        # when resultType="core", so we can skip the separate fetch step.
        if not query.strip():
            raise ValueError("Search query cannot be empty.")

        articles = []
        cursor   = "*"
        page_size = min(max_results, 100)

        while len(articles) < max_results:
            batch_size = min(page_size, max_results - len(articles))
            try:
                params = {
                    "query":      query,
                    "resultType": "core",
                    "pageSize":   batch_size,
                    "cursorMark": cursor,
                    "format":     "json",
                    "sort":       "CITED desc",
                }
                resp = requests.get(_BASE_URL, params=params, timeout=20)
                resp.raise_for_status()
                data = resp.json()

                results = data.get("resultList", {}).get("result", [])
                if not results:
                    break

                for r in results:
                    art = self._parse_result(r)
                    if art:
                        articles.append(art)
                    if len(articles) >= max_results:
                        break

                next_cursor = data.get("nextCursorMark")
                if not next_cursor or next_cursor == cursor:
                    break
                cursor = next_cursor
                time.sleep(0.1)

            except Exception as e:
                logger.error("Europe PMC search_and_fetch failed: %s", e)
                break

        return articles[:max_results]

    def _parse_result(self, r: dict) -> Optional[Article]:
        """Parse a single Europe PMC result dict into an Article."""
        try:
            source  = r.get("source", "MED")
            ext_id  = str(r.get("id", ""))

            # Use real PMID when available (source="MED"), else EPMC:id
            if source == "MED" and ext_id.isdigit():
                pmid = ext_id
            else:
                pmid = f"EPMC:{ext_id}"

            title    = r.get("title", "").rstrip(".")
            abstract_raw = r.get("abstractText", "") or ""
            # Europe PMC sometimes returns structured abstracts with HTML tags
            # like <h4>Background</h4> — strip them for clean display
            import re as _re
            abstract = _re.sub(r"<[^>]+>", " ", abstract_raw)
            abstract = _re.sub(r"\s+", " ", abstract).strip()

            # Authors
            author_list = r.get("authorList", {}).get("author", [])
            authors = []
            for a in author_list:
                name = a.get("fullName") or (
                    f"{a.get('lastName', '')} {a.get('initials', '')}".strip()
                )
                if name:
                    authors.append(name)

            journal       = (r.get("journalInfo", {}) or {}).get("journal", {}).get("title", "") or \
                            r.get("journalTitle", "") or ""
            year          = str(r.get("pubYear", "") or "")
            doi           = r.get("doi", None)
            citation_count = r.get("citedByCount", None)

            # Europe PMC URL
            epmc_url = f"https://europepmc.org/article/{source}/{ext_id}"

            # Publication type — Europe PMC provides pubTypeList
            pub_types = r.get("pubTypeList", {}).get("pubType", [])
            if isinstance(pub_types, str):
                pub_types = [pub_types]
            # Map to readable labels
            readable_types = [
                _PUBTYPE_MAP.get(pt.lower().replace(" ", "-"), pt)
                for pt in pub_types
            ]
            study_type = "; ".join(readable_types) if readable_types else ""

            # Use `venue` field to store study_type (reuses existing schema)
            return Article(
                pmid=pmid,
                title=title,
                abstract=abstract,
                authors=authors,
                journal=journal,
                year=year,
                doi=doi,
                source=ArticleSource.EUROPE_PMC,
                domain=ResearchDomain.MEDICAL,
                url=epmc_url,
                venue=study_type or None,
                citation_count=int(citation_count) if citation_count is not None else None,
            )
        except Exception as e:
            logger.warning("Europe PMC parse failed: %s", e)
            return None