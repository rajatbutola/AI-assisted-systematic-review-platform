# core/pubmed_client.py

# import time
# import logging
# from http.client import IncompleteRead
# from typing import List, Optional

# from Bio import Entrez

# from config.settings import NCBI_EMAIL, NCBI_API_KEY   # ← your original import
# from core.base_client import BaseLiteratureClient
# from models.schemas import Article, ArticleSource, ResearchDomain

# logger = logging.getLogger(__name__)

# Entrez.email   = NCBI_EMAIL        # ← your original config approach
# Entrez.api_key = NCBI_API_KEY

# class PubMedClient(BaseLiteratureClient):
#     """
#     Literature client for PubMed / MEDLINE (medical domain).
#     Wraps the NCBI Entrez E-utilities API via Biopython.

#     Identical logic to the original pubmed.py, restructured as a class
#     to satisfy the BaseLiteratureClient interface required by DomainRegistry.
#     """

#     @property
#     def domain(self) -> ResearchDomain:
#         return ResearchDomain.MEDICAL

#     @property
#     def source_name(self) -> str:
#         return "PubMed"

#     def search(self, query: str, max_results: int = 20,
#                max_retries: int = 3) -> List[str]:
#         """Return list of PMIDs matching the query."""
#         if not query.strip():
#             raise ValueError("Search query cannot be empty.")

#         for attempt in range(max_retries):
#             try:
#                 handle = Entrez.esearch(
#                     db="pubmed", term=query, retmax=max_results
#                 )
#                 try:
#                     record = Entrez.read(handle)
#                     logger.info("PubMed search returned %d IDs",
#                                 len(record["IdList"]))
#                     return record["IdList"]
#                 finally:
#                     handle.close()
#             except Exception as e:
#                 wait = 2 ** attempt
#                 logger.warning(
#                     "PubMed search attempt %d failed: %s. Retrying in %ds.",
#                     attempt + 1, e, wait
#                 )
#                 if attempt < max_retries - 1:   # ← no sleep on final attempt
#                     time.sleep(wait)

#         raise RuntimeError("PubMed search failed after all retries.")

#     def fetch(self, ids: List[str], batch_size: int = 10) -> List[Article]:
#         """Fetch and parse articles for a list of PMIDs."""
#         articles: List[Article] = []

#         if not ids:
#             return articles

#         for i in range(0, len(ids), batch_size):
#             batch = ids[i:i + batch_size]
#             raw   = self._fetch_batch_with_retry(batch)
#             if raw:
#                 articles.extend(self._parse_articles(raw))
#             time.sleep(0.15)

#         return articles

#     # ── internal helpers — logic identical to original pubmed.py ─────────────

#     def _fetch_batch_with_retry(self, pmids: List[str],
#                                  max_retries: int = 3) -> Optional[dict]:
#         for attempt in range(max_retries):
#             try:
#                 handle = Entrez.efetch(
#                     db="pubmed",
#                     id=",".join(pmids),
#                     rettype="abstract",
#                     retmode="xml"
#                 )
#                 try:
#                     return Entrez.read(handle)
#                 finally:
#                     handle.close()

#             except IncompleteRead:
#                 wait_time = 2 ** attempt
#                 logger.warning(
#                     "IncompleteRead on attempt %s. Retrying in %ss.",
#                     attempt + 1, wait_time
#                 )
#                 time.sleep(wait_time)

#             except Exception as e:
#                 logger.error("PubMed fetch error: %s", e)
#                 return None

#         return None

#     def _parse_articles(self, raw: dict) -> List[Article]:
#         parsed: List[Article] = []

#         for record in raw.get("PubmedArticle", []):
#             try:
#                 citation = record["MedlineCitation"]
#                 article  = citation["Article"]

#                 pmid     = str(citation["PMID"])
#                 title    = str(article.get("ArticleTitle", "No title"))

#                 abstract_parts = article.get("Abstract", {}).get("AbstractText", [])
#                 abstract = (
#                     " ".join(str(p) for p in abstract_parts)
#                     if abstract_parts else ""
#                 )

#                 authors = [
#                     f"{a.get('LastName', '')} {a.get('Initials', '')}".strip()
#                     for a in article.get("AuthorList", [])
#                     if "LastName" in a
#                 ]

#                 journal  = article.get("Journal", {}).get("Title", "")
#                 pub_date = (
#                     article.get("Journal", {})
#                             .get("JournalIssue", {})
#                             .get("PubDate", {})
#                 )
#                 year = str(pub_date.get("Year",
#                            pub_date.get("MedlineDate", "N/A")))

#                 parsed.append(Article(
#                     pmid=pmid,
#                     title=title,
#                     abstract=abstract,
#                     authors=authors,
#                     journal=journal,
#                     year=year,
#                     source=ArticleSource.PUBMED,    # ← only addition
#                     domain=ResearchDomain.MEDICAL,  # ← only addition
#                 ))

#             except (KeyError, AttributeError, TypeError) as e:
#                 logger.warning("Failed to parse one article: %s", e)

#         return parsed





# core/pubmed_client.py
#
# FIXES in this version:
#   1. Extract DOI from Article > ELocationID[@EIdType="doi"] in PubMed XML.
#      The original never set doi= on Article so PubMed articles had no DOI
#      link even though the DOI existed in PubMed.
#   2. Strip HTML/XML tags from abstract text.
#      PubMed structured abstracts contain tags like <h4>Purpose</h4> that
#      were rendering as raw markup in the UI.

import logging
import re
import time
from http.client import IncompleteRead
from typing import List, Optional

from Bio import Entrez

from config.settings import NCBI_EMAIL, NCBI_API_KEY
from core.base_client import BaseLiteratureClient
from models.schemas import Article, ArticleSource, ResearchDomain

logger = logging.getLogger(__name__)

Entrez.email   = NCBI_EMAIL or "research@example.com"
if NCBI_API_KEY:
    Entrez.api_key = NCBI_API_KEY


def _strip_html(text: str) -> str:
    """Remove HTML/XML tags and collapse whitespace."""
    if not text:
        return ""
    clean = re.sub(r"<[^>]+>", " ", str(text))
    return re.sub(r"\s+", " ", clean).strip()


def _extract_doi(article: dict) -> Optional[str]:
    """
    Extract DOI from PubMed Entrez article dict.

    PubMed XML:
        <ELocationID EIdType="doi" ValidYN="Y">10.1200/JCO.2017.72.8519</ELocationID>

    Biopython Entrez.read() returns each ELocationID as a StringElement whose
    string value IS the DOI and whose .attributes dict has {"EIdType": "doi"}.
    """
    for loc in article.get("ELocationID", []):
        try:
            eid_type = None
            if hasattr(loc, "attributes"):
                eid_type = loc.attributes.get("EIdType")
            elif isinstance(loc, dict):
                eid_type = loc.get("EIdType")

            if eid_type == "doi":
                doi = str(loc).strip()
                if doi:
                    return doi
        except Exception:
            continue
    return None


class PubMedClient(BaseLiteratureClient):
    """Literature client for PubMed / MEDLINE (medical domain)."""

    @property
    def domain(self) -> ResearchDomain:
        return ResearchDomain.MEDICAL

    @property
    def source_name(self) -> str:
        return "PubMed"

    def search(self, query: str, max_results: int = 20,
               max_retries: int = 3) -> List[str]:
        """Return list of PMIDs matching the query."""
        if not query.strip():
            raise ValueError("Search query cannot be empty.")
        for attempt in range(max_retries):
            try:
                handle = Entrez.esearch(db="pubmed", term=query, retmax=max_results)
                try:
                    record = Entrez.read(handle)
                    logger.info("PubMed search returned %d IDs", len(record["IdList"]))
                    return record["IdList"]
                finally:
                    handle.close()
            except Exception as e:
                wait = 2 ** attempt
                logger.warning("PubMed search attempt %d failed: %s. Retrying in %ds.",
                               attempt + 1, e, wait)
                if attempt < max_retries - 1:
                    time.sleep(wait)
        raise RuntimeError("PubMed search failed after all retries.")

    def fetch(self, ids: List[str], batch_size: int = 10) -> List[Article]:
        """Fetch and parse articles for a list of PMIDs."""
        articles: List[Article] = []
        if not ids:
            return articles
        for i in range(0, len(ids), batch_size):
            batch = ids[i:i + batch_size]
            raw   = self._fetch_batch_with_retry(batch)
            if raw:
                articles.extend(self._parse_articles(raw))
            time.sleep(0.15)
        return articles

    def _fetch_batch_with_retry(self, pmids: List[str],
                                max_retries: int = 3) -> Optional[dict]:
        for attempt in range(max_retries):
            try:
                handle = Entrez.efetch(
                    db="pubmed", id=",".join(pmids),
                    rettype="abstract", retmode="xml"
                )
                try:
                    return Entrez.read(handle)
                finally:
                    handle.close()
            except IncompleteRead:
                wait_time = 2 ** attempt
                logger.warning("IncompleteRead on attempt %s. Retrying in %ss.",
                               attempt + 1, wait_time)
                time.sleep(wait_time)
            except Exception as e:
                logger.error("PubMed fetch error: %s", e)
                return None
        return None

    def _parse_articles(self, raw: dict) -> List[Article]:
        parsed: List[Article] = []
        for record in raw.get("PubmedArticle", []):
            try:
                citation = record["MedlineCitation"]
                article  = citation["Article"]

                pmid  = str(citation["PMID"])
                title = _strip_html(str(article.get("ArticleTitle", "No title")))

                # Strip HTML from structured abstract parts
                abstract_parts = article.get("Abstract", {}).get("AbstractText", [])
                abstract = (
                    " ".join(_strip_html(str(p)) for p in abstract_parts)
                    if abstract_parts else ""
                )

                authors = [
                    f"{a.get('LastName', '')} {a.get('Initials', '')}".strip()
                    for a in article.get("AuthorList", [])
                    if "LastName" in a
                ]

                journal  = article.get("Journal", {}).get("Title", "")
                pub_date = (
                    article.get("Journal", {})
                           .get("JournalIssue", {})
                           .get("PubDate", {})
                )
                year = str(pub_date.get("Year",
                           pub_date.get("MedlineDate", "N/A")))

                # Extract DOI from ELocationID
                doi = _extract_doi(article)

                parsed.append(Article(
                    pmid=pmid,
                    title=title,
                    abstract=abstract,
                    authors=authors,
                    journal=journal,
                    year=year,
                    doi=doi,                       # ← DOI now populated
                    source=ArticleSource.PUBMED,
                    domain=ResearchDomain.MEDICAL,
                ))

            except (KeyError, AttributeError, TypeError) as e:
                logger.warning("Failed to parse one article: %s", e)

        return parsed



