import os
import time
import logging
from http.client import IncompleteRead
from typing import List, Optional

from Bio import Entrez
from config.settings import NCBI_EMAIL, NCBI_API_KEY
from models.schemas import Article

logger = logging.getLogger(__name__)

Entrez.email = NCBI_EMAIL
Entrez.api_key = NCBI_API_KEY


def search_pubmed(query: str, max_results: int = 20, max_retries: int = 3) -> List[str]:
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
            logger.warning("Search attempt %d failed: %s. Retrying in %ds.", attempt + 1, e, wait)
            if attempt < max_retries - 1:
                import time
                time.sleep(wait)
    
    raise RuntimeError("PubMed search failed after all retries.")


def fetch_articles(pmids: List[str], batch_size: int = 10) -> List[Article]:
    articles: List[Article] = []

    if not pmids:
        return articles

    for i in range(0, len(pmids), batch_size):
        batch = pmids[i:i + batch_size]
        raw = _fetch_batch_with_retry(batch)
        if raw:
            articles.extend(_parse_articles(raw))
        time.sleep(0.15)

    return articles


def _fetch_batch_with_retry(pmids: List[str], max_retries: int = 3) -> Optional[dict]:
    for attempt in range(max_retries):
        try:
            handle = Entrez.efetch(
                db="pubmed",
                id=",".join(pmids),
                rettype="abstract",
                retmode="xml"
            )
            try:
                result = Entrez.read(handle)
                return result
            finally:
                handle.close()

        except IncompleteRead:
            wait_time = 2 ** attempt
            logger.warning("IncompleteRead on attempt %s. Retrying in %ss.", attempt + 1, wait_time)
            time.sleep(wait_time)

        except Exception as e:
            logger.error("PubMed fetch error: %s", e)
            return None

    return None


def _parse_articles(raw: dict) -> List[Article]:
    parsed_articles: List[Article] = []

    for record in raw.get("PubmedArticle", []):
        try:
            citation = record["MedlineCitation"]
            article = citation["Article"]

            pmid = str(citation["PMID"])
            title = str(article.get("ArticleTitle", "No title"))

            abstract_parts = article.get("Abstract", {}).get("AbstractText", [])
            abstract = " ".join(str(part) for part in abstract_parts) if abstract_parts else ""

            authors = []
            for author in article.get("AuthorList", []):
                if "LastName" in author:
                    authors.append(
                        f"{author.get('LastName', '')} {author.get('Initials', '')}".strip()
                    )

            journal = article.get("Journal", {}).get("Title", "")
            pub_date = article.get("Journal", {}).get("JournalIssue", {}).get("PubDate", {})
            year = str(pub_date.get("Year", pub_date.get("MedlineDate", "N/A")))

            parsed_articles.append(
                Article(
                    pmid=pmid,
                    title=title,
                    abstract=abstract,
                    authors=authors,
                    journal=journal,
                    year=year,
                )
            )

        except (KeyError, AttributeError, TypeError) as e:
            logger.warning("Failed to parse one article: %s", e)

    return parsed_articles