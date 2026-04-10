# core/openalex_client.py

import logging
import os
import time
from typing import List, Optional, Tuple

import httpx

from core.base_client import BaseLiteratureClient
from models.schemas import Article, ArticleSource, ResearchDomain

logger = logging.getLogger(__name__)

OPENALEX_BASE  = "https://api.openalex.org"
OPENALEX_EMAIL = os.environ.get("OPENALEX_EMAIL", "")

class OpenAlexClient(BaseLiteratureClient):
    """
    Literature client for ML/AI papers via the OpenAlex API.

    OpenAlex is fully open and free with no API key required.
    Adding your email to the User-Agent accesses the "polite pool"
    with higher rate limits and priority support.

    API docs: https://docs.openalex.org/
    """

    def __init__(self):
        email = OPENALEX_EMAIL or "your_email@example.com"
        self._client = httpx.Client(
            headers={"User-Agent": f"sr-platform/1.0 (mailto:{email})"},
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0)
        )
        logger.info(
            "OpenAlexClient initialised. Polite pool email: %s",
            email if OPENALEX_EMAIL else "not set (add OPENALEX_EMAIL to .env)"
        )

    @property
    def domain(self) -> ResearchDomain:
        return ResearchDomain.ML_AI

    @property
    def source_name(self) -> str:
        return "OpenAlex"

    def search(self, query: str, max_results: int = 20) -> List[str]:
        """Return list of OpenAlex Work IDs matching the query."""
        ids      = []
        page     = 1
        per_page = min(max_results, 200)

        while len(ids) < max_results:
            batch = min(per_page, max_results - len(ids))
            try:
                resp = self._client.get(
                    f"{OPENALEX_BASE}/works",
                    params={
                        "search":   query,
                        "per-page": batch,
                        "page":     page,
                        "select":   "id",
                    }
                )
                resp.raise_for_status()
                data  = resp.json()
                works = data.get("results", [])
                if not works:
                    break
                ids.extend(w["id"] for w in works if w.get("id"))
                page += 1
                time.sleep(0.1)
            except httpx.HTTPStatusError as e:
                logger.error("OpenAlex search HTTP %d: %s", e.response.status_code, e)
                raise
            except Exception as e:
                logger.error("OpenAlex search error: %s", e, exc_info=True)
                raise

        logger.info("OpenAlex search returned %d IDs", len(ids))
        return ids[:max_results]

    def fetch(self, ids: List[str]) -> List[Article]:
        """Fetch full metadata for OpenAlex Work IDs."""
        articles: List[Article] = []

        for i in range(0, len(ids), 50):
            batch     = ids[i:i + 50]
            short_ids = [w.replace("https://openalex.org/", "") for w in batch]
            filter_str = "openalex_id:" + "|".join(short_ids)
            try:
                resp = self._client.get(
                    f"{OPENALEX_BASE}/works",
                    params={
                        "filter":   filter_str,
                        "per-page": len(batch),
                        "select": (
                            "id,title,abstract_inverted_index,authorships,"
                            "publication_year,primary_location,doi,"
                            "cited_by_count,open_access"
                        ),
                    }
                )
                resp.raise_for_status()
                for work in resp.json().get("results", []):
                    article = self._parse_work(work)
                    if article:
                        articles.append(article)
                time.sleep(0.1)
            except httpx.HTTPStatusError as e:
                logger.error("OpenAlex fetch HTTP %d: %s", e.response.status_code, e)
                raise
            except Exception as e:
                logger.error("OpenAlex fetch error: %s", e, exc_info=True)
                raise

        return articles

    def search_and_fetch(self, query: str,
                          max_results: int = 20) -> Tuple[List[Article], Optional[str]]:
        """
        Combined search and fetch. Returns (articles, error_message).
        On success: (articles, None). On failure: ([], "error description").

        BUG FIX: Same pattern as SemanticScholarClient — exceptions are now
        surfaced as ([], error_string) rather than silently swallowed.
        """
        articles = []
        page     = 1
        per_page = min(max_results, 200)

        logger.info("OpenAlex search_and_fetch: query=%r, max_results=%d", query, max_results)

        while len(articles) < max_results:
            batch = min(per_page, max_results - len(articles))
            try:
                resp = self._client.get(
                    f"{OPENALEX_BASE}/works",
                    params={
                        "search":   query,
                        "per-page": batch,
                        "page":     page,
                        "select": (
                            "id,title,abstract_inverted_index,authorships,"
                            "publication_year,primary_location,doi,"
                            "cited_by_count,open_access"
                        ),
                    }
                )

                logger.debug("OpenAlex response: status=%d", resp.status_code)
                resp.raise_for_status()

                data  = resp.json()
                works = data.get("results", [])
                total = data.get("meta", {}).get("count", 0)

                logger.info(
                    "OpenAlex response: total_available=%d, this_page=%d works",
                    total, len(works)
                )

                if not works:
                    break

                for work in works:
                    article = self._parse_work(work)
                    if article:
                        articles.append(article)

                page += 1
                time.sleep(0.1)

            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                body   = e.response.text[:500]
                logger.error("OpenAlex HTTP %d: %s", status, body)

                if status == 429:
                    return articles, (
                        "OpenAlex rate limit exceeded (HTTP 429). "
                        "Add your email as OPENALEX_EMAIL in .env for higher limits."
                    )
                elif status == 403:
                    return articles, f"OpenAlex access denied (HTTP 403): {body}"
                else:
                    return articles, f"OpenAlex API error (HTTP {status}): {body}"

            except httpx.TimeoutException:
                return articles, "OpenAlex request timed out. Try again."

            except httpx.ConnectError:
                return articles, "Cannot connect to OpenAlex. Check your internet connection."

            except Exception as e:
                logger.error("OpenAlex unexpected error: %s", e, exc_info=True)
                return articles, f"Unexpected error: {type(e).__name__}: {e}"

        logger.info("OpenAlex search_and_fetch complete: %d articles", len(articles))
        return articles[:max_results], None

    # ── adapter: OpenAlex Work JSON → Article ─────────────────────────────────

    def _parse_work(self, work: dict) -> Optional[Article]:
        if not work:
            return None
        try:
            work_id  = work.get("id", "")
            short_id = work_id.replace("https://openalex.org/", "")
            doi      = work.get("doi") or None
            # Normalise DOI — OpenAlex returns full URL: https://doi.org/10.xxx
            if doi and doi.startswith("https://doi.org/"):
                doi = doi.replace("https://doi.org/", "")
            uid = doi or f"oa:{short_id}"

            title    = (work.get("title") or "No title").strip()
            abstract = self._reconstruct_abstract(work.get("abstract_inverted_index"))
            year     = str(work.get("publication_year") or "N/A")

            authors = [
                (authorship.get("author") or {}).get("display_name", "").strip()
                for authorship in (work.get("authorships") or [])
                if (authorship.get("author") or {}).get("display_name")
            ]

            loc     = work.get("primary_location") or {}
            src     = loc.get("source") or {}
            journal = src.get("display_name", "")
            url     = loc.get("landing_page_url") or None

            return Article(
                pmid=uid,
                title=title,
                abstract=abstract,
                authors=authors,
                journal=journal,
                year=year,
                doi=doi,
                source=ArticleSource.OPENALEX,
                domain=ResearchDomain.ML_AI,
                venue=journal or None,
                citation_count=work.get("cited_by_count"),
                url=url,
            )
        except Exception as e:
            logger.warning(
                "OpenAlex _parse_work failed for id=%r: %s",
                work.get("id", "unknown"), e, exc_info=True
            )
            return None

    @staticmethod
    def _reconstruct_abstract(inverted_index: Optional[dict]) -> str:
        """Reconstruct abstract from OpenAlex inverted index format."""
        if not inverted_index:
            return ""
        try:
            positions: dict = {}
            for word, pos_list in inverted_index.items():
                for pos in pos_list:
                    positions[pos] = word
            return " ".join(positions[i] for i in sorted(positions))
        except Exception:
            return ""

    def __del__(self):
        try:
            self._client.close()
        except Exception:
            pass
