# core/semantic_scholar_client.py

import logging
import os
import time
from typing import List, Optional, Tuple

import httpx

from core.base_client import BaseLiteratureClient
from models.schemas import Article, ArticleSource, ResearchDomain

logger = logging.getLogger(__name__)

S2_BASE    = "https://api.semanticscholar.org/graph/v1"
S2_API_KEY = os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "")

# ── CRITICAL BUG FIX 1: Field list ───────────────────────────────────────────
#
# The original _PAPER_FIELDS included:
#   "publicationVenue" and "openAccessPdf"
#
# These fields require an API key on the S2 API (as of 2024).
# Without a key, requesting them returns HTTP 403 Forbidden.
# That 403 was caught by the exception handler, which silently broke the loop
# and returned an empty list — causing the "0 articles saved" symptom.
#
# Fix: use ONLY fields available to unauthenticated requests.
# When an API key is present, the extended field set is used automatically.
#
_PAPER_FIELDS_NO_KEY = (
    "paperId,title,abstract,authors,year,venue,externalIds,citationCount"
)
_PAPER_FIELDS_WITH_KEY = (
    "paperId,title,abstract,authors,year,venue,"
    "externalIds,citationCount,openAccessPdf,publicationVenue"
)

def _get_fields() -> str:
    """Return the correct field set based on whether an API key is configured."""
    return _PAPER_FIELDS_WITH_KEY if S2_API_KEY else _PAPER_FIELDS_NO_KEY

class SemanticScholarClient(BaseLiteratureClient):
    """
    Literature client for ML/AI papers via the Semantic Scholar Graph API.

    API docs: https://api.semanticscholar.org/api-docs/graph

    Rate limits:
      - Without API key: 1 req/second (free, no registration needed)
      - With    API key: 10 req/second (free key, register at s2.allenai.org)

    STRONGLY recommended: register for a free API key. It takes 5 minutes
    and unlocks 10x the rate limit plus additional metadata fields.
    Set SEMANTIC_SCHOLAR_API_KEY in your .env file.
    """

    def __init__(self):
        # ── CRITICAL BUG FIX 2: Always include User-Agent ────────────────────
        # Without User-Agent, some S2 API endpoints treat the request as
        # a bot and return 403. Identifying your application is good practice
        # and required by the S2 API Terms of Service.
        headers = {"User-Agent": "sr-platform/1.0"}
        if S2_API_KEY:
            headers["x-api-key"] = S2_API_KEY

        self._client = httpx.Client(
            headers=headers,
            timeout=httpx.Timeout(
                connect=10.0,   # connection establishment
                read=30.0,      # waiting for response bytes
                write=10.0,
                pool=5.0,
            )
        )
        logger.info(
            "SemanticScholarClient initialised. API key: %s",
            "YES" if S2_API_KEY else "NO (1 req/s rate limit applies)"
        )

    @property
    def domain(self) -> ResearchDomain:
        return ResearchDomain.ML_AI

    @property
    def source_name(self) -> str:
        return "Semantic Scholar"

    def search(self, query: str, max_results: int = 20) -> List[str]:
        """Return list of S2 paper IDs matching the query."""
        ids    = []
        offset = 0
        limit  = min(max_results, 100)

        while len(ids) < max_results:
            batch = min(limit, max_results - len(ids))
            try:
                resp = self._client.get(
                    f"{S2_BASE}/paper/search",
                    params={
                        "query":  query,
                        "limit":  batch,
                        "offset": offset,
                        "fields": "paperId",
                    }
                )
                # ── CRITICAL BUG FIX 3: Expose HTTP errors to the caller ─────
                # Previously: HTTPStatusError was caught here and silently broke
                # the loop, returning []. The caller had no way to know why.
                # Now: we let it propagate so search_and_fetch can handle it
                # properly and show the user an informative error.
                resp.raise_for_status()

                data   = resp.json()
                papers = data.get("data", [])

                logger.debug(
                    "S2 search page: offset=%d, returned=%d papers, total=%s",
                    offset, len(papers), data.get("total", "?")
                )

                if not papers:
                    break

                ids.extend(p["paperId"] for p in papers if p.get("paperId"))
                offset += len(papers)

                # Respect rate limit: 1 req/s without key, 10 req/s with key
                time.sleep(0.12 if S2_API_KEY else 1.1)

            except httpx.HTTPStatusError as e:
                logger.error(
                    "S2 search HTTP %d for query %r. Body: %s",
                    e.response.status_code, query,
                    e.response.text[:200]
                )
                raise   # ← re-raise so caller can show user a real error message
            except httpx.TimeoutException as e:
                logger.error("S2 search timed out for query %r: %s", query, e)
                raise
            except Exception as e:
                logger.error("S2 search unexpected error: %s", e, exc_info=True)
                raise

        logger.info("Semantic Scholar search returned %d IDs for %r", len(ids), query)
        return ids[:max_results]

    def fetch(self, ids: List[str]) -> List[Article]:
        """Fetch full metadata for a list of S2 paper IDs (batch endpoint)."""
        articles: List[Article] = []
        fields = _get_fields()

        for i in range(0, len(ids), 100):
            batch = ids[i:i + 100]
            try:
                resp = self._client.post(
                    f"{S2_BASE}/paper/batch",
                    params={"fields": fields},
                    json={"ids": batch}
                )
                resp.raise_for_status()
                raw_papers = resp.json()
                logger.debug("S2 batch fetch: requested=%d, returned=%d", len(batch), len(raw_papers))
                for paper in raw_papers:
                    article = self._parse_paper(paper)
                    if article:
                        articles.append(article)
                time.sleep(0.12 if S2_API_KEY else 1.1)
            except httpx.HTTPStatusError as e:
                logger.error("S2 batch fetch HTTP %d: %s", e.response.status_code, e.response.text[:200])
                raise
            except Exception as e:
                logger.error("S2 batch fetch error: %s", e, exc_info=True)
                raise

        return articles

    def search_and_fetch(self, query: str,
                          max_results: int = 20) -> Tuple[List[Article], Optional[str]]:
        """
        Override: S2's search endpoint returns full fields directly,
        saving a separate fetch round-trip.

        CRITICAL BUG FIX 4: This method previously swallowed all exceptions
        and returned an empty list with no indication of what went wrong.
        It now returns (articles, error_message) so the caller (app.py) can
        show the user a meaningful error in the Streamlit UI.

        Returns:
            Tuple of (list of Article objects, error string or None)
            On success: (articles, None)
            On failure: ([], "human-readable error message")
        """
        articles = []
        offset   = 0
        limit    = min(max_results, 100)
        fields   = _get_fields()

        logger.info(
            "S2 search_and_fetch: query=%r, max_results=%d, fields=%s",
            query, max_results, fields
        )

        while len(articles) < max_results:
            batch = min(limit, max_results - len(articles))
            try:
                resp = self._client.get(
                    f"{S2_BASE}/paper/search",
                    params={
                        "query":  query,
                        "limit":  batch,
                        "offset": offset,
                        "fields": fields,
                    }
                )

                # ── Log the raw response for debugging ───────────────────────
                logger.debug(
                    "S2 raw response: status=%d, url=%s",
                    resp.status_code, resp.url
                )

                resp.raise_for_status()

                data   = resp.json()
                papers = data.get("data", [])
                total  = data.get("total", 0)

                logger.info(
                    "S2 response: total_available=%d, this_page=%d papers",
                    total, len(papers)
                )

                if not papers:
                    logger.info("S2 returned 0 papers for query %r (total=%d)", query, total)
                    break

                parsed_this_page = 0
                for paper in papers:
                    article = self._parse_paper(paper)
                    if article:
                        articles.append(article)
                        parsed_this_page += 1

                logger.debug(
                    "S2 page parsed: %d/%d papers produced valid Article objects",
                    parsed_this_page, len(papers)
                )

                offset += len(papers)
                time.sleep(0.12 if S2_API_KEY else 1.1)

            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                body   = e.response.text[:500]
                logger.error("S2 HTTP %d: %s", status, body)

                # ── Return informative error messages per status code ─────────
                if status == 429:
                    return articles, (
                        f"Semantic Scholar rate limit exceeded (HTTP 429). "
                        f"Wait a moment and try again, or add a free API key "
                        f"to your .env file (SEMANTIC_SCHOLAR_API_KEY) to get "
                        f"10x higher rate limits."
                    )
                elif status == 403:
                    return articles, (
                        f"Semantic Scholar access denied (HTTP 403). "
                        f"This usually means the requested fields require an API key. "
                        f"Get a free key at https://www.semanticscholar.org/product/api "
                        f"and set SEMANTIC_SCHOLAR_API_KEY in your .env file."
                    )
                elif status == 400:
                    return articles, (
                        f"Semantic Scholar rejected the query (HTTP 400): {body}. "
                        f"Try simplifying the search terms."
                    )
                else:
                    return articles, f"Semantic Scholar API error (HTTP {status}): {body}"

            except httpx.TimeoutException:
                return articles, (
                    "Semantic Scholar request timed out. "
                    "The API may be slow without an API key. Try again or reduce Max Results."
                )

            except httpx.ConnectError:
                return articles, (
                    "Cannot connect to Semantic Scholar. "
                    "Check your internet connection."
                )

            except Exception as e:
                logger.error("S2 unexpected error: %s", e, exc_info=True)
                return articles, f"Unexpected error: {type(e).__name__}: {e}"

        logger.info(
            "S2 search_and_fetch complete: %d articles returned for %r",
            len(articles), query
        )
        return articles[:max_results], None

    # ── adapter: S2 JSON → Article ────────────────────────────────────────────

    def _parse_paper(self, paper: dict) -> Optional[Article]:
        """
        Adapter: transforms raw S2 API JSON into the Article schema.

        Returns None only if the paper dict is empty or has no paperId.
        All other missing fields are handled gracefully with defaults.
        """
        if not paper:
            return None

        try:
            paper_id = paper.get("paperId") or ""
            if not paper_id:
                logger.debug("S2 paper skipped: missing paperId")
                return None

            # ── Stable unique ID: prefer PubMed ID, then DOI, then S2 ID ─────
            ext_ids  = paper.get("externalIds") or {}
            doi      = ext_ids.get("DOI") or None
            pubmed_id = ext_ids.get("PubMed") or None
            uid      = pubmed_id or doi or f"s2:{paper_id}"

            title    = (paper.get("title") or "").strip() or "No title"
            abstract = (paper.get("abstract") or "").strip()
            year     = str(paper.get("year") or "N/A")

            authors = [
                a.get("name", "").strip()
                for a in (paper.get("authors") or [])
                if a.get("name")
            ]

            # ── Venue: try structured field first, fall back to legacy ────────
            # publicationVenue is only returned with API key
            pub_venue = paper.get("publicationVenue") or {}
            venue     = (
                pub_venue.get("name")
                or (paper.get("venue") or "").strip()
                or ""
            )
            journal = venue

            # openAccessPdf only returned with API key
            pdf_info = paper.get("openAccessPdf") or {}
            url      = pdf_info.get("url") or None

            article = Article(
                pmid=uid,
                title=title,
                abstract=abstract,
                authors=authors,
                journal=journal,
                year=year,
                doi=doi,
                source=ArticleSource.SEMANTIC_SCHOLAR,
                domain=ResearchDomain.ML_AI,
                venue=venue or None,
                citation_count=paper.get("citationCount"),
                url=url,
            )

            logger.debug("S2 parsed: uid=%s title=%r", uid, title[:60])
            return article

        except Exception as e:
            logger.warning(
                "S2 _parse_paper failed for paperId=%r: %s",
                paper.get("paperId", "unknown"), e, exc_info=True
            )
            return None

    def __del__(self):
        try:
            self._client.close()
        except Exception:
            pass
