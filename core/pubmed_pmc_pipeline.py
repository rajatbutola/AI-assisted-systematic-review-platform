# core/pubmed_pmc_pipeline.py   v1 25th March, 2026
# Two-step PubMed → PMC full-text pipeline.
#
# Step 1 (PubMed):    PubMedClient.search_and_fetch() → returns Article list
#                     with abstract-level data.  Displayed in "PubMed" mode.
#
# Step 2 (PMC):       From the PubMed PMID list, identify which articles have
#                     a corresponding PMC ID (using NCBI's elink API), then
#                     fetch full-text XML via PMCClient only for those articles.
#                     Displayed in "PMC Full-Text" mode.
#
# Design principles:
#   - Modular: PubMedPMCPipeline is the only class other code needs to call.
#   - Extensible: adding Scopus / Embase requires only a new adapter that
#     produces a list of Article objects; the PMC check is database-agnostic
#     as long as articles have a PMID.
#   - Non-destructive: PubMed results are always returned fully; PMC is an
#     *enrichment* layer that supplements, never replaces, PubMed data.

# core/pubmed_pmc_pipeline.py   v2
# 27th March


# core/pubmed_pmc_pipeline.py  — v2 31st March
#
# FIX: PMC articles missed because NCBI elink returns a *list* of PMCIDs for
# each PMID (one PMID can map to one PMCID), but the original code assumed a
# parallel zip(ids_from, pmcids_found) which is WRONG.
#
# The correct elink JSON structure is:
# {
#   "linksets": [{
#     "ids": ["34201565", "34110507", ...],   <-- ALL PMIDs in the batch
#     "linksetdbs": [{
#       "linkname": "pubmed_pmc",
#       "links": ["8490188", "7954678", ...]  <-- PMCIDs, SAME ORDER as ids
#     }]
#   }]
# }
#
# So ids[i] maps to links[i] — a positional 1:1 mapping when both lists have
# the same length.  BUT elink only returns PMCIDs for articles that HAVE a PMC
# entry; articles without PMC coverage are SKIPPED entirely, making the two
# lists different lengths.  The zip() silently truncated.
#
# FIX: Use the "idcheckresult" / "linksetdbhistories" approach, OR use the
# elink mode=llinks which returns per-PMID mappings explicitly.
# The cleanest fix: switch to elink with "cmd=neighbor_score" or use
# the efetch-based approach: POST each PMID to the elink endpoint
# individually in batches, parsing per-PMID.
#
# We use the correct approach: send one PMID at a time (or small batches)
# with retmode=json and parse the per-ID linksets properly.

import logging
import time
from typing import Dict, List, Tuple

import requests

from config.settings import NCBI_API_KEY, NCBI_EMAIL
from models.schemas import Article

logger = logging.getLogger(__name__)

_ELINK_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi"


class PubMedPMCPipeline:
    """
    Two-step pipeline:
      1. run_pubmed()      — returns Article list from PubMed client
      2. run_pmc_filter()  — maps PMIDs → PMCIDs, fetches full text
    """

    def __init__(self, pubmed_client, pmc_client):
        self.pubmed_client = pubmed_client
        self.pmc_client    = pmc_client
        self._params_base  = {
            "tool":  "SR_Platform",
            "email": NCBI_EMAIL or "research@example.com",
        }
        if NCBI_API_KEY:
            self._params_base["api_key"] = NCBI_API_KEY

    def run_pubmed(self, query: str, max_results: int) -> List[Article]:
        logger.info("PubMedPMCPipeline.run_pubmed: query=%r max=%d", query, max_results)
        return self.pubmed_client.search_and_fetch(query, max_results)

    def run_pmc_filter(
        self, pubmed_articles: List[Article]
    ) -> Tuple[List[Article], Dict[str, str]]:
        if not pubmed_articles:
            return [], {}

        pmids = [a.pmid for a in pubmed_articles if a.pmid]
        logger.info("PMC filter: checking %d PMIDs", len(pmids))

        pmid_to_pmcid = self._map_pmids_to_pmcids(pmids)
        if not pmid_to_pmcid:
            logger.info("No PMC IDs found.")
            return [], {}

        logger.info("PMC coverage: %d / %d", len(pmid_to_pmcid), len(pmids))

        pmcids      = list(pmid_to_pmcid.values())
        pmc_articles = self.pmc_client.fetch(pmcids)
        pmc_by_pmcid = {a.pmid: a for a in pmc_articles}

        enriched:  List[Article]     = []
        pmc_urls:  Dict[str, str]    = {}

        for article in pubmed_articles:
            pmcid = pmid_to_pmcid.get(article.pmid)
            if not pmcid:
                continue
            pmc_art = pmc_by_pmcid.get(pmcid)
            if pmc_art and pmc_art.full_text:
                enriched.append(article.model_copy(
                    update={"full_text": pmc_art.full_text}
                ))
            else:
                enriched.append(article)
            pmc_urls[article.pmid] = (
                f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/"
            )

        return enriched, pmc_urls

    def _map_pmids_to_pmcids(self, pmids: List[str]) -> Dict[str, str]:
        """
        Correct elink parsing: send ONE pmid at a time so the response has
        exactly one id in ids[] and at most one entry in links[].
        We batch in groups of 20 to avoid too many HTTP round-trips while
        still being able to parse the response correctly.
        
        The key insight: when multiple PMIDs are sent in one elink request,
        the response collapses them into a SINGLE linkset where `ids` contains
        ALL input PMIDs but `links` contains ONLY the subset that have PMC IDs.
        There is no positional correspondence — you cannot know which PMC ID
        belongs to which PMID from a batch response.
        
        Solution: send each PMID individually (batched to respect rate limits).
        """
        result: Dict[str, str] = {}
        # Rate: 3/s without key, 10/s with key
        delay = 0.11 if NCBI_API_KEY else 0.34

        for pmid in pmids:
            try:
                params = {
                    **self._params_base,
                    "dbfrom":  "pubmed",
                    "db":      "pmc",
                    "id":      pmid,
                    "retmode": "json",
                }
                resp = requests.get(_ELINK_URL, params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()

                for linkset in data.get("linksets", []):
                    for ldb in linkset.get("linksetdbs", []):
                        if ldb.get("linkname") == "pubmed_pmc":
                            links = ldb.get("links", [])
                            if links:
                                # Take first (should be only one per PMID)
                                result[str(pmid)] = f"PMC{links[0]}"
                                logger.debug("PMID %s → PMC%s", pmid, links[0])

                time.sleep(delay)

            except Exception as e:
                logger.warning("elink failed for PMID %s: %s", pmid, e)

        return result

    @staticmethod
    def get_pdf_url(pmcid: str) -> str:
        return f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/pdf/"













