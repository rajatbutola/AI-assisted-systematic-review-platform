# app.py v1 - 25th March
#
# AI-Assisted Systematic Review Platform
#
# Tab order (updated per spec):
#   1.  Search       — PubMed / PMC Full-Text / ML-AI retrieval
#   2.  Screening    — Multi-reviewer title/abstract screening
#   3.  AI Analysis  — Summarisation, PICO, Quality, Data Pooling
#                         (only included articles, AFTER screening)
#   4.  PRISMA       — Publication-quality PRISMA 2020 diagram
#
# Key changes from previous version:
#   - AI Analysis moved after Screening (dependency enforced in ui/ai_analysis_panel.py)
#   - PMC now uses two-step pipeline (PubMed search → PMC filter)
#   - PubMed results are shown immediately in "PubMed" mode
#   - PMC filter results shown separately in "PMC Full-Text" mode
#   - PRISMA redesigned with B&W + Colour dual modes, dynamic databases

# app.py v2 27th March

# ── BUG FIXES ────────────────────────────────────────────────────────────────
#
#  FIX 1 — Search results flash then disappear
#    Root cause: st.rerun() was called immediately after saving articles,
#    which re-executed the entire script. Because the fetched `articles` list
#    lived only in local variables inside the `if submitted:` block, it was
#    gone on the next render. The user saw results for a split second then they
#    vanished.
#    Fix: store search results in st.session_state keyed by (review_id, source).
#    The display block is placed OUTSIDE the `if submitted:` guard so it runs
#    on every render and always reads from session_state. st.rerun() is only
#    called after results are already safely persisted in session_state, so
#    they survive into the next render cycle.
#
#  FIX 2 — Blinded screening (see ui/screening_panel.py)
#    Primary reviewers now only see their own decisions. Other reviewers'
#    decisions are hidden until the Editor's conflict-resolution view.
#
#  FIX 3 — AI Analysis wrong counts / quality parse error (see ui/ai_analysis_panel.py)
#    - Adjudicator decision lookup now checks the correct reviewer_id key that
#      the AdjudicationRepository stores (current_reviewer_id of the Editor).
#    - "Pending" count is articles with zero decisions from any reviewer.
#    - Quality prompt no longer has the double-brace injection bug.
#
#  FIX 4 — PRISMA B&W toggle infinite rerun + customisation not applying
#    Mode is stored in session_state immediately; DB write happens only on
#    "Apply Changes". Dynamic db labels pulled from session_state automatically.
# ─────────────────────────────────────────────────────────────────────────────

# app.py  — v3 -27th March
#
# PubMed/PMC interface redesign:
#   - Removed "PMC Full-Text" from the Data Source selectbox entirely.
#   - After a PubMed search the results area shows TWO sub-tabs:
#       PubMed Results  — all articles retrieved from PubMed
#       PMC Full-Text   — subset with open-access full text + links
#   - The PMC sub-tab is populated by a "🔍 Find Full Texts in PMC" button
#     that only appears AFTER a PubMed search has been done.
#   - This makes clear that PMC is an enrichment step, not a separate search.


# app.py  — v3 30th March
#
# PubMed/PMC interface redesign:
#   - Removed "PMC Full-Text" from the Data Source selectbox entirely.
#   - After a PubMed search the results area shows TWO sub-tabs:
#        PubMed Results  — all articles retrieved from PubMed
#        PMC Full-Text   — subset with open-access full text + links
#   - The PMC sub-tab is populated by a "🔍 Find Full Texts in PMC" button
#     that only appears AFTER a PubMed search has been done.
#   - This makes clear that PMC is an enrichment step, not a separate search.



# app.py — v4 31st March
#
# NEW IN THIS VERSION:
#
# 1. Europe PMC as a second medical source (dropdown: PubMed / Europe PMC)
#    - Same PICO form, same study-type filters, same PMC full-text tab
#    - Europe PMC query uses different syntax (TITLE: ABSTRACT: fields)
#
# 2. Multi-source search with deduplication
#    - When both PubMed AND Europe PMC have been searched for the same review,
#      a "Combined + Deduplicate" button appears
#    - Deduplication is by PMID (exact match); EPMC-only articles keep their EPMC: id
#    - Duplicate count is tracked and stored in session_state for PRISMA
#
# 3. PRISMA dynamic database boxes
#    - Stores per-source counts: {"PubMed": 2500, "Europe PMC": 3150, "duplicates": 1150}
#    - PRISMA panel reads this and renders two identification boxes + dedup note
#
# 4. Enhanced article cards
#    - Authors (truncated to first 3 + et al.)
#    - Citation count with icon
#    - Study type / publication type badge
#    - Source badge (PubMed vs Europe PMC)
#
# 5. Sort functionality
#    - Sort by: Newest first / Oldest first / Most cited
#    - Applied client-side to the persisted results list



# app.py — v6 7th April
#
# FIX 1 — PRISMA source count accumulation (not overwrite)
#   Old: prisma_sources[source_name] = len(articles)
#        This REPLACED the old PubMed count with new PubMed count.
#        PRISMA showed wrong numbers after subsequent searches.
#   Fix: Store per-source counts CUMULATIVELY in a persistent DB table
#        (search_repository already tracks n_results per search_id).
#        The PRISMA panel reads the SUM of all searches per source from
#        the search_repository, not from session_state.
#        session_state still caches the LATEST search results for display,
#        but PRISMA counts come from the DB truth.
#        Additionally: if a user changes their PICO query under the same
#        review, a warning is shown offering to either (a) continue adding
#        to the same review, or (b) create a new review automatically.
#
# FIX 2 — Unified full-text button for all articles regardless of source
#   Old: separate "Find Full Texts in PMC" per source (PubMed vs Europe PMC)
#        and Europe PMC articles with EPMC: prefix were excluded from PMC lookup.
#   Fix: One unified "Find All Full Texts" button that:
#        (a) Takes ALL unique articles from BOTH sources (deduplicated)
#        (b) Checks NCBI PMC for all articles that have a real PMID
#        (c) Also checks Europe PMC's own full-text endpoint for EPMC: articles
#        (d) Shows source of each full text (PMC / Europe PMC)
#        Europe PMC articles that are also in PubMed (same PMID) are only
#        fetched once, not twice.
#
# FIX 3 — PRISMA diagram arrow rendering (see ui/prisma_panel.py)
#   Arrows now use SVG path shapes (go.layout.Shape with type="path")
#   instead of annotations with axref/ayref, which had inconsistent
#   coordinate mapping in Plotly and caused arrows to miss box edges.




# app.py — v6
#
# v6 additions:
#   PERF 1 — Parallel full-text retrieval in _run_unified_fulltext
#     All three passes now use ThreadPoolExecutor so articles are processed
#     concurrently instead of sequentially.
#     Pass 1: HEAD validation parallelised (was the biggest bottleneck — 8s per article)
#     Pass 2: Europe PMC search parallelised (one HTTP request per article)
#     Pass 3: Unpaywall parallelised (one HTTP request per DOI)
#     Pass 4: CORE Aggregate search for articles still missing a PDF
#     All shared dicts (pmc_urls, pdf_urls, full_texts, enriched_map) are
#     protected with threading.Lock() for thread-safe writes.
#     Expected speedup: ~8x for 40 articles, scales linearly with max_workers.
#   PERF 2 — Validated NCBI PDF URLs via shared _validate_ncbi_pdf() helper
#     (avoids storing broken /pdf/ URLs that redirect to Silverchair CDN tokens)





# app.py — v7
# v7: UMLS concept expansion integrated into PICO search form
#
# v5 fixes (unchanged):
#   FIX 1 — PRISMA source count accumulation (cumulative from DB, not overwrite)
#   FIX 2 — Unified full-text tab: 3-pass additive strategy (NCBI → Europe PMC → Unpaywall)
#   FIX 3 — PRISMA diagram arrow rendering (SVG path shapes)
#
# v6 additions:
#   PERF 1 — Parallel full-text retrieval in _run_unified_fulltext
#     All three passes now use ThreadPoolExecutor so articles are processed
#     concurrently instead of sequentially.
#     Pass 1: HEAD validation parallelised (was the biggest bottleneck — 8s per article)
#     Pass 2: Europe PMC search parallelised (one HTTP request per article)
#     Pass 3: Unpaywall parallelised (one HTTP request per DOI)
#     Pass 4: CORE Aggregate search for articles still missing a PDF
#     All shared dicts (pmc_urls, pdf_urls, full_texts, enriched_map) are
#     protected with threading.Lock() for thread-safe writes.
#     Expected speedup: ~8x for 40 articles, scales linearly with max_workers.
#   PERF 2 — Validated NCBI PDF URLs via shared _validate_ncbi_pdf() helper
#     (avoids storing broken /pdf/ URLs that redirect to Silverchair CDN tokens)









# app.py — v7
# v7: UMLS concept expansion integrated into PICO search form
#
# v5 fixes (unchanged):
#   FIX 1 — PRISMA source count accumulation (cumulative from DB, not overwrite)
#   FIX 2 — Unified full-text tab: 3-pass additive strategy (NCBI → Europe PMC → Unpaywall)
#   FIX 3 — PRISMA diagram arrow rendering (SVG path shapes)
#
# v6 additions:
#   PERF 1 — Parallel full-text retrieval in _run_unified_fulltext
#     All three passes now use ThreadPoolExecutor so articles are processed
#     concurrently instead of sequentially.
#     Pass 1: HEAD validation parallelised (was the biggest bottleneck — 8s per article)
#     Pass 2: Europe PMC search parallelised (one HTTP request per article)
#     Pass 3: Unpaywall parallelised (one HTTP request per DOI)
#     Pass 4: CORE Aggregate search for articles still missing a PDF
#     All shared dicts (pmc_urls, pdf_urls, full_texts, enriched_map) are
#     protected with threading.Lock() for thread-safe writes.
#     Expected speedup: ~8x for 40 articles, scales linearly with max_workers.
#   PERF 2 — Validated NCBI PDF URLs via shared _validate_ncbi_pdf() helper
#     (avoids storing broken /pdf/ URLs that redirect to Silverchair CDN tokens)









import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional

import streamlit as st

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("sr_platform.log"),
    ],
)
logger = logging.getLogger(__name__)

from storage.database          import init_database
from storage.migrations        import run_migrations
from storage.repository        import (
    ReviewRepository, ArticleRepository, ScreeningRepository,
    AdjudicationRepository,
)
from storage.search_repository import SearchRepository
from core.domain_registry      import build_default_registry
from core.query_builder        import build_query, build_ml_query, build_epmc_query
from core.pubmed_pmc_pipeline  import PubMedPMCPipeline
from models.schemas            import ResearchDomain, Article
from ui.prisma_panel           import render_prisma_diagram
from ui.screening_panel        import render_screening_panel
from ui.ai_analysis_panel      import render_ai_analysis_panel
from ui.styles                 import inject_styles

init_database()
run_migrations()

review_repo       = ReviewRepository()
article_repo      = ArticleRepository()
screen_repo       = ScreeningRepository()
adjudication_repo = AdjudicationRepository()
search_repo       = SearchRepository()


@st.cache_resource
def get_registry(_version=7):
    return build_default_registry()


registry = get_registry()

st.set_page_config(
    page_title="SciSynth — AI Systematic Review",
    page_icon="🔭",
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_styles()


# ══════════════════════════════════════════════════════════════════════════════
# SESSION-STATE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _sk(rid, key):
    return f"{key}_{rid}"

def _store(rid, key, value):
    st.session_state[_sk(rid, key)] = value

def _load(rid, key, default=None):
    return st.session_state.get(_sk(rid, key), default)


# ══════════════════════════════════════════════════════════════════════════════
# PRISMA STATE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _get_prisma_sources_from_db(review_id: int) -> Dict:
    try:
        source_totals = search_repo.get_source_totals(review_id)
    except Exception as e:
        logger.error("_get_prisma_sources_from_db failed: %s", e)
        return {}
    dupes = _load(review_id, "n_duplicates_removed", 0) or 0
    if dupes:
        source_totals["duplicates_removed"] = dupes
    return source_totals


def _update_prisma_info(review_id: int, source_name: str, n_articles: int,
                        n_dupes: int = 0) -> None:
    if n_dupes > 0:
        _store(review_id, "n_duplicates_removed", n_dupes)
    sources = _get_prisma_sources_from_db(review_id)
    st.session_state[f"prisma_db_{review_id}"] = sources


def _refresh_prisma_from_db(review_id: int) -> None:
    sources = _get_prisma_sources_from_db(review_id)
    st.session_state[f"prisma_db_{review_id}"] = sources


# ══════════════════════════════════════════════════════════════════════════════
# ARTICLE FIELD ACCESSOR
# Handles both Article dataclass objects and plain dicts (from DB restore).
# Use this everywhere instead of article.field — it is safe for both types.
# ══════════════════════════════════════════════════════════════════════════════

def _af(article, field, default=None):
    """Get a field from an Article object OR a dict safely."""
    if isinstance(article, dict):
        v = article.get(field)
    else:
        v = getattr(article, field, None)
    return v if v is not None else default


# ══════════════════════════════════════════════════════════════════════════════
# SORT HELPER
# ══════════════════════════════════════════════════════════════════════════════

def _sort_articles(articles, sort_by: str):
    if sort_by == "Newest first":
        return sorted(
            articles,
            key=lambda a: int(_af(a, "year", 0)) if str(_af(a, "year", "")).isdigit() else 0,
            reverse=True,
        )
    elif sort_by == "Oldest first":
        return sorted(
            articles,
            key=lambda a: int(_af(a, "year", 9999)) if str(_af(a, "year", "")).isdigit() else 9999,
        )
    elif sort_by == "Most cited":
        return sorted(
            articles,
            key=lambda a: _af(a, "citation_count", -1) if _af(a, "citation_count") is not None else -1,
            reverse=True,
        )
    return articles


# ══════════════════════════════════════════════════════════════════════════════
# DOI HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _clean_doi(doi_raw: str) -> str:
    doi = (doi_raw or "").strip().lower()
    for prefix in ("https://doi.org/", "http://doi.org/",
                   "https://dx.doi.org/", "doi:"):
        if doi.startswith(prefix):
            doi = doi[len(prefix):]
            break
    return doi.strip()


def _clean_doi_for_display(doi_raw: str) -> str:
    doi = doi_raw.strip()
    for prefix in ("https://doi.org/", "http://doi.org/",
                   "https://dx.doi.org/", "doi:"):
        if doi.lower().startswith(prefix.lower()):
            doi = doi[len(prefix):]
            break
    return f"https://doi.org/{doi.strip()}"


def _title_key(title: str) -> str:
    import re
    t = re.sub(r"[^a-z0-9 ]", "", (title or "").lower())
    t = re.sub(r"\s+", " ", t).strip()
    return t[:60]


# ══════════════════════════════════════════════════════════════════════════════
# DEDUPLICATION
# ══════════════════════════════════════════════════════════════════════════════

def _deduplicate(*lists) -> tuple:
    """
    Deduplicate articles across any number of source lists.
    Handles both Article dataclass objects and plain dicts.
    """
    seen_pmid:  Dict[str, object] = {}
    seen_doi:   Dict[str, object] = {}
    seen_title: Dict[str, object] = {}
    unique: list = []
    dupes = 0

    for art_list in lists:
        for art in (art_list or []):
            pmid  = (_af(art, "pmid") or "").strip()
            doi   = _clean_doi(_af(art, "doi") or "")
            if not doi and pmid.startswith("10."):
                doi = pmid
            title = _title_key(_af(art, "title") or "")

            is_dupe = (
                (pmid  and pmid  in seen_pmid)  or
                (doi   and doi   in seen_doi)   or
                (title and title in seen_title)
            )

            if is_dupe:
                dupes += 1
            else:
                unique.append(art)
                if pmid:  seen_pmid[pmid]   = art
                if doi:   seen_doi[doi]     = art
                if title: seen_title[title] = art

    return unique, dupes


# ══════════════════════════════════════════════════════════════════════════════
# PICO CHANGE DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def _pico_fingerprint(population, intervention, comparison, outcome,
                      year_from, year_to, study_type) -> str:
    return f"{population}|{intervention}|{comparison}|{outcome}|{year_from}|{year_to}|{study_type}"


def _check_pico_change(review_id: int, new_fp: str) -> bool:
    old_fp = st.session_state.get(f"pico_fp_{review_id}")
    if old_fp is None:
        return False
    return old_fp != new_fp


def _store_pico_fp(review_id: int, fp: str) -> None:
    st.session_state[f"pico_fp_{review_id}"] = fp


# ══════════════════════════════════════════════════════════════════════════════
# ARTICLE CARD RENDERER
# Works with both Article dataclass objects and plain dicts from DB.
# ══════════════════════════════════════════════════════════════════════════════

def _render_article_cards(articles, pmc_urls: dict,
                           pdf_urls: dict = None,
                           sort_by: str = "Newest first",
                           is_fulltext_view: bool = False) -> None:
    import re
    if not articles:
        st.info("No articles to display.")
        return
    if pdf_urls is None:
        pdf_urls = {}

    for article in _sort_articles(articles, sort_by):
        pmid = _af(article, "pmid", "")
        pmc_url = pmc_urls.get(pmid)

        # source can be an enum value or a string
        raw_source = _af(article, "source", "")
        source_label = getattr(raw_source, "value", str(raw_source)).lower()
        source_badge = {
            "pubmed":     "🔵 PubMed",
            "europe_pmc": "🟢 Europe PMC",
            "pmc":        "📄 PMC",
            "core":       "🟠 CORE",
        }.get(source_label, source_label.upper())

        title_str = _af(article, "title", "No title") or "No title"
        with st.expander(f"**{title_str[:85]}**", expanded=False):
            col_meta, col_links = st.columns([3, 1])

            with col_meta:
                authors = _af(article, "authors") or []
                if authors:
                    author_str = (
                        ", ".join(authors[:3]) + " et al."
                        if len(authors) > 3
                        else ", ".join(authors)
                    )
                    st.caption(f"👥 {author_str}")

                meta_parts = []
                journal = _af(article, "journal")
                year    = _af(article, "year")
                cite    = _af(article, "citation_count")
                venue   = _af(article, "venue")

                if journal:
                    meta_parts.append(f"**Journal:** {journal}")
                if year:
                    meta_parts.append(f"**Year:** {year}")
                if source_label == "pubmed" and pmid and not pmid.startswith("EPMC:"):
                    meta_parts.append(f"**PMID:** {pmid}")
                elif pmid and pmid.startswith("EPMC:"):
                    meta_parts.append(f"**ID:** {pmid}")
                if cite is not None:
                    meta_parts.append(f"📊 **Cited:** {cite}")
                st.caption("  |  ".join(meta_parts))

                if venue:
                    st.caption(f"📋 **Type:** {venue}")
                st.caption(f"🔍 **Source:** {source_badge}")

                abstract = _af(article, "abstract")
                if abstract:
                    clean_abstract = re.sub(r"<[^>]+>", " ", abstract)
                    clean_abstract = re.sub(r"\s+", " ", clean_abstract).strip()
                    st.write(clean_abstract[:400] +
                             ("…" if len(clean_abstract) > 400 else ""))
                else:
                    st.write("*No abstract available.*")

            with col_links:
                url = _af(article, "url") or ""
                doi = _af(article, "doi") or ""
                pdf_url = pdf_urls.get(pmid)

                if source_label == "pubmed" and pmid and not pmid.startswith("EPMC:"):
                    st.markdown(f"[🔗 PubMed](https://pubmed.ncbi.nlm.nih.gov/{pmid}/)")
                if url and "europepmc" in url:
                    st.markdown(f"[🟢 Europe PMC]({url})")
                if source_label == "core" and url:
                    st.markdown(f"[🟠 CORE]({url})")
                if source_label == "semantic_scholar" and pmid:
                    st.markdown(f"[🔬 Semantic Scholar](https://www.semanticscholar.org/paper/{pmid})")
                if source_label == "openalex" and url:
                    st.markdown(f"[🔍 OpenAlex]({url})")

                if is_fulltext_view and pmc_url and pmc_url != url:
                    if "ncbi.nlm.nih.gov/pmc" in pmc_url:
                        st.markdown(f"[📄 PMC Article]({pmc_url})")
                        if not pdf_url:
                            st.caption("💡 Open PMC Article → click PDF")
                    elif "europepmc.org" in pmc_url:
                        st.markdown(f"[📄 Europe PMC Article]({pmc_url})")
                    elif "doi.org" not in pmc_url:
                        st.markdown(f"[📄 Article Page]({pmc_url})")

                if is_fulltext_view and pdf_url:
                    st.markdown(f"[📥 Download PDF]({pdf_url})")
                elif is_fulltext_view and not pdf_url and doi and not pmc_url:
                    st.caption("💡 Open DOI page → click PDF")

                if doi:
                    doi_display = _clean_doi_for_display(doi)
                    st.markdown(f"[🌐 DOI]({doi_display})")

            # Full text indicator
            full_text = _af(article, "full_text")
            if full_text:
                if str(full_text).startswith("PDF:"):
                    if is_fulltext_view:
                        _core_pdf = str(full_text)[4:].strip()
                        pdf_url_local = pdf_urls.get(pmid)
                        if _core_pdf and not pdf_url_local:
                            st.markdown(f"[📥 Download PDF (CORE)]({_core_pdf})")
                else:
                    st.success("✅ Full text available")


# ══════════════════════════════════════════════════════════════════════════════
# UNIFIED FULL-TEXT TAB
# ══════════════════════════════════════════════════════════════════════════════

def _render_unified_fulltext_tab(review_id: int, pmc_cl, article_repo):
    pubmed_arts = _load(review_id, "articles_PubMed", []) or []
    epmc_arts   = _load(review_id, "articles_Europe PMC", []) or []
    core_arts   = _load(review_id, "articles_CORE", []) or []

    if not pubmed_arts and not epmc_arts and not core_arts:
        st.info("Search for articles first, then use this tab to find full texts.")
        return

    all_unique, _ = _deduplicate(pubmed_arts, epmc_arts, core_arts)
    n_total = len(all_unique)

    pmid_articles  = [a for a in all_unique
                      if _af(a,"pmid") and not str(_af(a,"pmid","")).startswith("EPMC:")
                      and not str(_af(a,"pmid","")).startswith("CORE:")]
    epmc_articles_ = [a for a in all_unique
                      if _af(a,"pmid") and (str(_af(a,"pmid","")).startswith("EPMC:")
                                            or str(_af(a,"pmid","")).startswith("CORE:"))]

    ft_results = _load(review_id, "unified_fulltext")
    ft_urls    = _load(review_id, "unified_fulltext_urls") or {}
    ft_pdfs    = _load(review_id, "unified_fulltext_pdfs") or {}

    if ft_results is None:
        st.info(
            f"**{n_total}** unique articles across all your searches. "
            f"Click below to search for full texts."
        )
        if st.button("🔍 Find All Full Texts (PMC + Europe PMC)",
                     type="primary", key=f"unified_ft_{review_id}"):
            _run_unified_fulltext(review_id, pmid_articles, epmc_articles_, pmc_cl, article_repo)
    elif len(ft_results) == 0:
        st.warning("No PMC or Europe PMC coverage found for any article.")
        if st.button("🔄 Re-check", key=f"unified_ft_retry_{review_id}"):
            _store(review_id, "unified_fulltext", None)
            st.rerun()
    else:
        n_with_text = sum(1 for a in ft_results if _af(a, "full_text"))
        n_link_only = len(ft_results) - n_with_text
        st.success(
            f"✅ **{len(ft_results)}** of {n_total} articles have PMC/Europe PMC coverage. "
            f"**{n_with_text}** have extractable full text · "
            f"**{n_link_only}** have a link only."
        )
        if st.button("🔄 Re-check", key=f"unified_ft_retry2_{review_id}"):
            _store(review_id, "unified_fulltext", None)
            st.rerun()
        sort_by = st.selectbox("Sort by", ["Newest first", "Oldest first", "Most cited"],
                               key=f"ft_sort_{review_id}")
        _render_article_cards(ft_results, pmc_urls=ft_urls, pdf_urls=ft_pdfs,
                              sort_by=sort_by, is_fulltext_view=True)


# ══════════════════════════════════════════════════════════════════════════════
# PARALLEL FULL-TEXT RETRIEVAL
# ══════════════════════════════════════════════════════════════════════════════

def _run_unified_fulltext(review_id, pmid_articles, epmc_articles_, pmc_cl, article_repo):
    from core.pubmed_pmc_pipeline import PubMedPMCPipeline
    from config.settings import NCBI_API_KEY, NCBI_EMAIL
    import requests
    import time

    all_articles = list(pmid_articles) + list(epmc_articles_)
    n_total      = len(all_articles)
    oa_email     = NCBI_EMAIL or "research@example.com"

    lock          = threading.Lock()
    pmc_urls:     Dict[str, str]     = {}
    pdf_urls:     Dict[str, str]     = {}
    full_texts:   Dict[str, str]     = {}
    enriched_map: Dict[str, Article] = {}

    def _try_url(url: str, timeout: int = 15) -> Optional[requests.Response]:
        try:
            r = requests.get(url, timeout=timeout,
                             headers={"User-Agent": f"SR-Platform/1.0 ({oa_email})"})
            if r.status_code == 200 and len(r.content) > 200:
                return r
        except Exception as e:
            logger.debug("HTTP fetch error %s: %s", url, e)
        return None

    def _clean_doi_local(doi_raw: str) -> str:
        doi = doi_raw.strip()
        for prefix in ("https://doi.org/", "http://doi.org/",
                        "https://dx.doi.org/", "doi:"):
            if doi.lower().startswith(prefix.lower()):
                doi = doi[len(prefix):]
                break
        return doi.strip()

    _EXPIRING_CDN_PATTERNS = (
        "silverchair", "atypon", "token=",
        "watermark", "access_token", "Authorization=",
    )

    def _validate_ncbi_pdf(pmcid: str) -> Optional[str]:
        candidates = [
            f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/pdf/",
            f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/pdf/",
        ]
        for url in candidates:
            try:
                head = requests.head(
                    url, timeout=8, allow_redirects=True,
                    headers={"User-Agent": f"SR-Platform/1.0 ({oa_email})"}
                )
                final_url = head.url
                is_cdn    = any(p in final_url for p in _EXPIRING_CDN_PATTERNS)
                if head.status_code == 200 and not is_cdn:
                    return final_url
            except Exception:
                continue
        return None

    # PASS 1 — NCBI PMC elink
    real_pmid_articles = [a for a in all_articles
                          if _af(a,"pmid") and not str(_af(a,"pmid","")).startswith("EPMC:")]
    pmid_to_pmcid: Dict[str, str] = {}
    pmc_by_pmcid:  Dict[str, Article] = {}

    if real_pmid_articles and pmc_cl:
        pipeline = PubMedPMCPipeline.__new__(PubMedPMCPipeline)
        pipeline.pubmed_client = None
        pipeline.pmc_client    = pmc_cl
        pipeline._params_base  = {"tool": "SR_Platform", "email": oa_email}
        if NCBI_API_KEY:
            pipeline._params_base["api_key"] = NCBI_API_KEY

        with st.spinner(f"Pass 1/4 — NCBI PMC elink: mapping {len(real_pmid_articles)} PMIDs…"):
            pmids         = [_af(a,"pmid") for a in real_pmid_articles]
            pmid_to_pmcid = pipeline._map_pmids_to_pmcids(pmids)

        if pmid_to_pmcid:
            with st.spinner(f"Pass 1/4 — NCBI PMC: fetching full text + validating PDFs…"):
                pmc_fetched  = pmc_cl.fetch(list(pmid_to_pmcid.values()))
                pmc_by_pmcid = {a.pmid: a for a in pmc_fetched}

                def _pass1_worker(article):
                    pmid  = _af(article, "pmid")
                    pmcid = pmid_to_pmcid.get(pmid)
                    if not pmcid:
                        return
                    article_page  = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/"
                    validated_pdf = _validate_ncbi_pdf(pmcid)
                    pmc_art       = pmc_by_pmcid.get(pmcid)
                    ft            = pmc_art.full_text if pmc_art and pmc_art.full_text else None
                    with lock:
                        pmc_urls[pmid]  = article_page
                        if validated_pdf: pdf_urls[pmid] = validated_pdf
                        if ft:            full_texts[pmid] = ft
                        enriched_map[pmid] = article

                with ThreadPoolExecutor(max_workers=10) as ex:
                    futs = [ex.submit(_pass1_worker, a) for a in real_pmid_articles]
                    for f in as_completed(futs):
                        try: f.result()
                        except Exception as e: logger.debug("Pass 1 worker: %s", e)

    # PASS 2 — Europe PMC
    with st.spinner(f"Pass 2/4 — Europe PMC: {n_total} articles in parallel…"):
        epmc_search = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"

        def _pass2_worker(article):
            pmid = _af(article, "pmid", "")
            raw  = pmid.replace("EPMC:", "").strip()
            doi  = _af(article, "doi") or ""

            if ":" not in raw and not raw.isdigit():
                src_tag, epmc_id = "MED", raw
                article_url = f"https://europepmc.org/article/{src_tag}/{epmc_id}"
                xml_url     = (f"https://www.ebi.ac.uk/europepmc/webservices/rest/"
                               f"{src_tag}/{epmc_id}/fullTextXML")
                ft_resp = _try_url(xml_url)
                with lock:
                    pmc_urls.setdefault(pmid, article_url)
                    if ft_resp and pmid not in full_texts:
                        full_texts[pmid] = ft_resp.text[:60000]
                    enriched_map[pmid] = article
                return

            if ":" not in raw and raw.isdigit():
                search_q = f'EXT_ID:{raw} AND SRC:MED'
            elif doi:
                search_q = f'DOI:"{_clean_doi_local(doi)}"'
            else:
                return

            try:
                params = {"query": search_q, "resultType": "core", "pageSize": 1, "format": "json"}
                sr = requests.get(epmc_search, params=params, timeout=15)
                if sr.status_code != 200: return
                results = sr.json().get("resultList", {}).get("result", [])
                if not results: return

                hit     = results[0]
                src     = hit.get("source", "MED")
                ext_id  = hit.get("id", raw)
                pmcid_h = hit.get("pmcid", "")
                is_oa   = hit.get("isOpenAccess", "N") == "Y"
                article_url = f"https://europepmc.org/article/{src}/{ext_id}"

                new_pdf_url = None
                if is_oa:
                    with lock:
                        already_has_pdf = pmid in pdf_urls
                    if not already_has_pdf:
                        if pmcid_h:
                            new_pdf_url = _validate_ncbi_pdf(pmcid_h)
                        if new_pdf_url is None and doi:
                            doi_c = _clean_doi_local(doi)
                            if doi_c.startswith("10.1101") or doi_c.startswith("10.64898"):
                                new_pdf_url = f"https://www.biorxiv.org/content/{doi_c}v1.full.pdf"

                new_ft = None
                if is_oa:
                    with lock:
                        already_has_ft = pmid in full_texts
                    if not already_has_ft:
                        xml_url = (f"https://www.ebi.ac.uk/europepmc/webservices/rest/"
                                   f"{src}/{ext_id}/fullTextXML")
                        ft_resp = _try_url(xml_url)
                        if ft_resp: new_ft = ft_resp.text[:60000]

                with lock:
                    pmc_urls.setdefault(pmid, article_url)
                    enriched_map[pmid] = article
                    if new_pdf_url and pmid not in pdf_urls: pdf_urls[pmid] = new_pdf_url
                    if new_ft and pmid not in full_texts:    full_texts[pmid] = new_ft

            except Exception as e:
                logger.debug("Pass 2 worker %s: %s", pmid, e)

        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = [ex.submit(_pass2_worker, a) for a in all_articles]
            for f in as_completed(futs):
                try: f.result()
                except Exception as e: logger.debug("Pass 2 exception: %s", e)

    # PASS 3 — Unpaywall
    articles_with_doi = [a for a in all_articles if _af(a, "doi")]
    if articles_with_doi:
        with st.spinner(f"Pass 3/4 — Unpaywall: {len(articles_with_doi)} DOIs…"):
            _EXPIRING_CDN_HOSTS = ("silverchair","atypon","token=","watermark","access_token","Authorization=")

            def _pass3_worker(article):
                doi_clean = _clean_doi_local(_af(article,"doi",""))
                pmid      = _af(article,"pmid","")
                oa_url    = f"https://api.unpaywall.org/v2/{doi_clean}?email={oa_email}"
                try:
                    r = requests.get(oa_url, timeout=15,
                                     headers={"User-Agent": f"SR-Platform/1.0 ({oa_email})"})
                    if r.status_code not in (200,): return
                    data       = r.json()
                    best       = data.get("best_oa_location") or {}
                    direct_pdf = best.get("url_for_pdf")

                    stable_pdf = None
                    if doi_clean.startswith("10.1002"):
                        stable_pdf = f"https://onlinelibrary.wiley.com/doi/epdf/{doi_clean}"
                    elif doi_clean.startswith("10.1101") or doi_clean.startswith("10.64898"):
                        for server in ("biorxiv","medrxiv"):
                            candidate = f"https://www.{server}.org/content/{doi_clean}v1.full.pdf"
                            try:
                                hd = requests.head(candidate, timeout=8, allow_redirects=True,
                                                   headers={"User-Agent": f"SR-Platform/1.0 ({oa_email})"})
                                if hd.status_code == 200:
                                    stable_pdf = candidate; break
                            except Exception: continue

                    unpaywall_pdf = None
                    if direct_pdf:
                        is_expiring = any(cdn in direct_pdf for cdn in _EXPIRING_CDN_HOSTS)
                        if not is_expiring: unpaywall_pdf = direct_pdf

                    chosen = stable_pdf or unpaywall_pdf
                    if chosen:
                        with lock:
                            if pmid not in pdf_urls: pdf_urls[pmid] = chosen
                except Exception as e:
                    logger.debug("Pass 3 Unpaywall %s: %s", pmid, e)

            with ThreadPoolExecutor(max_workers=8) as ex:
                futs = [ex.submit(_pass3_worker, a) for a in articles_with_doi]
                for f in as_completed(futs):
                    try: f.result()
                    except Exception as e: logger.debug("Pass 3 exception: %s", e)

    # PASS 4 — CORE
    from config.settings import CORE_API_KEY
    if CORE_API_KEY:
        articles_needing_core = [a for a in all_articles if _af(a,"pmid","") not in pdf_urls]
        if articles_needing_core:
            with st.spinner(f"Pass 4/4 — CORE: {len(articles_needing_core)} articles…"):
                _CORE_BASE   = "https://api.core.ac.uk/v3"
                _CORE_HEADER = {"Authorization": f"Bearer {CORE_API_KEY}",
                                "User-Agent": f"SR-Platform/1.0 ({oa_email})"}

                def _pass4_worker(article):
                    pmid  = _af(article,"pmid","")
                    doi_c = _clean_doi_local(_af(article,"doi",""))
                    title = _af(article,"title","")
                    core_hit = None

                    if doi_c:
                        try:
                            resp = requests.get(f"{_CORE_BASE}/search/works",
                                                params={"q": f"doi:{doi_c}", "limit": 1},
                                                headers=_CORE_HEADER, timeout=12)
                            if resp.status_code == 200:
                                items = resp.json().get("results", [])
                                if items: core_hit = items[0]
                        except Exception as e: logger.debug("CORE DOI %s: %s", doi_c, e)

                    if not core_hit and title:
                        try:
                            short_title = " ".join(title.split()[:10])
                            resp = requests.get(f"{_CORE_BASE}/search/works",
                                                params={"q": short_title, "limit": 3},
                                                headers=_CORE_HEADER, timeout=12)
                            if resp.status_code == 200:
                                items = resp.json().get("results", [])
                                title_lower = title.lower()
                                for item in items:
                                    ct = (item.get("title") or "").lower()
                                    if ct[:30] == title_lower[:30] and len(ct) > 10:
                                        core_hit = item; break
                        except Exception as e: logger.debug("CORE title %s: %s", pmid, e)

                    if not core_hit: return

                    download_url  = core_hit.get("downloadUrl") or ""
                    full_text_url = core_hit.get("fullTextLink") or ""
                    core_id       = core_hit.get("id","")
                    if not download_url:
                        for link in core_hit.get("links",[]):
                            if link.get("type") == "download" and link.get("url"):
                                download_url = link["url"]; break
                    core_page = f"https://core.ac.uk/works/{core_id}" if core_id else ""

                    with lock:
                        if core_page:          pmc_urls.setdefault(pmid, core_page)
                        elif full_text_url:     pmc_urls.setdefault(pmid, full_text_url)
                        if download_url and pmid not in pdf_urls:
                            pdf_urls[pmid] = download_url
                        enriched_map.setdefault(pmid, article)

                with ThreadPoolExecutor(max_workers=8) as ex:
                    futs = [ex.submit(_pass4_worker, a) for a in articles_needing_core]
                    for f in as_completed(futs):
                        try: f.result()
                        except Exception as e: logger.debug("Pass 4 exception: %s", e)

    # Merge full_texts into Article objects
    for article in all_articles:
        pmid = _af(article,"pmid","")
        ft   = full_texts.get(pmid)
        if pmid in enriched_map:
            if ft and not _af(enriched_map[pmid],"full_text"):
                try:
                    enriched_map[pmid] = enriched_map[pmid].model_copy(update={"full_text": ft})
                except Exception:
                    pass
        elif pmid in pmc_urls or ft:
            try:
                enriched_map[pmid] = article.model_copy(update={"full_text": ft}) if ft else article
            except Exception:
                enriched_map[pmid] = article

    # DOI fallback
    for article in all_articles:
        pmid = _af(article,"pmid","")
        doi  = _af(article,"doi","")
        if doi and pmid not in pmc_urls:
            pmc_urls[pmid] = f"https://doi.org/{_clean_doi_local(doi)}"

    covered_articles = list(enriched_map.values())
    ft_articles      = [a for a in covered_articles if _af(a,"full_text")]

    if ft_articles:
        try:
            article_repo.save_full_texts(ft_articles)
        except Exception as e:
            logger.error("Failed to save full texts: %s", e)

    _store(review_id, "unified_fulltext",      covered_articles if covered_articles else [])
    _store(review_id, "unified_fulltext_urls", pmc_urls)
    _store(review_id, "unified_fulltext_pdfs", pdf_urls)

    n_covered = len(covered_articles)
    n_ft      = len(ft_articles)
    n_pdfs    = len(pdf_urls)

    if n_covered == 0:
        st.warning(f"No open-access full text found for any of the {n_total} articles.")
    else:
        st.success(
            f"✅ **{n_covered}** of {n_total} articles found · "
            f"**{n_ft}** have extractable full text · "
            f"**{n_pdfs}** have a direct PDF link."
        )
    st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# MEDICAL SEARCH FORM
# ══════════════════════════════════════════════════════════════════════════════

def _render_pubmed_form(review_id, registry, search_repo, article_repo):
    from core.query_builder import STUDY_TYPE_FILTERS

    st.markdown("#### PICO Query Builder — Medical Research")

    clients   = registry.get_clients(ResearchDomain.MEDICAL)
    pubmed_cl = next((c for c in clients if c.source_name == "PubMed"), None)
    epmc_cl   = next((c for c in clients if c.source_name == "Europe PMC"), None)
    pmc_cl    = next((c for c in clients if c.source_name not in ("PubMed","Europe PMC")
                      and "pmc" in c.source_name.lower()), None)

    from config.settings import CORE_API_KEY as _CORE_KEY
    _core_options = ["PubMed","Europe PMC","CORE"] if _CORE_KEY else ["PubMed","Europe PMC"]

    source_name = st.selectbox("Data Source", options=_core_options, key="medical_source_select")

    from config.settings import UMLS_API_KEY as _UMLS_KEY
    _umls_available = bool(_UMLS_KEY)

    if _umls_available:
        use_umls = st.toggle("🔬 Use UMLS concept expansion", value=True, key="use_umls_toggle")
    else:
        use_umls = False
        st.info("💡 **UMLS expansion not active.** Add `UMLS_API_KEY` to your `.env`.")

    with st.form("pico_form"):
        population   = st.text_input("Population",   placeholder="e.g. adults with CLL")
        intervention = st.text_input("Intervention", placeholder="e.g. venetoclax")
        comparison   = st.text_input("Comparison (optional)", placeholder="e.g. ibrutinib")
        outcome      = st.text_input("Outcome",      placeholder="e.g. overall survival")
        col1, col2, col3 = st.columns(3)
        year_from    = col1.number_input("Year From", 1900, 2100, 2015)
        year_to      = col2.number_input("Year To",   1900, 2100, 2025)
        max_results  = col3.slider("Max Results", 1, 200, 20)
        study_type   = st.radio("Study Type Filter", options=list(STUDY_TYPE_FILTERS.keys()),
                                index=0, horizontal=True)
        submitted    = st.form_submit_button(f"🔍 Search {source_name}", type="primary")

    pending_key = f"pico_pending_{review_id}"
    pending     = st.session_state.get(pending_key, {})

    if submitted:
        new_fp        = _pico_fingerprint(population, intervention, comparison,
                                          outcome, year_from, year_to, study_type)
        pico_changed  = _check_pico_change(review_id, new_fp)
        existing_total = counts["total"]

        if pico_changed and existing_total > 0:
            st.session_state[pending_key] = {
                "source_name": source_name, "population": population,
                "intervention": intervention, "comparison": comparison,
                "outcome": outcome, "year_from": year_from, "year_to": year_to,
                "max_results": max_results, "study_type": study_type,
                "new_fp": new_fp, "use_umls": use_umls,
            }
            st.rerun()
        else:
            _store_pico_fp(review_id, new_fp)
            _do_search(review_id, source_name, population, intervention,
                       comparison, outcome, year_from, year_to,
                       max_results, study_type, pubmed_cl, epmc_cl,
                       search_repo, article_repo, use_umls=use_umls)

    if pending:
        p = pending
        st.warning(
            f"⚠️ **Your PICO query has changed.** This review already has "
            f"**{counts['total']}** articles. What would you like to do?"
        )
        c1, c2 = st.columns(2)
        with c1:
            if st.button("➕ Add to this review", type="primary", key=f"pico_add_same_{review_id}"):
                st.session_state.pop(pending_key, None)
                _store_pico_fp(review_id, p["new_fp"])
                _do_search(review_id, p["source_name"], p["population"], p["intervention"],
                           p["comparison"], p["outcome"], p["year_from"], p["year_to"],
                           p["max_results"], p["study_type"],
                           pubmed_cl, epmc_cl, search_repo, article_repo,
                           use_umls=p.get("use_umls", False))
        with c2:
            if st.button("🆕 Create new review", key=f"pico_new_review_{review_id}"):
                st.session_state.pop(pending_key, None)
                short_pico = f"{p['population'][:20]} / {p['intervention'][:20]}"
                review_repo.create_review(f"Review: {short_pico} ({p['year_from']}–{p['year_to']})")
                st.success("New review created — switch to it in the sidebar.")
                st.rerun()
        with st.columns(1)[0]:
            if st.button("✕ Cancel", key=f"pico_cancel_{review_id}"):
                st.session_state.pop(pending_key, None)
                st.rerun()
        return

    pubmed_articles = _load(review_id, "articles_PubMed", []) or []
    epmc_articles   = _load(review_id, "articles_Europe PMC", []) or []
    core_articles   = _load(review_id, "articles_CORE", []) or []

    if not pubmed_articles and not epmc_articles and not core_articles:
        _refresh_prisma_from_db(review_id)
        # Show DB count banner when session is empty but DB has articles
        _db_counts = _load(review_id, "db_article_counts") or {}
        if _db_counts:
            _parts = [f"{s}: **{n}**" for s, n in _db_counts.items()]
            st.info(
                f"📂 **{sum(_db_counts.values())} articles** saved from a previous search "
                f"({', '.join(_parts)}). Re-run the search above to reload them into this session."
            )
        return

    sort_by = st.selectbox("Sort results by", ["Newest first","Oldest first","Most cited"],
                           key=f"sort_medical_{review_id}")

    n_searched = sum(1 for x in [pubmed_articles, epmc_articles, core_articles] if x)
    if n_searched > 1:
        combined, n_dupes = _deduplicate(pubmed_articles, epmc_articles, core_articles)
        _store(review_id, "n_duplicates_removed", n_dupes)
        _refresh_prisma_from_db(review_id)
        db_total = counts["total"]
        parts = []
        if pubmed_articles: parts.append(f"PubMed: **{len(pubmed_articles)}**")
        if epmc_articles:   parts.append(f"Europe PMC: **{len(epmc_articles)}**")
        if core_articles:   parts.append(f"CORE: **{len(core_articles)}**")
        n_raw = sum(len(x) for x in [pubmed_articles, epmc_articles, core_articles] if x)
        parts.append(f"Duplicates removed: **{n_raw - db_total}**")
        parts.append(f"Unique in DB: **{db_total}**")
        st.info("📊 **Multi-source:** " + " | ".join(parts))

    tab_labels = []
    tab_data   = []
    if pubmed_articles: tab_labels.append(f"📘 PubMed ({len(pubmed_articles)})");   tab_data.append(pubmed_articles)
    if epmc_articles:   tab_labels.append(f"📗 Europe PMC ({len(epmc_articles)})"); tab_data.append(epmc_articles)
    if core_articles:   tab_labels.append(f"🟠 CORE ({len(core_articles)})");       tab_data.append(core_articles)
    tab_labels.append("📄 Full Texts")

    tabs_out     = st.tabs(tab_labels)
    _tab_sources = []
    if pubmed_articles: _tab_sources.append("PubMed")
    if epmc_articles:   _tab_sources.append("Europe PMC")
    if core_articles:   _tab_sources.append("CORE")

    for i, (tab, articles_list) in enumerate(zip(tabs_out, tab_data)):
        src = _tab_sources[i] if i < len(_tab_sources) else ""
        with tab:
            _q  = _load(review_id, f"query_{src}")
            _ql = _load(review_id, f"query_label_{src}", "Query")
            if _q:
                with st.expander(f"🔍 {_ql} — click to view / copy full query", expanded=False):
                    st.code(_q, language="text")
            _render_article_cards(articles_list, pmc_urls={}, sort_by=sort_by)

    with tabs_out[-1]:
        _render_unified_fulltext_tab(review_id, pmc_cl, article_repo)


# ══════════════════════════════════════════════════════════════════════════════
# SEARCH EXECUTION
# ══════════════════════════════════════════════════════════════════════════════

def _do_search(review_id, source_name, population, intervention,
               comparison, outcome, year_from, year_to,
               max_results, study_type, pubmed_cl, epmc_cl,
               search_repo, article_repo, use_umls: bool = False):
    try:
        from core.query_builder import STUDY_TYPE_FILTERS, EPMC_STUDY_TYPE_FILTERS

        if use_umls:
            _, concept_set = _build_umls_query(
                source_name="PubMed",
                population=population, intervention=intervention,
                comparison=comparison, outcome=outcome,
                year_from=int(year_from), year_to=int(year_to),
                study_type=study_type,
            )
            if concept_set:
                _render_umls_expansion_detail(concept_set, source_name)
        else:
            concept_set = None

        if source_name == "CORE":
            core_q = (concept_set.core_query if concept_set and concept_set.core_query else None)
            _do_core_search(review_id, population, intervention, comparison, outcome,
                            year_from, year_to, max_results, search_repo, article_repo,
                            expanded_query=core_q)
            return

        query = None
        if use_umls and concept_set:
            query = (concept_set.pubmed_query if source_name == "PubMed"
                     else concept_set.epmc_query) or None

        if not query:
            if source_name == "PubMed":
                query = build_query(population=population, intervention=intervention,
                                    comparison=comparison, outcome=outcome,
                                    year_from=int(year_from), year_to=int(year_to),
                                    study_type=study_type)
            else:
                query = build_epmc_query(population=population, intervention=intervention,
                                         comparison=comparison, outcome=outcome,
                                         year_from=int(year_from), year_to=int(year_to),
                                         study_type=study_type)

        _store(review_id, f"query_{source_name}", query)
        _store(review_id, f"query_label_{source_name}",
               "🔬 UMLS-expanded" if (use_umls and concept_set) else "📝 Original PICO")

        client = pubmed_cl if source_name == "PubMed" else epmc_cl

        with st.spinner(f"Searching {source_name}…"):
            articles  = client.search_and_fetch(query, max_results)
            search_id = search_repo.create_search(review_id=review_id, query=query,
                                                  n_results=len(articles), source_name=source_name)
            result    = article_repo.save_articles(articles, review_id, search_id)

        st.success(f"✅ **{len(articles)}** articles found | "
                   f"{result['saved']} new | {result['duplicates']} already in DB")

        _store(review_id, f"articles_{source_name}", articles)
        _store(review_id, f"search_id_{source_name}", search_id)
        _store(review_id, "unified_fulltext", None)
        _refresh_prisma_from_db(review_id)

        if result["saved"] > 0:
            st.rerun()

    except ValueError as e:
        st.error(str(e))
    except Exception as e:
        logger.exception(f"{source_name} search failed")
        st.error(f"Search error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# UMLS QUERY BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def _build_umls_query(source_name: str, population: str, intervention: str,
                      comparison: str, outcome: str,
                      year_from: int, year_to: int, study_type: str):
    from core.concept_expander import get_concept_expander
    from core.query_builder import STUDY_TYPE_FILTERS, EPMC_STUDY_TYPE_FILTERS
    try:
        expander = get_concept_expander()
        if not expander.umls_available:
            return None, None
        st_pubmed = STUDY_TYPE_FILTERS.get(study_type, "")
        st_epmc   = EPMC_STUDY_TYPE_FILTERS.get(study_type, "")
        with st.spinner("🔬 UMLS: mapping concepts and expanding synonyms…"):
            cs = expander.expand(population=population, intervention=intervention,
                                 comparison=comparison, outcome=outcome,
                                 year_from=year_from, year_to=year_to,
                                 study_type_filter_pubmed=st_pubmed,
                                 study_type_filter_epmc=st_epmc)
        query = cs.pubmed_query if source_name == "PubMed" else cs.epmc_query
        if not query:
            return None, None
        return query, cs
    except Exception as e:
        logger.error("UMLS query build failed: %s", e)
        st.warning(f"⚠️ UMLS expansion failed ({e}). Falling back to standard query.")
        return None, None


def _render_umls_expansion_detail(concept_set, source_name: str) -> None:
    with st.expander("🔬 UMLS Concept Expansion Detail", expanded=False):
        if not concept_set.umls_used:
            st.warning("UMLS was not used for any field — API may be unavailable.")
            return
        for label, fe in concept_set.fields:
            if not fe: continue
            cols = st.columns([1, 2, 2])
            cols[0].markdown(f"**{label}**")
            cols[0].caption(f"*{fe.original}*")
            if fe.concept:
                cols[1].markdown(f"**CUI:** `{fe.concept.cui}`")
                cols[1].caption(f"MeSH: {fe.concept.mesh_term or '—'}")
            else:
                cols[1].caption("No UMLS concept found — using raw text")
            if fe.synonyms:
                cols[2].markdown("**Synonyms used:**")
                for syn in fe.synonyms[:6]: cols[2].caption(f"  · {syn}")
            else:
                cols[2].caption("No synonyms added")
            src_badge = {"umls":"🔬 UMLS","chembl":"💊 ChEMBL","original":"📝 Raw text"}.get(fe.source, fe.source)
            cols[2].caption(f"Source: {src_badge}")
        st.divider()
        st.caption("**Expansion log:** " + " | ".join(concept_set.expansion_log[:6]))


# ══════════════════════════════════════════════════════════════════════════════
# CORE SEARCH
# ══════════════════════════════════════════════════════════════════════════════

def _do_core_search(review_id, population, intervention, comparison,
                    outcome, year_from, year_to, max_results,
                    search_repo, article_repo, expanded_query: str = None):
    import requests
    from models.schemas import Article, ArticleSource, ResearchDomain
    from config.settings import CORE_API_KEY, NCBI_EMAIL

    if not CORE_API_KEY:
        st.error("CORE_API_KEY not found in .env.")
        return

    if expanded_query:
        import re as _re
        query      = _re.sub(r' AND yearPublished[><=]+\d{4}', '', expanded_query).strip()
        umls_label = "🔬 UMLS-expanded"
    else:
        terms = [t.strip() for t in [population, intervention, comparison, outcome] if t.strip()]
        if not terms:
            st.error("Please fill at least one PICO field.")
            return
        query      = " AND ".join(f'("{t}")' for t in terms)
        umls_label = "📝 Original PICO"

    core_params: dict = {"q": query, "limit": max_results}
    if year_from:
        core_params["yearPublished"] = f">={int(year_from)}"

    _store(review_id, "query_CORE", query)
    _store(review_id, "query_label_CORE", umls_label)

    oa_email = NCBI_EMAIL or "research@example.com"
    headers  = {"Authorization": f"Bearer {CORE_API_KEY}",
                "User-Agent": f"SR-Platform/1.0 ({oa_email})"}

    with st.spinner(f"Searching CORE for up to {max_results} articles…"):
        try:
            resp = requests.get("https://api.core.ac.uk/v3/search/works",
                                params=core_params, headers=headers, timeout=30)
            if resp.status_code == 401:
                st.error("CORE API key rejected (401).")
                return
            if resp.status_code == 500:
                import time as _time; _time.sleep(1.5)
                resp = requests.get("https://api.core.ac.uk/v3/search/works",
                                    params={"q": query, "limit": max_results},
                                    headers=headers, timeout=30)
            if resp.status_code != 200:
                st.error(f"CORE API error {resp.status_code}: {resp.text[:200]}")
                return
            results = resp.json().get("results", [])
        except Exception as e:
            st.error(f"CORE search failed: {e}")
            return

    if not results:
        st.info("No results from CORE. Try broadening your PICO terms.")
        return

    articles: list = []
    for hit in results:
        try:
            core_id  = str(hit.get("id",""))
            title    = (hit.get("title") or "No title").strip()
            abstract = (hit.get("abstract") or "").strip()
            year     = str(hit.get("yearPublished") or "")
            doi      = (hit.get("doi") or "").strip()
            raw_authors = hit.get("authors") or []
            authors = []
            for a in raw_authors:
                name = (a.get("name") or
                        f"{a.get('lastName','')} {a.get('firstName','')}".strip()
                        ) if isinstance(a, dict) else str(a)
                if name.strip(): authors.append(name.strip())
            journal      = (hit.get("publisher") or (hit.get("journals") or [{}])[0].get("title",""))
            download_url = hit.get("downloadUrl") or ""
            full_text_url= hit.get("fullTextLink") or ""
            if not download_url:
                for link in hit.get("links",[]):
                    if isinstance(link, dict) and link.get("type") == "download":
                        download_url = link.get("url",""); break
            core_page   = f"https://core.ac.uk/works/{core_id}" if core_id else ""
            article_url = core_page or full_text_url or (f"https://doi.org/{doi}" if doi else "")
            uid = doi or f"CORE:{core_id}"
            try:
                _core_source = ArticleSource("core")
            except ValueError:
                _core_source = ArticleSource.EUROPE_PMC
            art = Article(pmid=uid, title=title, abstract=abstract, authors=authors,
                          journal=journal, year=year, doi=doi or None,
                          url=article_url, source=_core_source, domain=ResearchDomain.MEDICAL)
            if download_url:
                art = art.model_copy(update={"full_text": f"PDF:{download_url}"})
            articles.append(art)
        except Exception as e:
            logger.warning("CORE parse error: %s", e)

    if not articles:
        st.warning("CORE returned results but none could be parsed.")
        return

    search_id = search_repo.create_search(review_id=review_id, query=query,
                                          n_results=len(articles), source_name="CORE")
    result    = article_repo.save_articles(articles, review_id, search_id)
    st.success(f"✅ **{len(articles)}** CORE articles found | "
               f"{result['saved']} new | {result['duplicates']} already in DB")

    _store(review_id, "articles_CORE", articles)
    _store(review_id, "search_id_CORE", search_id)
    _store(review_id, "unified_fulltext", None)
    _refresh_prisma_from_db(review_id)

    if result["saved"] > 0:
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# ML/AI SEARCH FORM
# ══════════════════════════════════════════════════════════════════════════════

def _render_ml_form(review_id, registry, search_repo, article_repo):
    st.markdown("#### ML/AI Literature Search")
    clients    = registry.get_clients(ResearchDomain.ML_AI)
    client_map = {c.source_name: c for c in clients}

    source_name     = st.selectbox("Data Source", options=list(client_map.keys()), key="ml_source_select")
    selected_client = client_map[source_name]

    if source_name == "Semantic Scholar" and not os.environ.get("SEMANTIC_SCHOLAR_API_KEY"):
        st.info("💡 No Semantic Scholar API key — get one at https://www.semanticscholar.org/product/api")

    with st.form("ml_search_form"):
        topic        = st.text_input("Research Topic", placeholder="e.g. transformer attention mechanism")
        keywords     = st.text_input("Additional Keywords (comma-separated, optional)")
        venue_filter = st.multiselect("Filter by Conference (optional)",
                                     ["NeurIPS","ICML","ICLR","AAAI","IJCAI","CVPR","ACL","EMNLP"])
        col1, col2  = st.columns(2)
        year_from   = col1.number_input("Year From", 2010, 2026, 2019)
        year_to     = col2.number_input("Year To",   2010, 2026, 2025)
        max_results = st.slider("Max Results", 1, 50, 10)
        submitted   = st.form_submit_button(f"Search {source_name}", type="primary")

    if submitted:
        if not topic.strip():
            st.error("Please enter a research topic.")
            return
        try:
            query = build_ml_query(topic=topic, keywords=keywords,
                                   venues=venue_filter,
                                   year_from=int(year_from), year_to=int(year_to))
            _store(review_id, "query_CORE", query)
            _store(review_id, "query_label_CORE", "📝 Original PICO")

            with st.spinner(f"Searching {source_name}…"):
                result_data = selected_client.search_and_fetch(query, max_results)
                if isinstance(result_data, tuple):
                    articles, err_msg = result_data
                    if err_msg: st.warning(f"⚠️ {err_msg}")
                else:
                    articles = result_data
                search_id = search_repo.create_search(review_id=review_id, query=query,
                                                      n_results=len(articles))
                result = article_repo.save_articles(articles, review_id, search_id)

            st.success(f"✅ {result['saved']} new | {result['duplicates']} duplicates")
            _store(review_id, f"ml_{source_name}", articles)
            if result["saved"] > 0:
                st.rerun()
        except Exception as e:
            logger.exception("ML search failed")
            st.error(f"Search error: {e}")

    stored = _load(review_id, f"ml_{source_name}", [])
    if stored:
        _store(review_id, f"ml_{source_name}", stored)

    SOURCE_ICONS = {"Semantic Scholar":"🔬","OpenAlex":"🔍"}
    tab_labels       = []
    tab_results_data = []
    for src in client_map.keys():
        src_articles = _load(review_id, f"ml_{src}", [])
        if src_articles:
            icon = SOURCE_ICONS.get(src, "📄")
            tab_labels.append(f"{icon} {src} ({len(src_articles)})")
            tab_results_data.append((src, src_articles))

    if not tab_labels:
        return

    sort_by  = st.selectbox("Sort by", ["Newest first","Oldest first","Most cited"],
                            key=f"ml_sort_{review_id}")
    tabs_out = st.tabs(tab_labels)
    for tab, (src, articles_list) in zip(tabs_out, tab_results_data):
        with tab:
            _render_article_cards(articles_list, pmc_urls={}, sort_by=sort_by)


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("""
    <div style="padding:1.25rem 1rem 1rem;display:flex;align-items:center;gap:0.6rem;">
        <div style="width:28px;height:28px;background:#C9974C;border-radius:6px;
                    display:flex;align-items:center;justify-content:center;
                    font-size:14px;flex-shrink:0;">🔭</div>
        <div>
            <div style="font-size:0.9rem;font-weight:600;color:#fff;letter-spacing:-0.01em;">
                SciSynth</div>
            <div style="font-size:0.65rem;color:rgba(255,255,255,0.4);
                        text-transform:uppercase;letter-spacing:0.08em;margin-top:1px;">
                AI Systematic Review</div>
        </div>
    </div>""", unsafe_allow_html=True)

    st.markdown("""<div style="padding:0 1rem 0.35rem;font-size:0.68rem;font-weight:600;
        color:rgba(255,255,255,0.35);text-transform:uppercase;
        letter-spacing:0.09em;">Active Review</div>""", unsafe_allow_html=True)

    reviews = review_repo.list_reviews()
    if reviews:
        options   = {r["title"]: r["id"] for r in reviews}
        selected  = st.selectbox("", list(options.keys()),
                                  key="review_select_main", label_visibility="collapsed")
        review_id = options[selected]
    else:
        review_id = None
        st.markdown("""<div style="margin:0 1rem;padding:0.65rem 0.75rem;
            background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);
            border-radius:8px;font-size:0.8rem;color:rgba(255,255,255,0.35);
            text-align:center;">No reviews yet</div>""", unsafe_allow_html=True)

    with st.expander("＋  New Review", expanded=False):
        new_title = st.text_input("Title", placeholder="e.g. Venetoclax in CLL",
                                   key="new_review_title_input", label_visibility="collapsed")
        if st.button("Create", type="primary", use_container_width=True, key="create_review_btn"):
            if new_title.strip():
                review_id = review_repo.create_review(new_title.strip())
                st.success("Created.")
                st.rerun()
            else:
                st.warning("Enter a title.")

    st.divider()

    st.markdown("""<div style="padding:0 1rem 0.35rem;font-size:0.68rem;font-weight:600;
        color:rgba(255,255,255,0.35);text-transform:uppercase;
        letter-spacing:0.09em;">Identity</div>""", unsafe_allow_html=True)

    _ROLES = ["Reviewer 1","Reviewer 2","Reviewer 3","Editor"]
    selected_reviewer    = st.selectbox("", _ROLES, index=0,
                                         key="current_reviewer_select", label_visibility="collapsed")
    current_reviewer_id  = f"rev_{selected_reviewer.lower().replace(' ','_')}"
    st.session_state["current_reviewer_id"] = current_reviewer_id
    _is_editor   = "editor" in current_reviewer_id
    _role_color  = "#C9974C" if _is_editor else "rgba(255,255,255,0.4)"
    _role_desc   = "Unblinded · resolves conflicts" if _is_editor else "Blinded · own decisions only"

    st.markdown(f"""<div style="margin:0.4rem 0;padding:0.6rem 0.75rem;
        background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);
        border-radius:8px;">
        <div style="font-size:0.82rem;font-weight:500;color:rgba(255,255,255,0.85);">
            {"🔑" if _is_editor else "👤"} {selected_reviewer}</div>
        <div style="font-size:0.7rem;color:{_role_color};margin-top:0.15rem;">
            {_role_desc}</div>
    </div>""", unsafe_allow_html=True)

    st.markdown("""<div style="height:2rem"></div>""", unsafe_allow_html=True)
    st.markdown("""<div style="position:absolute;bottom:1rem;left:0;right:0;
        padding:0.75rem 1rem 0;border-top:1px solid rgba(255,255,255,0.07);">
        <div style="font-size:0.68rem;color:rgba(255,255,255,0.2);text-align:center;">
            SciSynth v7 · AI-Assisted SR</div>
    </div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN UI
# ══════════════════════════════════════════════════════════════════════════════

if not review_id:
    st.title("Welcome to the SR Platform")
    st.info("Create a new review in the sidebar to get started.")
    st.stop()

review = review_repo.get_review(review_id)
counts = article_repo.get_screening_counts(review_id)
s2_counts = article_repo.get_stage2_counts(review_id)

# Persistent state: on review switch, refresh PRISMA counts and store DB article counts
_last_review_key = "last_active_review_id"
if st.session_state.get(_last_review_key) != review_id:
    st.session_state[_last_review_key] = review_id
    _refresh_prisma_from_db(review_id)
    # Cache article counts per source for the Search tab banner
    _all_db = article_repo.get_articles_for_review(review_id)
    _src_map = {"pubmed":"PubMed","europe_pmc":"Europe PMC","core":"CORE",
                "semantic_scholar":"Semantic Scholar","openalex":"OpenAlex"}
    _db_cnts: Dict[str, int] = {}
    for _a in _all_db:
        _d = _src_map.get((_a.get("source") or "").lower(), "Other")
        _db_cnts[_d] = _db_cnts.get(_d, 0) + 1
    _store(review_id, "db_article_counts", _db_cnts)

# Page header
st.markdown(f"""
<div style="margin-bottom:1.5rem;padding-bottom:1rem;border-bottom:1px solid #E5E5E3;">
    <h1 style="margin:0!important;font-size:1.6rem!important;font-weight:600!important;
               letter-spacing:-0.03em!important;color:#1A1A19!important;">
        {review['title']}</h1>
    <div style="font-size:0.78rem;color:#9B9B9A;margin-top:0.2rem;">
        Systematic Review &nbsp;·&nbsp; AI-Assisted &nbsp;·&nbsp;
        <span style="color:#C9974C;">{counts['total']} articles</span>
        &nbsp;·&nbsp;
        <span style="color:#059669;">S2: {s2_counts.get('s2_included',0)} included</span>
    </div>
</div>""", unsafe_allow_html=True)

# Metrics dashboard — 7 columns
c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
_metric_styles = [
    ("Total",       counts["total"],                "📄", ""),
    ("S1 Included", counts["included"],             "✓",  "green"),
    ("S1 Excluded", counts["excluded"],             "✕",  "rose"),
    ("S1 Unsure",   counts["unsure"],               "?",  "amber"),
    ("S2 Included", s2_counts.get("s2_included",0),"✦",  "teal"),
    ("Conflicts",   counts.get("conflict", 0),      "⚡", "violet"),
    ("Pending",     counts["pending"],              "◷",  ""),
]
for col, (label, val, icon, color) in zip([c1,c2,c3,c4,c5,c6,c7], _metric_styles):
    col.metric(f"{icon}  {label}", val)

# Tabs
tabs = st.tabs([
    "🔍  Search",
    "📋  Screening",
    "✦  AI Analysis",
    "⬡  PRISMA",
])

with tabs[0]:
    domain_options = registry.domain_display_names()
    domain_choice  = st.selectbox("Research Domain", options=list(domain_options.keys()),
                                  format_func=lambda k: domain_options[k], key="domain_select")
    selected_domain = ResearchDomain(domain_choice)
    if selected_domain == ResearchDomain.MEDICAL:
        _render_pubmed_form(review_id, registry, search_repo, article_repo)
    else:
        _render_ml_form(review_id, registry, search_repo, article_repo)

with tabs[1]:
    render_screening_panel(review_id)

with tabs[2]:
    render_ai_analysis_panel(review_id)

with tabs[3]:
    render_prisma_diagram(review_id)
